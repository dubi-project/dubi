package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/engineruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/raft/logstore"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
	etcdraft "go.etcd.io/raft/v3"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

type snapshotValueHandler struct{}

func (snapshotValueHandler) Apply(tx *enginestate.Transaction, target router.Target, payload []byte) (enginestate.Outcome, error) {
	return enginestate.Outcome{}, tx.Set(target.RoutingKey, payload)
}

func awaitSnapshotCondition(t *testing.T, label string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(45 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-deadline.C:
			t.Fatal("timed out:", label)
		case <-tick.C:
		}
	}
}

func snapshotNodeTLS(t *testing.T, ids []uint64) ([]*tls.Config, []*tls.Config) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "snapshot-test-ca"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	var servers, clients []*tls.Config
	for i, id := range ids {
		public, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: fmt.Sprintf("raft-%d", id)},
			DNSNames: []string{"localhost"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, public, private)
		if err != nil {
			t.Fatal(err)
		}
		cert := tls.Certificate{Certificate: [][]byte{der, ca.Raw}, PrivateKey: key}
		servers = append(servers, &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})
		clients = append(clients, &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})
	}
	return servers, clients
}

// This uses real Server/Node Root and engine adapters -> Raft -> mTLS
// transport -> file import -> state restore -> shared disk Pack paths.
// All three processes are represented by independent in-process runtimes on
// local directories; this is not independent-host/physical-disk/WAN evidence.
func TestThreeNodeEngineFileSnapshotCatchupAndRestart(t *testing.T) {
	const groupID = "data-1"
	var roots [3]string
	var ids []uint64
	var identities [3]identity.Identity
	for i := range roots {
		roots[i] = t.TempDir()
		var err error
		if i == 0 {
			identities[i], err = identity.Bootstrap(roots[i])
		} else {
			if _, err = identity.PrepareJoin(roots[i]); err == nil {
				identities[i], err = identity.BindCluster(roots[i], identities[0].ClusterID)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, identities[i].RaftID)
	}
	serverTLS, clientTLS := snapshotNodeTLS(t, ids)
	directory := raftrpc.NewStaticDirectory()
	var listeners [3]net.Listener
	var addresses [3]string
	for i := range listeners {
		var err error
		listeners[i], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listeners[i].Close()
		addresses[i] = listeners[i].Addr().String()
		if err := directory.Update(ids[i], addresses[i]); err != nil {
			t.Fatal(err)
		}
	}
	registry := engineruntime.NewRegistry()
	if err := registry.Register("record", snapshotValueHandler{}); err != nil {
		t.Fatal(err)
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	var nodes [3]*Server
	var cancels [3]context.CancelFunc
	var results [3]chan error
	stop := func(i int) {
		if nodes[i] == nil {
			return
		}
		cancels[i]()
		select {
		case err := <-results[i]:
			if err != nil {
				t.Error("stop node", i, err)
			}
		case <-time.After(30 * time.Second):
			t.Error("node shutdown timed out", i)
		}
		nodes[i].Close()
		nodes[i] = nil
	}
	defer func() {
		for i := range nodes {
			stop(i)
		}
	}()
	start := func(i int, bootstrap bool) {
		if !bootstrap {
			var err error
			listeners[i], err = net.Listen("tcp", addresses[i])
			if err != nil {
				t.Fatal(err)
			}
		}
		options := Options{StoreRoot: roots[i], Listener: listeners[i], EngineMux: mux,
			ServerTLS: serverTLS[i], ClientTLS: clientTLS[i], BootstrapDirectory: directory,
			TickInterval: 30 * time.Millisecond, MembershipInterval: time.Second,
			AuthorizePeer: func(ctx context.Context, claimed uint64) error {
				p, _ := peer.FromContext(ctx)
				info := p.AuthInfo.(credentials.TLSInfo)
				if info.State.PeerCertificates[0].Subject.CommonName != fmt.Sprintf("raft-%d", claimed) {
					return errors.New("peer certificate does not bind claimed RaftID")
				}
				return nil
			}}
		if bootstrap {
			options.RootBootstrapPeers = ids
		}
		var err error
		nodes[i], err = Open(options)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancels[i], results[i] = cancel, make(chan error, 1)
		node, result := nodes[i], results[i]
		go func() { result <- node.Run(ctx) }()
	}
	for i := range nodes {
		start(i, true)
	}
	for i := range nodes {
		awaitSnapshotCondition(t, "initial node ready", func() bool {
			select {
			case <-nodes[i].Ready():
				return true
			default:
				return false
			}
		})
	}
	leader := func(group string) int {
		result := -1
		awaitSnapshotCondition(t, "leader for "+group, func() bool {
			for i, node := range nodes {
				if node == nil {
					continue
				}
				st, err := node.Runtime.Host.Status(group)
				if err == nil && st.Role == etcdraft.StateLeader {
					result = i
					return true
				}
			}
			return false
		})
		return result
	}
	rootCommand := func(command rootstate.Command) {
		data, err := rootstate.EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := nodes[leader(raft.RootGroupID)].Runtime.Host.Submit(raft.RootGroupID, data); err != nil {
			t.Fatal(err)
		}
		awaitSnapshotCondition(t, "Root command "+command.RequestID, func() bool {
			for _, node := range nodes {
				if node == nil {
					continue
				}
				result, found, err := node.Runtime.Root.Result(command.RequestID)
				if err != nil || !found {
					return false
				}
				if result.Error != "" {
					t.Fatal(result.Error)
				}
			}
			return true
		})
	}
	for i, id := range identities {
		rootCommand(rootstate.Command{Version: 1, Type: rootstate.RegisterNode, RequestID: fmt.Sprintf("node-%d", i),
			Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "region-a", Address: addresses[i]}})
	}
	rootCommand(rootstate.Command{Version: 1, Type: rootstate.RegisterGroup, RequestID: "group",
		Group: &rootstate.Group{GroupID: groupID, StateKind: rootstate.GroupStateEngine}})
	// Bootstrap only the first data participant. The production Root task
	// worker must prepare, add and promote the other two over authenticated,
	// task-bound membership RPCs (or the equivalent guarded local path).
	state, err := nodes[0].Runtime.Groups.NewState(context.Background(), groupID)
	if err != nil {
		t.Fatal(err)
	}
	if err := nodes[0].Runtime.Host.AddGroup(groupID, []uint64{ids[0]}, false, state); err != nil {
		t.Fatal(err)
	}
	var members []rootstate.GroupMember
	for _, id := range ids {
		members = append(members, rootstate.GroupMember{RaftID: id, Role: rootstate.GroupMemberVoter})
	}
	rootCommand(rootstate.Command{Version: 1, Type: rootstate.PublishGroupMembers, RequestID: "members",
		Membership: &rootstate.GroupMembership{GroupID: groupID, ExpectedEpoch: 1, Members: members[:1]}})
	rootCommand(rootstate.Command{Version: 1, Type: rootstate.PlanGroupMembers, RequestID: "grow-data-group", GroupPlan: &rootstate.GroupChangePlan{ChangeID: "grow-data-group", GroupID: groupID, ExpectedEpoch: 2, Members: members}})
	awaitSnapshotCondition(t, "task-bound Group growth", func() bool {
		for _, node := range nodes {
			change, found, err := node.Runtime.Root.GroupChange("grow-data-group")
			if err != nil || !found || change.State != rootstate.GroupChangeCompleted {
				return false
			}
			cut, err := node.Runtime.Host.MembershipCut(groupID)
			if err != nil || cut.Index == 0 || len(cut.Conf.GetVoters()) != len(ids) || len(cut.Conf.GetLearners()) != 0 {
				return false
			}
		}
		return true
	})
	// Exercise the production transfer path while the Group is still empty.
	// This is NOT a business-data drain or an automatic regional shrink.
	retired := ids[leader(groupID)]
	var retained []rootstate.GroupMember
	for _, member := range members {
		if member.RaftID != retired {
			retained = append(retained, member)
		}
	}
	rootCommand(rootstate.Command{Version: 1, Type: rootstate.PlanGroupMembers, RequestID: "retire-empty-leader", GroupPlan: &rootstate.GroupChangePlan{ChangeID: "retire-empty-leader", GroupID: groupID, ExpectedEpoch: 3, Members: retained}})
	awaitSnapshotCondition(t, "task-bound leadership transfer and removal", func() bool {
		for _, node := range nodes {
			change, found, err := node.Runtime.Root.GroupChange("retire-empty-leader")
			if err != nil || !found || change.State != rootstate.GroupChangeCompleted {
				return false
			}
		}
		return true
	})
	rootCommand(rootstate.Command{Version: 1, Type: rootstate.PlanGroupMembers, RequestID: "rejoin-empty-leader", GroupPlan: &rootstate.GroupChangePlan{ChangeID: "rejoin-empty-leader", GroupID: groupID, ExpectedEpoch: 4, Members: members}})
	var membershipIndex uint64
	awaitSnapshotCondition(t, "task-bound retired member rejoin", func() bool {
		var index uint64
		for _, node := range nodes {
			change, found, err := node.Runtime.Root.GroupChange("rejoin-empty-leader")
			if err != nil || !found || change.State != rootstate.GroupChangeCompleted {
				return false
			}
			cut, err := node.Runtime.Host.MembershipCut(groupID)
			if err != nil || cut.Index == 0 || len(cut.Conf.GetVoters()) != len(ids) || len(cut.Conf.GetLearners()) != 0 {
				return false
			}
			if index != 0 && index != cut.Index {
				return false
			}
			index = cut.Index
		}
		membershipIndex = index
		return true
	})
	descriptor := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", EngineID: "record", DatasetID: "items",
		OwnerGroupID: groupID, Epoch: 1, State: rootstate.RangeStateActive}
	activation, err := routedstate.EncodeActivateRange("activate", descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := nodes[leader(groupID)].Runtime.Host.Submit(groupID, activation); err != nil {
		t.Fatal(err)
	}
	waitResult := func(request string) {
		awaitSnapshotCondition(t, "engine result "+request, func() bool {
			for _, node := range nodes {
				if node == nil {
					continue
				}
				state, found := node.Runtime.Engines.Resolve(groupID)
				if !found {
					return false
				}
				result, found, err := state.Result(request)
				if err != nil || !found {
					return false
				}
				if !result.Applied {
					t.Fatal("engine command rejected:", result.Error)
				}
			}
			return true
		})
	}
	waitResult("activate")
	lagging := (leader(groupID) + 1) % len(nodes)
	stop(lagging)
	rootCommand(rootstate.Command{Version: 1, Type: rootstate.RegisterTenant, RequestID: "offline-root-write",
		Tenant: &rootstate.Tenant{TenantID: "offline-tenant"}})
	value := bytes.Repeat([]byte("v"), 1<<20)
	for i := range 80 {
		request := fmt.Sprintf("write-%03d", i)
		target := router.Target{Keyspace: descriptor.Keyspace(), RangeID: descriptor.RangeID, OwnerGroupID: groupID, Epoch: 1,
			RoutingKey: []byte(fmt.Sprintf("item-%03d", i))}
		command, err := routedstate.EncodeMutation(request, target, value)
		if err != nil {
			t.Fatal(err)
		}
		if err := nodes[leader(groupID)].Runtime.Host.Submit(groupID, command); err != nil {
			t.Fatal(err)
		}
		waitResult(request)
	}
	var cut, rootCut uint64
	for _, node := range nodes {
		if node == nil {
			continue
		}
		index, err := node.Runtime.Host.CreateSnapshot(groupID)
		if err != nil {
			t.Fatal(err)
		}
		if cut == 0 || index < cut {
			cut = index
		}
		index, err = node.Runtime.Host.CreateSnapshot(raft.RootGroupID)
		if err != nil {
			t.Fatal(err)
		}
		if rootCut == 0 || index < rootCut {
			rootCut = index
		}
	}
	start(lagging, false)
	awaitSnapshotCondition(t, "restarted lagging node ready", func() bool {
		select {
		case <-nodes[lagging].Ready():
			return true
		default:
			return false
		}
	})
	waitResult("write-079")
	checkValues := func() {
		membership, err := nodes[lagging].Runtime.Host.MembershipCut(groupID)
		if err != nil || membership.Index != membershipIndex {
			t.Fatal("snapshot/reopen lost membership position", membership, err)
		}
		if _, found, err := nodes[lagging].Runtime.Root.Tenant("offline-tenant"); err != nil || !found ||
			nodes[lagging].Runtime.Root.AppliedIndex() < rootCut {
			t.Fatal("Root snapshot did not restore offline mutation", found, err)
		}
		state, found := nodes[lagging].Runtime.Engines.Resolve(groupID)
		if !found || state.AppliedIndex() < cut {
			t.Fatal("lagging Group did not reach snapshot cut")
		}
		for i := range 80 {
			got, found, err := state.Value(descriptor.Keyspace(), []byte(fmt.Sprintf("item-%03d", i)))
			if err != nil || !found || !bytes.Equal(got, value) {
				t.Fatal("restored value differs", i, err)
			}
		}
	}
	checkValues()
	stop(lagging)
	lock, err := storelock.Acquire(roots[lagging])
	if err != nil {
		t.Fatal(err)
	}
	log, err := logstore.Open(filepath.Join(roots[lagging], "dubi-raft-groups", groupID))
	if err != nil {
		lock.Release()
		t.Fatal(err)
	}
	image, err := log.Snapshot()
	if err != nil {
		log.Close()
		lock.Release()
		t.Fatal(err)
	}
	ref, recognized, err := snapshot.DecodeReference(image.GetData())
	log.Close()
	lock.Release()
	if err != nil || !recognized || ref.Size <= 64<<20 || ref.Index < cut || len(image.GetData()) > snapshot.MaxReferenceBytes {
		t.Fatal("catch-up did not persist a bounded file reference", ref, recognized, err)
	}
	lock, err = storelock.Acquire(roots[lagging])
	if err != nil {
		t.Fatal(err)
	}
	rootLog, err := logstore.Open(filepath.Join(roots[lagging], "dubi-raft-groups", raft.RootGroupID))
	if err != nil {
		lock.Release()
		t.Fatal(err)
	}
	rootImage, err := rootLog.Snapshot()
	rootLog.Close()
	lock.Release()
	if err != nil {
		t.Fatal(err)
	}
	rootRef, recognized, err := snapshot.DecodeReference(rootImage.GetData())
	if err != nil || !recognized || rootRef.Index < rootCut || rootRef.GroupID != raft.RootGroupID {
		t.Fatal("Root catch-up did not persist file reference", rootRef, recognized, err)
	}
	// Preserve the old temporary-test DB but make Raft recover a fresh local
	// index solely from its durable artifact reference. No user Store is used.
	statePath := filepath.Join(roots[lagging], "dubi-engine-groups", groupID, "dubi-engine-state")
	if err := os.Rename(statePath, statePath+".retained"); err != nil {
		t.Fatal(err)
	}
	rootStatePath := filepath.Join(roots[lagging], "dubi-cluster-state", "dubi-root-state")
	if err := os.Rename(rootStatePath, rootStatePath+".retained"); err != nil {
		t.Fatal(err)
	}
	start(lagging, false)
	awaitSnapshotCondition(t, "artifact-backed startup recovery ready", func() bool {
		select {
		case <-nodes[lagging].Ready():
			return true
		default:
			return false
		}
	})
	checkValues()
}
