package raftrpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type memoryState struct {
	mu       sync.Mutex
	applied  uint64
	conf     *raftpb.ConfState
	commands []string
}

type memoryImage struct {
	Applied  uint64   `json:"applied"`
	Commands []string `json:"commands"`
}

func (s *memoryState) AppliedIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied
}

func (s *memoryState) ConfState() *raftpb.ConfState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conf == nil {
		return &raftpb.ConfState{}
	}
	return proto.Clone(s.conf).(*raftpb.ConfState)
}

func (s *memoryState) Apply(index uint64, command []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index != s.applied+1 {
		return errors.New("application index gap")
	}
	s.commands = append(s.commands, string(command))
	s.applied = index
	return nil
}

func (s *memoryState) Advance(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index != s.applied+1 {
		return errors.New("no-op index gap")
	}
	s.applied = index
	return nil
}

func (s *memoryState) ApplyMembership(index uint64, conf *raftpb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index != s.applied+1 {
		return errors.New("membership index gap")
	}
	s.conf = proto.Clone(conf).(*raftpb.ConfState)
	s.applied = index
	return nil
}

func (s *memoryState) Restore(index uint64, data []byte, conf *raftpb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(data) != 0 {
		var image memoryImage
		if err := json.Unmarshal(data, &image); err != nil {
			return err
		}
		if image.Applied != index {
			return errors.New("snapshot image index mismatch")
		}
		s.commands = image.Commands
	}
	s.applied = index
	s.conf = proto.Clone(conf).(*raftpb.ConfState)
	return nil
}

func (s *memoryState) Snapshot() ([]byte, uint64, *raftpb.ConfState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(memoryImage{Applied: s.applied, Commands: s.commands})
	return data, s.applied, proto.Clone(s.conf).(*raftpb.ConfState), err
}

func (s *memoryState) Close() error { return nil }

func (s *memoryState) has(command string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, value := range s.commands {
		if value == command {
			return true
		}
	}
	return false
}

type testNode struct {
	id        identity.Identity
	host      *raft.Host
	transport *Transport
	server    *grpc.Server
	listener  net.Listener
	state     *memoryState
}

func (n *testNode) close() {
	if n.server != nil {
		n.server.Stop()
	}
	if n.transport != nil {
		_ = n.transport.Close()
	}
	if n.host != nil {
		_ = n.host.Close()
	}
	if n.listener != nil {
		_ = n.listener.Close()
	}
}

func testCertificates(t *testing.T, ids []uint64) (*x509.CertPool, map[uint64]tls.Certificate) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "DUBI test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	certificates := make(map[uint64]tls.Certificate)
	for i, id := range ids {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)),
			Subject:  pkix.Name{CommonName: fmt.Sprintf("raft-%d", id)},
			DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour),
			NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		certificates[id] = tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: key}
	}
	return pool, certificates
}

func TestThreeNodeRaftOverMutualTLS(t *testing.T) {
	const groupID = "range-1"
	base := t.TempDir()
	var nodes [3]*testNode
	if err := ensureDirectory(filepath.Join(base, "n0")); err != nil {
		t.Fatal(err)
	}
	first, err := identity.Bootstrap(filepath.Join(base, "n0"))
	if err != nil {
		t.Fatal(err)
	}
	nodes[0] = &testNode{id: first}
	for i := 1; i < len(nodes); i++ {
		root := filepath.Join(base, fmt.Sprintf("n%d", i))
		if err := ensureDirectory(root); err != nil {
			t.Fatal(err)
		}
		if _, err := identity.PrepareJoin(root); err != nil {
			t.Fatal(err)
		}
		id, err := identity.BindCluster(root, first.ClusterID)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &testNode{id: id}
	}
	defer func() {
		for _, node := range nodes {
			if node != nil {
				node.close()
			}
		}
	}()
	ids := []uint64{nodes[0].id.RaftID, nodes[1].id.RaftID, nodes[2].id.RaftID}
	pool, certs := testCertificates(t, ids)
	var directories [3]*StaticDirectory
	for i := range directories {
		directories[i] = NewStaticDirectory()
	}
	type snapshotResult struct {
		to      uint64
		success bool
	}
	snapshotResults := make(chan snapshotResult, 16)
	for i, node := range nodes {
		root := filepath.Join(base, fmt.Sprintf("n%d", i))
		clientTLS := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certs[node.id.RaftID]},
			RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})
		var host *raft.Host
		transport, err := NewTransport(TransportOptions{ClusterID: first.ClusterID, RaftID: node.id.RaftID,
			Directory: directories[i], Credentials: clientTLS, RPCTimeout: time.Second,
			OnFailure: func(groupID string, raftID uint64) {
				if host != nil {
					_ = host.ReportUnreachable(groupID, raftID)
				}
			}, OnSnapshot: func(groupID string, raftID uint64, success bool) {
				if host != nil {
					_ = host.ReportSnapshot(groupID, raftID, success)
				}
				select {
				case snapshotResults <- snapshotResult{to: raftID, success: success}:
				default:
				}
			}})
		if err != nil {
			t.Fatal(err)
		}
		node.transport = transport
		host, err = raft.OpenHost(raft.HostOptions{StoreRoot: root, TickInterval: 10 * time.Millisecond, Transport: transport})
		if err != nil {
			t.Fatal(err)
		}
		node.host = host
		serverTLS := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certs[node.id.RaftID]},
			ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})
		server := grpc.NewServer(grpc.Creds(serverTLS), grpc.MaxRecvMsgSize(DefaultMaxMessageSize+1024))
		localID := node.id.RaftID
		handler, err := NewServer(first.ClusterID, localID, host, func(ctx context.Context, claimed uint64) error {
			remote, _ := peer.FromContext(ctx)
			info := remote.AuthInfo.(credentials.TLSInfo)
			if info.State.PeerCertificates[0].Subject.CommonName != fmt.Sprintf("raft-%d", claimed) {
				return errors.New("certificate does not match RaftID")
			}
			return nil
		}, 0)
		if err != nil {
			t.Fatal(err)
		}
		raftwire.RegisterRaftServiceServer(server, handler)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		node.server, node.listener = server, listener
		for _, directory := range directories {
			if err := directory.Update(localID, listener.Addr().String()); err != nil {
				t.Fatal(err)
			}
		}
		go func() { _ = server.Serve(listener) }()
	}
	for _, node := range nodes {
		node.state = &memoryState{}
		if err := node.host.AddGroup(groupID, ids, false, node.state); err != nil {
			t.Fatal(err)
		}
	}
	probe, err := grpc.NewClient(nodes[1].listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certs[nodes[0].id.RaftID]}, RootCAs: pool,
		ServerName: "localhost", MinVersion: tls.VersionTLS13})))
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	encoded, err := proto.Marshal(&raftpb.Message{From: proto.Uint64(nodes[2].id.RaftID),
		To: proto.Uint64(nodes[1].id.RaftID), Type: raftpb.MessageType_MsgHeartbeat.Enum()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = raftwire.NewRaftServiceClient(probe).Step(context.Background(), &raftwire.StepRequest{
		ClusterId: first.ClusterID, GroupId: groupID, FromRaftId: nodes[2].id.RaftID,
		ToRaftId: nodes[1].id.RaftID, Message: encoded})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("forged RaftID accepted: %v", err)
	}
	stream, err := raftwire.NewRaftServiceClient(probe).InstallSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Header{
		Header: &raftwire.SnapshotHeader{ClusterId: first.ClusterID, GroupId: groupID,
			FromRaftId: nodes[0].id.RaftID, ToRaftId: nodes[1].id.RaftID,
			EncodedSize: 3, Sha256Digest: make([]byte, 32)}}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{
		Data: []byte("bad")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.DataLoss {
		t.Fatalf("corrupt snapshot accepted: %v", err)
	}
	leader := awaitLeader(t, nodes[:], groupID)
	if err := nodes[leader].host.Submit(groupID, []byte("first")); err != nil {
		t.Fatal(err)
	}
	awaitApplied(t, nodes[:], "first")
	lagging := (leader + 2) % 3
	for i := range nodes {
		if i != lagging {
			if err := directories[i].Update(nodes[lagging].id.RaftID, "127.0.0.1:1"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := nodes[leader].host.Submit(groupID, []byte("before-snapshot")); err != nil {
		t.Fatal(err)
	}
	awaitApplied(t, []*testNode{nodes[leader], nodes[(leader+1)%3]}, "before-snapshot")
	if _, err := nodes[leader].host.CreateSnapshot(groupID); err != nil {
		t.Fatal(err)
	}
	for i := range nodes {
		if i != lagging {
			if err := directories[i].Update(nodes[lagging].id.RaftID, nodes[lagging].listener.Addr().String()); err != nil {
				t.Fatal(err)
			}
		}
	}
	awaitApplied(t, nodes[:], "before-snapshot")
	deadline := time.After(5 * time.Second)
waitSnapshot:
	for {
		select {
		case result := <-snapshotResults:
			if result.to == nodes[lagging].id.RaftID && result.success {
				break waitSnapshot
			}
		case <-deadline:
			t.Fatal("lagging peer caught up without a successful snapshot transfer")
		}
	}
	nodes[leader].close()
	remaining := []*testNode{nodes[(leader+1)%3], nodes[(leader+2)%3]}
	next := remaining[awaitLeader(t, remaining, groupID)]
	if err := next.host.Submit(groupID, []byte("second")); err != nil {
		t.Fatal(err)
	}
	awaitApplied(t, remaining, "second")
}

func ensureDirectory(path string) error { return os.MkdirAll(path, 0o750) }

func awaitLeader(t *testing.T, nodes []*testNode, groupID string) int {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		leader := -1
		for i, node := range nodes {
			status, err := node.host.Status(groupID)
			if err == nil && status.Role == etcdraft.StateLeader {
				leader = i
			}
		}
		if leader >= 0 {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Raft Group did not elect a leader over gRPC")
	return -1
}

func awaitApplied(t *testing.T, nodes []*testNode, command string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, node := range nodes {
			if !node.state.has(command) {
				all = false
			}
		}
		if all {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("command %q was not committed on all live nodes", command)
}

func TestServerRejectsUnauthenticatedAndWrongEnvelope(t *testing.T) {
	server, err := NewServer("cluster", 3, rejectStepper{}, func(context.Context, uint64) error { return nil }, 1024)
	if err != nil {
		t.Fatal(err)
	}
	message, _ := proto.Marshal(&raftpb.Message{From: proto.Uint64(1), To: proto.Uint64(3), Type: raftpb.MessageType_MsgHeartbeat.Enum()})
	request := &raftwire.StepRequest{ClusterId: "cluster", GroupId: "root", FromRaftId: 1, ToRaftId: 3, Message: message}
	if _, err := server.Step(context.Background(), request); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated Step = %v", err)
	}
	request.ClusterId = "other"
	if _, err := server.Step(context.Background(), request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong Cluster = %v", err)
	}
}

type rejectStepper struct{}

func (rejectStepper) Step(string, *raftpb.Message) error { return errors.New("must not be called") }
