// vps_record_holding is a read-only acceptance probe, not a server command.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/engine/record"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

type input struct {
	Status struct {
		Root rootjoin.View `json:"root"`
	} `json:"status"`
	Tenant       string `json:"tenant"`
	Dataset      string `json:"dataset"`
	Key          string `json:"key"`
	Value        string `json:"value"`
	OwnerGroupID string `json:"owner_group_id"`
}

type observation struct {
	NodeID    string `json:"node_id"`
	Name      string `json:"name"`
	Region    string `json:"region"`
	Held      bool   `json:"held"`
	StoreID   string `json:"store_id,omitempty"`
	VolumeID  string `json:"volume_id,omitempty"`
	RootIndex uint64 `json:"root_index,omitempty"`
	Error     string `json:"error,omitempty"`
}

func validID(value, prefix string) bool {
	if len(value) != len(prefix)+32 || !strings.HasPrefix(value, prefix) || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value[len(prefix):])
	return err == nil
}

func run() error {
	store := flag.String("store", "", "managed Node identity used only for authenticated probes")
	group := flag.String("group", "", "optional read-only Group membership observation instead of Record bytes")
	flag.Parse()
	if *store == "" || flag.NArg() != 0 {
		return fmt.Errorf("use --store and supply fixture/status JSON on stdin")
	}
	var in input
	d := json.NewDecoder(io.LimitReader(os.Stdin, 128<<10))
	if err := d.Decode(&in); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("trailing or oversized probe input")
	}
	id, err := identity.Load(*store)
	if err != nil {
		return err
	}
	if in.Status.Root.ClusterID != id.ClusterID || len(in.Status.Root.Peers) == 0 || len(in.Status.Root.Peers) > 64 {
		return fmt.Errorf("invalid probe cluster or peer count")
	}
	if *group != "" {
		return inspectGroup(*store, in.Status.Root, *group)
	}
	descriptor, err := records.Describe(rootstate.Keyspace{TenantID: in.Tenant, EngineID: record.EngineID, DatasetID: in.Dataset}, in.OwnerGroupID, []byte(in.Key), []byte(in.Value))
	if err != nil {
		return err
	}
	_, tls, err := config.LoadTLS(*store, id)
	if err != nil {
		return err
	}
	seen := make(map[uint64]bool)
	for _, node := range in.Status.Root.Peers {
		if seen[node.RaftID] || node.NodeID == "" {
			return fmt.Errorf("duplicate or invalid probe Node")
		}
		seen[node.RaftID] = true
	}
	encoded, err := json.Marshal(descriptor)
	if err != nil {
		return err
	}
	wantID, _ := descriptor.ID()
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	results := make([]observation, len(in.Status.Root.Peers))
	slots := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for i, node := range in.Status.Root.Peers {
		wg.Add(1)
		go func(i int, node rootstate.Node) {
			defer wg.Done()
			result := observation{NodeID: node.NodeID, Name: node.Name, Region: node.RegionID}
			defer func() { results[i] = result }()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				result.Error = ctx.Err().Error()
				return
			}
			attempt, done := context.WithTimeout(ctx, 20*time.Second)
			defer done()
			conn, err := grpc.NewClient("passthrough:///"+node.Address, grpc.WithTransportCredentials(tlscredentials.NewTLS(tls)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<10)))
			if err != nil {
				result.Error = err.Error()
				return
			}
			defer conn.Close()
			var actual peer.Peer
			holding, err := raftwire.NewRecordHoldingServiceClient(conn).Observe(attempt, &raftwire.ObserveRecordHoldingRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID, ToRaftId: node.RaftID, RecordDescriptor: encoded}, grpc.Peer(&actual))
			if err != nil {
				result.Error = err.Error()
				return
			}
			cert, err := identity.VerifiedPeerCertificate(peer.NewContext(attempt, &actual), time.Now())
			if err != nil {
				result.Error = err.Error()
				return
			}
			remote, err := identity.ParseNodeCertificate(cert)
			if err != nil || remote.ClusterID != id.ClusterID || remote.NodeID != node.NodeID || remote.RaftID != node.RaftID || holding.NodeId != node.NodeID || holding.RaftId != node.RaftID || holding.ClusterId != id.ClusterID || holding.RootConfirmedIndex == 0 || holding.RecordId != wantID || holding.Version != 1 || !bytes.Equal(holding.RecordDescriptor, encoded) || !validID(holding.StoreId, "store_") || !validID(holding.VolumeId, "volume_") {
				result.Error = "holding evidence identity or descriptor mismatch"
				return
			}
			result.Held = true
			result.StoreID = holding.StoreId
			result.VolumeID = holding.VolumeId
			result.RootIndex = holding.RootConfirmedIndex
		}(i, node)
	}
	wg.Wait()
	recordID, _ := descriptor.ID()
	return json.NewEncoder(os.Stdout).Encode(struct {
		RecordID string        `json:"record_id"`
		Nodes    []observation `json:"nodes"`
	}{recordID, results})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Inspect is the existing authenticated control-plane read; no membership or
// leadership mutation is issued. Errors from Nodes not hosting the Group are
// retained, never interpreted as a successful quorum observation.
func inspectGroup(store string, view rootjoin.View, group string) error {
	if raft.ValidateGroupID(group) != nil {
		return fmt.Errorf("invalid observed Group")
	}
	id, err := identity.Load(store)
	if err != nil {
		return err
	}
	_, tls, err := config.LoadTLS(store, id)
	if err != nil {
		return err
	}
	type groupObservation struct {
		NodeID         string `json:"node_id"`
		Name           string `json:"name"`
		RaftID         uint64 `json:"raft_id"`
		LeaderID       uint64 `json:"leader_id,omitempty"`
		ConfirmedIndex uint64 `json:"confirmed_index,omitempty"`
		Error          string `json:"error,omitempty"`
	}
	known := map[uint64]bool{}
	for _, n := range view.Peers {
		if known[n.RaftID] || n.RaftID == 0 || n.NodeID == "" {
			return fmt.Errorf("invalid observed Node set")
		}
		known[n.RaftID] = true
	}
	results := make([]groupObservation, len(view.Peers))
	slots := make(chan struct{}, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i, n := range view.Peers {
		wg.Add(1)
		go func(i int, n rootstate.Node) {
			defer wg.Done()
			item := groupObservation{NodeID: n.NodeID, Name: n.Name, RaftID: n.RaftID}
			defer func() { results[i] = item }()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				item.Error = ctx.Err().Error()
				return
			}
			attempt, done := context.WithTimeout(ctx, 15*time.Second)
			defer done()
			conn, err := grpc.NewClient("passthrough:///"+n.Address, grpc.WithTransportCredentials(tlscredentials.NewTLS(tls)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(300<<10)))
			if err != nil {
				item.Error = err.Error()
				return
			}
			defer conn.Close()
			if group == raft.RootGroupID {
				var actual peer.Peer
				reply, err := raftwire.NewGroupServiceClient(conn).ReadIndex(attempt, &raftwire.GroupReadIndexRequest{ClusterId: id.ClusterID, GroupId: group, FromRaftId: id.RaftID, ToRaftId: n.RaftID}, grpc.Peer(&actual))
				if err != nil {
					item.Error = err.Error()
					return
				}
				cert, err := identity.VerifiedPeerCertificate(peer.NewContext(attempt, &actual), time.Now())
				if err != nil {
					item.Error = err.Error()
					return
				}
				remote, err := identity.ParseNodeCertificate(cert)
				voter := false
				for _, candidate := range view.Voters {
					voter = voter || candidate == n.RaftID
				}
				if err != nil || remote.ClusterID != id.ClusterID || remote.NodeID != n.NodeID || remote.RaftID != n.RaftID || !voter || reply.ConfirmedIndex == 0 || reply.LeaderRaftId != n.RaftID || len(reply.ProtoReflect().GetUnknown()) != 0 {
					item.Error = "Node did not return its own current Root leadership quorum proof"
					return
				}
				item.LeaderID, item.ConfirmedIndex = reply.LeaderRaftId, reply.ConfirmedIndex
				return
			}
			var actual peer.Peer
			reply, err := raftwire.NewGroupMembershipServiceClient(conn).Inspect(attempt, &raftwire.GroupMembershipRequest{ClusterId: id.ClusterID, GroupId: group, FromRaftId: id.RaftID, ToRaftId: n.RaftID}, grpc.Peer(&actual))
			if err != nil {
				item.Error = err.Error()
				return
			}
			cert, err := identity.VerifiedPeerCertificate(peer.NewContext(attempt, &actual), time.Now())
			if err != nil {
				item.Error = err.Error()
				return
			}
			remote, err := identity.ParseNodeCertificate(cert)
			if err != nil || remote.ClusterID != id.ClusterID || remote.NodeID != n.NodeID || remote.RaftID != n.RaftID || reply.ClusterId != id.ClusterID || reply.GroupId != group || reply.RaftId != n.RaftID || reply.ConfirmedIndex == 0 || !known[reply.LeaderRaftId] || len(reply.ProtoReflect().GetUnknown()) != 0 {
				item.Error = "Group observation identity or confirmed leader mismatch"
				return
			}
			item.LeaderID, item.ConfirmedIndex = reply.LeaderRaftId, reply.ConfirmedIndex
		}(i, n)
	}
	wg.Wait()
	return json.NewEncoder(os.Stdout).Encode(struct {
		Group string             `json:"group_id"`
		Nodes []groupObservation `json:"nodes"`
	}{group, results})
}
