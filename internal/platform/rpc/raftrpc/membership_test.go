package raftrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

type sharedMembership struct {
	mu       sync.Mutex
	conf     *raftpb.ConfState
	leader   uint64
	prepared map[uint64]bool
	index    uint64
	term     uint64
}

type membershipBackend struct {
	local  uint64
	shared *sharedMembership
}

type membershipJoiner struct {
	local  uint64
	shared *sharedMembership
}

func (joiner *membershipJoiner) PrepareJoining(_ context.Context, task rootstate.GroupTaskReference, raftID uint64) error {
	if err := task.Validate(); err != nil {
		return err
	}
	if raftID != joiner.local {
		return errors.New("joining request targeted another Node")
	}
	joiner.shared.mu.Lock()
	joiner.shared.prepared[raftID] = true
	joiner.shared.mu.Unlock()
	return nil
}

type directRootControl struct {
	state *rootstate.State
	index uint64
}

func (control *directRootControl) Submit(groupID string, command []byte) error {
	if groupID != raft.RootGroupID {
		return errors.New("unexpected non-Root command")
	}
	control.index++
	return control.state.Apply(control.index, command)
}
func (control *directRootControl) ReadIndex(context.Context, string) (uint64, error) {
	return control.index, nil
}

func (backend *membershipBackend) ReadIndex(context.Context, string) (uint64, error) { return 1, nil }
func (backend *membershipBackend) Membership(string) (*raftpb.ConfState, error) {
	backend.shared.mu.Lock()
	defer backend.shared.mu.Unlock()
	return proto.Clone(backend.shared.conf).(*raftpb.ConfState), nil
}
func (backend *membershipBackend) Status(string) (raft.GroupStatus, error) {
	backend.shared.mu.Lock()
	defer backend.shared.mu.Unlock()
	role := etcdraft.StateFollower
	if backend.shared.leader == backend.local {
		role = etcdraft.StateLeader
	}
	return raft.GroupStatus{RaftID: backend.local, LeaderID: backend.shared.leader, Role: role, Term: backend.shared.term}, nil
}
func (backend *membershipBackend) MembershipCut(string) (raft.MembershipCut, error) {
	backend.shared.mu.Lock()
	defer backend.shared.mu.Unlock()
	return raft.MembershipCut{Index: backend.shared.index, Conf: proto.Clone(backend.shared.conf).(*raftpb.ConfState)}, nil
}
func (backend *membershipBackend) ChangeMembership(_ string, expected raft.MembershipCut, raftID uint64, operation raftpb.ConfChangeType) error {
	backend.shared.mu.Lock()
	defer backend.shared.mu.Unlock()
	if expected.Index != backend.shared.index || !proto.Equal(expected.Conf, backend.shared.conf) {
		return raft.ErrMembershipChanged
	}
	switch operation {
	case raftpb.ConfChangeType_ConfChangeAddLearnerNode:
		if !backend.shared.prepared[raftID] {
			return errors.New("target Group was not prepared")
		}
		backend.shared.conf.Learners = append(backend.shared.conf.Learners, raftID)
	case raftpb.ConfChangeType_ConfChangeAddNode:
		backend.shared.conf.Learners = withoutRaftID(backend.shared.conf.Learners, raftID)
		backend.shared.conf.Voters = append(backend.shared.conf.Voters, raftID)
	case raftpb.ConfChangeType_ConfChangeRemoveNode:
		backend.shared.conf.Learners = withoutRaftID(backend.shared.conf.Learners, raftID)
		backend.shared.conf.Voters = withoutRaftID(backend.shared.conf.Voters, raftID)
	default:
		return errors.New("unsupported test mutation")
	}
	backend.shared.index++
	return nil
}
func (backend *membershipBackend) TransferLeadershipFenced(
	_ context.Context, _ string, target uint64, expected raft.MembershipCut, term uint64,
) (raft.GroupStatus, error) {
	backend.shared.mu.Lock()
	defer backend.shared.mu.Unlock()
	if expected.Index != backend.shared.index || !proto.Equal(expected.Conf, backend.shared.conf) || term != backend.shared.term {
		return raft.GroupStatus{}, raft.ErrMembershipChanged
	}
	if !slices.Contains(backend.shared.conf.Voters, target) {
		return raft.GroupStatus{}, errors.New("target is not a Voter")
	}
	backend.shared.leader = target
	backend.shared.term++
	return raft.GroupStatus{RaftID: backend.local, LeaderID: target, Role: etcdraft.StateFollower, Term: backend.shared.term}, nil
}

func TestRoutedMembershipControlFollowsNewLeaderBeforeRootPublication(t *testing.T) {
	const leaderID uint64 = 101
	const newLeaderID uint64 = 103
	const clientID uint64 = 991
	const clusterID = "cluster-membership-test"
	pool, certificates := testCertificates(t, []uint64{leaderID, newLeaderID, clientID})
	authorize := func(ctx context.Context, claimed uint64) error {
		remote, ok := peer.FromContext(ctx)
		if !ok {
			return errors.New("peer is missing")
		}
		info := remote.AuthInfo.(credentials.TLSInfo)
		if info.State.PeerCertificates[0].Subject.CommonName != fmt.Sprintf("raft-%d", claimed) {
			return errors.New("certificate does not match RaftID")
		}
		return nil
	}
	shared := &sharedMembership{conf: &raftpb.ConfState{Voters: []uint64{leaderID}},
		leader: leaderID, prepared: make(map[uint64]bool), index: 1, term: 1}
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	directory := NewStaticDirectory()
	for _, raftID := range []uint64{leaderID, newLeaderID} {
		backend := &membershipBackend{local: raftID, shared: shared}
		server, err := NewMembershipServer(clusterID, raftID, backend,
			&membershipJoiner{local: raftID, shared: shared}, root, authorize)
		if err != nil {
			t.Fatal(err)
		}
		serverTLS := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificates[raftID]},
			ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})
		grpcServer := grpc.NewServer(grpc.Creds(serverTLS))
		t.Cleanup(grpcServer.Stop)
		raftwire.RegisterGroupMembershipServiceServer(grpcServer, server)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		go func() { _ = grpcServer.Serve(listener) }()
		if err := directory.Update(raftID, listener.Addr().String()); err != nil {
			t.Fatal(err)
		}
	}

	rootControl := &directRootControl{state: root}
	for _, node := range []rootstate.Node{
		{NodeID: fmt.Sprintf("node_%032x", leaderID), RaftID: leaderID, RegionID: "r1", Address: "n1"},
		{NodeID: fmt.Sprintf("node_%032x", newLeaderID), RaftID: newLeaderID, RegionID: "r1", Address: "n3"},
		{NodeID: fmt.Sprintf("node_%032x", clientID), RaftID: clientID, RegionID: "r1", Address: "caller"},
	} {
		applyMembershipRoot(t, rootControl, rootstate.Command{Version: 1,
			RequestID: fmt.Sprintf("node-%d", node.RaftID), Type: rootstate.RegisterNode, Node: &node})
	}
	rootControl.index++
	if err := root.ApplyMembership(rootControl.index, &raftpb.ConfState{Voters: []uint64{leaderID}, Learners: []uint64{newLeaderID, clientID}}); err != nil {
		t.Fatal(err)
	}
	applyMembershipRoot(t, rootControl, rootstate.Command{Version: 1, RequestID: "group-data",
		Type:  rootstate.RegisterGroup,
		Group: &rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateEngine}})
	applyMembershipRoot(t, rootControl, rootstate.Command{Version: 1, RequestID: "members-initial",
		Type: rootstate.PublishGroupMembers, Membership: &rootstate.GroupMembership{GroupID: "data-1",
			ExpectedEpoch: 1, Members: []rootstate.GroupMember{{RaftID: leaderID, Role: rootstate.GroupMemberVoter}}}})

	routes := NewStaticGroupRoutes()
	if err := routes.Update("data-1", []uint64{leaderID}); err != nil {
		t.Fatal(err)
	}
	clientTLS := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificates[clientID]},
		RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})
	client, err := NewGroupClient(GroupClientOptions{ClusterID: clusterID, RaftID: clientID,
		Directory: directory, Credentials: clientTLS, GroupRoutes: routes})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	control, err := NewRoutedMembershipControl(client, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := groupmembership.New(rootControl, control, root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	plan := rootstate.GroupChangePlan{ChangeID: "replace-leader", GroupID: "data-1", ExpectedEpoch: 2,
		Members: []rootstate.GroupMember{{RaftID: newLeaderID, Role: rootstate.GroupMemberVoter}}}
	if err := reconciler.Plan(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Reconcile(ctx, plan.ChangeID); !errors.Is(err, groupmembership.ErrLeadershipTransferred) {
		t.Fatalf("first reconciliation = %v", err)
	}
	// newLeaderID is absent from the published Group descriptor. It remains
	// reachable because the active Root task contributes control candidates.
	if err := reconciler.Reconcile(ctx, plan.ChangeID); err != nil {
		t.Fatal(err)
	}
	conf, err := control.Membership(ctx, "data-1")
	if err != nil || !slices.Equal(conf.GetVoters(), []uint64{newLeaderID}) || len(conf.GetLearners()) != 0 {
		t.Fatalf("final remote membership = %+v, %v", conf, err)
	}
	group, found, err := root.Group("data-1")
	if err != nil || !found || group.Epoch != 3 || !slices.Equal(group.Members, plan.Members) {
		t.Fatalf("published Root membership = %+v, %v, %v", group, found, err)
	}
}

func applyMembershipRoot(t *testing.T, control *directRootControl, command rootstate.Command) {
	t.Helper()
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := control.Submit(raft.RootGroupID, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := control.state.ResultForCommand(encoded)
	if err != nil || !found || result.Error != "" {
		t.Fatalf("Root command result = %+v, %v, %v", result, found, err)
	}
}

func withoutRaftID(ids []uint64, target uint64) []uint64 {
	result := ids[:0]
	for _, id := range ids {
		if id != target {
			result = append(result, id)
		}
	}
	return result
}
