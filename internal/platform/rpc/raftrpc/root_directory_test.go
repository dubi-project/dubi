package raftrpc

import (
	"slices"
	"strconv"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func applyRootCommand(t *testing.T, state *rootstate.State, index *uint64, command rootstate.Command) rootstate.Result {
	t.Helper()
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	*index++
	if err := state.Apply(*index, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := state.Result(command.RequestID)
	if err != nil || !found {
		t.Fatalf("Root result %q: %+v, %v, %v", command.RequestID, result, found, err)
	}
	return result
}

func TestRootDirectoryUsesAppliedRootConfStateIncludingJointMembership(t *testing.T) {
	state, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	directory := NewRootDirectory(state)
	if got := directory.Candidates(raft.RootGroupID); len(got) != 0 {
		t.Fatal("empty Root acquired routes", got)
	}
	conf := &raftpb.ConfState{Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 3}, Learners: []uint64{4}, LearnersNext: []uint64{3}, AutoLeave: proto.Bool(true)}
	if err := state.ApplyMembership(1, conf); err != nil {
		t.Fatal(err)
	}
	if _, found, err := state.Group(raft.RootGroupID); err != nil || found {
		t.Fatal("test unexpectedly has duplicate Root directory record", found, err)
	}
	if got := directory.Candidates(raft.RootGroupID); !slices.Equal(got, []uint64{1, 2, 3, 4}) {
		t.Fatal("lost joint membership routes", got)
	}
	directory.ObserveLeader(raft.RootGroupID, 3)
	if got := directory.Candidates(raft.RootGroupID); !slices.Equal(got, []uint64{3, 1, 2, 4}) {
		t.Fatal("Root leader preference absent", got)
	}
	directory.ObserveLeader(raft.RootGroupID, 99)
	if got := directory.Candidates(raft.RootGroupID); !slices.Equal(got, []uint64{3, 1, 2, 4}) {
		t.Fatal("unknown leader introduced", got)
	}
	if err := state.ApplyMembership(2, &raftpb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3, 4}}); err != nil {
		t.Fatal(err)
	}
	if err := state.ApplyMembership(3, &raftpb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{4}}); err != nil {
		t.Fatal(err)
	}
	if got := directory.Candidates(raft.RootGroupID); !slices.Equal(got, []uint64{1, 2, 4}) {
		t.Fatal("removed member remained through leader hint", got)
	}
	view, err := state.RootMembership()
	if err != nil {
		t.Fatal(err)
	}
	view.Voters[0] = 99
	if got := directory.Candidates(raft.RootGroupID); !slices.Equal(got, []uint64{1, 2, 4}) {
		t.Fatal("caller mutated persisted projection", got)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if got := directory.Candidates(raft.RootGroupID); len(got) != 0 {
		t.Fatal("closed Root still routes", got)
	}
}

func TestRootDirectoryDoesNotDialFencedRegisteredNode(t *testing.T) {
	state, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	var index uint64
	node := &rootstate.Node{NodeID: "node_00000000000000000000000000000002", RaftID: 2, RegionID: "local", Address: "node-2:9000"}
	if result := applyRootCommand(t, state, &index, rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: "register-two", Type: rootstate.RegisterNode, Node: node}); result.Error != "" {
		t.Fatal(result.Error)
	}
	directory := NewRootDirectory(state)
	if address, found := directory.Resolve(2); !found || address != node.Address {
		t.Fatal("registered Node did not resolve before Fence", address, found)
	}
	if result := applyRootCommand(t, state, &index, rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: "fence-two", Type: rootstate.FenceNode,
		NodeFence: &rootstate.NodeFenceRequest{NodeID: node.NodeID, RaftID: node.RaftID}}); result.Error != "" {
		t.Fatal(result.Error)
	}
	if address, found := directory.Resolve(2); found || address != "" {
		t.Fatal("fenced Node retained a dial route", address, found)
	}
}

func TestRootDirectoryProjectsPersistentMembershipAndTransientLeader(t *testing.T) {
	state, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	var index uint64
	for raftID, address := range map[uint64]string{1: "node-1:9000", 2: "node-2:9000", 3: "node-3:9000"} {
		suffix := strconv.FormatUint(raftID, 10)
		result := applyRootCommand(t, state, &index, rootstate.Command{Version: rootstate.FormatVersion,
			RequestID: "node-" + suffix, Type: rootstate.RegisterNode,
			Node: &rootstate.Node{NodeID: "node_0000000000000000000000000000000" + suffix,
				RaftID: raftID, RegionID: "region-a", Address: address}})
		if result.Error != "" {
			t.Fatal(result.Error)
		}
	}
	if result := applyRootCommand(t, state, &index, rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: "group-data", Type: rootstate.RegisterGroup,
		Group: &rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateCapability}}); result.Error != "" {
		t.Fatal(result.Error)
	}
	if result := applyRootCommand(t, state, &index, rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: "members-1", Type: rootstate.PublishGroupMembers,
		Membership: &rootstate.GroupMembership{GroupID: "data-1", ExpectedEpoch: 1,
			Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter},
				{RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberLearner}}}}); result.Error != "" {
		t.Fatal(result.Error)
	}

	directory := NewRootDirectory(state)
	if address, found := directory.Resolve(2); !found || address != "node-2:9000" {
		t.Fatalf("Resolve(2) = %q, %v", address, found)
	}
	if got := directory.Candidates("data-1"); !slices.Equal(got, []uint64{1, 2, 3}) {
		t.Fatalf("initial candidates = %v", got)
	}
	directory.ObserveLeader("data-1", 3)
	if got := directory.Candidates("data-1"); !slices.Equal(got, []uint64{3, 1, 2}) {
		t.Fatalf("leader-first candidates = %v", got)
	}
	directory.ObserveLeader("data-1", 99)
	if got := directory.Candidates("data-1"); !slices.Equal(got, []uint64{3, 1, 2}) {
		t.Fatalf("unknown Leader changed candidates = %v", got)
	}

	if result := applyRootCommand(t, state, &index, rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: "members-2", Type: rootstate.PublishGroupMembers,
		Membership: &rootstate.GroupMembership{GroupID: "data-1", ExpectedEpoch: 2,
			Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter},
				{RaftID: 2, Role: rootstate.GroupMemberVoter}}}}); result.Error != "" {
		t.Fatal(result.Error)
	}
	if got := directory.Candidates("data-1"); !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("candidates after Leader removal = %v", got)
	}
}
