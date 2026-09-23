package raftrpc

import (
	"slices"
	"strconv"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
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
