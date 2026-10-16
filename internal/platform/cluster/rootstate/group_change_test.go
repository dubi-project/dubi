package rootstate

import "testing"

func applyDirect(t *testing.T, state *State, index *uint64, command Command) Result {
	t.Helper()
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	*index++
	if err := state.Apply(*index, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := state.Result(command.RequestID)
	if err != nil || !found {
		t.Fatalf("result %q = %+v, %v, %v", command.RequestID, result, found, err)
	}
	return result
}

func TestGroupMembershipChangeIsPersistentAndCompletesAtomically(t *testing.T) {
	state, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	var index uint64
	for _, node := range []Node{
		{NodeID: "node_00000000000000000000000000000001", RaftID: 1, RegionID: "r1", Address: "n1:1"},
		{NodeID: "node_00000000000000000000000000000002", RaftID: 2, RegionID: "r1", Address: "n2:1"},
		{NodeID: "node_00000000000000000000000000000003", RaftID: 3, RegionID: "r1", Address: "n3:1"},
	} {
		if result := applyDirect(t, state, &index, Command{Version: FormatVersion,
			RequestID: "register-" + node.NodeID, Type: RegisterNode, Node: &node}); result.Error != "" {
			t.Fatal(result.Error)
		}
	}
	if result := applyDirect(t, state, &index, Command{Version: FormatVersion, RequestID: "register-group",
		Type: RegisterGroup, Group: &Group{GroupID: "data-1", StateKind: GroupStateEngine}}); result.Error != "" {
		t.Fatal(result.Error)
	}
	if result := applyDirect(t, state, &index, Command{Version: FormatVersion, RequestID: "publish-initial",
		Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "data-1", ExpectedEpoch: 1,
			Members: []GroupMember{{RaftID: 1, Role: GroupMemberVoter}}}}); result.Error != "" {
		t.Fatal(result.Error)
	}
	desired := []GroupMember{{RaftID: 1, Role: GroupMemberVoter},
		{RaftID: 2, Role: GroupMemberVoter}, {RaftID: 3, Role: GroupMemberLearner}}
	if result := applyDirect(t, state, &index, Command{Version: FormatVersion, RequestID: "plan-1",
		Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "change-1", GroupID: "data-1",
			ExpectedEpoch: 2, Members: desired}}); result.Error != "" {
		t.Fatal(result.Error)
	}
	if group, _, _ := state.Group("data-1"); group.Epoch != 2 || len(group.Members) != 1 {
		t.Fatalf("plan prematurely changed Group = %+v", group)
	}
	if pending, err := state.PendingGroupChanges(); err != nil || len(pending) != 1 || pending[0].ChangeID != "change-1" {
		t.Fatalf("pending changes = %+v, %v", pending, err)
	}
	if result := applyDirect(t, state, &index, Command{Version: FormatVersion, RequestID: "publish-bypass",
		Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "data-1", ExpectedEpoch: 2,
			Members: desired}}); result.Error == "" {
		t.Fatal("direct membership publication bypassed an active task")
	}
	if result := applyDirect(t, state, &index, Command{Version: FormatVersion, RequestID: "plan-2",
		Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "change-2", GroupID: "data-1",
			ExpectedEpoch: 2, Members: desired}}); result.Error == "" {
		t.Fatal("second active membership task was accepted")
	}
	if result := applyDirect(t, state, &index, Command{Version: FormatVersion, RequestID: "complete-1",
		Type:            CompleteGroupMembers,
		GroupCompletion: &GroupChangeCompletion{ChangeID: "change-1", GroupID: "data-1"}}); result.Error != "" {
		t.Fatal(result.Error)
	}
	group, found, err := state.Group("data-1")
	if err != nil || !found || group.Epoch != 3 || len(group.Members) != len(desired) {
		t.Fatalf("completed Group = %+v, %v, %v", group, found, err)
	}
	change, found, err := state.GroupChange("change-1")
	if err != nil || !found || change.State != GroupChangeCompleted || change.CompletedEpoch != 3 {
		t.Fatalf("completed change = %+v, %v, %v", change, found, err)
	}
	if pending, err := state.PendingGroupChanges(); err != nil || len(pending) != 0 {
		t.Fatalf("pending changes after completion = %+v, %v", pending, err)
	}
	if result := applyDirect(t, state, &index, Command{Version: FormatVersion, RequestID: "plan-demotion",
		Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "demote-1", GroupID: "data-1",
			ExpectedEpoch: 3, Members: []GroupMember{{RaftID: 1, Role: GroupMemberLearner},
				{RaftID: 2, Role: GroupMemberVoter}, {RaftID: 3, Role: GroupMemberLearner}}}}); result.Error == "" {
		t.Fatal("in-place Voter demotion was accepted")
	}
}
