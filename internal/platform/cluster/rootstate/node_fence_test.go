package rootstate

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func fenceTestNode(id uint64) *Node {
	return &Node{NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: "local", Address: fmt.Sprintf("n%d:7443", id)}
}

func applyFenceAttempt(t *testing.T, s *State, id uint64) string {
	t.Helper()
	command := Command{Version: FormatVersion, RequestID: fmt.Sprintf("fence-at-%d", s.AppliedIndex()+1), Type: FenceNode,
		NodeFence: &NodeFenceRequest{NodeID: fenceTestNode(id).NodeID, RaftID: id}}
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(s.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := s.Result(command.RequestID)
	if err != nil || !found {
		t.Fatal(result, found, err)
	}
	return result.Error
}

func TestNodeFenceOnlyBeforeClusterResponsibility(t *testing.T) {
	s := rootFileState(t, t.TempDir())
	for id := uint64(1); id <= 4; id++ {
		applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: fenceTestNode(id)})
	}
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	if responsibilities, err := s.NodeResponsibilities(1); err != nil || !responsibilities.RootMember || !slices.Equal(responsibilities.CurrentGroups, nil) {
		t.Fatal("Root responsibility view is wrong", responsibilities, err)
	}
	if got := applyFenceAttempt(t, s, 1); !strings.Contains(got, "Root member") {
		t.Fatal("current Root member was fenced", got)
	}
	applyRootFileCommand(t, s, Command{Type: RegisterGroup, Group: &Group{GroupID: "data", StateKind: GroupStateCapability}})
	applyRootFileCommand(t, s, Command{Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "data", ExpectedEpoch: 1,
		Members: []GroupMember{{RaftID: 2, Role: GroupMemberVoter}}}})
	if got := applyFenceAttempt(t, s, 2); !strings.Contains(got, "responsibility") {
		t.Fatal("current data Group member was fenced", got)
	}
	if responsibilities, err := s.NodeResponsibilities(2); err != nil || responsibilities.RootMember || !slices.Equal(responsibilities.CurrentGroups, []string{"data"}) {
		t.Fatal("current Group responsibility view is wrong", responsibilities, err)
	}
	applyRootFileCommand(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "add-four", GroupID: "data", ExpectedEpoch: 2,
		Members: []GroupMember{{RaftID: 2, Role: GroupMemberVoter}, {RaftID: 4, Role: GroupMemberVoter}}}})
	if got := applyFenceAttempt(t, s, 4); !strings.Contains(got, "responsibility") {
		t.Fatal("pending Group member was fenced", got)
	}
	if responsibilities, err := s.NodeResponsibilities(4); err != nil || !slices.Equal(responsibilities.PendingGroups, []string{"data"}) {
		t.Fatal("pending Group responsibility view is wrong", responsibilities, err)
	}
	if got := applyFenceAttempt(t, s, 3); got != "" {
		t.Fatal("unused registered Node was not fenced", got)
	}
	if responsibilities, err := s.NodeResponsibilities(3); err != nil || !responsibilities.Empty() {
		t.Fatal("unused Node responsibility view is not empty", responsibilities, err)
	}
	fence, found, err := s.NodeFence(3)
	if err != nil || !found || fence.NodeID != fenceTestNode(3).NodeID || fence.FencedIndex == 0 {
		t.Fatal("durable fence missing", fence, found, err)
	}
	if got := applyFenceAttempt(t, s, 3); got != "" {
		t.Fatal("same identity fence retry was not idempotent", got)
	}
	if _, _, _, err := s.Snapshot(); err != nil {
		t.Fatal("fence did not survive Root snapshot validation", err)
	}
	command := Command{Version: FormatVersion, RequestID: "fenced-group-member", Type: PlanGroupMembers,
		GroupPlan: &GroupChangePlan{ChangeID: "fenced-add", GroupID: "data", ExpectedEpoch: 2,
			Members: []GroupMember{{RaftID: 2, Role: GroupMemberVoter}, {RaftID: 3, Role: GroupMemberVoter}}}}
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	// Finish the previous plan first so the rejection comes from the fence,
	// not the ordinary active-task exclusion.
	applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{GroupID: "data", ChangeID: "add-four"}})
	command.GroupPlan.ExpectedEpoch = 3
	encoded, err = EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(s.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := s.Result(command.RequestID)
	if err != nil || !found || !strings.Contains(result.Error, "fenced") {
		t.Fatal("fenced Node was admitted to data Group", result, err)
	}
}
