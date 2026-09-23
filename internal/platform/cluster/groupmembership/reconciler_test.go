package groupmembership

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type fakeHost struct {
	root               *rootstate.State
	rootIndex          uint64
	conf               *raftpb.ConfState
	local              uint64
	leader             uint64
	operations         []string
	failTargetReadOnce bool
}

func (host *fakeHost) Submit(groupID string, command []byte) error {
	if groupID != raft.RootGroupID {
		return errors.New("unexpected data command")
	}
	host.rootIndex++
	return host.root.Apply(host.rootIndex, command)
}
func (host *fakeHost) ReadIndex(_ context.Context, groupID string) (uint64, error) {
	if groupID != raft.RootGroupID && host.failTargetReadOnce {
		host.failTargetReadOnce = false
		return 0, errors.New("injected confirmation loss")
	}
	return host.rootIndex, nil
}
func (host *fakeHost) Membership(_ context.Context, groupID string) (*raftpb.ConfState, error) {
	if groupID != raft.RootGroupID && host.failTargetReadOnce && len(host.operations) != 0 {
		host.failTargetReadOnce = false
		return nil, errors.New("injected confirmation loss")
	}
	return proto.Clone(host.conf).(*raftpb.ConfState), nil
}
func (host *fakeHost) PrepareJoining(_ context.Context, _ string, raftID uint64) error {
	host.operations = append(host.operations, fmt.Sprintf("prepare:%d", raftID))
	return nil
}
func (host *fakeHost) Status(context.Context, string) (raft.GroupStatus, error) {
	return raft.GroupStatus{RaftID: host.local, LeaderID: host.leader}, nil
}
func (host *fakeHost) AddLearner(_ context.Context, _ string, raftID uint64) error {
	host.operations = append(host.operations, fmt.Sprintf("add:%d", raftID))
	host.conf.Learners = append(host.conf.Learners, raftID)
	return nil
}
func (host *fakeHost) PromoteLearner(_ context.Context, _ string, raftID uint64) error {
	host.operations = append(host.operations, fmt.Sprintf("promote:%d", raftID))
	host.conf.Learners = removeID(host.conf.Learners, raftID)
	host.conf.Voters = append(host.conf.Voters, raftID)
	return nil
}
func (host *fakeHost) RemoveMember(_ context.Context, _ string, raftID uint64) error {
	host.operations = append(host.operations, fmt.Sprintf("remove:%d", raftID))
	host.conf.Voters = removeID(host.conf.Voters, raftID)
	host.conf.Learners = removeID(host.conf.Learners, raftID)
	return nil
}
func (host *fakeHost) TransferLeadership(_ context.Context, _ string, target uint64) (raft.GroupStatus, error) {
	host.operations = append(host.operations, fmt.Sprintf("transfer:%d", target))
	host.leader = target
	return raft.GroupStatus{RaftID: host.local, LeaderID: target}, nil
}

func TestReconcileAddsPromotesRemovesThenPublishes(t *testing.T) {
	state, host := openRootForReconcile(t, []rootstate.GroupMember{
		{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}})
	reconciler, err := New(host, host, state)
	if err != nil {
		t.Fatal(err)
	}
	desired := []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter},
		{RaftID: 3, Role: rootstate.GroupMemberVoter}}
	plan := rootstate.GroupChangePlan{ChangeID: "replace-2-with-3", GroupID: "data-1",
		ExpectedEpoch: 2, Members: desired}
	if err := reconciler.Plan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Reconcile(context.Background(), plan.ChangeID); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(host.operations, []string{"prepare:3", "add:3", "promote:3", "remove:2"}) {
		t.Fatalf("membership operations = %v", host.operations)
	}
	group, found, err := state.Group("data-1")
	if err != nil || !found || group.Epoch != 3 || !slices.Equal(group.Members, desired) {
		t.Fatalf("published Group = %+v, %v, %v", group, found, err)
	}
	change, found, err := state.GroupChange(plan.ChangeID)
	if err != nil || !found || change.State != rootstate.GroupChangeCompleted {
		t.Fatalf("completed task = %+v, %v, %v", change, found, err)
	}
	before := slices.Clone(host.operations)
	if err := reconciler.Reconcile(context.Background(), plan.ChangeID); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(host.operations, before) {
		t.Fatalf("completed reconciliation repeated operations: %v", host.operations)
	}
}

func TestReconcileTransfersBeforeRemovingLocalLeader(t *testing.T) {
	state, host := openRootForReconcile(t, []rootstate.GroupMember{
		{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}})
	reconciler, _ := New(host, host, state)
	plan := rootstate.GroupChangePlan{ChangeID: "remove-leader", GroupID: "data-1", ExpectedEpoch: 2,
		Members: []rootstate.GroupMember{{RaftID: 2, Role: rootstate.GroupMemberVoter}}}
	if err := reconciler.Plan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Reconcile(context.Background(), plan.ChangeID); !errors.Is(err, ErrLeadershipTransferred) {
		t.Fatalf("Leader removal result = %v", err)
	}
	if !slices.Equal(host.operations, []string{"transfer:2"}) {
		t.Fatalf("Leader removal operations = %v", host.operations)
	}
	change, _, _ := state.GroupChange(plan.ChangeID)
	if change.State != rootstate.GroupChangePending {
		t.Fatalf("task after leadership transfer = %+v", change)
	}
}

func TestReconcileResumesFromAppliedConfStateAfterConfirmationLoss(t *testing.T) {
	state, host := openRootForReconcile(t, []rootstate.GroupMember{
		{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}})
	reconciler, _ := New(host, host, state)
	plan := rootstate.GroupChangePlan{ChangeID: "resume-replacement", GroupID: "data-1", ExpectedEpoch: 2,
		Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter},
			{RaftID: 3, Role: rootstate.GroupMemberVoter}}}
	if err := reconciler.Plan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	host.failTargetReadOnce = true
	if err := reconciler.Reconcile(context.Background(), plan.ChangeID); err == nil {
		t.Fatal("injected confirmation loss did not interrupt reconciliation")
	}
	if !slices.Equal(host.operations, []string{"prepare:3", "add:3"}) {
		t.Fatalf("operations before restart = %v", host.operations)
	}
	restarted, _ := New(host, host, state)
	if err := restarted.Reconcile(context.Background(), plan.ChangeID); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(host.operations, []string{"prepare:3", "add:3", "promote:3", "remove:2"}) {
		t.Fatalf("operations after restart = %v", host.operations)
	}
}

func openRootForReconcile(t *testing.T, current []rootstate.GroupMember) (*rootstate.State, *fakeHost) {
	t.Helper()
	state, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	host := &fakeHost{root: state, conf: &raftpb.ConfState{}, local: 1, leader: 1}
	for raftID := uint64(1); raftID <= 3; raftID++ {
		command := rootstate.Command{Version: rootstate.FormatVersion, RequestID: fmt.Sprintf("node-%d", raftID),
			Type: rootstate.RegisterNode, Node: &rootstate.Node{
				NodeID: fmt.Sprintf("node_%032x", raftID), RaftID: raftID, RegionID: "r1", Address: fmt.Sprintf("n%d:1", raftID)}}
		submitFakeRoot(t, host, command)
	}
	submitFakeRoot(t, host, rootstate.Command{Version: rootstate.FormatVersion, RequestID: "group-1",
		Type:  rootstate.RegisterGroup,
		Group: &rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateCapability}})
	submitFakeRoot(t, host, rootstate.Command{Version: rootstate.FormatVersion, RequestID: "members-1",
		Type: rootstate.PublishGroupMembers, Membership: &rootstate.GroupMembership{
			GroupID: "data-1", ExpectedEpoch: 1, Members: current}})
	for _, member := range current {
		if member.Role == rootstate.GroupMemberVoter {
			host.conf.Voters = append(host.conf.Voters, member.RaftID)
		} else {
			host.conf.Learners = append(host.conf.Learners, member.RaftID)
		}
	}
	return state, host
}

func submitFakeRoot(t *testing.T, host *fakeHost, command rootstate.Command) {
	t.Helper()
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Submit(raft.RootGroupID, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := host.root.ResultForCommand(encoded)
	if err != nil || !found || result.Error != "" {
		t.Fatalf("Root command result = %+v, %v, %v", result, found, err)
	}
}

func removeID(ids []uint64, target uint64) []uint64 {
	result := ids[:0]
	for _, id := range ids {
		if id != target {
			result = append(result, id)
		}
	}
	return result
}
