package noderemoval

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"go.etcd.io/raft/v3/raftpb"
)

type fakeExecutor struct {
	root          *rootstate.State
	failReconcile bool
	planned       int
	reconciled    int
	removedRoot   int
	fenced        int
}

func (f *fakeExecutor) ConfirmRoot(context.Context) error { return nil }

func (f *fakeExecutor) CheckGroup(context.Context, string) error { return nil }

func (f *fakeExecutor) PlanGroup(_ context.Context, plan rootstate.GroupChangePlan) error {
	f.planned++
	return applyCommand(f.root, rootstate.Command{RequestID: "test-plan-" + plan.ChangeID, Type: rootstate.PlanGroupMembers, GroupPlan: &plan})
}

func (f *fakeExecutor) ReconcileGroup(_ context.Context, changeID string) error {
	f.reconciled++
	if f.failReconcile {
		f.failReconcile = false
		return errors.New("injected Group reconciliation interruption")
	}
	change, found, err := f.root.GroupChange(changeID)
	if err != nil || !found {
		return fmt.Errorf("load Group change: %v", err)
	}
	return applyCommand(f.root, rootstate.Command{RequestID: "test-complete-" + changeID, Type: rootstate.CompleteGroupMembers,
		GroupCompletion: &rootstate.GroupChangeCompletion{ChangeID: changeID, GroupID: change.GroupID}})
}

func (f *fakeExecutor) RetireRegionGroup(_ context.Context, groupID string) error {
	progress, found, err := f.root.RegionProgress(groupID)
	if err != nil || !found {
		return fmt.Errorf("load Region progress: %v", err)
	}
	return applyCommand(f.root, rootstate.Command{RequestID: "test-retire-" + groupID, Type: rootstate.RetireRegionGroup,
		GroupRetirement: &rootstate.RegionGroupRetirement{GroupID: groupID, RegionID: progress.Current.RegionID,
			Generation: progress.Current.Generation, GroupEpoch: progress.Current.GroupEpoch,
			FenceIndex: progress.Current.GroupFenceIndex, Nodes: append([]uint64(nil), progress.Current.Nodes...)}})
}

func (f *fakeExecutor) RemoveRootMember(_ context.Context, raftID uint64) error {
	f.removedRoot++
	conf, err := f.root.RootMembership()
	if err != nil {
		return err
	}
	filtered := make([]uint64, 0, len(conf.GetVoters()))
	for _, id := range conf.GetVoters() {
		if id != raftID {
			filtered = append(filtered, id)
		}
	}
	return f.root.ApplyMembership(f.root.AppliedIndex()+1, &raftpb.ConfState{Voters: filtered})
}

func (f *fakeExecutor) FenceNode(_ context.Context, request rootstate.NodeFenceRequest) error {
	f.fenced++
	return applyCommand(f.root, rootstate.Command{RequestID: "test-fence-" + fmt.Sprint(request.RaftID), Type: rootstate.FenceNode, NodeFence: &request})
}

func TestCoordinatorResumesFromPersistedGroupPlan(t *testing.T) {
	root := openFixture(t, []uint64{1, 2, 3})
	executor := &fakeExecutor{root: root, failReconcile: true}
	coordinator, err := NewCoordinator(root, executor)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := coordinator.Reconcile(ctx, 3); err == nil {
		t.Fatal("interrupted reconciliation was reported successful")
	}
	if _, err := coordinator.Reconcile(ctx, 3); err != nil {
		t.Fatal("persisted Group plan did not resume", err)
	}
	if executor.planned != 2 || executor.reconciled != 2 || executor.removedRoot != 1 || executor.fenced != 1 {
		t.Fatal("unexpected coordinator actions", executor)
	}
	group, found, err := root.Group("data-1")
	if err != nil || !found || len(group.Members) != 2 || group.Members[0].RaftID == 3 || group.Members[1].RaftID == 3 {
		t.Fatal("Group member was not removed", group, found, err)
	}
	if _, member, err := root.RootMember(3); err != nil || member {
		t.Fatal("Root member was not removed", member, err)
	}
	if fence, found, err := root.NodeFence(3); err != nil || !found || fence.NodeID != "node_00000000000000000000000000000003" {
		t.Fatal("old identity was not fenced", fence, found, err)
	}
}

func TestCoordinatorRetiresSingleNodeRegionBeforeRootRemoval(t *testing.T) {
	root := openFixtureWithRoot(t, []uint64{2}, []uint64{1, 2, 3})
	groupID := regiongroups.GroupID("west")
	if err := applyCommand(root, rootstate.Command{RequestID: "region-register", Type: rootstate.RegisterGroup,
		Group: &rootstate.Group{GroupID: groupID, StateKind: rootstate.GroupStateCapability}}); err != nil {
		t.Fatal(err)
	}
	if err := applyCommand(root, rootstate.Command{RequestID: "region-plan", Type: rootstate.PlanRegionCohort,
		RegionPlan: &rootstate.RegionCohortPlan{RegionID: "west", GroupID: groupID, Nodes: []uint64{1}}}); err != nil {
		t.Fatal(err)
	}
	if err := applyCommand(root, rootstate.Command{RequestID: "region-members", Type: rootstate.PublishGroupMembers,
		Membership: &rootstate.GroupMembership{GroupID: groupID, ExpectedEpoch: 1,
			Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}}}}); err != nil {
		t.Fatal(err)
	}
	if err := applyCommand(root, rootstate.Command{RequestID: "region-confirm", Type: rootstate.ConfirmRegionCohort,
		RegionConfirmation: &rootstate.RegionCohortConfirmation{RegionID: "west", Generation: 1, GroupID: groupID,
			GroupEpoch: 2, GroupFenceRequestID: "fence-single-node", GroupFenceIndex: 4}}); err != nil {
		t.Fatal(err)
	}
	executor := &fakeExecutor{root: root}
	coordinator, err := NewCoordinator(root, executor)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := coordinator.Reconcile(context.Background(), 1)
	if err != nil || len(plan.Groups) != 0 || plan.RootMember || !plan.AlreadyFenced {
		t.Fatalf("single-node Region removal result = %+v, err=%v", plan, err)
	}
	if retired, found, err := root.RegionGroupRetirement(groupID); err != nil || !found || retired.GroupID != groupID {
		t.Fatalf("Region retirement = %+v, found=%v, err=%v", retired, found, err)
	}
	if _, member, err := root.RootMember(1); err != nil || member {
		t.Fatalf("Root member remained after single-node removal: member=%v err=%v", member, err)
	}
}

func applyCommand(root *rootstate.State, command rootstate.Command) error {
	command.Version = rootstate.FormatVersion
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		return err
	}
	if err := root.Apply(root.AppliedIndex()+1, encoded); err != nil {
		return err
	}
	result, found, err := root.Result(command.RequestID)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("Root command result missing")
	}
	if result.Error != "" {
		return errors.New(result.Error)
	}
	return nil
}
