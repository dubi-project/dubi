package noderemoval

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"go.etcd.io/raft/v3/raftpb"
)

func TestBuildGeneratesDeterministicMaintenancePlans(t *testing.T) {
	root := openFixture(t, []uint64{1, 2, 3})
	plan, err := Build(root, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.RootMember || len(plan.Groups) != 1 || plan.Groups[0].GroupID != "data-1" {
		t.Fatal("unexpected removal plan", plan)
	}
	change := plan.Groups[0].Plan
	if !change.Maintenance || change.ExpectedEpoch != 2 || len(change.Members) != 2 || change.ChangeID == "" {
		t.Fatal("invalid maintenance plan", change)
	}
	retry, err := Build(root, 3)
	if err != nil || retry.Groups[0].Plan.ChangeID != change.ChangeID {
		t.Fatal("plan was not deterministic", retry, err)
	}
}

func TestBuildRejectsPendingWorkAndMajorityLoss(t *testing.T) {
	root := openFixture(t, []uint64{1, 2, 3})
	apply(t, root, rootstate.Command{Type: rootstate.PlanGroupMembers, GroupPlan: &rootstate.GroupChangePlan{
		ChangeID: "pending-remove", GroupID: "data-1", ExpectedEpoch: 2,
		Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}},
	}})
	if _, err := Build(root, 3); !errors.Is(err, ErrRemovalHasPendingWork) {
		t.Fatal("pending removal was not rejected", err)
	}

	two := openFixture(t, []uint64{1, 2})
	if _, err := Build(two, 2); !errors.Is(err, ErrRemovalWouldLoseMajority) {
		t.Fatal("two-voter removal was not rejected", err)
	}
}

func TestBuildRequiresExplicitRebuildForLastRegionVoter(t *testing.T) {
	root := openFixtureWithRoot(t, []uint64{1}, []uint64{1, 2})
	if err := root.ApplyMembership(root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{2, 3}, Learners: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := Build(root, 1); !errors.Is(err, ErrRegionGroupNeedsRebuild) {
		t.Fatalf("last voter removal did not fail with rebuild guidance: %v", err)
	}
}

func openFixture(t *testing.T, members []uint64) *rootstate.State {
	return openFixtureWithRoot(t, members, members)
}

func openFixtureWithRoot(t *testing.T, members, rootMembers []uint64) *rootstate.State {
	t.Helper()
	root, err := rootstate.Open(filepath.Join(t.TempDir(), "root"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	index := uint64(0)
	for id := uint64(1); id <= 3; id++ {
		applyAt(t, root, &index, rootstate.Command{Type: rootstate.RegisterNode, Node: &rootstate.Node{
			NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: "west", Address: fmt.Sprintf("n%d:7443", id),
		}})
	}
	applyAt(t, root, &index, rootstate.Command{Type: rootstate.RegisterGroup, Group: &rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateCapability}})
	groupMembers := make([]rootstate.GroupMember, 0, len(members))
	for _, id := range members {
		groupMembers = append(groupMembers, rootstate.GroupMember{RaftID: id, Role: rootstate.GroupMemberVoter})
	}
	applyAt(t, root, &index, rootstate.Command{Type: rootstate.PublishGroupMembers, Membership: &rootstate.GroupMembership{GroupID: "data-1", ExpectedEpoch: 1, Members: groupMembers}})
	if err := root.ApplyMembership(index+1, &raftpb.ConfState{Voters: rootMembers}); err != nil {
		t.Fatal(err)
	}
	return root
}

func apply(t *testing.T, root *rootstate.State, command rootstate.Command) {
	t.Helper()
	index := root.AppliedIndex()
	applyAt(t, root, &index, command)
}

func applyAt(t *testing.T, root *rootstate.State, index *uint64, command rootstate.Command) {
	t.Helper()
	command.Version = rootstate.FormatVersion
	if command.RequestID == "" {
		command.RequestID = fmt.Sprintf("test-%d", *index+1)
	}
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	*index++
	if err := root.Apply(*index, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := root.Result(command.RequestID)
	if err != nil || !found || result.Error != "" {
		t.Fatal(result, found, err)
	}
}
