package rootstate

import (
	"fmt"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestRegionGroupRetirementAndRebuildPersistIntent(t *testing.T) {
	s, root := cohortFixture(t)
	applyRootFileCommand(t, s, cohortPlan(0, 1, 2, 3))
	applyRootFileCommand(t, s, cohortConfirm(1, 2, 100000))

	retire := Command{Type: RetireRegionGroup, GroupRetirement: &RegionGroupRetirement{
		GroupID: "regional-west", RegionID: "west", Generation: 1, GroupEpoch: 2, FenceIndex: 100000, Nodes: []uint64{1, 2, 3},
	}}
	applyRootFileCommand(t, s, retire)
	stored, found, err := s.RegionGroupRetirement("regional-west")
	if err != nil || !found || stored.RetiredAt == 0 || !slices.Equal(stored.Nodes, []uint64{1, 2, 3}) {
		t.Fatalf("retirement = %+v, %v, %v", stored, found, err)
	}
	// A different request carrying the same immutable cut is harmless.
	applyRootFileCommand(t, s, retire)
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{4}, Learners: []uint64{5, 6, 7, 8}}); err != nil {
		t.Fatal(err)
	}

	rebuild := Command{Type: PlanRegionGroupRebuild, GroupRebuild: &RegionGroupRebuild{
		GroupID: "regional-west", RegionID: "west", PreviousGeneration: 1, PreviousGroupEpoch: 2,
		PreviousFenceIndex: 100000, PreviousNodes: []uint64{1, 2, 3}, TargetGeneration: 2, TargetNodes: []uint64{4, 5},
	}}
	applyRootFileCommand(t, s, rebuild)
	planned, found, err := s.RegionGroupRebuild("regional-west")
	if err != nil || !found || planned.PlannedAt == 0 || !slices.Equal(planned.TargetNodes, []uint64{4, 5}) {
		t.Fatalf("rebuild = %+v, %v, %v", planned, found, err)
	}
	group, found, err := s.Group("regional-west")
	if err != nil || !found || group.Epoch != 1 || len(group.Members) != 0 {
		t.Fatalf("fresh Group cut = %+v, %v, %v", group, found, err)
	}
	applyRootFileCommand(t, s, Command{Type: PlanRegionCohort, RegionPlan: &RegionCohortPlan{RegionID: "west", GroupID: "regional-west", ExpectedGeneration: 1, Nodes: []uint64{4, 5}}})
	cohort, found, err := s.RegionCohort("west", 2)
	if err != nil || !found || !cohort.Reformation || cohort.Formation {
		t.Fatalf("same-name formation cohort = %+v, %v, %v", cohort, found, err)
	}
	applyRootFileCommand(t, s, Command{Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "regional-west", ExpectedEpoch: 1, Members: []GroupMember{{RaftID: 4, Role: GroupMemberVoter}, {RaftID: 5, Role: GroupMemberVoter}}}})
	if retired, err := s.RegionGroupIsRetired("regional-west"); err != nil || retired {
		t.Fatalf("fresh same-name Group remained retired = %v, %v", retired, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = rootFileState(t, root)
	planned, found, err = s.RegionGroupRebuild("regional-west")
	if err != nil || !found || planned.PlannedAt == 0 {
		t.Fatalf("reopened rebuild = %+v, %v, %v", planned, found, err)
	}
}

func TestRegionGroupRebuildRejectsUnsafeIntent(t *testing.T) {
	s, _ := cohortFixture(t)
	applyRootFileCommand(t, s, cohortPlan(0, 1, 2, 3))
	applyRootFileCommand(t, s, cohortConfirm(1, 2, 100000))
	bad := Command{Type: PlanRegionGroupRebuild, GroupRebuild: &RegionGroupRebuild{
		GroupID: "regional-west", RegionID: "west", PreviousGeneration: 1, PreviousGroupEpoch: 2,
		PreviousFenceIndex: 100000, PreviousNodes: []uint64{1, 2, 3}, TargetGeneration: 2, TargetNodes: []uint64{3},
	}}
	if err := bad.GroupRebuild.validateInput(); err == nil {
		t.Fatal("overlapping rebuild unexpectedly accepted")
	}
	unknown := Command{Type: PlanRegionGroupRebuild, GroupRebuild: &RegionGroupRebuild{
		GroupID: "regional-west", RegionID: "west", PreviousGeneration: 1, PreviousGroupEpoch: 2,
		PreviousFenceIndex: 100000, PreviousNodes: []uint64{1, 2, 3}, TargetGeneration: 2, TargetNodes: []uint64{9},
	}}
	unknown.Version, unknown.RequestID = FormatVersion, fmt.Sprintf("unknown-%d", s.AppliedIndex()+1)
	index := s.AppliedIndex()
	result := applyDirect(t, s, &index, unknown)
	if result.Error == "" {
		t.Fatal("unknown target unexpectedly accepted")
	}
}

func TestRetiredRegionGroupStopsBlockingNodeFence(t *testing.T) {
	s, _ := cohortFixture(t)
	applyRootFileCommand(t, s, cohortPlan(0, 1, 2, 3))
	applyRootFileCommand(t, s, cohortConfirm(1, 2, 100000))
	before, err := s.NodeResponsibilities(1)
	if err != nil || !slices.Contains(before.CurrentGroups, "regional-west") {
		t.Fatalf("live Region responsibility = %+v, %v", before, err)
	}
	applyRootFileCommand(t, s, Command{Type: RetireRegionGroup, GroupRetirement: &RegionGroupRetirement{
		GroupID: "regional-west", RegionID: "west", Generation: 1, GroupEpoch: 2, FenceIndex: 100000, Nodes: []uint64{1, 2, 3},
	}})
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{2}, Learners: []uint64{3}}); err != nil {
		t.Fatal(err)
	}
	after, err := s.NodeResponsibilities(1)
	if err != nil || len(after.CurrentGroups) != 0 || after.RootMember {
		t.Fatalf("retired Region still blocks Root removal = %+v, %v", after, err)
	}
	applyRootFileCommand(t, s, Command{Type: FenceNode, NodeFence: &NodeFenceRequest{NodeID: "node_00000000000000000000000000000001", RaftID: 1}})
}
