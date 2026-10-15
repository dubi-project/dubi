package regiongroups

import (
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

func TestPlanRebuildRequiresFreshSortedNodes(t *testing.T) {
	progress := rootstate.RegionProgress{Eligible: true,
		Head: rootstate.RegionCohortHead{RegionID: "west", CurrentGeneration: 2},
		Current: rootstate.RegionCohort{RegionID: "west", GroupID: "regional-west", Generation: 2,
			Nodes: []uint64{1, 2}, State: rootstate.RegionCohortConfirmed, PlannedIndex: 8,
			ConfirmedIndex: 12, GroupEpoch: 4, GroupFenceRequestID: "fence-2", GroupFenceIndex: 11},
		Group: rootstate.Group{GroupID: "regional-west", Epoch: 4,
			Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}}}}
	target := []rootstate.Node{
		{RaftID: 7, NodeID: "node_00000000000000000000000000000007", RegionID: "west", Address: "w7:7443"},
	}
	plan, err := PlanRebuild(progress, target)
	if err != nil || plan.RequiredLocalVoters != 1 || plan.PreviousGeneration != 2 || plan.Validate() != nil {
		t.Fatalf("rebuild plan = %+v, err %v", plan, err)
	}
	bad := append([]rootstate.Node(nil), target...)
	bad[0].RaftID = 2
	if _, err := PlanRebuild(progress, bad); err == nil {
		t.Fatal("retired Node was accepted as a rebuild target")
	}
	bad = append([]rootstate.Node(nil), target...)
	bad[0].RegionID = "east"
	if _, err := PlanRebuild(progress, bad); err == nil {
		t.Fatal("cross-Region rebuild target was accepted")
	}
}
