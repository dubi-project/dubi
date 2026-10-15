package rangeplacement

import (
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

func progressFixture(region, group string, generation uint64) rootstate.RegionProgress {
	return rootstate.RegionProgress{Eligible: true, Current: rootstate.RegionCohort{RegionID: region, GroupID: group,
		Generation: generation, State: rootstate.RegionCohortConfirmed, Nodes: []uint64{1, 2, 3},
		PlannedIndex: 1, ConfirmedIndex: 2, GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 2},
		Group: rootstate.Group{GroupID: group, StateKind: rootstate.GroupStateCapability, Epoch: 2,
			Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}}
}

func TestPlanRangeUsesOnlyStableConfirmedRegions(t *testing.T) {
	r := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items",
		OwnerGroupID: "region-east", Epoch: 3, State: rootstate.RangeStateActive}
	placement, err := PlanRange(r, []rootstate.RegionProgress{
		progressFixture("west", "region-west", 1), progressFixture("east", "region-east", 2),
	})
	if err != nil || len(placement.Regions) != 2 || placement.Regions[0].RegionID != "east" || placement.Regions[1].RegionID != "west" {
		t.Fatal(placement, err)
	}
	pending := progressFixture("north", "region-north", 1)
	pending.Pending = &rootstate.RegionCohort{Generation: 2}
	rWest := r
	rWest.OwnerGroupID = "region-west"
	placement, err = PlanRange(rWest, []rootstate.RegionProgress{progressFixture("west", "region-west", 1), pending})
	if err != nil || len(placement.Regions) != 1 {
		t.Fatal(placement, err)
	}
	if _, err := PlanRange(r, []rootstate.RegionProgress{progressFixture("west", "region-west", 1)}); err == nil {
		t.Fatal("Range with missing Owner cohort was accepted")
	}
}
