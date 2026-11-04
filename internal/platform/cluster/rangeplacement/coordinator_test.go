package rangeplacement

import (
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

func progressFixture(region, group string, generation uint64) rootstate.RegionProgress {
	return rootstate.RegionProgress{Eligible: true, Current: rootstate.RegionCohort{RegionID: region, GroupID: group,
		Generation: generation, State: rootstate.RegionCohortConfirmed, Nodes: []uint64{1, 2, 3},
		PlannedIndex: 1, ConfirmedIndex: 2, GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 2},
		Group: rootstate.Group{GroupID: group, StateKind: rootstate.GroupStateEngine, Epoch: 2,
			Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}}
}

func TestPlanRangeUsesOnlyStableConfirmedRegions(t *testing.T) {
	r := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", EngineID: "record", DatasetID: "items",
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

func TestRefreshPlacementAdvancesOnlyTheCompleteExistingRegionSet(t *testing.T) {
	r := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", EngineID: "record", DatasetID: "items", OwnerGroupID: "region-east", Epoch: 3, State: rootstate.RangeStateActive}
	prior, err := PlanRange(r, []rootstate.RegionProgress{progressFixture("east", "region-east", 2), progressFixture("west", "region-west", 1)})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"advance", "new-region", "unchanged", "missing", "pending", "unconfirmed", "other-group", "backwards", "other-owner", "other-epoch"} {
		t.Run(mode, func(t *testing.T) {
			current := r
			views := []rootstate.RegionProgress{progressFixture("east", "region-east", 3), progressFixture("west", "region-west", 2)}
			pass := mode == "advance" || mode == "new-region" || mode == "unchanged"
			switch mode {
			case "new-region":
				views = append(views, progressFixture("north", "region-north", 1))
			case "unchanged":
				views[0].Current.Generation = 2
				views[1].Current.Generation = 1
			case "missing":
				views = views[:1]
			case "pending":
				views[1].Pending = &rootstate.RegionCohort{Generation: 3}
			case "unconfirmed":
				views[1].Current.State = rootstate.RegionCohortPlanned
			case "other-group":
				views[1] = progressFixture("west", "region-new-west", 2)
			case "backwards":
				views[0].Current.Generation = 1
			case "other-owner":
				current.OwnerGroupID = "region-west"
			case "other-epoch":
				current.Epoch++
			}
			updated, err := refreshPlacement(current, prior, views)
			if (err == nil) != pass {
				t.Fatal(updated, err)
			}
			if pass && (len(updated.Regions) != 2 || updated.RangeEpoch != prior.RangeEpoch || updated.OwnerGroupID != prior.OwnerGroupID || updated.Regions[0].RegionID != "east" || updated.Regions[1].RegionID != "west") {
				t.Fatal("changed placement identity", updated)
			}
		})
	}
}
