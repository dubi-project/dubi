package placement

import (
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

func geoFixture() rootstate.RegionPlacementView {
	progress, nodes := localFixture()
	progress.AppliedIndex = 7
	second := progress
	second.Current.RegionID = "east"
	second.Current.GroupID = "region-east"
	second.Head.RegionID = "east"
	second.Group.GroupID = "region-east"
	second.Group.Members = append([]rootstate.GroupMember(nil), progress.Group.Members...)
	second.Current.Nodes = []uint64{4, 5, 6}
	second.Group.Members = []rootstate.GroupMember{
		{RaftID: 4, Role: rootstate.GroupMemberVoter},
		{RaftID: 5, Role: rootstate.GroupMemberVoter},
		{RaftID: 6, Role: rootstate.GroupMemberVoter},
	}
	nodesEast := []rootstate.Node{
		{RaftID: 4, NodeID: "node_00000000000000000000000000000004", RegionID: "east", Address: "e1:7443"},
		{RaftID: 5, NodeID: "node_00000000000000000000000000000005", RegionID: "east", Address: "e2:7443"},
		{RaftID: 6, NodeID: "node_00000000000000000000000000000006", RegionID: "east", Address: "e3:7443"},
	}
	return rootstate.RegionPlacementView{AppliedIndex: 7, Regions: []rootstate.RegionPlacement{
		{Progress: progress, Nodes: nodes},
		{Progress: second, Nodes: nodesEast},
	}}
}

func TestPlanGeoRequiresOneStableCutAndDistinctRegions(t *testing.T) {
	view := geoFixture()
	plan, err := PlanGeo(view)
	if err != nil || len(plan.Regions()) != 2 || plan.RequiredRegions() != 2 {
		t.Fatal(plan, err)
	}
	if !plan.Matches(plan) {
		t.Fatal("a plan did not match itself")
	}
	view.Regions[1].Progress.AppliedIndex++
	if _, err := PlanGeo(view); err == nil {
		t.Fatal("mixed Root cuts were accepted")
	}
	view = geoFixture()
	view.Regions[1].Progress.Current.RegionID = view.Regions[0].Progress.Current.RegionID
	if _, err := PlanGeo(view); err == nil {
		t.Fatal("duplicate Regions were accepted")
	}
}

func TestGeoPlanCountsRegionMajorityWithoutShrinkingDenominator(t *testing.T) {
	plan, err := PlanGeo(geoFixture())
	if err != nil {
		t.Fatal(err)
	}
	evidence := []RegionHoldings{
		{RegionID: "west", Generation: 1, GroupEpoch: 2, FenceIndex: 4, Nodes: []uint64{1, 2, 3}, Holdings: []Holding{
			{RaftID: 1, NodeID: "node_00000000000000000000000000000001", StoreID: "store-west-1", RecordID: "record"},
			{RaftID: 2, NodeID: "node_00000000000000000000000000000002", StoreID: "store-west-2", RecordID: "record"},
		}},
		{RegionID: "east", Generation: 1, GroupEpoch: 2, FenceIndex: 4, Nodes: []uint64{4, 5, 6}, Holdings: []Holding{
			{RaftID: 4, NodeID: "node_00000000000000000000000000000004", StoreID: "store-east-1", RecordID: "record"},
			{RaftID: 5, NodeID: "node_00000000000000000000000000000005", StoreID: "store-east-2", RecordID: "record"},
		}},
	}
	coverage, err := plan.Assess("record", evidence)
	if err != nil || !coverage.HasMajority() || len(coverage.CoveredRegions) != 2 {
		t.Fatal(coverage, err)
	}
	// One region remains required even when the other region is unavailable.
	coverage, err = plan.Assess("record", evidence[:1])
	if err != nil || coverage.HasMajority() || len(coverage.CoveredRegions) != 1 {
		t.Fatal(coverage, err)
	}
	bad := append([]RegionHoldings(nil), evidence...)
	bad[1].Holdings[0].StoreID = evidence[0].Holdings[0].StoreID
	if _, err := plan.Assess("record", bad); err == nil {
		t.Fatal("the same Store was counted in two Regions")
	}
	bad = append([]RegionHoldings(nil), evidence...)
	bad[1].Generation++
	if _, err := plan.Assess("record", bad); err == nil {
		t.Fatal("stale Region generation was accepted")
	}
}

func TestGeoPlanBindsCoverageToOnePublicationCut(t *testing.T) {
	plan, err := PlanGeo(geoFixture())
	if err != nil {
		t.Fatal(err)
	}
	recordID := "record_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	publication := PublicationCut{RecordID: recordID, RequestID: "publish-1", Index: 8,
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:     "range_00000000000000000000000000000001", RangeEpoch: 1, RoutingKey: []byte("key")}
	evidence := []RegionHoldings{
		{RegionID: "west", Generation: 1, GroupEpoch: 2, FenceIndex: 4, Nodes: []uint64{1, 2, 3}, Holdings: []Holding{
			{RaftID: 1, NodeID: "node_00000000000000000000000000000001", StoreID: "store-west-1", RecordID: recordID},
			{RaftID: 2, NodeID: "node_00000000000000000000000000000002", StoreID: "store-west-2", RecordID: recordID},
		}},
		{RegionID: "east", Generation: 1, GroupEpoch: 2, FenceIndex: 4, Nodes: []uint64{4, 5, 6}, Holdings: []Holding{
			{RaftID: 4, NodeID: "node_00000000000000000000000000000004", StoreID: "store-east-1", RecordID: recordID},
			{RaftID: 5, NodeID: "node_00000000000000000000000000000005", StoreID: "store-east-2", RecordID: recordID},
		}},
	}
	coverage, err := plan.AssessPublication(publication, evidence)
	if err != nil || !coverage.HasBoundPublication() || coverage.ReferenceRequestID != publication.RequestID || coverage.ReferenceIndex != publication.Index {
		t.Fatal(coverage, err)
	}
	publication.CommandHash = "not-a-digest"
	if _, err := plan.AssessPublication(publication, evidence); err == nil {
		t.Fatal("invalid publication cut was accepted")
	}
	publication.CommandHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	publication.RecordID = "record-not-a-digest"
	if _, err := plan.AssessPublication(publication, evidence); err == nil {
		t.Fatal("invalid RecordID was accepted")
	}
}

func TestGeoPlanIncludesSmallRegionForCrossRegionConfirmation(t *testing.T) {
	view := geoFixture()
	view.Regions[0].Progress.Current.Nodes = []uint64{1, 2}
	view.Regions[0].Progress.Group.Members = view.Regions[0].Progress.Group.Members[:2]
	view.Regions[0].Nodes = view.Regions[0].Nodes[:2]
	plan, err := PlanGeo(view)
	if err != nil || len(plan.Regions()) != 2 || plan.Regions()[0].Required != 2 {
		t.Fatalf("small Region was not retained in the geo plan: %+v, %v", plan, err)
	}
}
