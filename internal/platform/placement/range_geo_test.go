package placement

import (
	"context"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

type rangeGeoBarrier struct{ index uint64 }

func (b rangeGeoBarrier) ReadIndex(context.Context, string) (uint64, error) { return b.index, nil }

type rangeGeoRoot struct {
	placement rootstate.RangePlacement
	view      rootstate.RegionPlacementView
}

func (r rangeGeoRoot) RangeRegionPlacementView(string, int, int) (rootstate.RangePlacement, rootstate.RegionPlacementView, error) {
	return r.placement, r.view, nil
}

func TestReadRangeGeoPlanUsesRootCutAndDurableMapping(t *testing.T) {
	view := geoFixture()
	placement := rootstate.RangePlacement{RangeID: "range-a", RangeEpoch: 3, OwnerGroupID: "region-east", Regions: []rootstate.RangeRegionPlacement{
		{RegionID: "east", GroupID: "region-east", Generation: 1},
		{RegionID: "west", GroupID: "region-west", Generation: 1},
	}}
	view.AppliedIndex = 7
	got, plan, err := ReadRangeGeoPlan(context.Background(), rangeGeoBarrier{index: 7}, rangeGeoRoot{placement: placement, view: view}, "range-a", 4, 16)
	if err != nil || got.RangeID != placement.RangeID || plan.RequiredRegions() != 2 {
		t.Fatal(got, plan, err)
	}
	if _, _, err := ReadRangeGeoPlan(context.Background(), rangeGeoBarrier{index: 8}, rangeGeoRoot{placement: placement, view: view}, "range-a", 4, 16); err == nil {
		t.Fatal("stale Root placement cut accepted")
	}
	if _, _, err := ReadRangeGeoPlan(context.Background(), rangeGeoBarrier{index: 7}, rangeGeoRoot{placement: placement, view: view}, "bad/range", 4, 16); err == nil {
		t.Fatal("invalid Range accepted")
	}
}

func TestReadRangeGeoPlanAtCutRequiresAppliedConfirmedCut(t *testing.T) {
	view := geoFixture()
	view.AppliedIndex = 7
	placement := rootstate.RangePlacement{RangeID: "range-a", RangeEpoch: 3, OwnerGroupID: "region-east", Regions: []rootstate.RangeRegionPlacement{
		{RegionID: "east", GroupID: "region-east", Generation: 1},
		{RegionID: "west", GroupID: "region-west", Generation: 1},
	}}
	root := rangeGeoRoot{placement: placement, view: view}

	got, plan, err := ReadRangeGeoPlanAtCut(7, root, "range-a", 4, 16)
	if err != nil || got.RangeID != placement.RangeID || plan.RequiredRegions() != 2 {
		t.Fatal(got, plan, err)
	}
	if _, _, err := ReadRangeGeoPlanAtCut(8, root, "range-a", 4, 16); err == nil {
		t.Fatal("stale Root placement cut accepted")
	}
	if _, _, err := ReadRangeGeoPlanAtCut(0, root, "range-a", 4, 16); err == nil {
		t.Fatal("empty Root cut accepted")
	}
}
