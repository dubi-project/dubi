package rootstate

import "testing"

func TestRangePlacementValidationRequiresSortedRegionsAndOwner(t *testing.T) {
	valid := RangePlacement{RangeID: "range-a", RangeEpoch: 3, OwnerGroupID: "region-east", Regions: []RangeRegionPlacement{
		{RegionID: "east", GroupID: "region-east", Generation: 2},
		{RegionID: "west", GroupID: "region-west", Generation: 1},
	}}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	unsorted := valid
	unsorted.Regions = []RangeRegionPlacement{valid.Regions[1], valid.Regions[0]}
	if err := unsorted.Validate(); err == nil {
		t.Fatal("unsorted Region placement accepted")
	}
	noOwner := valid
	noOwner.OwnerGroupID = "region-north"
	if err := noOwner.Validate(); err == nil {
		t.Fatal("placement without routing Owner accepted")
	}
	duplicate := valid
	duplicate.Regions = append([]RangeRegionPlacement(nil), valid.Regions...)
	duplicate.Regions[1].RegionID = duplicate.Regions[0].RegionID
	if err := duplicate.Validate(); err == nil {
		t.Fatal("duplicate Region placement accepted")
	}
}
