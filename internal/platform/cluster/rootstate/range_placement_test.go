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

func TestRangePlacementRefreshesSameRegionGeneration(t *testing.T) {
	state, _ := cohortFixture(t)
	applyRootFileCommand(t, state, cohortPlan(0, 1, 2, 3))
	applyRootFileCommand(t, state, cohortConfirm(1, 2, 100))
	applyRootFileCommand(t, state, Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: "tenant"}})
	applyRootFileCommand(t, state, Command{Type: CreateDataset, Dataset: &Dataset{TenantID: "tenant", EngineID: "record", DatasetID: "items", RangeID: "range-a", OwnerGroupID: "regional-west"}})
	applyRootFileCommand(t, state, Command{Type: ActivateRange, Activation: &RangeActivation{RangeID: "range-a", OwnerGroupID: "regional-west", Epoch: 1, OwnerRequestID: "owner-range-a", OwnerAppliedIndex: 1000}})

	initial := RangePlacement{RangeID: "range-a", RangeEpoch: 1, OwnerGroupID: "regional-west", Regions: []RangeRegionPlacement{{RegionID: "west", GroupID: "regional-west", Generation: 1}}}
	applyRootFileCommand(t, state, Command{Type: SetRangePlacement, RangePlacement: &initial})

	applyRootFileCommand(t, state, cohortPlan(1, 1, 2, 3, 4))
	cohortPublish(t, state, 2, []uint64{1, 2, 3, 4})
	applyRootFileCommand(t, state, cohortConfirm(2, 3, 1003))
	refreshed := initial
	refreshed.Regions = append([]RangeRegionPlacement(nil), initial.Regions...)
	refreshed.Regions[0].Generation = 2
	applyRootFileCommand(t, state, Command{Type: SetRangePlacement, RangePlacement: &refreshed})
	got, found, err := state.RangePlacement("range-a")
	if err != nil || !found || got.Regions[0].Generation != 2 {
		t.Fatalf("refreshed Range placement = %+v, found=%v, err=%v", got, found, err)
	}
	index := state.AppliedIndex()
	stale := applyDirect(t, state, &index, Command{Version: FormatVersion, RequestID: "stale-range-placement",
		Type: SetRangePlacement, RangePlacement: &initial})
	if stale.Error == "" {
		t.Fatal("historical Region generation was accepted after refresh")
	}
}
