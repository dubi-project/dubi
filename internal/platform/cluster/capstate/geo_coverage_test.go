package capstate

import (
	"context"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
)

func geoCoverageFixture(t *testing.T) (*State, router.Target, routedstate.GeoConfirmation) {
	t.Helper()
	state, target, local, _ := confirmationFixture(t)
	ref, found, err := readRecordReference(t, state, target)
	if err != nil || !found {
		t.Fatal(ref, found, err)
	}
	claim := routedstate.GeoConfirmation{
		Target: target, Key: []byte("item"), ReferenceRequestID: ref.RequestID,
		ReferenceIndex: ref.Index, ReferenceCommandHash: ref.CommandHash, RecordID: local.RecordID,
		Regions: []placement.RegionHoldings{
			{RegionID: "east", Generation: 1, GroupEpoch: 2, FenceIndex: 8, Nodes: []uint64{4, 5, 6}, Holdings: []placement.Holding{
				{RaftID: 4, NodeID: "node_00000000000000000000000000000004", StoreID: "store_00000000000000000000000000000004", RecordID: local.RecordID},
				{RaftID: 5, NodeID: "node_00000000000000000000000000000005", StoreID: "store_00000000000000000000000000000005", RecordID: local.RecordID},
			}},
			{RegionID: "west", Generation: 1, GroupEpoch: 2, FenceIndex: 2, Nodes: []uint64{1, 2, 3}, Holdings: []placement.Holding{
				{RaftID: 1, NodeID: "node_00000000000000000000000000000001", StoreID: "store_00000000000000000000000000000001", RecordID: local.RecordID},
				{RaftID: 2, NodeID: "node_00000000000000000000000000000002", StoreID: "store_00000000000000000000000000000002", RecordID: local.RecordID},
			}},
		},
	}
	if err := claim.Validate(); err != nil {
		t.Fatal(err)
	}
	return state, target, claim
}

func readGeoCoverage(t *testing.T, state *State, target router.Target) (GeoCoverageRecord, bool, error) {
	t.Helper()
	var record GeoCoverageRecord
	var found bool
	_, err := state.ReadRouted(context.Background(), &fakeReadBarrier{index: state.AppliedIndex()}, target,
		func(view ReadView) error {
			var readErr error
			record, found, readErr = view.GeoCoverage([]byte("item"))
			return readErr
		})
	return record, found, err
}

func TestGeoCoverageIncludesSingleNodeRemoteRegion(t *testing.T) {
	state, target, claim := geoCoverageFixture(t)
	claim.Regions[0].Nodes = claim.Regions[0].Nodes[:1]
	claim.Regions[0].Holdings = claim.Regions[0].Holdings[:1]
	if err := claim.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := state.ApplyGeoConfirmation(state.AppliedIndex()+1, "small-region-geo", claim); err != nil {
		t.Fatal(err)
	}
	result, found, err := state.ResultForGeoConfirmation("small-region-geo", claim)
	if err != nil || !found || !result.Applied || result.Error != "" {
		t.Fatalf("small Region durable confirmation = %+v, %v, %v", result, found, err)
	}
	got, found, err := readGeoCoverage(t, state, target)
	if err != nil || !found || len(got.Claim.Regions[0].Nodes) != 1 {
		t.Fatalf("small Region persisted coverage = %+v, %v, %v", got, found, err)
	}
	// The local-only envelope remains forbidden for small Regions.
	local := routedstate.LocalConfirmation{Target: target, Key: claim.Key,
		ReferenceRequestID: claim.ReferenceRequestID, ReferenceIndex: claim.ReferenceIndex,
		RecordID: claim.RecordID, RegionID: "east", Generation: 1, GroupEpoch: 2,
		FenceIndex: 8, Nodes: claim.Regions[0].Nodes, Holdings: claim.Regions[0].Holdings}
	if err := local.Validate(); err == nil {
		t.Fatal("single-node Geo evidence authorized local-only confirmation")
	}
}

func TestGeoCoveragePersistsWithReferenceAndSnapshot(t *testing.T) {
	state, target, claim := geoCoverageFixture(t)
	if err := state.ApplyGeoConfirmation(state.AppliedIndex()+1, "geo-confirm", claim); err != nil {
		t.Fatal(err)
	}
	record, found, err := readGeoCoverage(t, state, target)
	if err != nil || !found || record.Claim.ReferenceIndex != claim.ReferenceIndex {
		t.Fatal(record, found, err)
	}
	if result, found, err := state.ResultForGeoConfirmation("geo-confirm", claim); err != nil || !found || !result.Applied {
		t.Fatal(result, found, err)
	}
	encoded, err := routedstate.EncodeGeoConfirmation("geo-confirm", claim)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := routedstate.New(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Apply(state.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	replayed, found, err := readGeoCoverage(t, state, target)
	if err != nil || !found || replayed.Index != record.Index {
		t.Fatal(replayed, found, err)
	}
	image, index, conf, err := state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := Open(t.TempDir(), "data-1", referenceHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := restored.Restore(index, image, conf); err != nil {
		t.Fatal(err)
	}
	if got, found, err := readGeoCoverage(t, restored, target); err != nil || !found || got.Index != record.Index {
		t.Fatal(got, found, err)
	}
	artifacts, _ := fileArtifacts(t)
	prepared, err := state.PrepareSnapshotTo(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	streamed, err := prepared.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	streamRestored, err := Open(t.TempDir(), "data-1", referenceHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer streamRestored.Close()
	if err := streamRestored.RestoreFrom(context.Background(), artifacts, prepared.Index, streamed, prepared.Conf); err != nil {
		t.Fatal(err)
	}
	if got, found, err := readGeoCoverage(t, streamRestored, target); err != nil || !found || got.Index != record.Index {
		t.Fatal(got, found, err)
	}
}

func TestGeoCoverageRejectsStaleReferenceAndDeletesWithReference(t *testing.T) {
	state, target, claim := geoCoverageFixture(t)
	stale := claim
	stale.Regions = append([]placement.RegionHoldings(nil), claim.Regions...)
	stale.Regions[1].Generation++
	if err := state.ApplyGeoConfirmation(state.AppliedIndex()+1, "geo-stale", stale); err != nil {
		t.Fatal(err)
	}
	if result, found, err := state.ResultForGeoConfirmation("geo-stale", stale); err != nil || !found || result.Applied {
		t.Fatal(result, found, err)
	}
	if err := state.ApplyGeoConfirmation(state.AppliedIndex()+1, "geo-confirm", claim); err != nil {
		t.Fatal(err)
	}
	if _, found, err := readGeoCoverage(t, state, target); err != nil || !found {
		t.Fatal(found, err)
	}
	if err := state.ApplyRouted(state.AppliedIndex()+1, "unlink-geo", target, []byte("unlink")); err != nil {
		t.Fatal(err)
	}
	if _, found, err := readGeoCoverage(t, state, target); err != nil || found {
		t.Fatal("reference deletion retained geo coverage", found, err)
	}
	if result, found, err := state.ResultForGeoConfirmation("geo-confirm", claim); err != nil || !found || !result.Applied {
		t.Fatal("geo request result unexpectedly changed", result, found, err)
	}
}

func TestGeoMetadataMirrorPersistsAndRestores(t *testing.T) {
	state, target, claim := geoCoverageFixture(t)
	ref, found, err := readRecordReference(t, state, target)
	if err != nil || !found {
		t.Fatal(ref, found, err)
	}
	mirror := routedstate.GeoMetadataMirror{Target: target, Key: []byte("item"), DestinationGroupID: "data-1", RegionID: "west", Generation: 1,
		GroupEpoch: 2, FenceIndex: 2, RecordID: claim.RecordID, ReferenceRequestID: ref.RequestID,
		ReferenceIndex: ref.Index, ReferenceCommandHash: ref.CommandHash, Descriptor: ref.Descriptor}
	if err := mirror.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := routedstate.EncodeGeoMetadataMirror("mirror-geo", mirror)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := routedstate.New(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Apply(state.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	var got GeoMetadataMirrorRecord
	var gotFound bool
	_, err = state.ReadRouted(context.Background(), &fakeReadBarrier{index: state.AppliedIndex()}, target,
		func(view ReadView) error {
			var readErr error
			got, gotFound, readErr = view.GeoMetadata([]byte("item"))
			return readErr
		})
	if err != nil || !gotFound || got.Mirror.ReferenceIndex != mirror.ReferenceIndex {
		t.Fatal(got, gotFound, err)
	}
	image, index, conf, err := state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := Open(t.TempDir(), "data-1", referenceHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := restored.Restore(index, image, conf); err != nil {
		t.Fatal(err)
	}
	_, err = restored.ReadRouted(context.Background(), &fakeReadBarrier{index: restored.AppliedIndex()}, target,
		func(view ReadView) error {
			got, gotFound, err = view.GeoMetadata([]byte("item"))
			return err
		})
	if err != nil || !gotFound || got.Mirror.RecordID != mirror.RecordID {
		t.Fatal(got, gotFound, err)
	}
}

func TestGeoMetadataMirrorRejectsWrongLocalFence(t *testing.T) {
	state, target, claim := geoCoverageFixture(t)
	ref, _, _ := readRecordReference(t, state, target)
	mirror := routedstate.GeoMetadataMirror{Target: target, Key: []byte("item"), DestinationGroupID: "data-1", RegionID: "east", Generation: 1,
		GroupEpoch: 2, FenceIndex: 8, RecordID: claim.RecordID, ReferenceRequestID: ref.RequestID,
		ReferenceIndex: ref.Index, ReferenceCommandHash: ref.CommandHash, Descriptor: ref.Descriptor}
	if err := state.ApplyGeoMetadataMirror(state.AppliedIndex()+1, "mirror-wrong-region", mirror); err != nil {
		t.Fatal(err)
	}
	result, found, err := state.ResultForGeoMetadataMirror("mirror-wrong-region", mirror)
	if err != nil || !found || result.Applied {
		t.Fatal(result, found, err)
	}
}
