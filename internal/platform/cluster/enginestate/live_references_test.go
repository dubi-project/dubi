package enginestate

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

type discoveryReadBarrier struct {
	index uint64
	calls int
}

func (b *discoveryReadBarrier) ReadIndex(context.Context, string) (uint64, error) {
	b.calls++
	return b.index, nil
}

func TestGeoReferencePageKeepsReceiptsAndReferencesAtOneOwnerCut(t *testing.T) {
	state, target, claim := geoCoverageFixture(t)
	if err := state.ApplyGeoConfirmation(state.AppliedIndex()+1, "geo-item", claim); err != nil {
		t.Fatal(err)
	}
	handler := state.handler.(*packedHandler)
	handler.action = func(tx *Transaction, payload []byte) (Outcome, error) {
		key := payload[1:]
		if payload[0] == '-' {
			return Outcome{}, tx.DeleteRecordReference(key)
		}
		d, err := records.Describe(tx.keyspace, "origin-group", key, payload)
		if err != nil {
			return Outcome{}, err
		}
		return Outcome{}, tx.SetRecordReference(key, d)
	}
	if err := state.ApplyRouted(state.AppliedIndex()+1, "publish-b", target, []byte("+item-b")); err != nil {
		t.Fatal(err)
	}
	barrier := &discoveryReadBarrier{index: state.AppliedIndex()}
	visited := []string{}
	page, err := state.WalkCurrentReferencesWithGeoCoveragePage(context.Background(), barrier, nil, 8,
		func(ref RecordReference, receipt GeoCoverageRecord, found bool) (bool, error) {
			key := string(ref.Descriptor.Key)
			visited = append(visited, key)
			if key == "item" {
				if !found || receipt.RequestID != "geo-item" || receipt.Claim.ReferenceIndex != ref.Index {
					t.Fatal("receipt was not read with its publication", receipt, found)
				}
				// A concurrent unlink must neither remove the second snapshot row
				// nor invalidate its backlinks while this page is being visited.
				return true, state.ApplyRouted(state.AppliedIndex()+1, "unlink-b", target, []byte("-item-b"))
			}
			if key != "item-b" || found {
				t.Fatal("missing coverage was not distinguished from a valid receipt", ref, found)
			}
			return true, nil
		})
	if err != nil || !page.Done || page.Cut.Count != 2 || page.Cut.Index != barrier.index || barrier.calls != 1 ||
		!slices.Equal(visited, []string{"item", "item-b"}) {
		t.Fatal("Geo discovery repeated barriers or mixed Owner cuts", page, visited, barrier.calls, err)
	}
}

func TestGeoReferencePageRejectsCorruptCoverageBeforeVisit(t *testing.T) {
	for _, corrupt := range []string{"result", "receipt"} {
		t.Run(corrupt, func(t *testing.T) {
			state, target, claim := geoCoverageFixture(t)
			if err := state.ApplyGeoConfirmation(state.AppliedIndex()+1, "geo-item", claim); err != nil {
				t.Fatal(err)
			}
			var err error
			if corrupt == "result" {
				err = state.db.Delete(resultKey("geo-item"), pebble.Sync)
			} else {
				err = state.db.Set(geoCoverageKey(target.Keyspace, claim.Key), []byte("{}"), pebble.Sync)
			}
			if err != nil {
				t.Fatal(err)
			}
			visited := 0
			_, err = state.WalkCurrentReferencesWithGeoCoveragePage(context.Background(),
				&discoveryReadBarrier{index: state.AppliedIndex()}, nil, 8,
				func(RecordReference, GeoCoverageRecord, bool) (bool, error) {
					visited++
					return false, fmt.Errorf("corrupt receipt reached discovery")
				})
			if err == nil || visited != 0 {
				t.Fatal("corrupt coverage was used as a valid discovery hint", visited, err)
			}
		})
	}
}

func TestGeoReferencePageStopsAtRepairBudgetAndResumesWithoutSkipping(t *testing.T) {
	state, target, _, handler := confirmationFixture(t)
	handler.action = func(tx *Transaction, payload []byte) (Outcome, error) {
		d, err := records.Describe(tx.keyspace, "origin-group", payload, payload)
		if err != nil {
			return Outcome{}, err
		}
		return Outcome{}, tx.SetRecordReference(payload, d)
	}
	for _, key := range []string{"item-b", "item-c"} {
		if err := state.ApplyRouted(state.AppliedIndex()+1, "publish-"+key, target, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	barrier := &discoveryReadBarrier{index: state.AppliedIndex()}
	visited := []string{}
	visit := func(ref RecordReference, _ GeoCoverageRecord, found bool) (bool, error) {
		if found {
			t.Fatal("fixture unexpectedly has coverage")
		}
		visited = append(visited, string(ref.Descriptor.Key))
		return len(visited)%2 != 0, nil
	}
	first, err := state.WalkCurrentReferencesWithGeoCoveragePage(context.Background(), barrier, nil, 256, visit)
	if err != nil || first.Done || first.Cut.Count != 2 || len(first.Cursor) == 0 {
		t.Fatal("repair budget did not end a valid bounded metadata page", first, err)
	}
	second, err := state.WalkCurrentReferencesWithGeoCoveragePage(context.Background(), barrier, first.Cursor, 256, visit)
	if err != nil || !second.Done || second.Cut.Count != 1 || barrier.calls != 2 ||
		!slices.Equal(visited, []string{"item", "item-b", "item-c"}) {
		t.Fatal("early-stop cursor skipped or repeated references", second, visited, barrier.calls, err)
	}
}

func TestWalkCurrentReferencesUsesFixedOwnerCutWithoutBlockingMutations(t *testing.T) {
	state, target, claim, _ := confirmationFixture(t)
	if err := state.ApplyLocalConfirmation(state.AppliedIndex()+1, "confirm", claim); err != nil {
		t.Fatal(err)
	}
	before := state.AppliedIndex()
	visits := 0
	cut, err := state.WalkCurrentReferences(context.Background(), &fakeReadBarrier{index: before}, func(ref RecordReference) error {
		visits++
		if ref.RequestID != "publish" || ref.Index != claim.ReferenceIndex {
			t.Fatal("walk returned another publication", ref)
		}
		return state.ApplyRouted(state.AppliedIndex()+1, "unlink", target, []byte("unlink"))
	})
	if err != nil || cut.Index != before || cut.Count != 1 || visits != 1 {
		t.Fatal("fixed Owner cut changed during callback", cut, visits, err)
	}
	if _, found, err := readRecordReference(t, state, target); err != nil || found {
		t.Fatal("concurrent unlink did not apply", found, err)
	}
	empty, err := state.WalkCurrentReferences(context.Background(), &fakeReadBarrier{index: state.AppliedIndex()}, func(RecordReference) error {
		t.Fatal("deleted reference resurfaced in new cut")
		return nil
	})
	if err != nil || empty.Count != 0 {
		t.Fatal(empty, err)
	}
}

func TestWalkCurrentReferencesRejectsCorruptBacklinkBeforeVisit(t *testing.T) {
	state, _, _, _ := confirmationFixture(t)
	if err := state.db.Delete(resultKey("publish"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	visits := 0
	cut, err := state.WalkCurrentReferences(context.Background(), &fakeReadBarrier{index: state.AppliedIndex()}, func(RecordReference) error {
		visits++
		return nil
	})
	if err == nil || cut.Count != 0 || visits != 0 {
		t.Fatal("corrupt Owner metadata was used for repair discovery", cut, visits, err)
	}
}

func TestWalkCurrentReferencesPageBoundsSnapshotAndRestartsSweep(t *testing.T) {
	state, target, _, handler := confirmationFixture(t)
	handler.action = func(tx *Transaction, payload []byte) (Outcome, error) {
		key := payload[1:]
		if payload[0] == '-' {
			return Outcome{}, tx.DeleteRecordReference(key)
		}
		d, err := records.Describe(tx.keyspace, "origin-group", key, payload)
		if err != nil {
			return Outcome{}, err
		}
		return Outcome{}, tx.SetRecordReference(key, d)
	}
	apply := func(id, payload string) {
		t.Helper()
		if err := state.ApplyRouted(state.AppliedIndex()+1, id, target, []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	apply("publish-b", "+item-b")
	apply("publish-c", "+item-c")
	visited := []string{}
	visit := func(ref RecordReference) error {
		visited = append(visited, string(ref.Descriptor.Key))
		return nil
	}
	barrier := &fakeReadBarrier{index: state.AppliedIndex()}
	first, err := state.WalkCurrentReferencesPage(context.Background(), barrier, nil, 1, visit)
	if err != nil || first.Done || first.Cut.Count != 1 || len(first.Cursor) == 0 || !slices.Equal(visited, []string{"item"}) {
		t.Fatal("first page was not bounded", first, visited, err)
	}
	// New keys below the cursor are intentionally deferred until a new sweep;
	// deleting the cursor itself must not make the next page repeat or skip b.
	apply("delete-item", "-item")
	apply("publish-a", "+a")
	barrier.index = state.AppliedIndex()
	second, err := state.WalkCurrentReferencesPage(context.Background(), barrier, first.Cursor, 1, visit)
	if err != nil || second.Done || second.Cut.Count != 1 || !slices.Equal(visited, []string{"item", "item-b"}) {
		t.Fatal("second page did not resume after deleted cursor", second, visited, err)
	}
	third, err := state.WalkCurrentReferencesPage(context.Background(), barrier, second.Cursor, 1, visit)
	if err != nil || !third.Done || third.Cut.Count != 1 || !slices.Equal(visited, []string{"item", "item-b", "item-c"}) {
		t.Fatal("third page did not finish sweep", third, visited, err)
	}
	visited = nil
	restart, err := state.WalkCurrentReferencesPage(context.Background(), barrier, nil, 1, visit)
	if err != nil || restart.Done || !slices.Equal(visited, []string{"a"}) {
		t.Fatal("new sweep did not find earlier insertion", restart, visited, err)
	}
	if _, err := state.WalkCurrentReferencesPage(context.Background(), barrier, []byte("other/key"), 1, visit); err == nil {
		t.Fatal("invalid cursor accepted")
	}
	if _, err := state.WalkCurrentReferencesPage(context.Background(), barrier, nil, MaxLiveReferencePage+1, visit); err == nil {
		t.Fatal("unbounded page accepted")
	}
	if _, err := state.WalkCurrentReferencesPage(context.Background(), barrier, nil, 0, visit); err == nil {
		t.Fatal("zero page limit accepted")
	}
	callbackError := errors.New("stop")
	if _, err := state.WalkCurrentReferencesPage(context.Background(), barrier, nil, 1, func(RecordReference) error {
		return callbackError
	}); !errors.Is(err, callbackError) {
		t.Fatal("callback error was not returned", err)
	}
}
