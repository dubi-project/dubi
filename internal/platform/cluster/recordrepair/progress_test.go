package recordrepair

import (
	"reflect"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

func progressFixture() Progress {
	return Progress{Version: repairProgressVersion, GroupID: "regional-west", RegionID: "west", Generation: 4,
		GroupEpoch: 9, FenceIndex: 120, Nodes: []uint64{1, 2, 3}, Cursor: []byte("data/record-reference/west/next"),
		Unresolved: 2, FirstError: "source unavailable"}
}

func progressView() rootstate.RegionProgress {
	return rootstate.RegionProgress{Current: rootstate.RegionCohort{RegionID: "west", Generation: 4,
		GroupID: "regional-west", Nodes: []uint64{1, 2, 3}, State: rootstate.RegionCohortConfirmed,
		PlannedIndex: 100, ConfirmedIndex: 101, GroupEpoch: 9, GroupFenceRequestID: "fence", GroupFenceIndex: 120}}
}

func TestProgressStoreReopensAndClonesLedger(t *testing.T) {
	root := t.TempDir()
	store, err := OpenProgressStore(root)
	if err != nil {
		t.Fatal(err)
	}
	want := progressFixture()
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Load(want.GroupID)
	if err != nil || !found || !reflect.DeepEqual(got, want) {
		t.Fatal(got, found, err)
	}
	got.Nodes[0], got.Cursor[0] = 99, 'x'
	again, _, _ := store.Load(want.GroupID)
	if !reflect.DeepEqual(again, want) {
		t.Fatal("loaded progress aliases persisted state", again)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenProgressStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, found, err = store.Load(want.GroupID)
	if err != nil || !found || !reflect.DeepEqual(got, want) {
		t.Fatal("progress did not survive reopen", got, found, err)
	}
}

func TestProgressBindsToCurrentCohortAndCompletedSweep(t *testing.T) {
	progress := progressFixture()
	if !progress.Matches(progressView()) {
		t.Fatal("matching cohort rejected")
	}
	changed := progressView()
	changed.Current.GroupFenceIndex++
	if progress.Matches(changed) {
		t.Fatal("changed fence accepted")
	}
	progress.Done = true
	progress.Cursor = nil
	if err := progress.validate(); err != nil {
		t.Fatal(err)
	}
	progress.Cursor = []byte("stale")
	if err := progress.validate(); err == nil {
		t.Fatal("completed sweep retained a cursor")
	}
}
