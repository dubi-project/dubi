package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

func repairRestartView() rootstate.RegionProgress {
	return rootstate.RegionProgress{Current: rootstate.RegionCohort{RegionID: "west", Generation: 4,
		GroupID: "regional-west", Nodes: []uint64{1, 2, 3}, State: rootstate.RegionCohortConfirmed,
		PlannedIndex: 100, ConfirmedIndex: 101, GroupEpoch: 9, GroupFenceRequestID: "fence", GroupFenceIndex: 120}}
}

// Exercise the actual durable ledger and worker resume path. No fixture here
// is evidence of physical coverage; this tests retention of a known failure.
func TestLiveRepairRestartRetainsDebtUntilFullCleanSweep(t *testing.T) {
	for _, nextPage := range []bool{false, true} {
		name := "completed_failed_sweep"
		if nextPage {
			name = "next_clean_sweep_mid_page"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			store, err := recordrepair.OpenProgressStore(root)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			view := repairRestartView()
			group := view.Current.GroupID
			failed := liveRepairCursor{}
			failed.addFailures([]error{errors.New("missing Pack bytes")})
			if err := store.Save(repairProgressForPage(view, group, failed, true, nil)); err != nil {
				t.Fatal(err)
			}
			if nextPage {
				worker := &liveRecordRepair{progress: store, groups: make(map[string]liveRepairCursor)}
				next, err := worker.loadCursor(group, view)
				if err != nil {
					t.Fatal(err)
				}
				// A clean page in the new sweep must not overwrite outstanding debt.
				var debt *liveRepairRecordDebt
				err = worker.advanceCursor(view, group, nil, next, recordrepair.LivePageReport{AlreadyHeld: 8,
					Page: enginestate.LiveReferencePage{Cursor: []byte("next")}})
				if !errors.As(err, &debt) {
					t.Fatalf("partial clean page lost debt: %v", err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = recordrepair.OpenProgressStore(root)
			if err != nil {
				t.Fatal(err)
			}
			restarted := &liveRecordRepair{progress: store}
			resumed, err := restarted.loadCursor(group, view)
			if err != nil {
				t.Fatal(err)
			}
			if resumed.debtError(group) == nil {
				t.Fatal("restart erased known record debt before a full clean sweep")
			}
			if nextPage && (resumed.scanned != 8 || resumed.lastProgressUnix == 0 || resumed.lastCleanUnix != 0) {
				t.Fatal("partial progress was lost on restart", resumed)
			}
			// Restored debt is visible before the next network/barrier attempt.
			restarted.lastGroup = group
			restarted.publishProgress(group, "waiting_root", resumed)
			restarted.observeStep(context.Background(), nil)
			if len(restarted.snapshot().Issues) != 1 {
				t.Fatal("clean operation erased restored debt")
			}
			if resumed.unresolved != 0 {
				t.Fatal("new sweep double-counted the completed sweep's failures")
			}
			// Only the completed, durably committed clean sweep clears the latch.
			if err := restarted.advanceCursor(view, group, nil, resumed, recordrepair.LivePageReport{
				AlreadyHeld: 3, Page: enginestate.LiveReferencePage{Done: true}}); err != nil {
				t.Fatal(err)
			}
			restarted.observeStep(context.Background(), nil)
			if len(restarted.snapshot().Issues) != 0 {
				t.Fatal("full clean sweep retained debt")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = recordrepair.OpenProgressStore(root)
			if err != nil {
				t.Fatal(err)
			}
			clean, _, err := store.Load(group)
			wantScanned := uint64(3)
			if nextPage {
				wantScanned += 8
			}
			if err != nil || clean.NeedsCleanSweep || clean.Unresolved != 0 || clean.FirstError != "" || !clean.Done || clean.LastCleanUnix == 0 || clean.Scanned != wantScanned {
				t.Fatalf("clean completion was not durable: %+v, %v", clean, err)
			}
		})
	}
}

func TestLiveRepairCohortChangeRewindsButRetainsKnownDebt(t *testing.T) {
	store, err := recordrepair.OpenProgressStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	view := repairRestartView()
	group := view.Current.GroupID
	// Emulate the pre-latch on-disk format, with an incomplete failed sweep.
	saved := repairProgressForPage(view, group, liveRepairCursor{unresolved: 2, firstFailure: "prior missing bytes",
		scanned: 80, startedUnix: 100, lastProgressUnix: 200, lastCleanUnix: 50}, false, []byte("old-cursor"))
	if err := store.Save(saved); err != nil {
		t.Fatal(err)
	}
	worker := &liveRecordRepair{progress: store}
	same, err := worker.loadCursor(group, view)
	if err != nil || same.scanned != 80 || same.unresolved != 2 || same.debtError(group) == nil || string(same.cursor) != "old-cursor" {
		t.Fatalf("same-cohort legacy progress was not resumed: %+v, %v", same, err)
	}
	view.Current.GroupFenceIndex++
	changed, err := worker.loadCursor(group, view)
	if err != nil || len(changed.cursor) != 0 || changed.scanned != 0 || changed.unresolved != 0 || changed.lastCleanUnix != 0 || changed.debtError(group) == nil {
		t.Fatalf("cohort change retained stale progress or erased debt: %+v, %v", changed, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.loadCursor(group, view); err == nil {
		t.Fatal("closed ledger reported clean progress")
	}
}

func TestLiveRepairDoesNotPublishCleanCompletionBeforeCommit(t *testing.T) {
	store, err := recordrepair.OpenProgressStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	view := repairRestartView()
	cursor := liveRepairCursor{needsCleanSweep: true, firstFailure: "known debt"}
	worker := &liveRecordRepair{progress: store, lastGroup: view.Current.GroupID}
	worker.publishProgress(view.Current.GroupID, "repairing", cursor)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	err = worker.advanceCursor(view, view.Current.GroupID, nil, cursor, recordrepair.LivePageReport{Page: enginestate.LiveReferencePage{Done: true}})
	if err == nil || worker.lastDone {
		t.Fatal("failed commit published completed clean sweep")
	}
	worker.observeStep(context.Background(), err)
	observation := worker.snapshot()
	if len(observation.Issues) == 0 || !observation.Progress[0].NeedsCleanSweep || observation.Progress[0].LastCleanUnix != 0 {
		t.Fatal(observation)
	}
}

func TestLiveRepairFailureMessageFitsDurableLedger(t *testing.T) {
	store, err := recordrepair.OpenProgressStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cursor := liveRepairCursor{}
	cursor.addFailures([]error{errors.New(strings.Repeat("故障", 4096))})
	view := repairRestartView()
	if err := store.Save(repairProgressForPage(view, view.Current.GroupID, cursor, false, nil)); err != nil {
		t.Fatal(err)
	}
}

// A deliberately offloaded record still completed a physical-majority check.
// Count that work durably without treating a partial clean page as recovery.
func TestLiveRepairCountsOffloadedRowsWithoutClearingDebt(t *testing.T) {
	store, err := recordrepair.OpenProgressStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	view := repairRestartView()
	worker := &liveRecordRepair{progress: store, groups: make(map[string]liveRepairCursor)}
	cursor := liveRepairCursor{scanned: 10, needsCleanSweep: true, firstFailure: "earlier timeout"}
	err = worker.advanceCursor(view, view.Current.GroupID, nil, cursor, recordrepair.LivePageReport{
		AlreadyHeld: 2, Fetched: 1, Offloaded: 61,
		Page: enginestate.LiveReferencePage{Cursor: []byte("next")},
	})
	var debt *liveRepairRecordDebt
	if !errors.As(err, &debt) {
		t.Fatal("partial page cleared debt", err)
	}
	saved, found, err := store.Load(view.Current.GroupID)
	if err != nil || !found || saved.Scanned != 74 || !saved.NeedsCleanSweep || saved.LastCleanUnix != 0 {
		t.Fatal("offloaded work was lost or prematurely declared clean", saved, err)
	}
}
