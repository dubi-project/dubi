package recordrepair

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testReferences(t *testing.T) []enginestate.RecordReference {
	t.Helper()
	var out []enginestate.RecordReference
	for _, key := range []string{"one", "two"} {
		d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "record", DatasetID: "items"}, "region-west", []byte(key), []byte(key))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, enginestate.RecordReference{Descriptor: d})
	}
	return out
}

func testWalk(refs []enginestate.RecordReference) cutWalker {
	return func(visit func(enginestate.RecordReference) error) error {
		for _, ref := range refs {
			if err := visit(ref); err != nil {
				return err
			}
		}
		return nil
	}
}

func testSources() []rootstate.Node {
	return []rootstate.Node{{RaftID: 1}, {RaftID: 2}, {RaftID: 3}}
}

func TestFillLocalCutUsesOldCohortAndReverifiesFetchedBytes(t *testing.T) {
	refs := testReferences(t)
	local := map[string]bool{"one": true}
	var attempted []uint64
	verify := func(d records.Descriptor) error {
		if local[string(d.Key)] {
			return nil
		}
		return records.ErrNotHeld
	}
	fetch := func(_ context.Context, source rootstate.Node, d records.Descriptor) error {
		attempted = append(attempted, source.RaftID)
		if source.RaftID == 1 {
			return status.Error(codes.Unavailable, "source offline")
		}
		if source.RaftID != 2 || string(d.Key) != "two" {
			t.Fatal("unexpected source or record", source, d)
		}
		local[string(d.Key)] = true
		return nil
	}
	report, err := fill(context.Background(), []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, testSources(), testWalk(refs), verify, fetch)
	if err != nil || report != (Report{References: 2, AlreadyHeld: 1, Fetched: 1, ReferencedBytes: 6}) || !reflect.DeepEqual(attempted, []uint64{1, 2}) {
		t.Fatal(report, attempted, err)
	}
	// A retry sees the physically reverified bytes; it does not fetch again.
	attempted = nil
	report, err = fill(context.Background(), []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, testSources(), testWalk(refs), verify, fetch)
	if err != nil || report.AlreadyHeld != 2 || report.Fetched != 0 || len(attempted) != 0 {
		t.Fatal(report, attempted, err)
	}
}

func TestFillRecoveryUsesOnlyLiveOldMemberSubset(t *testing.T) {
	refs := testReferences(t)[:1]
	held := false
	report, err := fillMode(context.Background(), []uint64{1, 2, 3}, []uint64{1, 2, 4}, 4,
		[]rootstate.Node{{RaftID: 1}, {RaftID: 2}}, true, testWalk(refs), func(records.Descriptor) error {
			if held {
				return nil
			}
			return records.ErrNotHeld
		}, func(_ context.Context, source rootstate.Node, _ records.Descriptor) error {
			if source.RaftID == 1 {
				return status.Error(codes.Unavailable, "retired source unavailable")
			}
			held = true
			return nil
		})
	if err != nil || report != (Report{References: 1, Fetched: 1, ReferencedBytes: 3}) {
		t.Fatal(report, err)
	}
}

func TestFillLocalCutSwitchesOnlyOnPhysicalSourceFailure(t *testing.T) {
	refs := testReferences(t)[:1]
	for _, tc := range []struct {
		name       string
		firstError error
		fallback   bool
	}{
		{"interrupted-stream", status.Error(codes.Unavailable, "stream interrupted"), true},
		{"source-timeout", status.Error(codes.DeadlineExceeded, "source timed out"), true},
		{"source-busy", status.Error(codes.ResourceExhausted, "source busy"), true},
		{"source-corrupt", status.Error(codes.DataLoss, "source Pack damaged"), true},
		{"local-source-unavailable", records.ErrUnavailable, true},
		{"membership-changed", status.Error(codes.FailedPrecondition, "source registration changed"), false},
		{"identity-revoked", status.Error(codes.PermissionDenied, "source identity revoked"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempted []uint64
			held := false
			report, err := fill(context.Background(), []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4,
				testSources(), testWalk(refs), func(records.Descriptor) error {
					if held {
						return nil
					}
					return records.ErrNotHeld
				}, func(_ context.Context, source rootstate.Node, _ records.Descriptor) error {
					attempted = append(attempted, source.RaftID)
					if source.RaftID == 1 {
						return tc.firstError
					}
					held = true
					return nil
				})
			if tc.fallback {
				if err != nil || report.Fetched != 1 || !reflect.DeepEqual(attempted, []uint64{1, 2}) {
					t.Fatal("healthy old cohort source was not used", report, attempted, err)
				}
			} else if !errors.Is(err, tc.firstError) || report != (Report{}) || !reflect.DeepEqual(attempted, []uint64{1}) || held {
				t.Fatal("authority failure was hidden by source fallback", report, attempted, err)
			}
		})
	}
}

func TestPendingFillLeavesTimeForFallbackWhenFirstSourceStalls(t *testing.T) {
	refs := testReferences(t)[:1]
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	held := false
	var attempted []uint64
	report, err := fill(ctx, []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4,
		testSources(), testWalk(refs), func(records.Descriptor) error {
			if held {
				return nil
			}
			return records.ErrNotHeld
		}, func(attempt context.Context, source rootstate.Node, _ records.Descriptor) error {
			attempted = append(attempted, source.RaftID)
			if source.RaftID == 1 {
				<-attempt.Done()
				return attempt.Err()
			}
			held = true
			return nil
		})
	if err != nil || report.Fetched != 1 || ctx.Err() != nil || !reflect.DeepEqual(attempted, []uint64{1, 2}) {
		t.Fatal("stalled old source consumed the whole pending fill", report, attempted, ctx.Err(), err)
	}
}

func TestFillLocalCutResumesAfterPartialTransferFailure(t *testing.T) {
	refs := testReferences(t)
	local := make(map[string]bool)
	firstAttempt := true
	var attempted []string
	verify := func(d records.Descriptor) error {
		if local[string(d.Key)] {
			return nil
		}
		return records.ErrNotHeld
	}
	fetch := func(_ context.Context, source rootstate.Node, d records.Descriptor) error {
		key := string(d.Key)
		attempted = append(attempted, key)
		if key == "two" && firstAttempt {
			return status.Error(codes.Unavailable, "source interrupted")
		}
		if source.RaftID != 1 {
			return status.Error(codes.NotFound, "not held")
		}
		local[key] = true
		return nil
	}
	old, next := []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}
	if report, err := fill(context.Background(), old, next, 4, testSources(), testWalk(refs), verify, fetch); err == nil || report != (Report{}) || !local["one"] || local["two"] {
		t.Fatal("partial transfer was incorrectly completed", report, local, err)
	}
	firstAttempt = false
	attempted = nil
	report, err := fill(context.Background(), old, next, 4, testSources(), testWalk(refs), verify, fetch)
	if err != nil || report != (Report{References: 2, AlreadyHeld: 1, Fetched: 1, ReferencedBytes: 6}) || !reflect.DeepEqual(attempted, []string{"two"}) {
		t.Fatal("retry did not reuse the first verified record", report, attempted, err)
	}
}

func TestFillLocalCutRejectsIncompleteCohortAndPhysicalDamage(t *testing.T) {
	refs := testReferences(t)
	missing := func(records.Descriptor) error { return records.ErrNotHeld }
	noFetch := func(context.Context, rootstate.Node, records.Descriptor) error {
		t.Fatal("unexpected fetch")
		return nil
	}
	for _, tc := range []struct {
		name    string
		old     []uint64
		next    []uint64
		local   uint64
		sources []rootstate.Node
	}{
		{"omitted", []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, testSources()[:2]},
		{"duplicate", []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, []rootstate.Node{{RaftID: 1}, {RaftID: 1}, {RaftID: 3}}},
		{"foreign", []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, []rootstate.Node{{RaftID: 1}, {RaftID: 2}, {RaftID: 9}}},
		{"not-next", []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 5, testSources()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := fill(context.Background(), tc.old, tc.next, tc.local, tc.sources, testWalk(refs), missing, noFetch); err == nil {
				t.Fatal("invalid cohort accepted")
			}
		})
	}
	damaged := errors.New("damaged local index")
	if _, err := fill(context.Background(), []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, testSources(), testWalk(refs), func(records.Descriptor) error { return damaged }, noFetch); !errors.Is(err, damaged) {
		t.Fatal("local corruption was hidden", err)
	}
}

func TestFillLocalCutDoesNotClaimCompletionOnFailedFetchOrWalk(t *testing.T) {
	refs := testReferences(t)
	missing := func(records.Descriptor) error { return records.ErrNotHeld }
	_, err := fill(context.Background(), []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, testSources(), testWalk(refs), missing,
		func(context.Context, rootstate.Node, records.Descriptor) error {
			return status.Error(codes.NotFound, "not held")
		})
	if err == nil {
		t.Fatal("missing physical record was reported as filled")
	}
	if _, err := fill(context.Background(), []uint64{1}, []uint64{1, 2}, 1, []rootstate.Node{{RaftID: 1}}, testWalk(refs), missing,
		func(context.Context, rootstate.Node, records.Descriptor) error {
			t.Fatal("self-fetch attempted")
			return nil
		}); err == nil {
		t.Fatal("a missing record with no independent source was reported as filled")
	}
	local := map[string]bool{}
	_, err = fill(context.Background(), []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, testSources(), testWalk(refs),
		func(d records.Descriptor) error {
			if local[string(d.Key)] {
				return nil
			}
			return records.ErrNotHeld
		}, func(context.Context, rootstate.Node, records.Descriptor) error { return nil })
	if !errors.Is(err, records.ErrNotHeld) {
		t.Fatal("successful fetch without actual local data was accepted", err)
	}
	walkFailure := errors.New("manifest close failed")
	_, err = fill(context.Background(), []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, testSources(), func(visit func(enginestate.RecordReference) error) error {
		if err := visit(refs[0]); err != nil {
			return err
		}
		return walkFailure
	}, func(records.Descriptor) error { return nil }, func(context.Context, rootstate.Node, records.Descriptor) error { return nil })
	if !errors.Is(err, walkFailure) {
		t.Fatal("late manifest failure was lost", err)
	}
}
