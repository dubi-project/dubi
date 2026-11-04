package recordwrite

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pageBatchHoldingObserver struct {
	base                   Observer
	batches, singles, rows atomic.Int32
	err                    error
	malformed              bool
	entered                chan struct{}
	release                chan struct{}
}

func (o *pageBatchHoldingObserver) ObserveRecordHolding(ctx context.Context, n uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	o.singles.Add(1)
	return o.base.ObserveRecordHolding(ctx, n, d)
}
func (o *pageBatchHoldingObserver) ObserveRecordHoldings(ctx context.Context, n uint64, ds []records.Descriptor) ([]raftrpc.RecordHoldingResult, error) {
	o.batches.Add(1)
	o.rows.Add(int32(len(ds)))
	if o.entered != nil {
		o.entered <- struct{}{}
		<-o.release
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.err != nil {
		return nil, o.err
	}
	rows := make([]raftrpc.RecordHoldingResult, len(ds))
	for i, d := range ds {
		rows[i].Observation, rows[i].Err = o.base.ObserveRecordHolding(ctx, n, d)
	}
	if o.malformed {
		return rows[:len(rows)-1], nil
	}
	return rows, nil
}

func TestGeoPageBatchesNodeObservationsWithoutLosingPerRowProof(t *testing.T) {
	w, refs, b, _, base, mirrors := geoBatchFixture(t, 32)
	observer := &pageBatchHoldingObserver{base: base}
	w.Observer = observer
	results, err := w.RefreshGeoConfirmationPage(context.Background(), refs, 3, 3)
	if err != nil || len(results) != 32 || b.rootCalls != 2 || observer.batches.Load() != 2 || observer.singles.Load() != 0 || observer.rows.Load() != 64 || base.calls != 64 || len(mirrors.mirrors) != 32 {
		t.Fatal("Geo page did not share exact per-node physical observations", results, err, b.rootCalls, observer.batches.Load(), observer.singles.Load(), observer.rows.Load(), base.calls, len(mirrors.mirrors))
	}
	for i, r := range results {
		if !r.Confirmed || r.Err != nil {
			t.Fatal(i, r)
		}
	}
	assertLocalGeoMirrors(t, w, refs)
	results, err = w.RefreshGeoConfirmationPage(context.Background(), refs, 3, 3)
	if err != nil || observer.batches.Load() != 2 || base.calls != 64 || len(mirrors.mirrors) != 32 {
		t.Fatal("already-current page repeated physical IO", err, observer.batches.Load(), base.calls)
	}
	for _, r := range results {
		if r.Confirmed || r.Err != nil {
			t.Fatal("historical receipt counted as fresh proof", r)
		}
	}
}

func TestGeoPageFallbackRequiresExplicitUnsupportedBatch(t *testing.T) {
	for _, mode := range []string{"legacy", "denied", "unavailable", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			w, refs, _, _, base, mirrors := geoBatchFixture(t, 32)
			observer := &pageBatchHoldingObserver{base: base}
			w.Observer = observer
			switch mode {
			case "legacy":
				observer.err = status.Error(codes.Unimplemented, "old peer")
			case "denied":
				observer.err = status.Error(codes.PermissionDenied, "fenced")
			case "unavailable":
				observer.err = status.Error(codes.Unavailable, "no Root quorum")
			case "malformed":
				observer.malformed = true
			}
			results, err := w.RefreshGeoConfirmationPage(context.Background(), refs, 3, 3)
			if err != nil || len(results) != 32 {
				t.Fatal(results, err)
			}
			for _, r := range results {
				if mode == "legacy" {
					if !r.Confirmed || r.Err != nil {
						t.Fatal(r)
					}
				} else if r.Confirmed || r.Err == nil {
					t.Fatal("unsafe batch cleared row debt", mode, r)
				}
			}
			if mode == "legacy" {
				if observer.singles.Load() != 64 || observer.batches.Load() != 2 {
					t.Fatal("legacy peer lost original row path", observer.singles.Load(), observer.batches.Load())
				}
			} else if observer.singles.Load() != 0 || len(mirrors.mirrors) != 0 {
				t.Fatal("denied or bad batch permitted fallback or confirmation", mode, observer.singles.Load(), len(mirrors.mirrors))
			}
		})
	}
}

func TestGeoPageBatchProofStillRequiresFreshFinalAuthority(t *testing.T) {
	for _, change := range []bool{false, true} {
		w, refs, b, root, base, _ := geoBatchFixture(t, 32)
		w.Observer = &pageBatchHoldingObserver{base: base}
		b.final = func() error {
			if change {
				root.view.Regions[0].Progress.Current.Generation++
				root.view.Regions[0].Progress.Head.CurrentGeneration++
				return nil
			}
			return errors.New("Root quorum lost")
		}
		results, err := w.RefreshGeoConfirmationPage(context.Background(), refs, 3, 3)
		if err == nil || results != nil {
			t.Fatal("batch proof escaped page authority recheck", change, results, err)
		}
	}
}

func TestGeoPageBatchCancellationJoinsOwnedNodeRequest(t *testing.T) {
	w, refs, _, _, base, _ := geoBatchFixture(t, 32)
	o := &pageBatchHoldingObserver{base: base, entered: make(chan struct{}, 1), release: make(chan struct{})}
	w.Observer = o
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := w.RefreshGeoConfirmationPage(ctx, refs, 3, 3); done <- err }()
	released := false
	defer func() {
		if !released {
			cancel()
			close(o.release)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}
	}()
	select {
	case <-o.entered:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("batch request did not start")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatal("page returned while batch IO was owned", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(o.release)
	released = true
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("batch workers did not exit")
	}
}
