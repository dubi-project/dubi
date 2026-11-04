package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type offloadBatchFake struct {
	*liveBatchFake
	mode                    string
	batchCalls, singleCalls int
	counts                  []int
	bytes                   []uint64
}

func (f *offloadBatchFake) ObserveRecordHolding(ctx context.Context, target uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	f.singleCalls++
	return f.repairSourceFake.ObserveRecordHolding(ctx, target, d)
}

func (f *offloadBatchFake) ObserveRecordHoldings(ctx context.Context, target uint64, ds []records.Descriptor) ([]raftrpc.RecordHoldingResult, error) {
	f.batchCalls++
	f.counts = append(f.counts, len(ds))
	var size uint64
	for _, d := range ds {
		size += d.Size
	}
	f.bytes = append(f.bytes, size)
	if f.mode == "legacy" {
		return nil, status.Error(codes.Unimplemented, "legacy")
	}
	if f.mode == "denied" {
		return nil, status.Error(codes.PermissionDenied, "fenced")
	}
	if f.mode == "short" {
		return nil, nil
	}
	rows := make([]raftrpc.RecordHoldingResult, len(ds))
	for i, d := range ds {
		rows[i].Observation, rows[i].Err = f.repairSourceFake.ObserveRecordHolding(ctx, target, d)
		if f.mode == "lost" && target == 3 && i == 0 {
			rows[i].Err = status.Error(codes.NotFound, "missing")
		}
		if f.mode == "wrong-node" {
			rows[i].Observation.NodeID = "wrong"
		}
		if f.mode == "wrong-record" {
			rows[i].Observation.Holding.RecordID = "wrong"
		}
		if f.mode == "zero-cut" {
			rows[i].Observation.RootConfirmedIndex = 0
		}
		if f.mode == "duplicate-store" {
			rows[i].Observation.Holding.StoreID = "store_00000000000000000000000000000001"
		}
	}
	return rows, nil
}

func offloadFixture(t *testing.T, count, size int) (*records.Store, []enginestate.RecordReference, *offloadBatchFake) {
	t.Helper()
	store, refs, fake := liveBatchFixture(t, count, size, false)
	ctx := context.Background()
	plan := repairTestPlan(t)
	for _, ref := range refs {
		d := ref.Descriptor
		id, _ := d.ID()
		if _, err := store.Put(ctx, d, fake.bodies[id]); err != nil {
			t.Fatal(err)
		}
		task, err := store.BeginNodeBalance(ctx, d, plan.RetentionScope(), plan.Nodes()[1:])
		if err != nil {
			t.Fatal(err)
		}
		if err := store.DropBalanceHolding(ctx, task.Sequence); err != nil {
			t.Fatal(err)
		}
		if err := store.FinishNodeBalance(ctx, task.Sequence); err != nil {
			t.Fatal(err)
		}
	}
	return store, refs, &offloadBatchFake{liveBatchFake: fake}
}

func TestOffloadBatchPreservesPhysicalChecksAndBounds(t *testing.T) {
	for _, size := range []int{128, 2 << 20, (4 << 20) + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			count := 64
			if size > 128 {
				count = 5
			}
			store, refs, fake := offloadFixture(t, count, size)
			plan := repairTestPlan(t)
			var report LivePageReport
			for len(refs) != 0 {
				n := offloadPageSize(refs)
				observe, err := observeOffloadPage(plan, refs[:n], store, fake)
				if err != nil {
					t.Fatal(err)
				}
				for _, ref := range refs[:n] {
					if err := processRepairReferenceObserved(context.Background(), plan, ref, 1, store, fake, observe, 64, &report); err != nil {
						t.Fatal(err)
					}
					if _, err := store.Verify(context.Background(), ref.Descriptor); !errors.Is(err, records.ErrNotHeld) {
						t.Fatal("recreated offloaded bytes", err)
					}
				}
				refs = refs[n:]
			}
			if report.Offloaded != uint64(count) || len(fake.observed) != 2*count || len(fake.fetchedFrom) != 0 {
				t.Fatal(report, fake.observed)
			}
			if size == 128 && (fake.batchCalls != 4 || fake.singleCalls != 0) {
				t.Fatal("did not reduce 128 RPCs to 4", fake.batchCalls, fake.singleCalls)
			}
			if size > 4<<20 && (fake.batchCalls != 0 || fake.singleCalls != 2*count) {
				t.Fatal("oversized row was batched")
			}
			for i, n := range fake.counts {
				if n > 32 || fake.bytes[i] > 4<<20 {
					t.Fatal("unbounded batch")
				}
			}
		})
	}
}

func TestOffloadBatchFailuresNeverBecomeMajority(t *testing.T) {
	for _, mode := range []string{"lost", "legacy", "denied", "short", "wrong-node", "wrong-record", "zero-cut", "duplicate-store", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			store, refs, fake := offloadFixture(t, 4, 32)
			fake.mode = mode
			plan := repairTestPlan(t)
			observe, err := observeOffloadPage(plan, refs, store, fake)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			var report LivePageReport
			err = processRepairReferenceObserved(ctx, plan, refs[0], 1, store, fake, observe, 64, &report)
			switch mode {
			case "legacy":
				if err != nil || report.Offloaded != 1 || fake.singleCalls != 2 {
					t.Fatal(report, err)
				}
			case "lost":
				if err != nil || report.Offloaded != 0 || report.Fetched != 1 {
					t.Fatal("lost majority did not trigger actual repair", report, err)
				}
				if _, err := store.Verify(ctx, refs[0].Descriptor); err != nil {
					t.Fatal(err)
				}
			default:
				if err == nil || report.Offloaded != 0 || len(fake.fetchedFrom) != 0 {
					t.Fatal("bad observation accepted or silently retried", report, err)
				}
			}
		})
	}
}

func TestOffloadBatchExpiresAtPageBoundary(t *testing.T) {
	store, refs, fake := offloadFixture(t, 4, 32)
	plan := repairTestPlan(t)
	for run := 0; run < 2; run++ {
		observe, err := observeOffloadPage(plan, refs, store, fake)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := keepCapacityOffloadObserved(context.Background(), plan, refs[0].Descriptor, 1, store, observe); err != nil {
			t.Fatal(err)
		}
	}
	if fake.batchCalls != 4 {
		t.Fatal("observation survived a page boundary", fake.batchCalls)
	}
}

func TestOffloadBatchBoundsCacheAndRejectsExpiredContext(t *testing.T) {
	store, refs, fake := offloadFixture(t, 33, 32)
	plan := repairTestPlan(t)
	if _, err := observeOffloadPage(plan, refs, store, fake); err == nil {
		t.Fatal("oversized page accepted")
	}
	observe, err := observeOffloadPage(plan, refs[:1], store, fake)
	if err != nil {
		t.Fatal(err)
	}
	for target := uint64(2); target <= 10; target++ {
		_, _ = observe(context.Background(), target, refs[0].Descriptor)
	}
	if fake.batchCalls != 8 || fake.singleCalls != 1 {
		t.Fatal("unbounded target cache", fake.batchCalls, fake.singleCalls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := observe(ctx, 2, refs[0].Descriptor); !errors.Is(err, context.Canceled) {
		t.Fatal("expired context used cached proof", err)
	}
}
