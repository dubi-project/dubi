package recordwrite

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
)

type delayedLocalOwnerBarrier struct{ Barrier }

func (b delayedLocalOwnerBarrier) ReadIndex(ctx context.Context, group string) (uint64, error) {
	if group != raft.RootGroupID {
		timer := time.NewTimer(3 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
	return b.Barrier.ReadIndex(ctx, group)
}

func TestLocalRepairBatchesOwnerCutsAndPreservesEveryReceipt(t *testing.T) {
	var directCalls int
	for _, batched := range []bool{false, true} {
		w, refs, b, _, _ := repairBatchFixture(t, 32)
		w.Barrier = delayedLocalOwnerBarrier{w.Barrier}
		single := w.Submit
		var mu sync.Mutex
		batches, commands, maxBatch := 0, 0, 0
		w.SubmitBatch = func(ctx context.Context, g string, cs [][]byte) ([]error, error) {
			mu.Lock()
			batches++
			commands += len(cs)
			maxBatch = max(maxBatch, len(cs))
			mu.Unlock()
			rows := make([]error, len(cs))
			for i, c := range cs {
				rows[i] = single(ctx, g, c)
			}
			return rows, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ctx = router.WithConfirmedRootIndex(ctx, 5)
		if batched {
			failures, err := w.RefreshLocalConfirmationPage(ctx, refs)
			if err != nil || len(failures) != 0 {
				t.Fatal(failures, err)
			}
			if commands != len(refs) || maxBatch < 2 || maxBatch > raft.SubmitBatchEntries {
				t.Fatal("local proposals were not bounded batches", batches, commands, maxBatch)
			}
			if b.ownerCalls >= directCalls/2 {
				t.Fatal("Owner cuts were not amortized", directCalls, b.ownerCalls)
			}
			// Reconciliation must see all independently persisted receipts as current.
			pending, err := w.PendingLocalConfirmations(ctx, refs)
			if err != nil || len(pending) != 0 {
				t.Fatal("missing durable per-row confirmations", len(pending), err)
			}
			t.Logf("Owner cuts direct=%d batched=%d; proposal batches=%d commands=%d max_batch=%d", directCalls, b.ownerCalls-1, batches, commands, maxBatch)
		} else {
			var wg sync.WaitGroup
			failures := make(chan error, len(refs))
			jobs := make(chan int)
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range jobs {
						_, err := w.EnsureCurrentLocalConfirmation(ctx, refs[i])
						failures <- err
					}
				}()
			}
			for i := range refs {
				jobs <- i
			}
			close(jobs)
			wg.Wait()
			close(failures)
			for err := range failures {
				if err != nil {
					t.Fatal(err)
				}
			}
			directCalls = b.ownerCalls
		}
		cancel()
	}
}

func TestLocalRepairBatchRejectsPartialOrUnappliedResults(t *testing.T) {
	for _, mode := range []string{"partial", "ack-only", "wrong-count", "quorum-loss"} {
		t.Run(mode, func(t *testing.T) {
			w, refs, _, _, _ := repairBatchFixture(t, 8)
			w.Barrier = delayedLocalOwnerBarrier{w.Barrier}
			single := w.Submit
			rejected := 0
			w.SubmitBatch = func(ctx context.Context, g string, cs [][]byte) ([]error, error) {
				if mode == "wrong-count" {
					return nil, nil
				}
				if mode == "quorum-loss" {
					return nil, errors.New("Owner quorum unavailable")
				}
				result := make([]error, len(cs))
				for i, c := range cs {
					if mode == "ack-only" {
						continue
					}
					if rejected == 0 {
						result[i] = errors.New("row rejected")
						rejected++
						continue
					}
					result[i] = single(ctx, g, c)
				}
				return result, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			failures, err := w.RefreshLocalConfirmationPage(ctx, refs)
			if mode == "partial" {
				if err != nil || len(failures) != 1 {
					t.Fatal("partial result lost", len(failures), err)
				}
			} else if err == nil && len(failures) == 0 {
				t.Fatal("unsafe batch accepted")
			}
			pending, checkErr := w.PendingLocalConfirmations(context.Background(), refs)
			want := len(refs)
			if mode == "partial" {
				want = 1
			}
			if checkErr != nil || len(pending) != want {
				t.Fatal("unconfirmed rows lost", len(pending), want, checkErr)
			}
		})
	}
}
