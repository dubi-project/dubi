package recordwrite

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
)

const localConfirmationRepairPageLimit = 64

// ReconcileLocalConfirmationPage checks and refreshes one background page
// under the same Root cut. Filtering already-current receipts retains its
// single Owner barrier. Every refreshed publication still runs the complete
// physical-majority and durable-result checks. A fresh final Root/plan check
// gates returning the page; neither this cut nor its results authorize reads
// or writes outside this call.
func (w Writer) ReconcileLocalConfirmationPage(ctx context.Context, refs []enginestate.RecordReference) ([]error, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	if ctx == nil || w.Barrier == nil || w.Root == nil || w.Owner == nil || len(refs) > localConfirmationRepairPageLimit {
		return nil, errors.New("local confirmation reconciliation requires a bounded page and complete runtime")
	}
	index, err := w.Barrier.ReadIndex(ctx, raft.RootGroupID)
	if err != nil {
		return nil, err
	}
	before, err := placement.ReadLocalPlanAtCut(index, w.Root, w.Owner.GroupID())
	if err != nil {
		return nil, err
	}
	child := w
	child.Barrier = reconfirmationPageCut{Barrier: w.Barrier, index: index}
	work := router.WithConfirmedRootIndex(ctx, index)
	pending, err := child.PendingLocalConfirmations(work, refs)
	if err != nil {
		return nil, fmt.Errorf("check local confirmation page: %w", err)
	}
	failures, err := child.RefreshLocalConfirmationPage(work, pending)
	if err != nil {
		return nil, fmt.Errorf("refresh local confirmation page: %w", err)
	}
	after, err := placement.ReadLocalPlan(ctx, w.Barrier, w.Root, w.Owner.GroupID())
	if err != nil {
		return nil, err
	}
	if !before.Matches(after) {
		return nil, errors.New("Region plan changed during local confirmation reconciliation")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return failures, nil
}

// RefreshLocalConfirmationPage is for bounded background repair, not a write
// ACK or a read authorization. Every row still verifies its exact publication,
// physical majority, live Owner/Range fence and durable confirmation result.
// Root cuts are shared only within this call and freshly rechecked at the end.
// A failed final check leaves safe, individually durable metadata behind, but
// must not advance a repair cursor or clear debt. Foreground reads independently
// require fresh Root/Owner currentness and actual physical majority as before.
func (w Writer) RefreshLocalConfirmationPage(ctx context.Context, refs []enginestate.RecordReference) ([]error, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	if ctx == nil || w.Barrier == nil || w.Root == nil || w.Owner == nil || len(refs) > localConfirmationRepairPageLimit {
		return nil, errors.New("local confirmation repair requires a bounded page and complete runtime")
	}
	index, err := w.Barrier.ReadIndex(ctx, raft.RootGroupID)
	if err != nil {
		return nil, err
	}
	before, err := placement.ReadLocalPlanAtCut(index, w.Root, w.Owner.GroupID())
	if err != nil {
		return nil, err
	}
	child := w
	child.Barrier = reconfirmationPageCut{Barrier: w.Barrier, index: index}
	work := router.WithConfirmedRootIndex(ctx, index)
	// Overlap independent per-row Raft waits under the same bounded IO and
	// memory policy as Geo confirmation pages. One slow publication must not
	// serialize every healthy row; each row still obtains its own durable
	// result, and all workers finish before the final Root check or return.
	if geoConfirmationWorkers(refs) > 1 {
		ownerBatch := newOwnerConfirmationBatch(work, w.Barrier, w.Owner.GroupID())
		defer ownerBatch.close()
		child.Barrier = reconfirmationPageCut{Barrier: ownerBatch, index: index}
		if w.SubmitBatch != nil {
			submits := newOwnerSubmitBatch(work, w)
			defer submits.close()
			child.Submit = submits.Submit
		}
	}
	results := make([]error, len(refs))
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range geoConfirmationWorkers(refs) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				_, results[i] = child.EnsureCurrentLocalConfirmation(work, refs[i])
			}
		}()
	}
schedule:
	for i := range refs {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break schedule
		}
	}
	close(jobs)
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var failures []error
	for i, err := range results {
		if err != nil {
			failures = append(failures, fmt.Errorf("reconfirm %s: %w", refs[i].RequestID, err))
		}
	}
	after, err := placement.ReadLocalPlan(ctx, w.Barrier, w.Root, w.Owner.GroupID())
	if err != nil {
		return nil, err
	}
	if !before.Matches(after) {
		return nil, errors.New("Region plan changed during local confirmation repair page")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return failures, nil
}

type reconfirmationPageCut struct {
	Barrier
	index uint64
}

func (c reconfirmationPageCut) ReadIndex(ctx context.Context, group string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if group == raft.RootGroupID {
		if c.index == 0 {
			return 0, errors.New("local confirmation page lacks a confirmed Root cut")
		}
		return c.index, nil
	}
	return c.Barrier.ReadIndex(ctx, group)
}
