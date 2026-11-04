package recordwrite

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
)

// GeoConfirmationRepairPageLimit amortizes the two Root barriers across a
// bounded background page. It is independent of the IO worker count.
const GeoConfirmationRepairPageLimit = 32
const geoConfirmationConcurrentPageBytes = 4 << 20
const geoConfirmationPageConcurrency = 8

// GeoConfirmationRepairPage selects a bounded prefix for background workers.
// Only small publications share the enlarged page. Larger payloads retain the
// previous eight-reference page, so enlarging metadata batches does not turn
// a formerly concurrent page into a long sequential payload scan.
func GeoConfirmationRepairPage(refs []enginestate.RecordReference) []enginestate.RecordReference {
	page := refs[:min(GeoConfirmationRepairPageLimit, len(refs))]
	if len(page) > geoConfirmationPageConcurrency && geoConfirmationWorkers(page) == 1 {
		return page[:geoConfirmationPageConcurrency]
	}
	return page
}

// GeoConfirmationRefresh describes one background attempt. Confirmed is true
// only after fresh physical Region majorities and a durable exact receipt.
// An already-current historical receipt is false: it cannot clear a physical
// repair task without a separate actual holding observation.
type GeoConfirmationRefresh struct {
	Confirmed bool
	Err       error
}

// RefreshGeoConfirmationPage shares Root barriers within one bounded background
// page. Owner reads, physical observations, mirrors and durable results remain
// per publication. A failed final Root/placement check returns no results, so
// callers cannot clear debt from a page whose authority changed during IO.
func (w Writer) RefreshGeoConfirmationPage(ctx context.Context, refs []enginestate.RecordReference,
	maxRegions, maxNodes int,
) ([]GeoConfirmationRefresh, error) {
	return w.refreshGeoConfirmationPage(ctx, refs, maxRegions, maxNodes, 0)
}

// RefreshGeoConfirmationPageAtCut continues the bounded discovery operation
// that just obtained index from a Root quorum. This is an admission cut, not a
// cached authorization: callers may pass it to only the first page in that
// same attempt. Physical observations and exact durable results remain
// mandatory, and the final Root barrier always goes to w.Barrier afresh.
// A retry or later page must use RefreshGeoConfirmationPage instead.
func (w Writer) RefreshGeoConfirmationPageAtCut(ctx context.Context, refs []enginestate.RecordReference,
	maxRegions, maxNodes int, index uint64,
) ([]GeoConfirmationRefresh, error) {
	if index == 0 {
		return nil, errors.New("Geo confirmation discovery has no confirmed Root cut")
	}
	return w.refreshGeoConfirmationPage(ctx, refs, maxRegions, maxNodes, index)
}

func (w Writer) refreshGeoConfirmationPage(ctx context.Context, refs []enginestate.RecordReference,
	maxRegions, maxNodes int, index uint64,
) ([]GeoConfirmationRefresh, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	root, ok := w.Root.(rangeGeoRoot)
	if ctx == nil || w.Barrier == nil || w.Owner == nil || !ok || len(refs) > GeoConfirmationRepairPageLimit {
		return nil, errors.New("Geo confirmation repair requires a bounded page and complete runtime")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if index == 0 {
		var err error
		index, err = w.Barrier.ReadIndex(ctx, raft.RootGroupID)
		if err != nil {
			return nil, err
		}
	}
	type cut struct {
		mapping rootstate.RangePlacement
		plan    placement.GeoPlan
	}
	before := make(map[string]cut)
	for _, ref := range refs {
		if ref.OwnerGroupID != w.Owner.GroupID() || ref.Descriptor.Validate() != nil || ref.Index == 0 {
			return nil, errors.New("Geo confirmation page contains an invalid or foreign publication")
		}
		if _, found := before[ref.RangeID]; found {
			continue
		}
		mapping, plan, err := placement.ReadRangeGeoPlanAtCut(index, root, ref.RangeID, maxRegions, maxNodes)
		if err != nil {
			return nil, err
		}
		before[ref.RangeID] = cut{mapping, plan}
	}
	child := w
	work := router.WithConfirmedRootIndex(ctx, index)
	child.Barrier = reconfirmationPageCut{Barrier: w.Barrier, index: index}
	child.Observer = geoPageHoldingObserver(work, w.Observer, refs, func(rangeID string) placement.GeoPlan { return before[rangeID].plan })
	results := make([]GeoConfirmationRefresh, len(refs))
	if _, batching := w.MirrorGeo.(GeoMetadataMirrorBatchTransport); batching && geoConfirmationWorkers(refs) > 1 {
		// All initial routed reads follow one newly acquired Owner cut. This cut
		// is used ONLY to prepare references, never to acknowledge later commands.
		ownerCut, err := w.Barrier.ReadIndex(work, w.Owner.GroupID())
		if err != nil {
			return nil, err
		}
		if ownerCut == 0 {
			return nil, errors.New("Geo preparation has no confirmed Owner cut")
		}
		preparation := child
		preparation.Barrier = reconfirmationPageCut{Barrier: ownerPageReadCut{group: w.Owner.GroupID(), index: ownerCut}, index: index}
		ready := make([]*geoConfirmationReady, len(refs))
		runGeoPageWorkers(work, len(refs), geoConfirmationWorkers(refs), func(i int) {
			ready[i], results[i].Err = preparation.prepareGeoConfirmation(work, refs[i], maxRegions, maxNodes)
		})
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var completed *geoMirrorCompletions
		if w.MirrorRetry != nil {
			// The prepared page contains only metadata. Persist its retry debt
			// together before allowing the 32 commit workers to deliver anything.
			// Per-publication saves remain in place: if another repair worker
			// deletes an entry meanwhile, delivery records it again safely.
			var mirrors []routedstate.GeoMetadataMirror
			for _, prepared := range ready {
				if prepared != nil {
					mirrors = append(mirrors, prepared.mirrors...)
				}
			}
			started := time.Now()
			err := w.MirrorRetry.saveMirrors(mirrors)
			Trace(work, "repair.mirror_save", started)
			if err != nil {
				return nil, err
			}
			completed = &geoMirrorCompletions{limit: len(mirrors)}
			child.mirrorCompleted = completed.add
		}
		// Only bounded metadata remains. Admit the ready page together instead of
		// serializing its batches behind independent per-record physical IO.
		ownerBatch := newOwnerConfirmationBatch(work, w.Barrier, w.Owner.GroupID())
		defer ownerBatch.close()
		child.Barrier = reconfirmationPageCut{Barrier: ownerBatch, index: index}
		if w.SubmitBatch != nil {
			submits := newOwnerSubmitBatch(work, w)
			defer submits.close()
			child.Submit = submits.Submit
		}
		var closeMirrors func()
		child.MirrorGeo, closeMirrors = newGeoMirrorPagePool(work, w.MirrorGeo)
		defer closeMirrors()
		runGeoPageWorkers(work, len(refs), len(refs), func(i int) {
			if ready[i] != nil {
				results[i].Confirmed, results[i].Err = child.commitGeoConfirmation(work, ready[i])
			}
		})
		// All workers have joined, including on cancellation. Flush only their
		// exact successful mirrors before returning any page results. A failure
		// leaves uncertain cleanup as debt, never as a successful page return.
		if completed != nil {
			started := time.Now()
			err := w.MirrorRetry.deleteMirrors(completed.mirrors)
			Trace(work, "repair.mirror_delete", started)
			if err != nil {
				return nil, errors.Join(ctx.Err(), err)
			}
		}
	} else {
		runGeoPageWorkers(work, len(refs), geoConfirmationWorkers(refs), func(i int) {
			results[i].Confirmed, results[i].Err = child.EnsureCurrentGeoConfirmation(work, refs[i], maxRegions, maxNodes)
		})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	finalIndex, err := w.Barrier.ReadIndex(ctx, raft.RootGroupID)
	if err != nil {
		return nil, err
	}
	for rangeID, previous := range before {
		mapping, plan, err := placement.ReadRangeGeoPlanAtCut(finalIndex, root, rangeID, maxRegions, maxNodes)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(previous.mapping, mapping) || !previous.plan.Matches(plan) {
			return nil, errors.New("Range or Region placement changed during Geo confirmation repair page")
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// At most eight publications overlap their WAN/Raft waits at a time, even
// when more references share the page's Root barriers. The payload-byte gate
// still limits the entire concurrent page to 4 MiB;
// larger pages retain sequential verification. Cancellation joins all workers.
func geoConfirmationWorkers(refs []enginestate.RecordReference) int {
	var total uint64
	for _, ref := range refs {
		if ref.Descriptor.Size > geoConfirmationConcurrentPageBytes-total {
			return 1
		}
		total += ref.Descriptor.Size
	}
	return min(geoConfirmationPageConcurrency, len(refs))
}

// Joins every worker even on cancellation; no page can return while a queued
// confirmation is still using its bounded metadata or the Owner state.
func runGeoPageWorkers(ctx context.Context, count, concurrency int, run func(int)) {
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				run(i)
			}
		}()
	}
schedule:
	for i := 0; i < count; i++ {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break schedule
		}
	}
	close(jobs)
	workers.Wait()
}
