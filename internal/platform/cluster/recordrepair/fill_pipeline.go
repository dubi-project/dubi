package recordrepair

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
)

// Overlap two small pages using the existing two transfer slots and shared
// buffer/IO budgets. Pages retain the original per-record verification and
// source fallback protocol. An oversized record drains both pages before its
// ordinary single-record transfer. No progress or holding proof is cached.
func fillModePipelined(ctx context.Context, oldIDs, nextIDs []uint64, local uint64,
	sources []rootstate.Node, recovery bool, walk cutWalker, verify verifier, fetch fetcher, batch batchFetcher,
) (Report, error) {
	if walk == nil {
		return Report{}, errors.New("record fill requires a cut walker")
	}
	// Validate the same sealed source/destination set before scheduling any IO,
	// including for an empty cut. The ordinary page path rechecks it as well.
	if _, err := fillModeBatched(ctx, oldIDs, nextIDs, local, sources, recovery,
		func(func(enginestate.RecordReference) error) error { return nil }, verify, fetch, batch); err != nil {
		return Report{}, err
	}
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan []enginestate.RecordReference)
	var workers, outstanding sync.WaitGroup
	var mu sync.Mutex
	var report Report
	var firstErr error
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cancel()
	}
	process := func(page []enginestate.RecordReference) {
		if work.Err() != nil {
			return
		}
		result, err := fillModeBatched(work, oldIDs, nextIDs, local, sources, recovery,
			func(visit func(enginestate.RecordReference) error) error {
				for _, ref := range page {
					if err := visit(ref); err != nil {
						return err
					}
				}
				return nil
			}, verify, fetch, batch)
		if err != nil {
			fail(err)
			return
		}
		mu.Lock()
		if result.References > ^uint64(0)-report.References || result.ReferencedBytes > ^uint64(0)-report.ReferencedBytes ||
			result.AlreadyHeld > ^uint64(0)-report.AlreadyHeld || result.Fetched > ^uint64(0)-report.Fetched {
			if firstErr == nil {
				firstErr = errors.New("pipelined record fill summary overflow")
			}
			cancel()
		} else {
			report.References += result.References
			report.ReferencedBytes += result.ReferencedBytes
			report.AlreadyHeld += result.AlreadyHeld
			report.Fetched += result.Fetched
		}
		mu.Unlock()
	}
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for page := range jobs {
				process(page)
				outstanding.Done()
			}
		}()
	}
	var page []enginestate.RecordReference
	var size uint64
	flush := func() error {
		if len(page) == 0 {
			return work.Err()
		}
		if err := work.Err(); err != nil {
			return err
		}
		outstanding.Add(1)
		select {
		case jobs <- page:
			page, size = nil, 0 // Ownership passes to the worker.
			return nil
		case <-work.Done():
			outstanding.Done()
			return work.Err()
		}
	}
	err := walk(func(ref enginestate.RecordReference) error {
		if err := work.Err(); err != nil {
			return err
		}
		d := ref.Descriptor
		if err := d.Validate(); err != nil {
			return err
		}
		if d.Size > raftrpc.FetchRecordBatchBytes {
			if err := flush(); err != nil {
				return err
			}
			outstanding.Wait()
			if err := work.Err(); err != nil {
				return err
			}
			process([]enginestate.RecordReference{{Descriptor: d}})
			return work.Err()
		}
		if len(page) == raftrpc.FetchRecordBatchLimit || d.Size > raftrpc.FetchRecordBatchBytes-size {
			if err := flush(); err != nil {
				return err
			}
		}
		// Record-cut walkers may reuse their frame after the callback returns.
		d.Key = slices.Clone(d.Key)
		page = append(page, enginestate.RecordReference{Descriptor: d})
		size += d.Size
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		fail(err)
	}
	close(jobs)
	workers.Wait() // Canceled RPCs/Store writes retain their ownership until done.
	if firstErr != nil {
		return Report{}, firstErr
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	return report, nil
}
