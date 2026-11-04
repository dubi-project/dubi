package recordwrite

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type holdingPageObserver interface {
	ObserveRecordHoldings(context.Context, uint64, []records.Descriptor) ([]raftrpc.RecordHoldingResult, error)
}
type geoHoldingNodePage struct {
	once sync.Once
	rows map[string]raftrpc.RecordHoldingResult
	err  error
}
type geoHoldingPage struct {
	ctx   context.Context
	base  Observer
	batch holdingPageObserver
	refs  []enginestate.RecordReference
	plan  func(string) placement.GeoPlan
	mu    sync.Mutex
	nodes map[uint64]*geoHoldingNodePage
}

// Cache only this bounded page's point-in-time physical observations. Every
// row still verifies its exact Owner publication and commits its own receipt;
// the enclosing page's fresh final Root/plan check gates clearing any debt.
// At most eight target pages retain metadata. More targets use the ordinary
// path; worker count and the aggregate 4 MiB physical payload gate do not grow.
func geoPageHoldingObserver(ctx context.Context, base Observer, refs []enginestate.RecordReference, plan func(string) placement.GeoPlan) Observer {
	batch, ok := base.(holdingPageObserver)
	if !ok || geoConfirmationWorkers(refs) <= 1 {
		return base
	}
	owned := make([]enginestate.RecordReference, len(refs))
	copy(owned, refs)
	for i := range owned {
		owned[i].Descriptor.Key = bytes.Clone(owned[i].Descriptor.Key)
	}
	return &geoHoldingPage{ctx: ctx, base: base, batch: batch, refs: owned, plan: plan, nodes: make(map[uint64]*geoHoldingNodePage)}
}
func (o *geoHoldingPage) ObserveRecordHolding(ctx context.Context, target uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	if err := ctx.Err(); err != nil {
		return raftrpc.HoldingObservation{}, err
	}
	o.mu.Lock()
	page := o.nodes[target]
	if page == nil {
		if len(o.nodes) == geoConfirmationPageConcurrency {
			o.mu.Unlock()
			return o.base.ObserveRecordHolding(ctx, target, d)
		}
		page = &geoHoldingNodePage{}
		o.nodes[target] = page
	}
	o.mu.Unlock()
	page.once.Do(func() {
		var ds []records.Descriptor
		seen := make(map[string]bool)
		for _, ref := range o.refs {
			includes := false
			for _, region := range o.plan(ref.RangeID).Regions() {
				for _, n := range region.Nodes() {
					if n.RaftID == target {
						includes = true
						break
					}
				}
				if includes {
					break
				}
			}
			id, _ := ref.Descriptor.ID()
			if includes && !seen[id] {
				seen[id] = true
				ds = append(ds, ref.Descriptor)
			}
		}
		if len(ds) == 0 {
			page.err = errors.New("holding target is outside this page's Region plans")
			return
		}
		rows, err := o.batch.ObserveRecordHoldings(o.ctx, target, ds)
		if err != nil {
			page.err = err
			return
		}
		if len(rows) != len(ds) {
			page.err = errors.New("holding batch response has a different row count")
			return
		}
		page.rows = make(map[string]raftrpc.RecordHoldingResult, len(ds))
		for i, row := range rows {
			id, _ := ds[i].ID()
			page.rows[id] = row
		}
	})
	if status.Code(page.err) == codes.Unimplemented {
		// Old peers keep the original per-row worker concurrency during a rollout.
		// This is a capability fallback, never a retry after a denied/bad batch.
		return o.base.ObserveRecordHolding(ctx, target, d)
	}
	if page.err != nil {
		return raftrpc.HoldingObservation{}, page.err
	}
	if err := ctx.Err(); err != nil {
		return raftrpc.HoldingObservation{}, err
	}
	id, err := d.ID()
	if err != nil {
		return raftrpc.HoldingObservation{}, err
	}
	row, found := page.rows[id]
	if !found {
		return raftrpc.HoldingObservation{}, errors.New("holding descriptor is outside this page")
	}
	return row.Observation, row.Err
}
