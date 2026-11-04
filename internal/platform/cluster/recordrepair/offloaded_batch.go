package recordrepair

import (
	"context"
	"errors"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type offloadBatchClient interface {
	ObserveRecordHoldings(context.Context, uint64, []records.Descriptor) ([]raftrpc.RecordHoldingResult, error)
}

func offloadPageSize(refs []enginestate.RecordReference) int {
	var size uint64
	for i, ref := range refs {
		if ref.Descriptor.Size > raftrpc.FetchRecordBatchBytes {
			return max(1, i) // Oversized records use a single observation.
		}
		if i == raftrpc.FetchRecordBatchLimit || ref.Descriptor.Size > raftrpc.FetchRecordBatchBytes-size {
			return i
		}
		size += ref.Descriptor.Size
	}
	return len(refs)
}

// Cache only this bounded page's offloaded descriptors, for at most eight
// targets. Every row still passes keepCapacityOffloadObserved's exact identity,
// record and distinct-Store majority checks. The caller discards this observer
// before the next page; normal repair and retries always make fresh requests.
func observeOffloadPage(plan placement.LocalPlan, refs []enginestate.RecordReference,
	store *records.Store, client SourceClient) (offloadObserve, error) {
	batch, ok := client.(offloadBatchClient)
	if !ok {
		return client.ObserveRecordHolding, nil
	}
	if len(refs) == 0 || offloadPageSize(refs) != len(refs) {
		return nil, errors.New("offload observation page exceeds bounds")
	}
	var ds []records.Descriptor
	indices := make(map[string]int)
	for _, ref := range refs {
		d := ref.Descriptor
		scope, offloaded, err := store.CapacityOffload(d)
		if err != nil {
			return nil, err
		}
		if !offloaded || scope != plan.RetentionScope() || d.Size > raftrpc.FetchRecordBatchBytes {
			continue
		}
		id, err := d.ID()
		if err != nil {
			return nil, err
		}
		if _, seen := indices[id]; seen {
			continue
		}
		d.Key = slices.Clone(d.Key)
		indices[id] = len(ds)
		ds = append(ds, d)
	}
	type answer struct {
		rows []raftrpc.RecordHoldingResult
		err  error
	}
	answers := make(map[uint64]answer)
	return func(ctx context.Context, target uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
		if err := ctx.Err(); err != nil {
			return raftrpc.HoldingObservation{}, err
		}
		id, err := d.ID()
		if err != nil {
			return raftrpc.HoldingObservation{}, err
		}
		i, found := indices[id]
		if !found {
			return client.ObserveRecordHolding(ctx, target, d)
		}
		a, found := answers[target]
		if !found {
			if len(answers) == 8 {
				return client.ObserveRecordHolding(ctx, target, d)
			}
			a.rows, a.err = batch.ObserveRecordHoldings(ctx, target, ds)
			if a.err == nil && len(a.rows) != len(ds) {
				a.err = errors.New("offload holding batch omitted rows")
			}
			answers[target] = a
		}
		if status.Code(a.err) == codes.Unimplemented {
			return client.ObserveRecordHolding(ctx, target, d)
		}
		if a.err != nil {
			return raftrpc.HoldingObservation{}, a.err
		}
		if err := ctx.Err(); err != nil {
			return raftrpc.HoldingObservation{}, err
		}
		return a.rows[i].Observation, a.rows[i].Err
	}, nil
}
