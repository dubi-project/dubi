package recordrepair

import (
	"context"
	"errors"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type candidateRemoteBatch func(context.Context, uint64, []records.Descriptor) ([]raftrpc.RecordHoldingResult, error)

// Buffer descriptor metadata only, with the existing 32-row/4-MiB RPC bounds.
// The original verifier still checks every local record and exact remote
// identity/descriptor/Store before counting its majority. A page's remote
// observations are discarded before walking the next page. No concurrency,
// retention claim, durable cursor or generation-publication shortcut is added.
func verifyCandidateReferencesBatched(ctx context.Context, cut enginestate.RecordCut, candidates []rootstate.Node, local uint64,
	verify candidateLocalVerify, barrier PendingBarrier, single candidateRemoteObserve, batch candidateRemoteBatch, walk cutWalker,
) (Report, error) {
	if ctx == nil || barrier == nil || single == nil || batch == nil || walk == nil {
		return Report{}, errors.New("candidate batch verification requires bounded authenticated observers")
	}
	budget := &candidateObservationCut{barrier: barrier, minimum: cut.Seal.Intent.Next.PlannedIndex}
	observeSingle := func(call context.Context, target uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
		work, err := budget.admit(call, 1)
		if err != nil {
			return raftrpc.HoldingObservation{}, err
		}
		return single(work, target, d)
	}
	var current candidateRemoteObserve
	observe := func(call context.Context, target uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
		if current == nil {
			return raftrpc.HoldingObservation{}, errors.New("candidate observation outside its page")
		}
		return current(call, target, d)
	}
	pagedWalk := func(visit func(enginestate.RecordReference) error) error {
		var page []enginestate.RecordReference
		var size uint64
		seen := map[string]bool{}
		flush := func() error {
			if len(page) == 0 {
				return ctx.Err()
			}
			current = candidateBatchPage(page, budget, observeSingle, batch)
			defer func() { current = nil }()
			for _, ref := range page {
				if err := visit(ref); err != nil {
					return err
				}
			}
			page, size, seen = nil, 0, map[string]bool{}
			return ctx.Err()
		}
		err := walk(func(ref enginestate.RecordReference) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			d := ref.Descriptor
			id, err := d.ID()
			if err != nil {
				return err
			}
			if d.Size > raftrpc.FetchRecordBatchBytes {
				if err := flush(); err != nil {
					return err
				}
				current = observeSingle
				err := visit(ref)
				current = nil
				return err
			}
			if len(page) == raftrpc.FetchRecordBatchLimit || d.Size > raftrpc.FetchRecordBatchBytes-size || seen[id] {
				if err := flush(); err != nil {
					return err
				}
			}
			// The manifest walker can reuse its decoded frame after this callback.
			d.Key = slices.Clone(d.Key)
			page = append(page, enginestate.RecordReference{Descriptor: d})
			size += d.Size
			seen[id] = true
			return nil
		})
		if err != nil {
			return err
		}
		return flush()
	}
	return verifyCandidateReferences(ctx, cut, candidates, local, verify, observe, pagedWalk)
}

func candidateBatchPage(page []enginestate.RecordReference, budget *candidateObservationCut,
	single candidateRemoteObserve, batch candidateRemoteBatch) candidateRemoteObserve {
	ds := make([]records.Descriptor, len(page))
	indices := make(map[string]int, len(page))
	for i, ref := range page {
		ds[i] = ref.Descriptor
		id, _ := ds[i].ID()
		indices[id] = i
	}
	type answer struct {
		rows []raftrpc.RecordHoldingResult
		err  error
	}
	// A large candidate cohort must not turn bounded page metadata into an
	// unbounded per-node cache. Further targets retain the single-row path.
	answers := map[uint64]answer{}
	return func(ctx context.Context, target uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
		if err := ctx.Err(); err != nil {
			return raftrpc.HoldingObservation{}, err
		}
		id, err := d.ID()
		i, found := indices[id]
		if err != nil || !found {
			return raftrpc.HoldingObservation{}, errors.New("candidate descriptor outside its observation page")
		}
		a, found := answers[target]
		if !found {
			if len(answers) == 8 {
				return single(ctx, target, d)
			}
			work, err := budget.admit(ctx, len(ds))
			if err != nil {
				return raftrpc.HoldingObservation{}, err
			}
			a.rows, a.err = batch(work, target, ds)
			if a.err == nil && len(a.rows) != len(ds) {
				a.err = errors.New("candidate holding batch omitted rows")
			}
			answers[target] = a
		}
		if status.Code(a.err) == codes.Unimplemented {
			return single(ctx, target, d)
		}
		if a.err != nil {
			return raftrpc.HoldingObservation{}, a.err
		}
		if err := ctx.Err(); err != nil {
			return raftrpc.HoldingObservation{}, err
		}
		return a.rows[i].Observation, a.rows[i].Err
	}
}
