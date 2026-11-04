package recordrepair

import (
	"context"
	"errors"
	"fmt"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/diskbudget"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

type batchSourceClient interface {
	FetchRecords(context.Context, rootstate.Node, []records.Descriptor, *records.Store) error
	RepairRecords(context.Context, rootstate.Node, []records.Descriptor, *records.Store) error
}

// Prefill only a bounded, quorum-read reference page. The caller subsequently
// runs the ordinary per-record checks and revalidates the Root cohort. Copies
// are immutable bytes, never new logical references or durability receipts.
func prefillRepairPage(ctx context.Context, plan placement.LocalPlan, refs []enginestate.RecordReference,
	local uint64, store *records.Store, client batchSourceClient) (map[string]bool, error) {
	filled := make(map[string]bool)
	var pending []records.Descriptor
	var size uint64
	relocate := false
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		nodes := plan.Nodes()
		remaining := len(nodes) - 1
		for _, source := range nodes {
			if source.RaftID == local {
				continue
			}
			attempt, cancel := sourceAttemptContext(ctx, remaining)
			remaining--
			var err error
			if relocate {
				err = client.RepairRecords(attempt, source, pending, store)
			} else {
				err = client.FetchRecords(attempt, source, pending, store)
			}
			cancel()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == nil {
				break
			}
			if errors.Is(err, volume.ErrNoWritableVolume) || errors.Is(err, diskbudget.ErrSpace) {
				break // Let ordinary repair select another current destination.
			}
			if !retryableRecordSource(err) {
				return fmt.Errorf("prefill live repair page from Node %d: %w", source.RaftID, err)
			}
		}
		// A partial local write or disjoint source holdings may leave rows
		// missing. The ordinary single-record path handles those individually.
		for _, descriptor := range pending {
			if _, err := store.Verify(ctx, descriptor); err == nil {
				id, _ := descriptor.ID()
				filled[id] = true
			}
		}
		pending, size = nil, 0
		return nil
	}
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		d := ref.Descriptor
		if d.Size > raftrpc.FetchRecordBatchBytes {
			continue // Large records retain the existing framed transfer path.
		}
		if _, offloaded, err := store.CapacityOffload(d); err != nil || offloaded {
			continue // Preserve the ordinary fresh retention/majority checks.
		}
		_, verifyErr := store.Verify(ctx, d)
		if verifyErr == nil {
			continue
		}
		repair := !errors.Is(verifyErr, records.ErrNotHeld)
		if repair {
			needed, err := store.RelocationNeeded(ctx, d)
			if err != nil || !needed {
				continue // Never replace a corrupt index or unavailable Store.
			}
		}
		if len(pending) != 0 && (relocate != repair || len(pending) == raftrpc.FetchRecordBatchLimit || d.Size > raftrpc.FetchRecordBatchBytes-size) {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		relocate = repair
		pending = append(pending, d)
		size += d.Size
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return filled, nil
}
