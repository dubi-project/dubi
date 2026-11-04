package recordrepair

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// A local hint is never a reusable durability receipt. Recheck the fixed
// cohort's actual other-node majority on every sweep; if it is lost or the
// plan changes, clear the hint and let normal Repair restore this Node too.
func keepCapacityOffload(ctx context.Context, plan placement.LocalPlan, descriptor records.Descriptor, local uint64,
	destination *records.Store, client SourceClient) (bool, error) {
	return keepCapacityOffloadObserved(ctx, plan, descriptor, local, destination, client.ObserveRecordHolding)
}

type offloadObserve func(context.Context, uint64, records.Descriptor) (raftrpc.HoldingObservation, error)

func keepCapacityOffloadObserved(ctx context.Context, plan placement.LocalPlan, descriptor records.Descriptor, local uint64,
	destination *records.Store, observe offloadObserve) (bool, error) {
	scope, offloaded, err := destination.CapacityOffload(descriptor)
	if err != nil || !offloaded {
		return false, err
	}
	if scope == plan.RetentionScope() {
		id, _ := descriptor.ID()
		var holdings []placement.Holding
		nodes := plan.Nodes()
		remaining := len(nodes) - 1
		for _, node := range nodes {
			if node.RaftID == local {
				continue
			}
			attempt, cancel := sourceAttemptContext(ctx, remaining)
			remaining--
			observed, err := observe(attempt, node.RaftID, descriptor)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return false, ctx.Err()
				}
				if !retryableRecordSource(err) {
					return false, err
				}
				continue
			}
			actualID, err := observed.Holding.Descriptor.ID()
			if err != nil || actualID != id || observed.NodeID != node.NodeID || observed.RaftID != node.RaftID ||
				observed.RootConfirmedIndex == 0 || observed.Holding.RecordID != id {
				return false, errors.New("offload keeper observation differs from the current Node/record")
			}
			holdings = append(holdings, placement.Holding{RaftID: node.RaftID, NodeID: node.NodeID,
				StoreID: observed.Holding.StoreID, RecordID: id})
			count, err := plan.Count(id, holdings)
			if err != nil {
				return false, err
			}
			if count >= plan.Required {
				return true, nil
			}
		}
	}
	return false, destination.ClearCapacityOffload(ctx, descriptor)
}
