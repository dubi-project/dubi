package enginedata

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
)

const readConfirmationMaxBytes = 4 << 20
const readConfirmationTimeout = 5 * time.Second

// No queue or goroutine: excess reads retain their normal error/Geo/forward
// path while background repair continues. A Service is owned by one Node.
type readConfirmationRefresh struct{ active atomic.Bool }

func (r *readConfirmationRefresh) run(ctx context.Context, size uint64, leader bool,
	refresh func(context.Context) error) (bool, error) {
	if !leader || size > readConfirmationMaxBytes || !r.active.CompareAndSwap(false, true) {
		return false, nil
	}
	defer r.active.Store(false)
	attempt, cancel := context.WithTimeout(ctx, readConfirmationTimeout)
	defer cancel()
	if err := attempt.Err(); err != nil {
		return true, err
	}
	if err := refresh(attempt); err != nil {
		return true, err
	}
	return true, attempt.Err()
}

func (service *Service) refreshReadConfirmation(ctx context.Context, ref enginestate.RecordReference) (bool, error) {
	status, err := service.runtime.Host.Status(ref.OwnerGroupID)
	if err != nil || status.LeaderID != service.runtime.Identity.RaftID {
		return false, nil
	}
	return service.readRefresh.run(ctx, ref.Descriptor.Size, true, func(attempt context.Context) error {
		owner, found := service.runtime.Engines.Resolve(ref.OwnerGroupID)
		if !found || owner == nil {
			return ErrOwnerNotLocal
		}
		writer := recordwrite.Writer{Barrier: engineReadBarrier{runtime: service.runtime, client: service.client},
			Root: service.runtime.Root, Owner: owner, LocalID: service.runtime.Identity.RaftID,
			LocalNodeID: service.runtime.Identity.NodeID, Store: service.runtime.Records, Observer: service.client,
			Submit: func(call context.Context, group string, command []byte) error {
				if err := call.Err(); err != nil {
					return err
				}
				return service.runtime.Host.Submit(group, command)
			}}
		_, err := writer.EnsureCurrentLocalConfirmation(attempt, ref)
		return err
	})
}
