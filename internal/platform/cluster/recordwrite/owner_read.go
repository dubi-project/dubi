package recordwrite

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

// An Owner follower can lag the ReadIndex it just confirmed. Wait for its
// local state machine to apply that cut without weakening the read barrier.
func readOwner(ctx context.Context, barrier Barrier, owner *capstate.State, target router.Target,
	read func(capstate.ReadView) error) error {
	for {
		_, err := owner.ReadRouted(ctx, barrier, target, read)
		if !errors.Is(err, capstate.ErrStateBehind) {
			return err
		}
		if err := pauseRead(ctx); err != nil {
			return err
		}
	}
}
