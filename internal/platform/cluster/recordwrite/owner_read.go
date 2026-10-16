package recordwrite

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

// An Owner follower can lag the ReadIndex it just confirmed. Wait for its
// local state machine to apply that cut without weakening the read barrier.
func readOwner(ctx context.Context, barrier Barrier, owner *enginestate.State, target router.Target,
	read func(enginestate.ReadView) error) error {
	for {
		_, err := owner.ReadRouted(ctx, barrier, target, read)
		if !errors.Is(err, enginestate.ErrStateBehind) {
			return err
		}
		if err := pauseRead(ctx); err != nil {
			return err
		}
	}
}
