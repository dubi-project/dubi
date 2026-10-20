package enginedata

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/raft"
)

type rootGroupReader interface {
	ReadIndex(context.Context, string) (uint64, error)
}

// rootRouteBarrier delegates to the Runtime's single authoritative Root
// router. Runtime.ReadIndex obtains an authenticated quorum cut through the
// current Root voters and waits for the local Root state to apply it. Doing a
// second, independent voter fanout here duplicates barriers and can consume
// the whole routing deadline before Owner resolution begins.
type rootRouteBarrier struct {
	local rootGroupReader
}

func (barrier rootRouteBarrier) ReadIndex(ctx context.Context, groupID string) (uint64, error) {
	if groupID != raft.RootGroupID {
		return 0, errors.New("engine data route barrier only confirms Root")
	}
	if barrier.local == nil {
		return 0, errors.New("engine data route barrier has no Runtime Root reader")
	}
	index, err := barrier.local.ReadIndex(ctx, groupID)
	if err != nil {
		return 0, err
	}
	if index == 0 {
		return 0, errors.New("Runtime Root reader returned an empty read cut")
	}
	return index, nil
}
