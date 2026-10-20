package enginedata

import (
	"context"
	"errors"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/raft"
)

type routeLocalRead func(context.Context, string) (uint64, error)

func (read routeLocalRead) ReadIndex(ctx context.Context, group string) (uint64, error) {
	return read(ctx, group)
}

func TestRootRouteBarrierUsesRuntimeConfirmedCut(t *testing.T) {
	calls := 0
	barrier := rootRouteBarrier{local: routeLocalRead(func(_ context.Context, group string) (uint64, error) {
		calls++
		if group != raft.RootGroupID {
			t.Fatalf("Runtime Root reader was called for wrong Group %q", group)
		}
		return 10, nil
	})}
	index, err := barrier.ReadIndex(context.Background(), raft.RootGroupID)
	if err != nil || index != 10 || calls != 1 {
		t.Fatal("Runtime-confirmed Root cut was not used", index, calls, err)
	}
}

func TestRootRouteBarrierPreservesRetryableRuntimeFailure(t *testing.T) {
	barrier := rootRouteBarrier{local: routeLocalRead(func(context.Context, string) (uint64, error) {
		return 0, raft.ErrNoLeader
	})}
	if index, err := barrier.ReadIndex(context.Background(), raft.RootGroupID); index != 0 || !errors.Is(err, raft.ErrNoLeader) {
		t.Fatal("Runtime Root failure was hidden or changed", index, err)
	}
}

func TestRootRouteBarrierRejectsEmptyCut(t *testing.T) {
	barrier := rootRouteBarrier{local: routeLocalRead(func(context.Context, string) (uint64, error) {
		return 0, nil
	})}
	if index, err := barrier.ReadIndex(context.Background(), raft.RootGroupID); index != 0 || err == nil {
		t.Fatal("empty Root cut was accepted", index, err)
	}
}
