package raftrpc

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	transferWaiterConcurrency = 2
	transferQueueTimeout      = 15 * time.Second
)

// acquireQueuedSlot keeps active transfer concurrency fixed while allowing a
// small bounded queue. Callers beyond the queue are rejected immediately;
// queued callers remain subject to their request deadline.
func acquireQueuedSlot(ctx context.Context, active, waiters chan struct{}, timeout time.Duration,
	busyMessage string,
) (func(), error) {
	if ctx == nil {
		return nil, status.Error(codes.InvalidArgument, "transfer admission requires a context")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	select {
	case active <- struct{}{}:
		return func() { <-active }, nil
	default:
	}
	if waiters == nil {
		return nil, status.Error(codes.ResourceExhausted, busyMessage)
	}
	select {
	case waiters <- struct{}{}:
	default:
		return nil, status.Error(codes.ResourceExhausted, busyMessage)
	}
	defer func() { <-waiters }()
	queued, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case active <- struct{}{}:
		return func() { <-active }, nil
	case <-queued.Done():
		return nil, status.FromContextError(queued.Err()).Err()
	}
}
