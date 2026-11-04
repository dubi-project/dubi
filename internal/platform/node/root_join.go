package node

import (
	"context"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A single dropped read request/response must not strand startup forever.
// Leader absence, unavailable peers and an expired attempt are retried;
// storage/consensus and authorization failures remain visible. The caller
// owns the whole startup lifetime. Each
// attempt asks Raft for a fresh barrier and still waits for local application.
func confirmStartupRoot(ctx context.Context, read func(context.Context) error, initial time.Duration) error {
	duration := initial
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		attempt, cancel := context.WithTimeout(ctx, duration)
		err := read(attempt)
		expired := errors.Is(attempt.Err(), context.DeadlineExceeded)
		cancel()
		if err == nil {
			return ctx.Err()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !startupRootRetryable(err, expired) {
			return err
		}
		// Avoid a hot retry loop while an election is in progress.
		timer := time.NewTimer(min(initial, 100*time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		duration = min(duration*2, 30*time.Second)
	}
}

// A Root read can aggregate failures from several voters. Retry only when
// every failure is transient; one unavailable peer must not mask a permanent
// authorization, storage or malformed-response failure from another peer.
func startupRootRetryable(err error, expired bool) bool {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !startupRootRetryable(child, expired) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return startupRootRetryable(wrapped.Unwrap(), expired)
	}
	return errors.Is(err, raft.ErrNoLeader) || status.Code(err) == codes.Unavailable ||
		(expired && (errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded))
}

// A heartbeat can reveal a leader before an empty learner has installed any
// membership. RawNode rejects ReadIndexResp from an unknown peer; that read is
// not automatically reissued after snapshot installation. Wait for the durable
// admission cut before asking for a new quorum barrier. The cached view is only
// a lower bound, never a substitute for locally applied membership or ReadIndex.
func (runtime *Runtime) waitForRootJoinPosition(ctx context.Context) error {
	if runtime.RootJoin == nil {
		return nil
	}
	local, err := runtime.RootJoin.Load()
	if err != nil {
		return err
	}
	return waitRootJoinPosition(ctx, local, runtime.Root.RootPosition)
}

func waitRootJoinPosition(ctx context.Context, local rootjoin.LocalState, position func() (uint64, *raftpb.ConfState, error)) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		index, conf, err := position()
		if err != nil {
			return err
		}
		if err := local.CheckRecovered(index, conf); err != nil {
			return err
		}
		if index >= local.View.AppliedIndex {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// RootJoinView supplies already validated, identity-bound fallback routes for
// the joining listener. These addresses never become bootstrap voter IDs.
func (runtime *Runtime) RootJoinView() (rootjoin.View, bool, error) {
	if runtime == nil || runtime.RootJoin == nil {
		return rootjoin.View{}, false, nil
	}
	state, err := runtime.RootJoin.Load()
	if err != nil {
		return rootjoin.View{}, false, err
	}
	return state.View.CloneCanonical(), true, nil
}

// activateRootJoin requires the caller's preceding local Root ReadIndex.
// RootJoin tasks are immutable once admitted, while routing is captured in a
// single applied-state cut. No remote response alone reaches this transition.
func (runtime *Runtime) activateRootJoin() error {
	if runtime.RootJoin == nil {
		return nil
	}
	local, err := runtime.RootJoin.Load()
	if err != nil {
		return err
	}
	task, found, err := runtime.Root.RootJoin(local.GrantID)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("local Root has not applied its authorized join task")
	}
	cut, err := runtime.Root.RootRoutingView(rootjoin.MaxViewPeers)
	if err != nil {
		return err
	}
	view, err := rootjoin.FromRootCut(runtime.Identity.ClusterID, cut, 0)
	if err != nil {
		return err
	}
	_, err = runtime.RootJoin.Activate(view, task)
	return err
}
