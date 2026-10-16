package enginedata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

const rootReadAttemptTimeout = 3 * time.Second

type rootReader interface {
	RootMembership() (*raftpb.ConfState, error)
	AppliedIndex() uint64
}

type rootGroupReader interface {
	ReadIndex(context.Context, uint64, string) (uint64, error)
	GroupCandidates(string) []uint64
}

// rootRouteBarrier recovers a route read when this Node's local Root learner
// retains a stale leader after that leader stops. A remote cut is usable only
// if it came from a locally known Root voter and the local state has applied
// at least the confirmed index. Otherwise routing fails closed.
type rootRouteBarrier struct {
	local interface {
		ReadIndex(context.Context, string) (uint64, error)
	}
	root   rootReader
	client rootGroupReader
	self   uint64
}

func (barrier rootRouteBarrier) ReadIndex(ctx context.Context, groupID string) (uint64, error) {
	if groupID != raft.RootGroupID {
		return 0, errors.New("engine data route barrier only confirms Root")
	}
	localCtx, cancel := context.WithTimeout(ctx, rootReadAttemptTimeout)
	index, localErr := barrier.local.ReadIndex(localCtx, groupID)
	cancel()
	if localErr == nil && index > 0 {
		return index, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	conf, err := barrier.root.RootMembership()
	if err != nil {
		return 0, errors.Join(localErr, fmt.Errorf("read Root voters: %w", err))
	}
	voters := make(map[uint64]bool, len(conf.GetVoters())+len(conf.GetVotersOutgoing()))
	for _, id := range conf.GetVoters() {
		voters[id] = true
	}
	for _, id := range conf.GetVotersOutgoing() {
		voters[id] = true
	}
	var remoteErr error
	for _, id := range barrier.client.GroupCandidates(groupID) {
		if id == barrier.self || !voters[id] {
			continue
		}
		attempt, done := context.WithTimeout(ctx, rootReadAttemptTimeout)
		index, err = barrier.client.ReadIndex(attempt, id, groupID)
		if err == nil && index > 0 {
			ticker := time.NewTicker(20 * time.Millisecond)
			for barrier.root.AppliedIndex() < index && attempt.Err() == nil {
				select {
				case <-attempt.Done():
				case <-ticker.C:
				}
			}
			ticker.Stop()
			if barrier.root.AppliedIndex() >= index {
				done()
				return index, nil
			}
			err = errors.New("local Root directory has not applied remote read cut")
		}
		done()
		remoteErr = errors.Join(remoteErr, err)
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
	}
	return 0, fmt.Errorf("confirm Root route through current voters: %w", errors.Join(localErr, remoteErr))
}

var _ rootReader = (*rootstate.State)(nil)
