package enginedata

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

type routeLocalRead func(context.Context, string) (uint64, error)

func (read routeLocalRead) ReadIndex(ctx context.Context, group string) (uint64, error) {
	return read(ctx, group)
}

type routeRoot struct {
	conf    *raftpb.ConfState
	applied uint64
}

func (root routeRoot) RootMembership() (*raftpb.ConfState, error) { return root.conf, nil }
func (root routeRoot) AppliedIndex() uint64                       { return root.applied }

type routeGroupReader struct {
	candidates []uint64
	read       func(context.Context, uint64, string) (uint64, error)
}

func (reader routeGroupReader) GroupCandidates(string) []uint64 { return reader.candidates }
func (reader routeGroupReader) ReadIndex(ctx context.Context, id uint64, group string) (uint64, error) {
	return reader.read(ctx, id, group)
}

func TestRootRouteBarrierUsesOnlyAppliedVoterCut(t *testing.T) {
	calls := 0
	barrier := rootRouteBarrier{self: 4, local: routeLocalRead(func(context.Context, string) (uint64, error) {
		return 0, raft.ErrNoLeader
	}), root: routeRoot{conf: &raftpb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3, 4}}, applied: 10},
		client: routeGroupReader{candidates: []uint64{3, 2, 4}, read: func(_ context.Context, id uint64, group string) (uint64, error) {
			calls++
			if id != 2 || group != raft.RootGroupID {
				t.Fatal("routed Root read to a learner or wrong Group", id, group)
			}
			return 10, nil
		}}}
	index, err := barrier.ReadIndex(context.Background(), raft.RootGroupID)
	if err != nil || index != 10 || calls != 1 {
		t.Fatal("confirmed applied Root voter cut was not used", index, calls, err)
	}
}

func TestRootRouteBarrierRejectsUnappliedRemoteCut(t *testing.T) {
	barrier := rootRouteBarrier{self: 4, local: routeLocalRead(func(context.Context, string) (uint64, error) {
		return 0, raft.ErrNoLeader
	}), root: routeRoot{conf: &raftpb.ConfState{Voters: []uint64{2}, Learners: []uint64{4}}, applied: 9},
		client: routeGroupReader{candidates: []uint64{2}, read: func(context.Context, uint64, string) (uint64, error) {
			return 10, nil
		}}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if index, err := barrier.ReadIndex(ctx, raft.RootGroupID); index != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unapplied remote Root cut was accepted", index, err)
	}
}
