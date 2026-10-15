package rootstate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// WithSnapshots adapts a Root state to durable file-backed Raft snapshots.
// The caller owns artifacts and ctx; cancel recovery before closing the Host,
// and close artifacts only after all Groups and preparations have closed.
func WithSnapshots(state *State, artifacts *snapshot.Store, ctx context.Context) (raft.StateMachine, error) {
	if state == nil || artifacts == nil || ctx == nil {
		return nil, errors.New("Root file state requires state, artifacts and recovery context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &fileState{State: state, artifacts: artifacts, ctx: ctx}, nil
}

type fileState struct {
	*State
	artifacts *snapshot.Store
	ctx       context.Context
}

var _ raft.SnapshotPreparer = (*fileState)(nil)

func (s *fileState) PrepareSnapshot() (*raft.PreparedSnapshot, error) {
	return s.PrepareSnapshotTo(s.artifacts)
}

func (s *fileState) Restore(index uint64, encoded []byte, conf *raftpb.ConfState) error {
	_, recognized, err := snapshot.DecodeReference(encoded)
	if err != nil {
		return err
	}
	if recognized {
		return s.RestoreFrom(s.ctx, s.artifacts, index, encoded, conf)
	}
	return s.State.Restore(index, encoded, conf)
}

// PrepareSnapshotTo fixes the logical cut under the state lock, then performs
// bounded streaming IO without holding that lock. Close releases its view.
func (s *State) PrepareSnapshotTo(artifacts *snapshot.Store) (*raft.PreparedSnapshot, error) {
	if artifacts == nil {
		return nil, errors.New("Root snapshot requires artifact Store")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	if s.applied == 0 {
		return nil, errors.New("cannot snapshot unapplied Root state")
	}
	conf, err := proto.Marshal(s.conf)
	if err != nil {
		return nil, err
	}
	index := s.applied
	view := s.db.NewSnapshot()
	s.snapshotWG.Add(1)
	var mu sync.Mutex
	var built, closed bool
	var closeErr error
	return &raft.PreparedSnapshot{Index: index, Conf: proto.Clone(s.conf).(*raftpb.ConfState),
		Build: func(ctx context.Context) ([]byte, error) {
			mu.Lock()
			defer mu.Unlock()
			if built || closed {
				return nil, errors.New("Root snapshot preparation is closed or already built")
			}
			built = true
			header := snapshot.Header{Version: snapshot.StreamVersion, Format: fmt.Sprintf("rootstate-v%d", FormatVersion),
				GroupID: raft.RootGroupID, Index: index, Conf: conf}
			ref, err := artifacts.Create(ctx, header, snapshot.MaxStreamBytes, func(encoder *snapshot.Encoder) error {
				return snapshot.Walk(ctx, view, func(key, value []byte) error {
					if !bytes.HasPrefix(key, []byte("data/")) && !bytes.HasPrefix(key, []byte("result/")) {
						return nil
					}
					if err := validateStreamEntry(view, index, key, value); err != nil {
						return err
					}
					return encoder.Add(key, value)
				})
			})
			if err != nil {
				return nil, err
			}
			return ref.Encode()
		},
		Close: func() error {
			mu.Lock()
			defer mu.Unlock()
			if !closed {
				closed = true
				closeErr = view.Close()
				s.snapshotWG.Done()
			}
			return closeErr
		}}, nil
}
