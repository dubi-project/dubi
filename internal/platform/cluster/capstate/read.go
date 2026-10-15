package capstate

import (
	"context"
	"errors"
	"fmt"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

var ErrStateBehind = errors.New("capability state is behind the confirmed read index")

type ReadBarrier interface {
	ReadIndex(ctx context.Context, groupID string) (uint64, error)
}

// ReadHandler is optional. Capabilities that expose routed reads interpret the
// opaque payload while the platform supplies a fenced, linearizable ReadView.
type ReadHandler interface {
	Read(view ReadView, target router.Target, payload []byte) (Outcome, error)
}

// ReadView exposes only keys in the Target's immutable Keyspace. It is valid for the duration of
// the ReadRouted callback and cannot access Fence, result, or Raft metadata.
type ReadView struct {
	state    *State
	keyspace rootstate.Keyspace
}

func (view ReadView) Get(key []byte) ([]byte, bool, error) {
	if view.state == nil || validateCapabilityKey(key) != nil {
		return nil, false, errors.New("invalid capability read key")
	}
	return view.state.getValue(capabilityKey(view.keyspace, key))
}

// ReadRouted establishes a quorum read barrier on this Owner Group, then holds
// one local state view while checking the exact Range Fence and reading data.
// The callback must be bounded and must not call mutation methods on State.
func (s *State) ReadRouted(
	ctx context.Context,
	barrier ReadBarrier,
	target router.Target,
	read func(ReadView) error,
) (uint64, error) {
	if barrier == nil || read == nil {
		return 0, errors.New("routed capability read requires barrier and callback")
	}
	if err := target.Validate(); err != nil {
		return 0, err
	}
	index, err := barrier.ReadIndex(ctx, s.groupID)
	if err != nil {
		return 0, fmt.Errorf("confirm Owner Group read: %w", err)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return 0, err
	}
	if s.applied < index {
		return 0, ErrStateBehind
	}
	descriptor, found, err := s.fence(target.RangeID)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, router.ErrNoRoute
	}
	if err := router.CheckOwner(s.groupID, descriptor, target); err != nil {
		return 0, err
	}
	if err := read(ReadView{state: s, keyspace: target.Keyspace}); err != nil {
		return 0, err
	}
	return index, nil
}

func (s *State) QueryRouted(
	ctx context.Context,
	barrier ReadBarrier,
	target router.Target,
	payload []byte,
) (Outcome, uint64, error) {
	reader, ok := s.handler.(ReadHandler)
	if !ok {
		return Outcome{}, 0, errors.New("capability does not implement routed reads")
	}
	if len(payload) == 0 {
		return Outcome{}, 0, errors.New("routed capability read payload is empty")
	}
	var outcome Outcome
	index, err := s.ReadRouted(ctx, barrier, target, func(view ReadView) error {
		var readErr error
		outcome, readErr = reader.Read(view, target, payload)
		return readErr
	})
	if err != nil {
		return Outcome{}, 0, err
	}
	return outcome, index, nil
}
