package groupmembership

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/raft"
)

type JoiningControl interface {
	PrepareJoining(ctx context.Context, groupID string, raftID uint64) error
}

type JoiningHost interface {
	HasGroup(groupID string) bool
	AddGroup(groupID string, peers []uint64, joining bool, state raft.StateMachine) error
}

type StateFactory func(ctx context.Context, groupID string) (raft.StateMachine, error)

// JoinManager opens an empty local Group before its RaftID is proposed as a
// Learner. The injected factory owns capability-specific state construction;
// the Raft layer never guesses a module's persistent format.
type JoinManager struct {
	mu      sync.Mutex
	host    JoiningHost
	raftID  uint64
	factory StateFactory
}

func NewJoinManager(host JoiningHost, raftID uint64, factory StateFactory) (*JoinManager, error) {
	if host == nil || raftID == 0 || factory == nil {
		return nil, errors.New("Group join manager requires Host, RaftID, and State factory")
	}
	return &JoinManager{host: host, raftID: raftID, factory: factory}, nil
}

func (manager *JoinManager) PrepareJoining(ctx context.Context, groupID string, raftID uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if raftID != manager.raftID || raft.ValidateGroupID(groupID) != nil || groupID == raft.RootGroupID {
		return errors.New("invalid local joining Group target")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.host.HasGroup(groupID) {
		return nil
	}
	state, err := manager.factory(ctx, groupID)
	if err != nil {
		return fmt.Errorf("create joining Group state: %w", err)
	}
	if state == nil {
		return errors.New("joining Group State factory returned nil")
	}
	if err := manager.host.AddGroup(groupID, nil, true, state); err != nil {
		_ = state.Close()
		return fmt.Errorf("open joining Group: %w", err)
	}
	return nil
}

var _ JoiningControl = (*JoinManager)(nil)
