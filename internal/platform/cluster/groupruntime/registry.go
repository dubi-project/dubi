// Package groupruntime maps persistent Group StateKind descriptors to local
// StateMachine factories without introducing engine dependencies into the
// Raft or cluster platform layers.
package groupruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

type RootBarrier interface {
	ReadIndex(ctx context.Context, groupID string) (uint64, error)
}

type Factory func(ctx context.Context, descriptor rootstate.Group) (raft.StateMachine, error)

type Registry struct {
	mu        sync.RWMutex
	root      *rootstate.State
	barrier   RootBarrier
	factories map[string]Factory
}

func New(root *rootstate.State, barrier RootBarrier) (*Registry, error) {
	if root == nil || barrier == nil {
		return nil, errors.New("Group runtime registry requires Root state and read barrier")
	}
	return &Registry{root: root, barrier: barrier, factories: make(map[string]Factory)}, nil
}

func (registry *Registry) Register(stateKind string, factory Factory) error {
	stateKind = strings.TrimSpace(stateKind)
	if stateKind == "" || len(stateKind) > 128 || factory == nil {
		return errors.New("Group StateKind and factory are required")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.factories[stateKind] != nil {
		return fmt.Errorf("Group StateKind %q is already registered", stateKind)
	}
	registry.factories[stateKind] = factory
	return nil
}

// NewState is suitable for groupmembership.NewJoinManager. It first performs a
// Root read barrier so a joining Node cannot construct a state machine from a
// stale or caller-invented Group descriptor.
func (registry *Registry) NewState(ctx context.Context, groupID string) (raft.StateMachine, error) {
	if _, err := registry.barrier.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return nil, fmt.Errorf("confirm Root Group descriptor: %w", err)
	}
	descriptor, found, err := registry.root.Group(groupID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("joining Group is not registered in Root")
	}
	return registry.NewStateFromDescriptor(ctx, descriptor)
}

// NewStateFromDescriptor constructs a state from a descriptor that the caller
// already read behind a Root barrier. Startup recovery uses this form after
// its stronger RootReadIndex path; repeating a local Host.ReadIndex here can
// deadlock a healthy voter while its leader hint is catching up.
func (registry *Registry) NewStateFromDescriptor(ctx context.Context, descriptor rootstate.Group) (raft.StateMachine, error) {
	if registry == nil {
		return nil, errors.New("Group runtime registry is nil")
	}
	registry.mu.RLock()
	factory := registry.factories[descriptor.StateKind]
	registry.mu.RUnlock()
	if factory == nil {
		return nil, fmt.Errorf("Group StateKind %q has no registered factory", descriptor.StateKind)
	}
	state, err := factory(ctx, descriptor)
	if err != nil {
		return nil, fmt.Errorf("construct Group StateKind %q: %w", descriptor.StateKind, err)
	}
	if state == nil {
		return nil, errors.New("Group StateKind factory returned nil")
	}
	return state, nil
}
