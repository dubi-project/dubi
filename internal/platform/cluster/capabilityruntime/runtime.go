// Package capabilityruntime binds registered capability handlers to the
// generic capability Group state machine.
package capabilityruntime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/groupruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	UnknownCapabilityRejection = "capability is not registered"
	UnsupportedReadRejection   = "capability does not implement routed reads"
)

// Registry collects process-local capability implementations during startup.
// Seal takes an immutable snapshot; registration after sealing is rejected so
// every Group opened by this process observes the same handler set.
type Registry struct {
	mu       sync.Mutex
	handlers map[string]capstate.Handler
	sealed   bool
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]capstate.Handler)}
}

func (registry *Registry) Register(capabilityID string, handler capstate.Handler) error {
	if registry == nil || rootstate.ValidateCapabilityID(capabilityID) != nil || handler == nil {
		return errors.New("valid CapabilityID and handler are required")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.sealed {
		return errors.New("capability registry is sealed")
	}
	if registry.handlers[capabilityID] != nil {
		return fmt.Errorf("CapabilityID %q is already registered", capabilityID)
	}
	registry.handlers[capabilityID] = handler
	return nil
}

func (registry *Registry) Seal() (*Mux, error) {
	if registry == nil {
		return nil, errors.New("capability registry is nil")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.sealed {
		return nil, errors.New("capability registry is already sealed")
	}
	registry.sealed = true
	handlers := make(map[string]capstate.Handler, len(registry.handlers))
	for capabilityID, handler := range registry.handlers {
		handlers[capabilityID] = handler
	}
	return &Mux{handlers: handlers}, nil
}

// Mux is immutable after construction and dispatches only by the CapabilityID
// already bound into the validated Route Target.
type Mux struct {
	handlers map[string]capstate.Handler
}

var _ capstate.Handler = (*Mux)(nil)
var _ capstate.ReadHandler = (*Mux)(nil)

func (mux *Mux) Apply(transaction *capstate.Transaction, target router.Target, payload []byte) (capstate.Outcome, error) {
	handler := mux.handler(target.Keyspace.CapabilityID)
	if handler == nil {
		return capstate.Outcome{Rejection: UnknownCapabilityRejection}, nil
	}
	return handler.Apply(transaction, target, payload)
}

func (mux *Mux) Read(view capstate.ReadView, target router.Target, payload []byte) (capstate.Outcome, error) {
	handler := mux.handler(target.Keyspace.CapabilityID)
	if handler == nil {
		return capstate.Outcome{Rejection: UnknownCapabilityRejection}, nil
	}
	reader, ok := handler.(capstate.ReadHandler)
	if !ok {
		return capstate.Outcome{Rejection: UnsupportedReadRejection}, nil
	}
	return reader.Read(view, target, payload)
}

func (mux *Mux) handler(capabilityID string) capstate.Handler {
	if mux == nil {
		return nil
	}
	return mux.handlers[capabilityID]
}

// Runtime owns the local directory of open capability Group states. The
// directory is the shared lookup boundary for Range and Capability RPCs; a
// Group disappears from it as part of StateMachine.Close.
type Runtime struct {
	stateRoot       string
	mux             *Mux
	packs           *pack.Pool
	artifacts       *snapshot.Store
	recoveryContext context.Context

	mu     sync.RWMutex
	states map[string]*capstate.State
}

func NewRuntime(stateRoot string, mux *Mux) (*Runtime, error) {
	return NewRuntimeWithPacks(stateRoot, mux, nil)
}

// NewRuntimeWithPacks shares the node's mixed-capability Pool across all Groups.
// Each Group owns its local reference index; the caller owns Pool lifetime.
func NewRuntimeWithPacks(stateRoot string, mux *Mux, packs *pack.Pool) (*Runtime, error) {
	if stateRoot == "" || mux == nil {
		return nil, errors.New("capability state root and sealed Mux are required")
	}
	return &Runtime{stateRoot: stateRoot, mux: mux, packs: packs, states: make(map[string]*capstate.State)}, nil
}

// NewRuntimeWithSnapshots enables the file-reference path only when the node
// supplies durable shared artifacts and a lifetime context for recovery.
// Cancel recovery on node shutdown, not on an individual client's deadline:
// the Raft reference may already be committed when a client disconnects.
func NewRuntimeWithSnapshots(stateRoot string, mux *Mux, packs *pack.Pool, artifacts *snapshot.Store, recoveryContext context.Context) (*Runtime, error) {
	if artifacts == nil || recoveryContext == nil {
		return nil, errors.New("snapshot-enabled runtime needs artifacts and recovery context")
	}
	runtime, err := NewRuntimeWithPacks(stateRoot, mux, packs)
	if err != nil {
		return nil, err
	}
	runtime.artifacts, runtime.recoveryContext = artifacts, recoveryContext
	return runtime, nil
}

func (runtime *Runtime) Resolve(groupID string) (*capstate.State, bool) {
	if runtime == nil {
		return nil, false
	}
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	state := runtime.states[groupID]
	return state, state != nil
}

func (runtime *Runtime) NewState(ctx context.Context, descriptor rootstate.Group) (raft.StateMachine, error) {
	if runtime == nil {
		return nil, errors.New("capability runtime is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.recoveryContext != nil {
		if err := runtime.recoveryContext.Err(); err != nil {
			return nil, err
		}
	}
	if descriptor.StateKind != rootstate.GroupStateCapability ||
		raft.ValidateGroupID(descriptor.GroupID) != nil || descriptor.GroupID == raft.RootGroupID {
		return nil, errors.New("invalid capability Group descriptor")
	}
	runtime.mu.RLock()
	existing := runtime.states[descriptor.GroupID]
	runtime.mu.RUnlock()
	if existing != nil {
		return nil, fmt.Errorf("capability Group %q state is already open", descriptor.GroupID)
	}
	state, err := capstate.OpenWithPacks(filepath.Join(runtime.stateRoot, "dubi-capability-groups", descriptor.GroupID),
		descriptor.GroupID, runtime.mux, runtime.packs)
	if err != nil {
		return nil, err
	}
	adapter, err := routedstate.New(state)
	if err != nil {
		_ = state.Close()
		return nil, err
	}
	runtime.mu.Lock()
	if runtime.states[descriptor.GroupID] != nil {
		runtime.mu.Unlock()
		_ = state.Close()
		return nil, fmt.Errorf("capability Group %q state is already open", descriptor.GroupID)
	}
	runtime.states[descriptor.GroupID] = state
	runtime.mu.Unlock()
	managed := &managedState{Adapter: adapter, onClose: func() {
		runtime.mu.Lock()
		if runtime.states[descriptor.GroupID] == state {
			delete(runtime.states, descriptor.GroupID)
		}
		runtime.mu.Unlock()
	}}
	if runtime.artifacts != nil {
		return &streamedState{managedState: managed, state: state, artifacts: runtime.artifacts, ctx: runtime.recoveryContext}, nil
	}
	return managed, nil
}

type streamedState struct {
	*managedState
	state     *capstate.State
	artifacts *snapshot.Store
	ctx       context.Context
}

var _ raft.SnapshotPreparer = (*streamedState)(nil)
var _ raft.StateMachine = (*streamedState)(nil)

func (state *streamedState) PrepareSnapshot() (*raft.PreparedSnapshot, error) {
	if err := state.ctx.Err(); err != nil {
		return nil, err
	}
	return state.state.PrepareSnapshotTo(state.artifacts)
}

func (state *streamedState) Restore(index uint64, data []byte, conf *raftpb.ConfState) error {
	_, recognized, err := snapshot.DecodeReference(data)
	if err != nil {
		return err
	}
	if recognized {
		return state.state.RestoreFrom(state.ctx, state.artifacts, index, data, conf)
	}
	return state.Adapter.Restore(index, data, conf)
}

func (runtime *Runtime) Register(registry *groupruntime.Registry) error {
	if runtime == nil || registry == nil {
		return errors.New("capability and Group runtimes are required")
	}
	return registry.Register(rootstate.GroupStateCapability, runtime.NewState)
}

type managedState struct {
	*routedstate.Adapter
	once    sync.Once
	onClose func()
}

func (state *managedState) Close() error {
	err := state.Adapter.Close()
	state.once.Do(state.onClose)
	return err
}

// NewFactory constructs the StateKind=capability runtime factory. Each Group
// gets a distinct state directory beneath stateRoot.
func NewFactory(stateRoot string, mux *Mux) (groupruntime.Factory, error) {
	runtime, err := NewRuntime(stateRoot, mux)
	if err != nil {
		return nil, err
	}
	return runtime.NewState, nil
}

// RegisterFactory installs the generic capability factory under its persistent
// StateKind. The caller should invoke it while assembling the node runtime.
func RegisterFactory(registry *groupruntime.Registry, stateRoot string, mux *Mux) error {
	if registry == nil {
		return errors.New("Group runtime registry is required")
	}
	runtime, err := NewRuntime(stateRoot, mux)
	if err != nil {
		return err
	}
	return runtime.Register(registry)
}
