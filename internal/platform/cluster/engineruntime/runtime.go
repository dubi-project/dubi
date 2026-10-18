// Package engineruntime binds registered engine handlers to the
// generic engine Group state machine.
package engineruntime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/groupruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
)

// ErrGroupStateAlreadyOpen means another local recovery/join path is already
// constructing the durable state for this engine Group. Callers may wait
// for the owning path to attach the Raft Group instead of treating the race as
// a storage failure.
var ErrGroupStateAlreadyOpen = errors.New("engine Group state is already open")

const (
	UnknownEngineRejection   = "engine is not registered"
	UnsupportedReadRejection = "engine does not implement routed reads"
)

// Registry collects process-local engine implementations during startup.
// Seal takes an immutable snapshot; registration after sealing is rejected so
// every Group opened by this process observes the same handler set.
type Registry struct {
	mu       sync.Mutex
	handlers map[string]enginestate.Handler
	sealed   bool
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]enginestate.Handler)}
}

func (registry *Registry) Register(engineID string, handler enginestate.Handler) error {
	if registry == nil || rootstate.ValidateEngineID(engineID) != nil || handler == nil {
		return errors.New("valid EngineID and handler are required")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.sealed {
		return errors.New("engine registry is sealed")
	}
	if registry.handlers[engineID] != nil {
		return fmt.Errorf("EngineID %q is already registered", engineID)
	}
	registry.handlers[engineID] = handler
	return nil
}

func (registry *Registry) Seal() (*Mux, error) {
	if registry == nil {
		return nil, errors.New("engine registry is nil")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.sealed {
		return nil, errors.New("engine registry is already sealed")
	}
	registry.sealed = true
	handlers := make(map[string]enginestate.Handler, len(registry.handlers))
	for engineID, handler := range registry.handlers {
		handlers[engineID] = handler
	}
	return &Mux{handlers: handlers}, nil
}

// Mux is immutable after construction and dispatches only by the EngineID
// already bound into the validated Route Target.
type Mux struct {
	handlers map[string]enginestate.Handler
}

var _ enginestate.Handler = (*Mux)(nil)
var _ enginestate.ReadHandler = (*Mux)(nil)

func (mux *Mux) Apply(transaction *enginestate.Transaction, target router.Target, payload []byte) (enginestate.Outcome, error) {
	handler := mux.handler(target.Keyspace.EngineID)
	if handler == nil {
		return enginestate.Outcome{Rejection: UnknownEngineRejection}, nil
	}
	return handler.Apply(transaction, target, payload)
}

func (mux *Mux) Read(view enginestate.ReadView, target router.Target, payload []byte) (enginestate.Outcome, error) {
	handler := mux.handler(target.Keyspace.EngineID)
	if handler == nil {
		return enginestate.Outcome{Rejection: UnknownEngineRejection}, nil
	}
	reader, ok := handler.(enginestate.ReadHandler)
	if !ok {
		return enginestate.Outcome{Rejection: UnsupportedReadRejection}, nil
	}
	return reader.Read(view, target, payload)
}

func (mux *Mux) handler(engineID string) enginestate.Handler {
	if mux == nil {
		return nil
	}
	return mux.handlers[engineID]
}

// Runtime owns the local directory of open engine Group states. The
// directory is the shared lookup boundary for Range and Engine RPCs; a
// Group disappears from it as part of StateMachine.Close.
type Runtime struct {
	stateRoot       string
	mux             *Mux
	packs           *pack.Pool
	artifacts       *snapshot.Store
	recoveryContext context.Context

	mu sync.RWMutex
	// openMu serializes the check/open/register sequence. The durable enginestate
	// store is file-locked, so two startup or membership paths must not open the
	// same Group concurrently before either one has registered it in states.
	openMu sync.Mutex
	states map[string]*enginestate.State
}

// StateSnapshot is an opaque, point-in-time image of one local Engine Group.
// The recovery layer stores Data without interpreting its format; the Group
// runtime remains responsible for producing and later consuming the image.
type StateSnapshot struct {
	GroupID string
	Index   uint64
	Data    []byte
}

// SnapshotStates captures every currently open Engine Group in deterministic
// Group order. A backup must fail if an open Group cannot produce a coherent
// image instead of silently publishing an incomplete Recovery Point.
func (runtime *Runtime) SnapshotStates(ctx context.Context) ([]StateSnapshot, error) {
	if runtime == nil {
		return nil, errors.New("engine runtime is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runtime.mu.RLock()
	groups := make([]string, 0, len(runtime.states))
	for groupID := range runtime.states {
		groups = append(groups, groupID)
	}
	slices.Sort(groups)
	states := make([]*enginestate.State, len(groups))
	for index, groupID := range groups {
		states[index] = runtime.states[groupID]
	}
	runtime.mu.RUnlock()
	result := make([]StateSnapshot, 0, len(groups))
	for index, groupID := range groups {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		image, applied, _, err := states[index].Snapshot()
		if err != nil {
			return nil, fmt.Errorf("snapshot Engine Group %q: %w", groupID, err)
		}
		result = append(result, StateSnapshot{GroupID: groupID, Index: applied, Data: append([]byte(nil), image...)})
	}
	return result, nil
}

func NewRuntime(stateRoot string, mux *Mux) (*Runtime, error) {
	return NewRuntimeWithPacks(stateRoot, mux, nil)
}

// NewRuntimeWithPacks shares the node's mixed-engine Pool across all Groups.
// Each Group owns its local reference index; the caller owns Pool lifetime.
func NewRuntimeWithPacks(stateRoot string, mux *Mux, packs *pack.Pool) (*Runtime, error) {
	if stateRoot == "" || mux == nil {
		return nil, errors.New("engine state root and sealed Mux are required")
	}
	return &Runtime{stateRoot: stateRoot, mux: mux, packs: packs, states: make(map[string]*enginestate.State)}, nil
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

func (runtime *Runtime) Resolve(groupID string) (*enginestate.State, bool) {
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
		return nil, errors.New("engine runtime is nil")
	}
	runtime.openMu.Lock()
	defer runtime.openMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.recoveryContext != nil {
		if err := runtime.recoveryContext.Err(); err != nil {
			return nil, err
		}
	}
	if descriptor.StateKind != rootstate.GroupStateEngine ||
		raft.ValidateGroupID(descriptor.GroupID) != nil || descriptor.GroupID == raft.RootGroupID {
		return nil, errors.New("invalid engine Group descriptor")
	}
	runtime.mu.RLock()
	existing := runtime.states[descriptor.GroupID]
	runtime.mu.RUnlock()
	if existing != nil {
		return nil, fmt.Errorf("%w: %q", ErrGroupStateAlreadyOpen, descriptor.GroupID)
	}
	state, err := enginestate.OpenWithPacks(filepath.Join(runtime.stateRoot, "dubi-engine-groups", descriptor.GroupID),
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
		return nil, fmt.Errorf("%w: %q", ErrGroupStateAlreadyOpen, descriptor.GroupID)
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
	state     *enginestate.State
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
		return errors.New("engine and Group runtimes are required")
	}
	return registry.Register(rootstate.GroupStateEngine, runtime.NewState)
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

// NewFactory constructs the StateKind=engine runtime factory. Each Group
// gets a distinct state directory beneath stateRoot.
func NewFactory(stateRoot string, mux *Mux) (groupruntime.Factory, error) {
	runtime, err := NewRuntime(stateRoot, mux)
	if err != nil {
		return nil, err
	}
	return runtime.NewState, nil
}

// RegisterFactory installs the generic engine factory under its persistent
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
