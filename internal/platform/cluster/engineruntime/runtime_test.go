package engineruntime

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
)

type valueHandler struct {
	readable bool
}

func (handler *valueHandler) Apply(transaction *enginestate.Transaction, _ router.Target, payload []byte) (enginestate.Outcome, error) {
	if err := transaction.Set([]byte("shared"), payload); err != nil {
		return enginestate.Outcome{}, err
	}
	return enginestate.Outcome{Value: payload}, nil
}

type readableValueHandler struct{ valueHandler }

func (*readableValueHandler) Read(view enginestate.ReadView, _ router.Target, payload []byte) (enginestate.Outcome, error) {
	value, found, err := view.Get(payload)
	if err != nil {
		return enginestate.Outcome{}, err
	}
	if !found {
		return enginestate.Outcome{Rejection: "not found"}, nil
	}
	return enginestate.Outcome{Value: value}, nil
}

type fixedBarrier struct{ index uint64 }

func (barrier fixedBarrier) ReadIndex(context.Context, string) (uint64, error) {
	return barrier.index, nil
}

func TestEmptyRegistrySealsAsRejectAllMux(t *testing.T) {
	registry := NewRegistry()
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := mux.Apply(nil, router.Target{Keyspace: rootstate.Keyspace{EngineID: "record"}}, nil)
	if err != nil || outcome.Rejection != UnknownEngineRejection {
		t.Fatalf("empty Mux result = %+v, err %v", outcome, err)
	}
	if _, err := NewRuntime(t.TempDir(), mux); err != nil {
		t.Fatal(err)
	}
}

func TestMuxIsolatesEngineKeyspacesAndDispatchesReads(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("record", &readableValueHandler{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("search", &readableValueHandler{}); err != nil {
		t.Fatal(err)
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	state, err := enginestate.Open(t.TempDir(), "data-1", mux)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	record := activate(t, state, 1, "record-range", "record")
	search := activate(t, state, 2, "search-range", "search")
	if err := state.ApplyRouted(3, "record-write", record, []byte("record-value")); err != nil {
		t.Fatal(err)
	}
	if err := state.ApplyRouted(4, "search-write", search, []byte("search-value")); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		target router.Target
		want   string
	}{{record, "record-value"}, {search, "search-value"}} {
		outcome, index, err := state.QueryRouted(context.Background(), fixedBarrier{4}, check.target, []byte("shared"))
		if err != nil || index != 4 || outcome.Rejection != "" || string(outcome.Value) != check.want {
			t.Fatalf("query %q = %+v, index %d, err %v", check.target.Keyspace.EngineID, outcome, index, err)
		}
	}
}

func TestMuxReturnsStableEngineRejections(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("record", &valueHandler{}); err != nil {
		t.Fatal(err)
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	state, err := enginestate.Open(t.TempDir(), "data-1", mux)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	record := activate(t, state, 1, "record-range", "record")
	search := activate(t, state, 2, "search-range", "search")
	if err := state.ApplyRouted(3, "unknown-write", search, []byte("lost")); err != nil {
		t.Fatal(err)
	}
	if result, found, err := state.Result("unknown-write"); err != nil || !found || result.Applied || result.Error != UnknownEngineRejection {
		t.Fatalf("unknown engine result = %+v, found %v, err %v", result, found, err)
	}
	outcome, _, err := state.QueryRouted(context.Background(), fixedBarrier{3}, record, []byte("shared"))
	if err != nil || outcome.Rejection != UnsupportedReadRejection {
		t.Fatalf("unsupported read = %+v, err %v", outcome, err)
	}
}

func TestRegistrySealsAndFactoryCreatesPerGroupState(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("record", &valueHandler{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("record", &valueHandler{}); err == nil {
		t.Fatal("duplicate engine registration was accepted")
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("search", &valueHandler{}); err == nil {
		t.Fatal("registration after Seal was accepted")
	}
	runtime, err := NewRuntime(t.TempDir(), mux)
	if err != nil {
		t.Fatal(err)
	}
	managed, err := runtime.NewState(context.Background(), rootstate.Group{
		GroupID: "managed-1", StateKind: rootstate.GroupStateEngine})
	if err != nil {
		t.Fatal(err)
	}
	if _, found := runtime.Resolve("managed-1"); !found {
		t.Fatal("managed engine state was not published")
	}
	if _, ok := managed.(raft.CommandValidator); !ok {
		t.Fatal("managed state lost CommandValidator")
	}
	if _, ok := managed.(raft.Snapshotter); !ok {
		t.Fatal("managed state lost Snapshotter")
	}
	if err := managed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, found := runtime.Resolve("managed-1"); found {
		t.Fatal("closed managed engine state remained published")
	}
	factory, err := NewFactory(t.TempDir(), mux)
	if err != nil {
		t.Fatal(err)
	}
	first, err := factory(context.Background(), rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateEngine})
	if err != nil {
		t.Fatal(err)
	}
	second, err := factory(context.Background(), rootstate.Group{GroupID: "data-2", StateKind: rootstate.GroupStateEngine})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := factory(context.Background(), rootstate.Group{GroupID: raft.RootGroupID, StateKind: rootstate.GroupStateEngine}); err == nil {
		t.Fatal("Root Group was accepted by engine factory")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := factory(cancelled, rootstate.Group{GroupID: "data-3", StateKind: rootstate.GroupStateEngine}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled factory = %v", err)
	}
}

func TestConcurrentNewStateSerializesDurableOpen(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("record", &valueHandler{}); err != nil {
		t.Fatal(err)
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(t.TempDir(), mux)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := rootstate.Group{GroupID: "concurrent", StateKind: rootstate.GroupStateEngine}
	start := make(chan struct{})
	errs := make(chan error, 2)
	states := make(chan raft.StateMachine, 2)
	var group sync.WaitGroup
	group.Add(2)
	for range 2 {
		go func() {
			defer group.Done()
			<-start
			state, openErr := runtime.NewState(context.Background(), descriptor)
			if state != nil {
				states <- state
			}
			errs <- openErr
		}()
	}
	close(start)
	group.Wait()
	close(errs)
	close(states)

	var success int
	for openErr := range errs {
		if openErr == nil {
			success++
			continue
		}
		if !errors.Is(openErr, ErrGroupStateAlreadyOpen) {
			t.Fatalf("concurrent open error = %v", openErr)
		}
	}
	if success != 1 {
		t.Fatalf("concurrent open successes = %d, want 1", success)
	}
	for state := range states {
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSnapshotStatesReturnsDeterministicOpaqueGroupImages(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register("record", &valueHandler{}); err != nil {
		t.Fatal(err)
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(t.TempDir(), mux)
	if err != nil {
		t.Fatal(err)
	}
	first, err := runtime.NewState(context.Background(), rootstate.Group{GroupID: "group-b", StateKind: rootstate.GroupStateEngine})
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.NewState(context.Background(), rootstate.Group{GroupID: "group-a", StateKind: rootstate.GroupStateEngine})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	defer second.Close()
	states, err := runtime.SnapshotStates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 || states[0].GroupID != "group-a" || states[1].GroupID != "group-b" {
		t.Fatalf("snapshots were not sorted: %+v", states)
	}
	if len(states[0].Data) == 0 || len(states[1].Data) == 0 {
		t.Fatal("empty Engine Group snapshot")
	}
	states[0].Data[0] ^= 0xff
	again, err := runtime.SnapshotStates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(states[0].Data, again[0].Data) {
		t.Fatal("snapshot data aliases runtime state")
	}
}

func activate(t *testing.T, state *enginestate.State, index uint64, rangeID, engineID string) router.Target {
	t.Helper()
	descriptor := rootstate.Range{RangeID: rangeID, TenantID: "tenant-a", EngineID: engineID,
		DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	if err := state.ApplyRange(index, "activate-"+rangeID, descriptor); err != nil {
		t.Fatal(err)
	}
	return router.Target{Keyspace: descriptor.Keyspace(), RangeID: rangeID,
		OwnerGroupID: "data-1", Epoch: 1, RoutingKey: []byte("key")}
}
