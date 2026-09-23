package capstate

import (
	"context"
	"errors"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

type fakeReadBarrier struct {
	groupID string
	index   uint64
	err     error
}

type readableHandler struct{ testHandler }

func (handler *readableHandler) Read(view ReadView, _ router.Target, payload []byte) (Outcome, error) {
	value, found, err := view.Get(payload)
	if err != nil {
		return Outcome{}, err
	}
	if !found {
		return Outcome{Rejection: "not found"}, nil
	}
	return Outcome{Value: value}, nil
}

func (barrier *fakeReadBarrier) ReadIndex(_ context.Context, groupID string) (uint64, error) {
	barrier.groupID = groupID
	return barrier.index, barrier.err
}

func prepareReadableState(t *testing.T) (*State, router.Target) {
	t.Helper()
	state, err := Open(t.TempDir(), "data-1", &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", CapabilityID: "record",
		DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	if err := state.ApplyRange(1, "activate-a", descriptor); err != nil {
		t.Fatal(err)
	}
	target := router.Target{Keyspace: descriptor.Keyspace(), RangeID: descriptor.RangeID,
		OwnerGroupID: descriptor.OwnerGroupID, Epoch: 1, RoutingKey: []byte("key")}
	payload := encodeTestMutation(t, testMutation{Key: "item", Value: "value"})
	if err := state.ApplyRouted(2, "write-a", target, payload); err != nil {
		t.Fatal(err)
	}
	return state, target
}

func TestReadRoutedUsesConfirmedFencedView(t *testing.T) {
	state, target := prepareReadableState(t)
	defer state.Close()
	barrier := &fakeReadBarrier{index: state.AppliedIndex()}
	var value []byte
	index, err := state.ReadRouted(context.Background(), barrier, target, func(view ReadView) error {
		var found bool
		var readErr error
		value, found, readErr = view.Get([]byte("item"))
		if readErr != nil {
			return readErr
		}
		if !found {
			return errors.New("item is missing")
		}
		return nil
	})
	if err != nil || index != state.AppliedIndex() || string(value) != "value" || barrier.groupID != "data-1" {
		t.Fatalf("routed read = index %d, value %q, Group %q, err %v", index, value, barrier.groupID, err)
	}
}

func TestReadRoutedFailsClosedBeforeCallback(t *testing.T) {
	state, target := prepareReadableState(t)
	defer state.Close()
	called := false
	read := func(ReadView) error {
		called = true
		return nil
	}
	barrier := &fakeReadBarrier{err: context.DeadlineExceeded}
	if _, err := state.ReadRouted(context.Background(), barrier, target, read); !errors.Is(err, context.DeadlineExceeded) || called {
		t.Fatalf("failed barrier read = %v, called = %v", err, called)
	}
	barrier = &fakeReadBarrier{index: state.AppliedIndex() + 1}
	if _, err := state.ReadRouted(context.Background(), barrier, target, read); !errors.Is(err, ErrStateBehind) || called {
		t.Fatalf("behind state read = %v, called = %v", err, called)
	}
	barrier.index = state.AppliedIndex()
	target.Epoch++
	if _, err := state.ReadRouted(context.Background(), barrier, target, read); !errors.Is(err, router.ErrRouteFenced) || called {
		t.Fatalf("stale Target read = %v, called = %v", err, called)
	}
}

func TestQueryRoutedDelegatesOpaqueReadPayload(t *testing.T) {
	state, err := Open(t.TempDir(), "data-1", &readableHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	descriptor := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", CapabilityID: "record",
		DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	if err := state.ApplyRange(1, "activate-a", descriptor); err != nil {
		t.Fatal(err)
	}
	target := router.Target{Keyspace: descriptor.Keyspace(), RangeID: descriptor.RangeID,
		OwnerGroupID: descriptor.OwnerGroupID, Epoch: 1, RoutingKey: []byte("key")}
	mutation := encodeTestMutation(t, testMutation{Key: "item", Value: "value"})
	if err := state.ApplyRouted(2, "write-a", target, mutation); err != nil {
		t.Fatal(err)
	}
	outcome, index, err := state.QueryRouted(context.Background(), &fakeReadBarrier{index: 2}, target, []byte("item"))
	if err != nil || index != 2 || outcome.Rejection != "" || string(outcome.Value) != "value" {
		t.Fatalf("routed query = %+v, index %d, err %v", outcome, index, err)
	}
}
