package groupruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

type registryBarrier struct {
	calls int
	err   error
}

func (barrier *registryBarrier) ReadIndex(context.Context, string) (uint64, error) {
	barrier.calls++
	return 1, barrier.err
}

type registryState struct{}

func (*registryState) AppliedIndex() uint64                            { return 0 }
func (*registryState) ConfState() *raftpb.ConfState                    { return &raftpb.ConfState{} }
func (*registryState) Apply(uint64, []byte) error                      { return nil }
func (*registryState) Advance(uint64) error                            { return nil }
func (*registryState) ApplyMembership(uint64, *raftpb.ConfState) error { return nil }
func (*registryState) Restore(uint64, []byte, *raftpb.ConfState) error { return nil }
func (*registryState) Close() error                                    { return nil }

func TestRegistryConstructsStateFromPersistentKindAfterRootBarrier(t *testing.T) {
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	applyRegistryRoot(t, root, 1, rootstate.Command{Version: 1, RequestID: "group-data",
		Type: rootstate.RegisterGroup, Group: &rootstate.Group{
			GroupID: "data-1", StateKind: rootstate.GroupStateCapability}})
	barrier := &registryBarrier{}
	registry, err := New(root, barrier)
	if err != nil {
		t.Fatal(err)
	}
	var received rootstate.Group
	if err := registry.Register(rootstate.GroupStateCapability,
		func(_ context.Context, descriptor rootstate.Group) (raft.StateMachine, error) {
			received = descriptor
			return &registryState{}, nil
		}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(rootstate.GroupStateCapability,
		func(context.Context, rootstate.Group) (raft.StateMachine, error) { return &registryState{}, nil }); err == nil {
		t.Fatal("duplicate StateKind factory was accepted")
	}
	state, err := registry.NewState(context.Background(), "data-1")
	if err != nil || state == nil || barrier.calls != 1 || received.GroupID != "data-1" ||
		received.StateKind != rootstate.GroupStateCapability {
		t.Fatalf("constructed state = %T, descriptor %+v, barriers %d, err %v",
			state, received, barrier.calls, err)
	}
}

func TestRegistryFailsClosedForStaleRootOrUnknownKind(t *testing.T) {
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	applyRegistryRoot(t, root, 1, rootstate.Command{Version: 1, RequestID: "group-special",
		Type: rootstate.RegisterGroup, Group: &rootstate.Group{GroupID: "special-1", StateKind: "special"}})
	barrier := &registryBarrier{err: errors.New("no Root quorum")}
	registry, _ := New(root, barrier)
	if _, err := registry.NewState(context.Background(), "special-1"); err == nil {
		t.Fatal("state was constructed without Root read barrier")
	}
	barrier.err = nil
	if _, err := registry.NewState(context.Background(), "special-1"); err == nil {
		t.Fatal("state was constructed without a registered StateKind factory")
	}
}

func applyRegistryRoot(t *testing.T, state *rootstate.State, index uint64, command rootstate.Command) {
	t.Helper()
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Apply(index, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := state.ResultForCommand(encoded)
	if err != nil || !found || result.Error != "" {
		t.Fatalf("Root command result = %+v, %v, %v", result, found, err)
	}
}
