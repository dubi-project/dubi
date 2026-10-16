package groupmembership

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

type joinState struct{ closed bool }

func joinTask() rootstate.GroupTaskReference {
	return rootstate.GroupTaskReference{GroupID: "data-1", ChangeID: "join", ExpectedEpoch: 1}
}

func (*joinState) AppliedIndex() uint64                            { return 0 }
func (*joinState) ConfState() *raftpb.ConfState                    { return &raftpb.ConfState{} }
func (*joinState) Apply(uint64, []byte) error                      { return nil }
func (*joinState) Advance(uint64) error                            { return nil }
func (*joinState) ApplyMembership(uint64, *raftpb.ConfState) error { return nil }
func (*joinState) Restore(uint64, []byte, *raftpb.ConfState) error { return nil }
func (state *joinState) Close() error                              { state.closed = true; return nil }

type joinHost struct {
	open    map[string]bool
	groupID string
	peers   []uint64
	joining bool
	addErr  error
}

func (host *joinHost) HasGroup(groupID string) bool { return host.open[groupID] }
func (host *joinHost) AddGroup(groupID string, peers []uint64, joining bool, _ raft.StateMachine) error {
	host.groupID, host.peers, host.joining = groupID, peers, joining
	if host.addErr == nil {
		host.open[groupID] = true
	}
	return host.addErr
}

func TestJoinManagerOpensJoiningGroupOnce(t *testing.T) {
	host := &joinHost{open: make(map[string]bool)}
	created := 0
	manager, err := NewJoinManager(host, 7, func(_ context.Context, task rootstate.GroupTaskReference) (raft.StateMachine, error) {
		created++
		if task != joinTask() {
			t.Fatalf("factory task = %+v", task)
		}
		return &joinState{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PrepareJoining(context.Background(), joinTask(), 7); err != nil {
		t.Fatal(err)
	}
	if err := manager.PrepareJoining(context.Background(), joinTask(), 7); err != nil {
		t.Fatal(err)
	}
	if created != 1 || host.groupID != "data-1" || !host.joining || len(host.peers) != 0 {
		t.Fatalf("joining open: created=%d group=%q joining=%v peers=%v",
			created, host.groupID, host.joining, host.peers)
	}
	if err := manager.PrepareJoining(context.Background(), joinTask(), 8); err == nil {
		t.Fatal("joining Group accepted another RaftID")
	}
}

func TestJoinManagerTreatsConcurrentOpenAsIdempotent(t *testing.T) {
	host := &joinHost{open: make(map[string]bool)}
	manager, err := NewJoinManager(host, 7, func(context.Context, rootstate.GroupTaskReference) (raft.StateMachine, error) {
		return nil, ErrJoiningGroupAlreadyOpen
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PrepareJoining(context.Background(), joinTask(), 7); err != nil {
		t.Fatal(err)
	}
	if host.groupID != "" || len(host.open) != 0 {
		t.Fatal("concurrent open path attempted a duplicate local Group attach")
	}
}

func TestJoinManagerClosesStateWhenHostRejectsOpen(t *testing.T) {
	host := &joinHost{open: make(map[string]bool), addErr: errors.New("closed Host")}
	state := &joinState{}
	manager, _ := NewJoinManager(host, 7,
		func(context.Context, rootstate.GroupTaskReference) (raft.StateMachine, error) { return state, nil })
	if err := manager.PrepareJoining(context.Background(), joinTask(), 7); err == nil || !state.closed {
		t.Fatalf("rejected joining open = %v, state closed=%v", err, state.closed)
	}
}

func TestJoinManagerCancellationDoesNotOpenGroup(t *testing.T) {
	for _, mode := range []string{"before", "waiting", "factory"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			host := &joinHost{open: make(map[string]bool)}
			state := &joinState{}
			created := 0
			manager, _ := NewJoinManager(host, 7, func(_ context.Context, task rootstate.GroupTaskReference) (raft.StateMachine, error) {
				created++
				if task != joinTask() {
					t.Error("original task lost")
				}
				cancel()
				return state, nil
			})
			if mode == "before" {
				cancel()
			}
			if mode == "waiting" {
				manager.mu.Lock()
				done := make(chan error, 1)
				go func() { done <- manager.PrepareJoining(ctx, joinTask(), 7) }()
				cancel()
				manager.mu.Unlock()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else if err := manager.PrepareJoining(ctx, joinTask(), 7); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if host.groupID != "" || len(host.open) != 0 {
				t.Fatal("canceled preparation opened Group")
			}
			if mode == "factory" {
				if created != 1 || !state.closed {
					t.Fatal("canceled factory state leaked")
				}
			} else if created != 0 {
				t.Fatal("canceled request entered factory")
			}
		})
	}
}

func TestJoinManagerOpensRealRaftHostInJoiningMode(t *testing.T) {
	root := t.TempDir()
	identityValue, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: root, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	manager, err := NewJoinManager(host, identityValue.RaftID,
		func(context.Context, rootstate.GroupTaskReference) (raft.StateMachine, error) {
			return &joinState{}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PrepareJoining(context.Background(), joinTask(), identityValue.RaftID); err != nil {
		t.Fatal(err)
	}
	if !host.HasGroup("data-1") {
		t.Fatal("real Raft Host did not retain prepared joining Group")
	}
}
