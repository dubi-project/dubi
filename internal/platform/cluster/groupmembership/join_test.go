package groupmembership

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

type joinState struct{ closed bool }

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
	manager, err := NewJoinManager(host, 7, func(_ context.Context, groupID string) (raft.StateMachine, error) {
		created++
		if groupID != "data-1" {
			t.Fatalf("factory GroupID = %q", groupID)
		}
		return &joinState{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PrepareJoining(context.Background(), "data-1", 7); err != nil {
		t.Fatal(err)
	}
	if err := manager.PrepareJoining(context.Background(), "data-1", 7); err != nil {
		t.Fatal(err)
	}
	if created != 1 || host.groupID != "data-1" || !host.joining || len(host.peers) != 0 {
		t.Fatalf("joining open: created=%d group=%q joining=%v peers=%v",
			created, host.groupID, host.joining, host.peers)
	}
	if err := manager.PrepareJoining(context.Background(), "data-1", 8); err == nil {
		t.Fatal("joining Group accepted another RaftID")
	}
}

func TestJoinManagerClosesStateWhenHostRejectsOpen(t *testing.T) {
	host := &joinHost{open: make(map[string]bool), addErr: errors.New("closed Host")}
	state := &joinState{}
	manager, _ := NewJoinManager(host, 7,
		func(context.Context, string) (raft.StateMachine, error) { return state, nil })
	if err := manager.PrepareJoining(context.Background(), "data-1", 7); err == nil || !state.closed {
		t.Fatalf("rejected joining open = %v, state closed=%v", err, state.closed)
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
		func(context.Context, string) (raft.StateMachine, error) { return &joinState{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PrepareJoining(context.Background(), "data-1", identityValue.RaftID); err != nil {
		t.Fatal(err)
	}
	if !host.HasGroup("data-1") {
		t.Fatal("real Raft Host did not retain prepared joining Group")
	}
}
