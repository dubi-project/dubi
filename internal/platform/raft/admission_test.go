package raft

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/storage/diskpressure"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestHostAdmissionRejectsNewWorkButPreservesReads(t *testing.T) {
	root := t.TempDir()
	id, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	var pressured atomic.Bool
	host, err := OpenHost(HostOptions{StoreRoot: root, TickInterval: time.Hour, ProposalGuard: func() error {
		if pressured.Load() {
			return diskpressure.ErrPressure
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if host.DiskStatus().Pressured {
		t.Skip("requires Store headroom")
	}
	for _, groupID := range []string{RootGroupID, "data-1"} {
		state := hostTestState(t, root, groupID)
		if err := host.AddGroup(groupID, []uint64{id.RaftID}, false, state); err != nil {
			t.Fatal(err)
		}
		if err := host.Campaign(groupID); err != nil {
			t.Fatal(err)
		}
		if err := host.Submit(groupID, []byte("confirmed")); err != nil {
			t.Fatal(err)
		}
	}
	pressured.Store(true)
	for _, groupID := range []string{RootGroupID, "data-1"} {
		before, err := host.Status(groupID)
		if err != nil {
			t.Fatal(err)
		}
		if err := host.Submit(groupID, []byte("reject")); !errors.Is(err, diskpressure.ErrPressure) {
			t.Fatal(err)
		}
		if err := host.Step(groupID, &raftpb.Message{Type: raftpb.MessageType_MsgProp.Enum(), From: proto.Uint64(99), To: proto.Uint64(id.RaftID),
			Entries: []*raftpb.Entry{{Data: []byte("forwarded-reject")}}}); !errors.Is(err, diskpressure.ErrPressure) {
			t.Fatal("forwarded proposal bypass", err)
		}
		if err := host.AddLearner(groupID, 99); !errors.Is(err, diskpressure.ErrPressure) {
			t.Fatal("membership bypass", err)
		}
		if _, err := host.CreateSnapshot(groupID); !errors.Is(err, diskpressure.ErrPressure) {
			t.Fatal("snapshot bypass", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		index, err := host.ReadIndex(ctx, groupID)
		cancel()
		if err != nil || index != before.AppliedIndex {
			t.Fatal("pressure broke confirmed reads", index, err)
		}
		after, err := host.Status(groupID)
		if err != nil || after.CommitIndex != before.CommitIndex {
			t.Fatal("rejected work reached Raft", err)
		}
		if err := host.GroupError(groupID); err != nil {
			t.Fatal("admission made Group fatal", err)
		}
	}
	if err := host.TickAll(); err != nil {
		t.Fatal("ticks blocked by pressure", err)
	}
	pressured.Store(false)
	if err := host.Submit("data-1", []byte("recovered")); err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateSnapshot("data-1"); err != nil {
		t.Fatal(err)
	}
}

func TestPressuredFollowerStillAppliesCommittedReplication(t *testing.T) {
	groups := make(map[uint64]*Group)
	transport := &queuedTransport{groups: groups, blocked: make(map[uint64]bool)}
	states := make(map[uint64]*testState)
	for _, id := range []uint64{1, 2, 3} {
		root := t.TempDir()
		state := openTestState(t, root)
		guard := func() error {
			if id == 2 {
				return diskpressure.ErrPressure
			}
			return nil
		}
		group, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: id,
			BootstrapPeers: []uint64{1, 2, 3}, State: state, Transport: transport, ProposalGuard: guard})
		if err != nil {
			t.Fatal(err)
		}
		defer group.Close()
		groups[id], states[id] = group, state
	}
	if err := groups[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	if err := groups[1].Submit([]byte("replicate-during-pressure")); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	if len(states[2].disk.Commands) != 1 || string(states[2].disk.Commands[0]) != "replicate-during-pressure" {
		t.Fatal("follower failed to apply replicated commit")
	}
	if err := groups[2].Submit([]byte("new-local-work")); !errors.Is(err, diskpressure.ErrPressure) {
		t.Fatal("follower accepted fresh proposal", err)
	}
	// Fall behind a compacted log, then force snapshot catch-up while the
	// follower still rejects new proposals. Committed recovery must bypass it.
	transport.blocked[2] = true
	for i := 0; i < 5; i++ {
		if err := groups[1].Submit([]byte("after-partition")); err != nil {
			t.Fatal(err)
		}
		transport.deliver(t)
	}
	if _, err := groups[1].CreateSnapshot(); err != nil {
		t.Fatal(err)
	}
	transport.blocked[2] = false
	for i := 0; i < 20 && len(states[2].disk.Commands) < 6; i++ {
		if err := groups[1].Tick(); err != nil {
			t.Fatal(err)
		}
		transport.deliver(t)
	}
	if len(states[2].disk.Commands) != 6 {
		t.Fatal("pressure blocked committed snapshot recovery")
	}
}
