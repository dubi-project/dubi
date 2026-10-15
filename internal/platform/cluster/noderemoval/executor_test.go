package noderemoval

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

type unusedGroups struct{ t *testing.T }

func (g unusedGroups) CheckAvailable(context.Context, string) error {
	return nil
}

func (g unusedGroups) Plan(context.Context, rootstate.GroupChangePlan) error {
	g.t.Error("unexpected data Group plan")
	return errors.New("unexpected data Group")
}
func (g unusedGroups) Reconcile(context.Context, string) error {
	g.t.Error("unexpected data Group reconciliation")
	return errors.New("unexpected data Group")
}

type discardTransport struct{}

func (discardTransport) Send(string, *raftpb.Message) error { return nil }

type lostReplies struct{ *raft.Host }

func (h lostReplies) RemoveRootMember(conf *raftpb.ConfState, id uint64) error {
	if err := h.Host.RemoveRootMember(conf, id); err != nil {
		return err
	}
	return errors.New("injected lost membership reply")
}

func (h lostReplies) Submit(group string, command []byte) error {
	if err := h.Host.Submit(group, command); err != nil {
		return err
	}
	return errors.New("injected lost command reply")
}

// This uses actual Root Raft proposals and durable state, not ApplyMembership
// from a fake executor. An offline learner is intentionally not a data Node;
// this test does not claim regional data rebalancing or voter removal coverage.
func TestRaftExecutorRemovesOfflineRootLearnerAndFencesWithLostReplies(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	root, err := rootstate.Open(filepath.Join(store, "state"))
	if err != nil {
		t.Fatal(err)
	}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: store, TickInterval: time.Hour, Transport: discardTransport{}})
	if err != nil {
		_ = root.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	if err := host.AddGroup(raft.RootGroupID, []uint64{id.RaftID}, false, root); err != nil {
		t.Fatal(err)
	}
	if err := host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	submit := func(command rootstate.Command) {
		t.Helper()
		command.Version = rootstate.FormatVersion
		encoded, err := rootstate.EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := host.Submit(raft.RootGroupID, encoded); err != nil {
			t.Fatal(err)
		}
		if _, err := host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			t.Fatal(err)
		}
		result, found, err := root.ResultForCommand(encoded)
		if err != nil || !found || result.Error != "" {
			t.Fatal(result, found, err)
		}
	}
	target := id.RaftID + 1
	if target == 0 {
		target = 1
	}
	submit(rootstate.Command{RequestID: "leader", Type: rootstate.RegisterNode,
		Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "west", Address: "leader:7443"}})
	submit(rootstate.Command{RequestID: "offline-learner", Type: rootstate.RegisterNode,
		Node: &rootstate.Node{NodeID: "node_00000000000000000000000000000002", RaftID: target, RegionID: "west", Address: "offline:7443"}})
	if err := host.AddLearner(raft.RootGroupID, target); err != nil {
		t.Fatal(err)
	}
	if _, err := host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	oldConf, err := root.RootMembership()
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewRaftExecutor(root, lostReplies{host}, unusedGroups{t})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(root, executor)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := coordinator.Reconcile(ctx, target)
	if err != nil || !plan.AlreadyFenced || plan.RootMember || len(plan.Groups) != 0 {
		t.Fatal("real Raft removal/Fence failed", plan, err)
	}
	if err := host.RemoveRootMember(oldConf, target); !errors.Is(err, raft.ErrTopologyChanged) {
		t.Fatal("stale Root membership accepted", err)
	}
	if _, _, _, err := root.Snapshot(); err != nil {
		t.Fatal("removed identity did not validate in Root snapshot", err)
	}
	before := root.AppliedIndex()
	if retry, err := coordinator.Reconcile(ctx, target); err != nil || !retry.AlreadyFenced || root.AppliedIndex() != before {
		t.Fatal("completed removal retry changed state", retry, err)
	}
	if _, err := coordinator.Reconcile(ctx, id.RaftID); !errors.Is(err, ErrRemovalWouldLoseMajority) {
		t.Fatal("last Root voter removal accepted", err)
	}
}
