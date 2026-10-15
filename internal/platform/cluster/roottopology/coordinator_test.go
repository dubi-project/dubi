package roottopology

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type testHost struct {
	status             raft.GroupStatus
	readErr, changeErr error
	reads, changes     int
	target             uint64
	expected           *raftpb.ConfState
	desired            []uint64
}

func (h *testHost) Status(string) (raft.GroupStatus, error)           { return h.status, nil }
func (h *testHost) ReadIndex(context.Context, string) (uint64, error) { h.reads++; return 1, h.readErr }
func (h *testHost) ReconfigureVoters(_ string, conf *raftpb.ConfState, voters []uint64) error {
	h.changes++
	h.expected, h.desired = proto.Clone(conf).(*raftpb.ConfState), slices.Clone(voters)
	return h.changeErr
}
func (h *testHost) TransferLeadership(_ context.Context, _ string, target uint64) (raft.GroupStatus, error) {
	h.target = target
	return raft.GroupStatus{LeaderID: target}, nil
}

func coordinatorFixture(t *testing.T, view rootjoin.View) (*Coordinator, *testHost) {
	t.Helper()
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	for _, node := range view.Peers {
		cmd, err := rootstate.EncodeCommand(rootstate.Command{Version: 1, RequestID: fmt.Sprintf("node-%d", node.RaftID), Type: rootstate.RegisterNode, Node: &node})
		if err != nil {
			t.Fatal(err)
		}
		if err := root.Apply(root.AppliedIndex()+1, cmd); err != nil {
			t.Fatal(err)
		}
	}
	if err := root.ApplyMembership(root.AppliedIndex()+1, &raftpb.ConfState{Voters: view.Voters, Learners: view.Learners,
		VotersOutgoing: view.VotersOutgoing, LearnersNext: view.LearnersNext, AutoLeave: proto.Bool(view.AutoLeave)}); err != nil {
		t.Fatal(err)
	}
	host := &testHost{status: raft.GroupStatus{RaftID: 1, LeaderID: 1, Role: etcdraft.StateLeader}}
	c, err := New(host, root, testCluster)
	if err != nil {
		t.Fatal(err)
	}
	return c, host
}

func TestRootTopologyCoordinatorConfirmedCutAndRetry(t *testing.T) {
	c, host := coordinatorFixture(t, planView(3))
	host.changeErr = raft.ErrTopologyPending
	for i := 0; i < 3; i++ {
		if err := c.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(host.desired, []uint64{1, 2, 3}) || !slices.Equal(host.expected.GetVoters(), []uint64{1}) {
			t.Fatal("pending health weakened desired topology", host.desired, host.expected)
		}
	}
	if host.reads != 3 || host.changes != 3 {
		t.Fatal(host)
	}
	host.changeErr = raft.ErrTopologyChanged
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	host.readErr = errors.New("quorum unavailable")
	if err := c.Reconcile(context.Background()); !errors.Is(err, host.readErr) || host.changes != 4 {
		t.Fatal("unconfirmed cut triggered change", err, host.changes)
	}
	host.readErr = nil
	host.status.Role = etcdraft.StateFollower
	if err := c.Reconcile(context.Background()); err != nil || host.changes != 4 || host.reads != 5 {
		t.Fatal("follower performed control work", err, host)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Reconcile(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRootTopologyCoordinatorTransfersBeforeDemotionAndLeavesJointAlone(t *testing.T) {
	view := planView(3, 3, 3)
	view.Voters, view.Learners = []uint64{1, 2, 3}, []uint64{4, 5, 6, 7, 8, 9}
	c, host := coordinatorFixture(t, view)
	host.status.RaftID, host.status.LeaderID = 2, 2
	if err := c.Reconcile(context.Background()); err != nil || host.target != 1 || host.changes != 0 {
		t.Fatal("did not transfer to retained voter before local demotion", err, host)
	}
	view.VotersOutgoing, view.AutoLeave = []uint64{1}, true
	c, host = coordinatorFixture(t, view)
	if err := c.Reconcile(context.Background()); err != nil || host.changes != 0 || host.target != 0 {
		t.Fatal("joint consensus interrupted", err, host)
	}
}
