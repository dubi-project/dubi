package regiongroups

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testHost struct {
	root        *rootstate.State
	leader      bool
	readErr     error
	submissions int
	localReads  map[string][]uint64
}

func (h *testHost) Status(group string) (raft.GroupStatus, error) {
	s := raft.GroupStatus{RaftID: 1, LeaderID: 1, Role: etcdraft.StateFollower}
	if group != raft.RootGroupID {
		cohort, _, err := h.root.GroupInitialCohort(group)
		if err != nil || len(cohort.Nodes) == 0 {
			return s, errors.New("missing local Group")
		}
		s.LeaderID = cohort.Nodes[0]
	}
	if h.leader {
		s.Role = etcdraft.StateLeader
	}
	return s, nil
}
func (h *testHost) Membership(group string) (*raftpb.ConfState, error) {
	c, _, err := h.root.GroupInitialCohort(group)
	return &raftpb.ConfState{Voters: slices.Clone(c.Nodes)}, err
}
func (h *testHost) ReadIndex(ctx context.Context, group string) (uint64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if group != raft.RootGroupID {
		h.localReads[group] = append(h.localReads[group], 1)
	}
	return h.root.AppliedIndex(), h.readErr
}
func (h *testHost) Submit(_ string, data []byte) error {
	h.submissions++
	return h.root.Apply(h.root.AppliedIndex()+1, data)
}

type testRecovery struct{ calls int }

func (r *testRecovery) RecoverLocalGroups(ctx context.Context) error { r.calls++; return ctx.Err() }

type testInspect struct {
	root     *rootstate.State
	failures map[string]error
	calls    map[string][]uint64
	mismatch bool
}

func (i *testInspect) ReadIndex(ctx context.Context, id uint64, group string) (uint64, error) {
	if id == 1 {
		return 0, errors.New("RPC self-target forbidden")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	i.calls[group] = append(i.calls[group], id)
	return 50, i.failures[group]
}
func (i *testInspect) InspectMembership(_ context.Context, _ uint64, group string) (*raftpb.ConfState, uint64, error) {
	c, _, err := i.root.GroupInitialCohort(group)
	if err != nil {
		return nil, 0, err
	}
	voters := slices.Clone(c.Nodes)
	if i.mismatch {
		voters = voters[:2]
	}
	return &raftpb.ConfState{Voters: voters}, c.Nodes[0], nil
}
func fixture(t *testing.T) (*Coordinator, *testHost, *testRecovery, *testInspect) {
	t.Helper()
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	h := &testHost{root: root, leader: true, localReads: map[string][]uint64{}}
	recovery := &testRecovery{}
	inspect := &testInspect{root: root, failures: map[string]error{}, calls: map[string][]uint64{}}
	for id := uint64(1); id <= 7; id++ {
		region := "west"
		if id >= 4 && id <= 6 {
			region = "east"
		}
		apply(t, root, rootstate.Command{Type: rootstate.RegisterNode, Node: &rootstate.Node{NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: region, Address: fmt.Sprintf("127.0.0.1:%d", 10000+id)}})
	}
	if err := root.ApplyMembership(root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 4}, Learners: []uint64{2, 3, 5, 6}}); err != nil {
		t.Fatal(err)
	}
	c, err := New(h, root, inspect, recovery)
	if err != nil {
		t.Fatal(err)
	}
	return c, h, recovery, inspect
}
func apply(t *testing.T, s *rootstate.State, command rootstate.Command) {
	t.Helper()
	command.Version, command.RequestID = rootstate.FormatVersion, fmt.Sprintf("request-%d", s.AppliedIndex()+1)
	data, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(s.AppliedIndex()+1, data); err != nil {
		t.Fatal(err)
	}
	r, found, err := s.ResultForCommand(data)
	if err != nil || !found || r.Error != "" {
		t.Fatal(r, found, err)
	}
}

func TestRegionalFormationPlansAdmittedNodesAndPublishesOnlyObservedMembership(t *testing.T) {
	c, h, recovery, inspect := fixture(t)
	for n := 0; n < 2; n++ {
		if err := c.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for region, nodes := range map[string][]uint64{"west": {1, 2, 3}, "east": {4, 5, 6}} {
		id := GroupID(region)
		cohort, found, err := h.root.GroupInitialCohort(id)
		if err != nil || !found || !cohort.Formation || !slices.Equal(cohort.Nodes, nodes) {
			t.Fatal(cohort, found, err)
		}
		group, found, err := h.root.Group(id)
		if err != nil || !found || group.Epoch != 2 || len(group.Members) != 3 {
			t.Fatal(group, found, err)
		}
		observed := append(slices.Clone(h.localReads[id]), inspect.calls[id]...)
		slices.Sort(observed)
		if !slices.Equal(observed, nodes) {
			t.Fatal("not every participant observed", observed)
		}
		head, _, err := h.root.RegionCohortHead(region)
		if err != nil || head.CurrentGeneration != 0 || head.PendingGeneration != 1 {
			t.Fatal("formation pretended to confirm data cohort", head, err)
		}
	}
	if recovery.calls != 2 {
		t.Fatal(recovery.calls)
	}
	before := h.submissions
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if before != h.submissions {
		t.Fatal("completed formation replayed publication")
	}
}

func TestRegionalFormationUnreadyRegionDoesNotBlockOtherRegionOrShrinkPlan(t *testing.T) {
	c, h, _, inspect := fixture(t)
	inspect.failures[GroupID("west")] = status.Error(codes.Unavailable, "offline")
	for n := 0; n < 4; n++ {
		if err := c.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	west, _, _ := h.root.Group(GroupID("west"))
	east, _, _ := h.root.Group(GroupID("east"))
	if west.Epoch != 1 || east.Epoch != 2 {
		t.Fatal(west, east)
	}
	cohort, _, _ := h.root.GroupInitialCohort(west.GroupID)
	if !slices.Equal(cohort.Nodes, []uint64{1, 2, 3}) {
		t.Fatal(cohort)
	}
	inspect.failures[west.GroupID] = status.Error(codes.PermissionDenied, "unauthorized")
	if err := c.Reconcile(context.Background()); status.Code(err) != codes.PermissionDenied {
		t.Fatal("authorization error hidden", err)
	}
	delete(inspect.failures, west.GroupID)
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	west, _, _ = h.root.Group(west.GroupID)
	if west.Epoch != 2 {
		t.Fatal(west)
	}
}

func TestRegionalFormationFollowerAndUnconfirmedRootDoNotPlan(t *testing.T) {
	c, h, r, _ := fixture(t)
	h.leader = false
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.submissions != 0 || r.calls != 1 {
		t.Fatal(h.submissions, r.calls)
	}
	h.leader = true
	h.readErr = errors.New("no quorum")
	if err := c.Reconcile(context.Background()); !errors.Is(err, h.readErr) {
		t.Fatal(err)
	}
	if h.submissions != 0 {
		t.Fatal("planned without Root barrier")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := r.calls
	if err := c.Reconcile(ctx); !errors.Is(err, context.Canceled) || r.calls != before {
		t.Fatal(err, r.calls)
	}
}

func TestRegionalFormationRejectsMismatchedRealMembership(t *testing.T) {
	c, h, _, inspect := fixture(t)
	inspect.mismatch = true
	if err := c.Reconcile(context.Background()); err == nil {
		t.Fatal("mismatched membership published")
	}
	groups, _ := h.root.Groups()
	for _, g := range groups {
		if g.Epoch != 1 {
			t.Fatal(g)
		}
	}
}

func TestRegionalFormationPlansSingleNodeRegion(t *testing.T) {
	c, h, _, _ := fixture(t)
	apply(t, h.root, rootstate.Command{Type: rootstate.RegisterNode,
		Node: &rootstate.Node{NodeID: "node_00000000000000000000000000000008", RaftID: 8, RegionID: "north", Address: "127.0.0.1:10008"}})
	if err := h.root.ApplyMembership(h.root.AppliedIndex()+1, &raftpb.ConfState{
		Voters: []uint64{1, 4}, Learners: []uint64{2, 3, 5, 6, 8},
	}); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 5; n++ {
		if err := c.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	cohort, found, err := h.root.GroupInitialCohort(GroupID("north"))
	if err != nil || !found || !cohort.Formation || !slices.Equal(cohort.Nodes, []uint64{8}) {
		t.Fatalf("single-Node Region cohort = %+v, found=%v, err=%v", cohort, found, err)
	}
	group, found, err := h.root.Group(GroupID("north"))
	if err != nil || !found || group.Epoch != 2 || len(group.Members) != 1 || group.Members[0].RaftID != 8 {
		t.Fatalf("single-Node Region Group = %+v, found=%v, err=%v", group, found, err)
	}
}
