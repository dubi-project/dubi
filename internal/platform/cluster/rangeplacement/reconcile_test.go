package rangeplacement

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type placementHost struct {
	root                           *rootstate.State
	pending                        []byte
	reads, submissions, applyAfter int
	afterApply                     func()
}

func (h *placementHost) Status(string) (raft.GroupStatus, error) {
	return raft.GroupStatus{RaftID: 1, LeaderID: 1, Role: etcdraft.StateLeader}, nil
}
func (h *placementHost) Submit(_ string, encoded []byte) error {
	if h.pending != nil {
		return errors.New("previous proposal not observed")
	}
	h.submissions++
	h.pending, h.reads = append([]byte(nil), encoded...), 0
	return nil
}
func (h *placementHost) ReadIndex(ctx context.Context, _ string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if h.pending != nil {
		h.reads++
		if h.reads == h.applyAfter {
			if err := h.root.Apply(h.root.AppliedIndex()+1, h.pending); err != nil {
				return 0, err
			}
			h.pending = nil
			if h.afterApply != nil {
				h.afterApply()
			}
		}
	}
	return h.root.AppliedIndex(), nil
}

func placementFixture(t *testing.T, count int) (*Coordinator, *placementHost) {
	t.Helper()
	s, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2, 3, 4}}); err != nil {
		t.Fatal(err)
	}
	apply := func(command rootstate.Command) { applyPlacementFixture(t, s, command) }
	for id := uint64(1); id <= 4; id++ {
		apply(rootstate.Command{Type: rootstate.RegisterNode, Node: &rootstate.Node{NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: "west", Address: fmt.Sprintf("127.0.0.1:%d", 10000+id)}})
	}
	apply(rootstate.Command{Type: rootstate.RegisterGroup, Group: &rootstate.Group{GroupID: "regional-west", StateKind: rootstate.GroupStateEngine}})
	apply(rootstate.Command{Type: rootstate.PublishGroupMembers, Membership: &rootstate.GroupMembership{GroupID: "regional-west", ExpectedEpoch: 1, Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}})
	apply(rootstate.Command{Type: rootstate.PlanRegionCohort, RegionPlan: &rootstate.RegionCohortPlan{RegionID: "west", ExpectedGeneration: 0, GroupID: "regional-west", Nodes: []uint64{1, 2, 3}}})
	apply(rootstate.Command{Type: rootstate.ConfirmRegionCohort, RegionConfirmation: &rootstate.RegionCohortConfirmation{RegionID: "west", Generation: 1, GroupID: "regional-west", GroupEpoch: 2, GroupFenceRequestID: "fence-1", GroupFenceIndex: 100}})
	apply(rootstate.Command{Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: "tenant"}})
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("range-%03d", i)
		apply(rootstate.Command{Type: rootstate.CreateDataset, Dataset: &rootstate.Dataset{TenantID: "tenant", EngineID: "record", DatasetID: fmt.Sprintf("dataset-%03d", i), RangeID: id, OwnerGroupID: "regional-west"}})
		apply(rootstate.Command{Type: rootstate.ActivateRange, Activation: &rootstate.RangeActivation{RangeID: id, OwnerGroupID: "regional-west", Epoch: 1, OwnerRequestID: "owner-" + id, OwnerAppliedIndex: 1000}})
	}
	h := &placementHost{root: s, applyAfter: 1}
	c, err := New(h, s)
	if err != nil {
		t.Fatal(err)
	}
	return c, h
}

func applyPlacementFixture(t *testing.T, s *rootstate.State, command rootstate.Command) {
	t.Helper()
	command.Version, command.RequestID = rootstate.FormatVersion, fmt.Sprintf("fixture-%d", s.AppliedIndex()+1)
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(s.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := s.ResultForCommand(encoded)
	if err != nil || !found || result.Error != "" {
		t.Fatal(command.Type, result, found, err)
	}
}

func TestReconcilePublishesBoundedBatchesAndRefreshesAllExistingRanges(t *testing.T) {
	c, h := placementFixture(t, 35)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, want := range []int{16, 32, 35} {
		if err := c.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		if h.submissions != want {
			t.Fatal("Ranges still require one timer each, or round was unbounded", h.submissions, want)
		}
	}
	// Advance the confirmed cohort through the ordinary growth authorization.
	applyPlacementFixture(t, h.root, rootstate.Command{Type: rootstate.PlanRegionCohort, RegionPlan: &rootstate.RegionCohortPlan{RegionID: "west", ExpectedGeneration: 1, GroupID: "regional-west", Nodes: []uint64{1, 2, 3, 4}}})
	next, _, err := h.root.RegionCohort("west", 2)
	if err != nil {
		t.Fatal(err)
	}
	applyPlacementFixture(t, h.root, rootstate.Command{Type: rootstate.AuthorizeRegionGrowth, RegionGrowth: &rootstate.RegionGrowthSeal{RegionID: "west", GroupID: "regional-west", Generation: 2, PlannedIndex: next.PlannedIndex, ExpectedEpoch: 2, PreviousFenceRequestID: "fence-1", PreviousFenceIndex: 100, PreviousMembershipIndex: 1, SealRequestID: "seal-2", SealIndex: 101, ChangeID: "grow-2", Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}, {RaftID: 4, Role: rootstate.GroupMemberLearner}}}})
	applyPlacementFixture(t, h.root, rootstate.Command{Type: rootstate.CompleteGroupMembers, GroupCompletion: &rootstate.GroupChangeCompletion{GroupID: "regional-west", ChangeID: "grow-2"}})
	applyPlacementFixture(t, h.root, rootstate.Command{Type: rootstate.ConfirmRegionCohort, RegionConfirmation: &rootstate.RegionCohortConfirmation{RegionID: "west", Generation: 2, GroupID: "regional-west", GroupEpoch: 3, GroupFenceRequestID: "fence-2", GroupFenceIndex: 1003}})
	for _, want := range []int{51, 67, 70} {
		if err := c.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		if h.submissions != want {
			t.Fatal(h.submissions, want)
		}
	}
	for i := 0; i < 35; i++ {
		p, found, err := h.root.RangePlacement(fmt.Sprintf("range-%03d", i))
		if err != nil || !found || len(p.Regions) != 1 || p.Regions[0].Generation != 2 {
			t.Fatal(p, found, err)
		}
	}
	if err := c.Reconcile(ctx); err != nil || h.submissions != 70 {
		t.Fatal("stable placements were reproposed", err, h.submissions)
	}
}

func TestPlacementWaitsForExactDurableResultWithoutResubmission(t *testing.T) {
	c, h := placementFixture(t, 1)
	h.applyAfter = 3
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if h.submissions != 1 || h.reads != 3 {
		t.Fatal(h.submissions, h.reads)
	}
}

func TestPlacementWaitPreservesRejectionAndCancellation(t *testing.T) {
	for _, mode := range []string{"rejected", "canceled-after-submit", "cancel-round"} {
		t.Run(mode, func(t *testing.T) {
			c, h := placementFixture(t, 3)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if mode == "rejected" {
				h.applyAfter = 2
				p := rootstate.RangePlacement{RangeID: "missing", RangeEpoch: 1, OwnerGroupID: "regional-west", Regions: []rootstate.RangeRegionPlacement{{RegionID: "west", GroupID: "regional-west", Generation: 1}}}
				if err := c.publish(ctx, p); err == nil || !strings.Contains(err.Error(), "Range is not registered") {
					t.Fatal(err)
				}
			} else {
				if mode == "cancel-round" {
					h.afterApply = cancel
				} else {
					h.applyAfter = 0
				}
				if err := c.Reconcile(ctx); ctx.Err() == nil || !errors.Is(err, ctx.Err()) {
					t.Fatal(err, ctx.Err())
				}
			}
			if h.submissions != 1 {
				t.Fatal("proposal replayed or cancellation ignored", h.submissions)
			}
		})
	}
}
