package regiongroups

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type confirmationHandler struct{}

func (confirmationHandler) Apply(*capstate.Transaction, router.Target, []byte) (capstate.Outcome, error) {
	return capstate.Outcome{}, errors.New("fence called module handler")
}

type testParticipants struct {
	root          *rootstate.State
	states        map[string]map[uint64]*capstate.State
	proposals     map[string]int
	observed      map[string][]uint64
	beforeObserve func(context.Context, uint64, string) error
	afterObserve  func(uint64, string, *Observation)
	lostResponse  bool
}

func (p *testParticipants) Observe(ctx context.Context, id uint64, group string) (Observation, error) {
	if err := ctx.Err(); err != nil {
		return Observation{}, err
	}
	if p.beforeObserve != nil {
		if err := p.beforeObserve(ctx, id, group); err != nil {
			return Observation{}, err
		}
	}
	p.observed[group] = append(p.observed[group], id)
	v, err := p.states[group][id].RegionFence()
	o := Observation{RootConfirmedIndex: p.root.AppliedIndex(), GroupConfirmedIndex: v.AppliedIndex, View: v}
	if p.afterObserve != nil {
		p.afterObserve(id, group, &o)
	}
	return o, err
}
func (p *testParticipants) Propose(ctx context.Context, _ uint64, group string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.proposals[group]++
	for _, s := range p.states[group] {
		a, err := routedstate.New(s)
		if err != nil {
			return err
		}
		if err := a.Apply(s.AppliedIndex()+1, data); err != nil {
			return err
		}
	}
	if p.lostResponse {
		p.lostResponse = false
		return status.Error(codes.Unavailable, "lost proposal response")
	}
	return nil
}
func confirmationFixture(t *testing.T) (*Confirmer, *testHost, *testParticipants) {
	t.Helper()
	formation, h, _, _ := fixture(t)
	for range 2 {
		if err := formation.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	p := &testParticipants{root: h.root, states: map[string]map[uint64]*capstate.State{}, proposals: map[string]int{}, observed: map[string][]uint64{}}
	for _, region := range []string{"west", "east"} {
		id := GroupID(region)
		v, found, err := h.root.PendingRegionConfirmation(id)
		if err != nil || !found {
			t.Fatal(v, found, err)
		}
		p.states[id] = map[uint64]*capstate.State{}
		for _, node := range v.Cohort.Nodes {
			s, err := capstate.Open(t.TempDir(), id, confirmationHandler{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			if err := s.ApplyMembership(1, &raftpb.ConfState{Voters: slices.Clone(v.Cohort.Nodes)}); err != nil {
				t.Fatal(err)
			}
			p.states[id][node] = s
		}
	}
	c, err := NewConfirmer(h, h.root, p)
	if err != nil {
		t.Fatal(err)
	}
	return c, h, p
}
func requireHead(t *testing.T, h *testHost, region string, confirmed bool) {
	t.Helper()
	head, found, err := h.root.RegionCohortHead(region)
	if err != nil || !found || (head.CurrentGeneration == 1) != confirmed || (head.PendingGeneration == 0) != confirmed {
		t.Fatal(region, head, found, err)
	}
}

func TestRegionalConfirmationAllParticipantsAndIdempotentRecovery(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "lost-response"}[lost], func(t *testing.T) {
			c, h, p := confirmationFixture(t)
			p.lostResponse = lost
			for range 5 {
				if err := c.Reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
				// A new leader/coordinator has no volatile progress to restore.
				c, _ = NewConfirmer(h, h.root, p)
			}
			for _, region := range []string{"west", "east"} {
				requireHead(t, h, region, true)
				id := GroupID(region)
				if p.proposals[id] != 1 {
					t.Fatal("fence unnecessarily reproposed", p.proposals)
				}
				cohort, _, _ := h.root.GroupInitialCohort(id)
				for _, node := range cohort.Nodes {
					if !slices.Contains(p.observed[id], node) {
						t.Fatal("participant skipped", node)
					}
				}
				for _, s := range p.states[id] {
					v, err := s.RegionFence()
					if err != nil || !v.Active || v.Record.RequestID != cohort.GroupFenceRequestID || v.Record.Index != cohort.GroupFenceIndex {
						t.Fatal(v, err)
					}
				}
			}
			before := h.submissions
			if err := c.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if h.submissions != before {
				t.Fatal("confirmed plan reproposed")
			}
		})
	}
}

func TestRegionalConfirmationUnavailableRegionDoesNotShrinkOrStarve(t *testing.T) {
	c, h, p := confirmationFixture(t)
	p.beforeObserve = func(_ context.Context, id uint64, group string) error {
		if group == GroupID("west") && id == 3 {
			return status.Error(codes.Unavailable, "offline")
		}
		return nil
	}
	for range 4 {
		if err := c.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	requireHead(t, h, "west", false)
	requireHead(t, h, "east", true)
	plan, _, _ := h.root.GroupInitialCohort(GroupID("west"))
	if !slices.Equal(plan.Nodes, []uint64{1, 2, 3}) {
		t.Fatal(plan)
	}
	p.beforeObserve = nil
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireHead(t, h, "west", true)
}

func TestRegionalConfirmationRejectsStaleCutsAndPlanRaces(t *testing.T) {
	for _, mode := range []string{"old-root", "unapplied", "wrong-members", "membership-aba", "missing-fence", "inactive", "different-record", "root-removed", "new-epoch", "active-task", "growth-handoff"} {
		t.Run(mode, func(t *testing.T) {
			c, h, p := confirmationFixture(t)
			group := GroupID("west")
			plan, _, _ := h.root.PendingRegionConfirmation(group)
			p.afterObserve = func(id uint64, g string, o *Observation) {
				if g != group {
					return
				}
				switch mode {
				case "growth-handoff":
					if id == 3 && o.View.Found {
						next := rootstate.RegionCohort{RegionID: "west", GroupID: group, Generation: 2, PlannedIndex: plan.Cohort.PlannedIndex + 1, Nodes: []uint64{1, 2, 3, 7}, State: rootstate.RegionCohortPlanned}
						o.View.Handoff = &regionfence.Handoff{Intent: regionfence.HandoffIntent{Previous: o.View.Record, Next: next}, SealRequestID: "premature-seal", SealIndex: o.View.AppliedIndex + 1}
						o.View.AppliedIndex++
						o.GroupConfirmedIndex = o.View.AppliedIndex
						if err := o.View.Handoff.Validate(); err != nil {
							t.Fatal(err)
						}
					}
				case "old-root":
					o.RootConfirmedIndex = plan.Cohort.PlannedIndex - 1
				case "unapplied":
					o.GroupConfirmedIndex = o.View.AppliedIndex + 1
				case "wrong-members":
					o.View.Conf = &raftpb.ConfState{Voters: []uint64{1, 2, 4}}
				case "membership-aba":
					if o.View.Found {
						o.View.MembershipIndex++
					}
				case "missing-fence":
					if id == 3 {
						o.View.Found = false
					}
				case "inactive":
					if id == 3 {
						o.View.Active = false
					}
				case "different-record":
					if id == 3 {
						o.View.Record.Index++
						o.View.AppliedIndex++
					}
				case "root-removed":
					if id == 3 {
						if err := h.root.ApplyMembership(h.root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 4}, Learners: []uint64{2, 5, 6}}); err != nil {
							t.Fatal(err)
						}
					}
				case "new-epoch":
					if id == 3 {
						apply(t, h.root, rootstate.Command{Type: rootstate.PublishGroupMembers, Membership: &rootstate.GroupMembership{GroupID: group, ExpectedEpoch: 2, Members: plan.Group.Members}})
					}
				case "active-task":
					if id == 3 {
						apply(t, h.root, rootstate.Command{Type: rootstate.PlanGroupMembers, GroupPlan: &rootstate.GroupChangePlan{ChangeID: "racing", GroupID: group, ExpectedEpoch: 2, Members: plan.Group.Members}})
					}
				}
			}
			if err := c.confirm(context.Background(), plan); err == nil {
				t.Fatal("unsafe confirmation succeeded")
			}
			requireHead(t, h, "west", false)
		})
	}
}

func TestRegionalConfirmationLeaderAndCancellation(t *testing.T) {
	c, h, p := confirmationFixture(t)
	h.leader = false
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.observed) != 0 {
		t.Fatal("follower collected confirmation")
	}
	h.leader = true
	h.readErr = errors.New("Root quorum unavailable")
	if err := c.Reconcile(context.Background()); !errors.Is(err, h.readErr) {
		t.Fatal(err)
	}
	if len(p.observed) != 0 {
		t.Fatal("unconfirmed Root collected confirmation")
	}
	h.readErr = nil
	p.beforeObserve = func(ctx context.Context, _ uint64, _ string) error { <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := c.Reconcile(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	requireHead(t, h, "west", false)
	requireHead(t, h, "east", false)
}

func TestRegionalConfirmationDoesNotAutomaticallyActivateGrowth(t *testing.T) {
	c, h, p := confirmationFixture(t)
	for range 2 {
		if err := c.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	requireHead(t, h, "west", true)
	group := GroupID("west")
	if err := h.root.ApplyMembership(h.root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 4}, Learners: []uint64{2, 3, 5, 6, 7}}); err != nil {
		t.Fatal(err)
	}
	apply(t, h.root, rootstate.Command{Type: rootstate.PlanRegionCohort, RegionPlan: &rootstate.RegionCohortPlan{RegionID: "west", GroupID: group, ExpectedGeneration: 1, Nodes: []uint64{1, 2, 3, 7}}})
	previous, _, _ := h.root.RegionCohort("west", 1)
	next, _, _ := h.root.RegionCohort("west", 2)
	apply(t, h.root, rootstate.Command{Type: rootstate.AuthorizeRegionGrowth, RegionGrowth: &rootstate.RegionGrowthSeal{RegionID: "west", GroupID: group, Generation: 2, PlannedIndex: next.PlannedIndex, ExpectedEpoch: 2,
		PreviousFenceRequestID: previous.GroupFenceRequestID, PreviousFenceIndex: previous.GroupFenceIndex, PreviousMembershipIndex: 1, SealRequestID: "seal", SealIndex: previous.GroupFenceIndex + 1, ChangeID: "grow",
		Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}, {RaftID: 7, Role: rootstate.GroupMemberLearner}}}})
	apply(t, h.root, rootstate.Command{Type: rootstate.CompleteGroupMembers, GroupCompletion: &rootstate.GroupChangeCompletion{GroupID: group, ChangeID: "grow"}})
	if _, ready, err := h.root.PendingRegionConfirmation(group); err != nil || !ready {
		t.Fatal(ready, err)
	}
	before := len(p.observed[group])
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	head, _, _ := h.root.RegionCohortHead("west")
	if head.CurrentGeneration != 1 || head.PendingGeneration != 2 || len(p.observed[group]) != before || p.proposals[group] != 1 {
		t.Fatal("growth activated without handoff protocol", head)
	}
}

type confirmationRaceHost struct {
	*testHost
	beforeConfirmation func()
}

func (h *confirmationRaceHost) Submit(group string, data []byte) error {
	var command rootstate.Command
	if err := json.Unmarshal(data, &command); err != nil {
		return err
	}
	if group == raft.RootGroupID && command.Type == rootstate.ConfirmRegionCohort && h.beforeConfirmation != nil {
		f := h.beforeConfirmation
		h.beforeConfirmation = nil
		f()
	}
	return h.testHost.Submit(group, data)
}

func TestRegionalConfirmationApplyRaceDoesNotPoisonRetry(t *testing.T) {
	c, h, p := confirmationFixture(t)
	racing := &confirmationRaceHost{testHost: h}
	racing.beforeConfirmation = func() {
		if err := h.root.ApplyMembership(h.root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 4}, VotersOutgoing: []uint64{1}, Learners: []uint64{2, 3, 5, 6}}); err != nil {
			t.Fatal(err)
		}
	}
	c.host = racing
	group := GroupID("west")
	plan, _, _ := h.root.PendingRegionConfirmation(group)
	if err := c.confirm(context.Background(), plan); err == nil {
		t.Fatal("Root apply accepted raced joint configuration")
	}
	requireHead(t, h, "west", false)
	if err := h.root.ApplyMembership(h.root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 4}, Learners: []uint64{2, 3, 5, 6}}); err != nil {
		t.Fatal(err)
	}
	plan, _, _ = h.root.PendingRegionConfirmation(group)
	if err := c.confirm(context.Background(), plan); err != nil {
		t.Fatal("durable rejection poisoned retry", err)
	}
	requireHead(t, h, "west", true)
	if p.proposals[group] != 1 {
		t.Fatal("retry rewrote data fence", p.proposals)
	}
}
