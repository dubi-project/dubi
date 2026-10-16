package regiongroups

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type growthTestRepairer struct {
	fill   func(uint64, regionfence.Handoff) error
	verify func(uint64, regionfence.Handoff) error
}

func (r growthTestRepairer) FillPending(_ context.Context, target uint64, seal regionfence.Handoff) error {
	if r.fill != nil {
		return r.fill(target, seal)
	}
	return nil
}

func (r growthTestRepairer) VerifyPending(_ context.Context, target uint64, seal regionfence.Handoff) error {
	if r.verify != nil {
		return r.verify(target, seal)
	}
	return nil
}

func growthFixture(t *testing.T) (*Grower, *testHost, *testParticipants) {
	t.Helper()
	c, h, p := confirmationFixture(t)
	for range 2 {
		if err := c.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	conf := h.root.ConfState()
	conf.Learners = append(conf.Learners, 7)
	if err := h.root.ApplyMembership(h.root.AppliedIndex()+1, conf); err != nil {
		t.Fatal(err)
	}
	g, err := NewGrower(h, h.root, p, growthTestRepairer{})
	if err != nil {
		t.Fatal(err)
	}
	return g, h, p
}

// Simulate ONLY the already independently tested membership-task executor.
// The grower must itself produce every seal/candidate/Root confirmation/activation.
func completeGrowthTask(t *testing.T, h *testHost, p *testParticipants, group string) {
	t.Helper()
	task, found, err := h.root.ActiveGroupChange(group)
	if err != nil || !found {
		t.Fatal(task, found, err)
	}
	old := p.states[group][task.Members[0].RaftID]
	inline, index, conf, err := old.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	next := &raftpb.ConfState{}
	for _, m := range task.Members {
		if m.Role == rootstate.GroupMemberVoter {
			next.Voters = append(next.Voters, m.RaftID)
		} else {
			next.Learners = append(next.Learners, m.RaftID)
		}
		if p.states[group][m.RaftID] == nil {
			s, err := enginestate.Open(t.TempDir(), group, confirmationHandler{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			if err := s.Restore(index, inline, conf); err != nil {
				t.Fatal(err)
			}
			p.states[group][m.RaftID] = s
		}
	}
	for _, s := range p.states[group] {
		if err := s.ApplyMembership(s.AppliedIndex()+1, next); err != nil {
			t.Fatal(err)
		}
	}
	apply(t, h.root, rootstate.Command{Type: rootstate.CompleteGroupMembers, GroupCompletion: &rootstate.GroupChangeCompletion{GroupID: group, ChangeID: task.ChangeID}})
}

func advanceGrowth(t *testing.T, g *Grower, rounds int) {
	t.Helper()
	for range rounds {
		if err := g.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRegionalGrowthCompleteHandoffAndCoordinatorRestart(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprint(lost), func(t *testing.T) {
			g, h, p := growthFixture(t)
			group := GroupID("west")
			for range 9 {
				p.lostResponse = lost
				advanceGrowth(t, g, 1)
				if _, found, err := h.root.ActiveGroupChange(group); err != nil {
					t.Fatal(err)
				} else if found {
					completeGrowthTask(t, h, p, group)
				}
				g, _ = NewGrower(h, h.root, p, growthTestRepairer{}) // No volatile progress survives.
			}
			head, _, _ := h.root.RegionCohortHead("west")
			if head.CurrentGeneration != 2 || head.PendingGeneration != 0 {
				t.Fatal(head)
			}
			current, _, _ := h.root.RegionCohort("west", 2)
			for _, id := range []uint64{1, 2, 3, 7} {
				v, err := p.states[group][id].RegionFence()
				if err != nil || !v.Active || v.Handoff == nil || v.Handoff.Activation == nil || !reflect.DeepEqual(v.Handoff.Activation.Confirmed, current) || !slices.Equal(v.Conf.Voters, []uint64{1, 2, 3, 7}) {
					t.Fatal(id, v, err)
				}
			}
			if p.proposals[group] != 4 {
				t.Fatal("repeated durable action after lost response", p.proposals)
			} // initial + seal + candidate + activation
			before := h.submissions
			advanceGrowth(t, g, 3)
			if before != h.submissions || p.proposals[group] != 4 {
				t.Fatal("activated generation reproposed")
			}
		})
	}
}

func TestRegionalGrowthWaitsForNewNodeRecordFill(t *testing.T) {
	g, h, p := growthFixture(t)
	group := GroupID("west")
	advanceGrowth(t, g, 3)
	completeGrowthTask(t, h, p, group)
	advanceGrowth(t, g, 1) // Publish the candidate fence, not Root confirmation.
	before := h.submissions
	var targets []uint64
	g.repair = growthTestRepairer{fill: func(target uint64, seal regionfence.Handoff) error {
		targets = append(targets, target)
		if target != 7 || seal.SealRequestID == "" || seal.Activation != nil {
			t.Fatal("growth sent a different repair target or seal", target, seal)
		}
		return errors.New("new Node has not filled records")
	}}
	if err := g.Reconcile(context.Background()); err == nil || h.submissions != before {
		t.Fatal("Root generation advanced despite failed physical fill", err, h.submissions, before)
	}
	head, _, err := h.root.RegionCohortHead("west")
	if err != nil || head.CurrentGeneration != 1 || head.PendingGeneration != 2 || !reflect.DeepEqual(targets, []uint64{7}) {
		t.Fatal(head, targets, err)
	}
	g.repair = growthTestRepairer{fill: func(target uint64, _ regionfence.Handoff) error {
		targets = append(targets, target)
		return nil
	}}
	advanceGrowth(t, g, 1)
	head, _, err = h.root.RegionCohortHead("west")
	if err != nil || head.CurrentGeneration != 2 || head.PendingGeneration != 0 || h.submissions != before+1 || !reflect.DeepEqual(targets, []uint64{7, 7}) {
		t.Fatal(head, targets, h.submissions, err)
	}
}

func TestRegionalGrowthRequiresCandidatePhysicalMajority(t *testing.T) {
	g, h, p := growthFixture(t)
	group := GroupID("west")
	advanceGrowth(t, g, 3)
	completeGrowthTask(t, h, p, group)
	advanceGrowth(t, g, 1)
	before := h.submissions
	verified := 0
	g.repair = growthTestRepairer{verify: func(target uint64, seal regionfence.Handoff) error {
		verified++
		if target != 7 || seal.Validate() != nil || seal.Activation != nil {
			t.Fatal("candidate verification used a different target or seal", target, seal)
		}
		if verified == 1 {
			return errors.New("one sealed reference lacks a pending physical majority")
		}
		return nil
	}}
	if err := g.Reconcile(context.Background()); err == nil || h.submissions != before {
		t.Fatal("candidate was published without physical majority", err, h.submissions, before)
	}
	head, _, err := h.root.RegionCohortHead("west")
	if err != nil || head.CurrentGeneration != 1 || head.PendingGeneration != 2 {
		t.Fatal("failed verification changed the denominator", head, err)
	}
	advanceGrowth(t, g, 1)
	if verified != 2 || h.submissions != before+1 {
		t.Fatal("candidate did not publish after physical verification", verified, h.submissions, before)
	}
}

func TestRegionalGrowthPreheatsNewNodeBeforeOfflineOldCandidate(t *testing.T) {
	g, h, p := growthFixture(t)
	group := GroupID("west")
	advanceGrowth(t, g, 3)
	completeGrowthTask(t, h, p, group)
	advanceGrowth(t, g, 1) // Candidate fence is durable, Root is not confirmed.
	var filled []uint64
	g.repair = growthTestRepairer{fill: func(target uint64, seal regionfence.Handoff) error {
		if seal.Validate() != nil || seal.Activation != nil {
			t.Fatal("new Node was given an invalid pending seal", seal)
		}
		filled = append(filled, target)
		return nil
	}}
	p.beforeObserve = func(_ context.Context, id uint64, _ string) error {
		if id == 3 {
			return status.Error(codes.Unavailable, "old participant permanently offline")
		}
		return nil
	}
	before := h.submissions
	_ = g.Reconcile(context.Background()) // Offline observation may be suppressed as retryable.
	if !reflect.DeepEqual(filled, []uint64{7}) || h.submissions != before {
		t.Fatal("physical preheat or Root confirmation was wrong", filled, h.submissions, before)
	}
	head, _, err := h.root.RegionCohortHead("west")
	if err != nil || head.CurrentGeneration != 1 || head.PendingGeneration != 2 {
		t.Fatal("offline old candidate changed confirmed denominator", head, err)
	}
}

func TestRegionalGrowthRequiresEveryNewCandidate(t *testing.T) {
	for _, mode := range []string{"offline", "old-root-cut", "old-group-cut", "different-record", "missing-handoff", "different-seal"} {
		t.Run(mode, func(t *testing.T) {
			g, h, p := growthFixture(t)
			group := GroupID("west")
			advanceGrowth(t, g, 3)
			completeGrowthTask(t, h, p, group)
			advanceGrowth(t, g, 1)
			target := uint64(7)
			p.beforeObserve = func(_ context.Context, id uint64, _ string) error {
				if id == target && mode == "offline" {
					return status.Error(codes.Unavailable, "offline")
				}
				return nil
			}
			p.afterObserve = func(id uint64, _ string, o *Observation) {
				if id != target {
					return
				}
				switch mode {
				case "old-root-cut":
					o.RootConfirmedIndex = 1
				case "old-group-cut":
					o.GroupConfirmedIndex = o.View.AppliedIndex + 1
				case "different-record":
					o.View.Record.RequestID = "other"
				case "missing-handoff":
					o.View.Handoff = nil
				case "different-seal":
					o.View.Handoff.SealRequestID = "other"
				}
			}
			before := h.submissions
			_ = g.Reconcile(context.Background())
			if h.submissions != before {
				t.Fatal("partial observation advanced Root")
			}
			head, _, _ := h.root.RegionCohortHead("west")
			if head.CurrentGeneration != 1 || head.PendingGeneration != 2 {
				t.Fatal(head)
			}
			p.beforeObserve, p.afterObserve = nil, nil
			advanceGrowth(t, g, 1)
			if h.submissions != before+1 {
				t.Fatal("did not recover after full observation")
			}
		})
	}
}

func TestRegionalGrowthOldSealRequiresExactOldMajority(t *testing.T) {
	for _, mode := range []string{"one-offline", "one-stale", "two-offline", "two-stale"} {
		t.Run(mode, func(t *testing.T) {
			g, h, p := growthFixture(t)
			group := GroupID("west")
			advanceGrowth(t, g, 2) // Plan and seal while the old quorum is intact.
			p.beforeObserve = func(_ context.Context, id uint64, _ string) error {
				if (mode == "one-offline" && id == 1) || (mode == "two-offline" && id <= 2) {
					return status.Error(codes.Unavailable, "old participant offline")
				}
				return nil
			}
			p.afterObserve = func(id uint64, _ string, o *Observation) {
				if (mode == "one-stale" && id == 1) || (mode == "two-stale" && id <= 2) {
					o.View.Handoff.SealIndex = o.View.AppliedIndex + 1
				}
			}
			before := h.submissions
			err := g.Reconcile(context.Background())
			majority := mode == "one-offline" || mode == "one-stale"
			if majority {
				if err != nil || h.submissions != before+1 {
					t.Fatal("old exact majority did not authorize growth", err, h.submissions, before)
				}
				if task, found, err := h.root.ActiveGroupChange(group); err != nil || !found || task.State != rootstate.GroupChangePending {
					t.Fatal("old majority did not create a durable membership task", task, found, err)
				}
			} else if h.submissions != before {
				t.Fatal("old minority authorized growth", err, h.submissions, before)
			}
		})
	}
}

type growthRaceHost struct {
	*testHost
	beforeSubmit func(rootstate.Command)
	lostType     string
}

func (h *growthRaceHost) Submit(group string, data []byte) error {
	var c rootstate.Command
	if err := json.Unmarshal(data, &c); err != nil {
		return err
	}
	if h.beforeSubmit != nil {
		h.beforeSubmit(c)
	}
	if err := h.testHost.Submit(group, data); err != nil {
		return err
	}
	if c.Type == h.lostType {
		h.lostType = ""
		return status.Error(codes.Unavailable, "lost Root result")
	}
	return nil
}

func TestRegionalGrowthRootTemporaryRejectionAndLostSuccess(t *testing.T) {
	for _, commandType := range []string{rootstate.PlanRegionCohort, rootstate.AuthorizeRegionGrowth, rootstate.ConfirmRegionCohort} {
		for _, mode := range []string{"joint-rejection", "lost-success"} {
			t.Run(commandType+"/"+mode, func(t *testing.T) {
				g, h, p := growthFixture(t)
				group := GroupID("west")
				if commandType != rootstate.PlanRegionCohort {
					advanceGrowth(t, g, 2)
				}
				if commandType == rootstate.ConfirmRegionCohort {
					advanceGrowth(t, g, 1)
					completeGrowthTask(t, h, p, group)
					advanceGrowth(t, g, 1)
				}
				stable := h.root.ConfState()
				host := &growthRaceHost{testHost: h}
				if mode == "lost-success" {
					host.lostType = commandType
				} else {
					host.beforeSubmit = func(c rootstate.Command) {
						if c.Type != commandType {
							return
						}
						joint := h.root.ConfState()
						joint.VotersOutgoing = slices.Clone(joint.Voters)
						if err := h.root.ApplyMembership(h.root.AppliedIndex()+1, joint); err != nil {
							t.Fatal(err)
						}
					}
				}
				g, _ = NewGrower(host, h.root, p, growthTestRepairer{})
				err := g.Reconcile(context.Background())
				if mode == "joint-rejection" && err == nil {
					t.Fatal("temporary rejection lost")
				}
				host.beforeSubmit = nil
				if mode == "joint-rejection" {
					if err := h.root.ApplyMembership(h.root.AppliedIndex()+1, stable); err != nil {
						t.Fatal(err)
					}
				}
				for range 9 {
					advanceGrowth(t, g, 1)
					if _, found, _ := h.root.ActiveGroupChange(group); found {
						completeGrowthTask(t, h, p, group)
					}
				}
				v, _ := p.states[group][1].RegionFence()
				if v.Handoff == nil || v.Handoff.Activation == nil {
					t.Fatal("Root retry poisoned progress", v)
				}
			})
		}
	}
}

func TestRegionalGrowthRevalidatesBeforeRootAuthorization(t *testing.T) {
	for _, mode := range []string{"leadership", "removed", "joint", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			g, h, p := growthFixture(t)
			advanceGrowth(t, g, 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p.afterObserve = func(id uint64, _ string, _ *Observation) {
				if id != 2 {
					return
				}
				switch mode {
				case "leadership":
					h.leader = false
				case "canceled":
					cancel()
				case "removed", "joint":
					conf := h.root.ConfState()
					if mode == "removed" {
						conf.Learners = []uint64{2, 3, 5, 6}
					} else {
						conf.VotersOutgoing = slices.Clone(conf.Voters)
					}
					if err := h.root.ApplyMembership(h.root.AppliedIndex()+1, conf); err != nil {
						t.Fatal(err)
					}
				}
			}
			before := h.submissions
			if err := g.Reconcile(ctx); err == nil {
				t.Fatal("stale Root authority accepted")
			}
			if h.submissions != before {
				t.Fatal("changed authority submitted Root authorization")
			}
		})
	}
}

func TestRegionalGrowthActivationCanUseAnotherParticipant(t *testing.T) {
	g, h, p := growthFixture(t)
	group := GroupID("west")
	advanceGrowth(t, g, 3)
	completeGrowthTask(t, h, p, group)
	advanceGrowth(t, g, 2) // Root has confirmed, data is still sealed.
	p.beforeObserve = func(_ context.Context, id uint64, _ string) error {
		if id == 1 {
			return status.Error(codes.Unavailable, "old first participant offline")
		}
		return nil
	}
	g, _ = NewGrower(h, h.root, p, growthTestRepairer{})
	advanceGrowth(t, g, 1)
	v, _ := p.states[group][2].RegionFence()
	if v.Handoff.Activation == nil {
		t.Fatal("activation depended on first participant")
	}
}

func TestRegionalGrowthNewGenerationWaitsForCurrentActivation(t *testing.T) {
	g, h, p := growthFixture(t)
	group := GroupID("west")
	advanceGrowth(t, g, 3)
	completeGrowthTask(t, h, p, group)
	advanceGrowth(t, g, 2)
	apply(t, h.root, rootstate.Command{Type: rootstate.RegisterNode, Node: &rootstate.Node{NodeID: fmt.Sprintf("node_%032x", 8), RaftID: 8, RegionID: "west", Address: "127.0.0.1:10008"}})
	conf := h.root.ConfState()
	conf.Learners = append(conf.Learners, 8)
	if err := h.root.ApplyMembership(h.root.AppliedIndex()+1, conf); err != nil {
		t.Fatal(err)
	}
	// Even an already persisted next plan must not let a restart skip activation.
	apply(t, h.root, rootstate.Command{Type: rootstate.PlanRegionCohort, RegionPlan: &rootstate.RegionCohortPlan{RegionID: "west", GroupID: group, ExpectedGeneration: 2, Nodes: []uint64{1, 2, 3, 7, 8}}})
	g, _ = NewGrower(h, h.root, p, growthTestRepairer{})
	advanceGrowth(t, g, 1)
	v, _ := p.states[group][1].RegionFence()
	if v.Handoff.Activation == nil || v.Handoff.Intent.Next.Generation != 2 {
		t.Fatal("skipped old activation", v)
	}
	advanceGrowth(t, g, 2)
	completeGrowthTask(t, h, p, group)
	advanceGrowth(t, g, 3)
	v, _ = p.states[group][1].RegionFence()
	if v.Handoff.Activation == nil || v.Handoff.Activation.Confirmed.Generation != 3 {
		t.Fatal("second growth did not activate", v)
	}
}

func TestRegionalGrowthUnavailableRegionDoesNotStarveOtherRegion(t *testing.T) {
	g, h, p := growthFixture(t)
	apply(t, h.root, rootstate.Command{Type: rootstate.RegisterNode, Node: &rootstate.Node{NodeID: fmt.Sprintf("node_%032x", 8), RaftID: 8, RegionID: "east", Address: "127.0.0.1:10008"}})
	conf := h.root.ConfState()
	conf.Learners = append(conf.Learners, 8)
	if err := h.root.ApplyMembership(h.root.AppliedIndex()+1, conf); err != nil {
		t.Fatal(err)
	}
	p.beforeObserve = func(_ context.Context, id uint64, group string) error {
		if group == GroupID("west") && id == 3 {
			return status.Error(codes.Unavailable, "offline")
		}
		return nil
	}
	for range 18 {
		advanceGrowth(t, g, 1)
		if _, found, _ := h.root.ActiveGroupChange(GroupID("east")); found {
			completeGrowthTask(t, h, p, GroupID("east"))
		}
	}
	west, _, _ := h.root.RegionCohortHead("west")
	if west.CurrentGeneration != 1 || west.PendingGeneration != 2 {
		t.Fatal(west)
	}
	v, err := p.states[GroupID("east")][4].RegionFence()
	if err != nil || v.Handoff == nil || v.Handoff.Activation == nil || v.Handoff.Activation.Confirmed.Generation != 2 {
		t.Fatal(v, err)
	}
}
