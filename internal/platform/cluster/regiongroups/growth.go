package regiongroups

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
)

// Grower advances one Region per round from durable Root/data state. Volatile
// cursors only schedule work; they never authorize it. Membership tasks are
// consumed by the existing independent task reconciler.
type Grower struct {
	host         Host
	root         *rootstate.State
	participants Participants
	repair       Repairer
	cursor       string
	probe        uint64
}

// Repairer completes one newly added Node's physical record backfill for the
// exact seal. Returning nil is a one-shot result, not a durable quorum receipt.
type Repairer interface {
	FillPending(context.Context, uint64, regionfence.Handoff) error
	// VerifyPending checks every sealed reference against the pending cohort's
	// physical majority after all newly added members have been filled.
	VerifyPending(context.Context, uint64, regionfence.Handoff) error
}

func NewGrower(host Host, root *rootstate.State, participants Participants, repair Repairer) (*Grower, error) {
	if host == nil || root == nil || participants == nil || repair == nil {
		return nil, errors.New("regional growth requires Host, Root, participants and record repair")
	}
	return &Grower{host: host, root: root, participants: participants, repair: repair}, nil
}

func (g *Grower) Reconcile(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	leader, err := g.isLeader()
	if err != nil || !leader {
		return err
	}
	if _, err := g.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	pending, err := g.root.PendingRootJoins()
	if err != nil {
		return err
	}
	if len(pending) != 0 {
		return nil
	}
	routing, err := g.root.RootRoutingView(rootstate.MaxRegionCohortNodes)
	if err != nil {
		return err
	}
	groups, err := g.root.Groups()
	if err != nil {
		return err
	}
	type candidate struct {
		view  rootstate.RegionProgress
		nodes []uint64
	}
	var ready []candidate
	for _, group := range groups {
		if group.GroupID == raft.RootGroupID {
			continue
		}
		view, found, err := g.root.RegionProgress(group.GroupID)
		if err != nil {
			return err
		}
		if !found || !view.Eligible {
			continue
		}
		var nodes []uint64
		for _, node := range routing.Nodes {
			if node.RegionID == view.Current.RegionID {
				nodes = append(nodes, node.RaftID)
			}
		}
		slices.Sort(nodes)
		if view.Pending == nil && view.Current.Generation == 1 && !growthCandidate(nodes, view.Current.Nodes, view.Group.Members) {
			continue
		}
		ready = append(ready, candidate{view, nodes})
	}
	if len(ready) == 0 {
		return nil
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].view.Group.GroupID < ready[j].view.Group.GroupID })
	selected := ready[0]
	for _, c := range ready {
		if c.view.Group.GroupID > g.cursor {
			selected = c
			break
		}
	}
	g.cursor = selected.view.Group.GroupID
	// A repair round may copy more than one Record before the generation can be
	// confirmed. Keep it bounded, but do not impose a 20-second ceiling on every
	// backfill attempt; partial physical copies remain safe to retry.
	attempt, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	err = g.advance(attempt, selected.view, selected.nodes)
	if pendingInspection(err) && ctx.Err() == nil {
		return nil
	}
	return err
}

func strictSuperset(next, previous []uint64) bool {
	if len(next) <= len(previous) {
		return false
	}
	for _, id := range next {
		if len(previous) > 0 && id == previous[0] {
			previous = previous[1:]
		}
	}
	return len(previous) == 0
}

func growthCandidate(next, previous []uint64, current []rootstate.GroupMember) bool {
	if len(next) == 0 {
		return false
	}
	for _, member := range current {
		if !slices.Contains(next, member.RaftID) {
			return false
		}
	}
	if strictSuperset(next, previous) {
		return true
	}
	added := false
	for _, id := range next {
		if !slices.Contains(previous, id) {
			added = true
			break
		}
	}
	return added && len(current) < len(next)
}

func (g *Grower) isLeader() (bool, error) {
	s, err := g.host.Status(raft.RootGroupID)
	return err == nil && s.Role == etcdraft.StateLeader && s.LeaderID != 0 && s.LeaderID == s.RaftID, err
}

func (g *Grower) revalidate(ctx context.Context, original rootstate.RegionProgress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if leader, err := g.isLeader(); err != nil {
		return err
	} else if !leader {
		return errors.New("regional growth lost Root leadership")
	}
	if _, err := g.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	now, found, err := g.root.RegionProgress(original.Group.GroupID)
	if err != nil {
		return err
	}
	now.AppliedIndex, original.AppliedIndex = 0, 0
	if !found || !now.Eligible || !reflect.DeepEqual(now, original) {
		return errors.New("regional growth Root authority changed during observation")
	}
	return ctx.Err()
}

// Probe failure changes only which participant is queried first, not either
// required denominator. Old seal authorization uses a strict old majority;
// candidate publication still inspects every pending participant.
func (g *Grower) observeAny(ctx context.Context, nodes []uint64, group string) (uint64, Observation, error) {
	start := int(g.probe % uint64(len(nodes)))
	g.probe++
	var last error
	for n := range nodes {
		id := nodes[(start+n)%len(nodes)]
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		o, err := g.participants.Observe(attempt, id, group)
		cancel()
		if err == nil {
			return id, o, nil
		}
		last = err
		if ctx.Err() != nil {
			return 0, Observation{}, ctx.Err()
		}
		if !pendingInspection(err) {
			return 0, Observation{}, err
		}
	}
	return 0, Observation{}, last
}

func progressObservation(view rootstate.RegionProgress, o Observation) bool {
	v := o.View
	if o.RootConfirmedIndex < view.AppliedIndex || o.GroupConfirmedIndex == 0 || v.AppliedIndex < o.GroupConfirmedIndex || v.MembershipIndex == 0 || v.MembershipIndex > v.AppliedIndex || !v.Found || v.Record.Validate() != nil || v.Record.Index > v.AppliedIndex || v.Record.Fence.MembershipIndex > v.MembershipIndex {
		return false
	}
	if v.Active && v.Record.Fence.MembershipIndex == v.MembershipIndex && v.Record.Fence.Matches(v.Conf) {
		return true
	}
	// After an explicit maintenance removal the old fence remains the
	// authoritative sealed record, but the live ConfState has advanced and no
	// longer matches it. The recovery handoff is allowed to seal only from this
	// inactive, otherwise exact old record.
	return view.Group.Epoch > view.Current.GroupEpoch && recordConfirms(view.Current, v.Record) && v.MembershipIndex > v.Record.Fence.MembershipIndex
}

func recordConfirms(c rootstate.RegionCohort, r regionfence.Record) bool {
	f := r.Fence
	return r.Validate() == nil && c.RegionID == f.RegionID && c.GroupID == f.GroupID && c.Generation == f.Generation && c.PlannedIndex == f.PlannedIndex && c.GroupEpoch == f.GroupEpoch && c.GroupFenceRequestID == r.RequestID && c.GroupFenceIndex == r.Index && slices.Equal(c.Nodes, f.Nodes())
}

func handoffAuthorized(h *regionfence.Handoff, a *rootstate.RegionGrowth) bool {
	if h == nil || a == nil || h.Validate() != nil || a.Validate() != nil {
		return false
	}
	p, old := a.Seal, h.Intent.Previous
	return h.SealRequestID == p.SealRequestID && h.SealIndex == p.SealIndex && h.Intent.Next.RegionID == p.RegionID && h.Intent.Next.GroupID == p.GroupID && h.Intent.Next.Generation == p.Generation && h.Intent.Next.PlannedIndex == p.PlannedIndex && slices.Equal(h.Intent.Next.Nodes, memberIDs(p.Members)) && old.Fence.GroupEpoch <= p.ExpectedEpoch && old.RequestID == p.PreviousFenceRequestID && old.Index == p.PreviousFenceIndex && old.Fence.MembershipIndex == p.PreviousMembershipIndex && fenceContainsMembers(old.Fence.Members, a.BaseMembers)
}

func fenceContainsMembers(fence, current []rootstate.GroupMember) bool {
	roles := make(map[uint64]string, len(fence))
	for _, member := range fence {
		roles[member.RaftID] = member.Role
	}
	for _, member := range current {
		if roles[member.RaftID] != member.Role {
			return false
		}
	}
	return true
}

func memberIDs(members []rootstate.GroupMember) []uint64 {
	ids := make([]uint64, len(members))
	for i, m := range members {
		ids[i] = m.RaftID
	}
	return ids
}

// A sealed Owner log is authoritative only after the same exact seal has been
// applied by a strict majority of the immutable old cohort. Failed/stale
// observations never reduce the denominator or count toward that proof.
func (g *Grower) observeOldSealMajority(ctx context.Context, view rootstate.RegionProgress,
	old regionfence.Record, seal *regionfence.Handoff) error {
	if seal == nil || seal.Validate() != nil || len(view.Current.Nodes) == 0 {
		return errors.New("invalid old regional seal proof")
	}
	required := len(view.Current.Nodes)/2 + 1
	confirmed := 0
	var last error
	live := make(map[uint64]struct{}, len(view.Group.Members))
	for _, member := range view.Group.Members {
		live[member.RaftID] = struct{}{}
	}
	for _, id := range view.Current.Nodes {
		// An explicit maintenance removal has already removed this identity
		// from the live Group and Root directory. It remains in the immutable
		// old denominator, but cannot be queried as a source anymore.
		if _, ok := live[id]; !ok {
			continue
		}
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		o, err := g.participants.Observe(attempt, id, view.Group.GroupID)
		cancel()
		if err != nil {
			last = err
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !pendingInspection(err) {
				return err
			}
		} else if progressObservation(view, o) && reflect.DeepEqual(o.View.Record, old) &&
			reflect.DeepEqual(o.View.Handoff, seal) && seal.SealIndex <= o.View.AppliedIndex {
			confirmed++
		}
		if confirmed >= required {
			return nil
		}
	}
	return fmt.Errorf("old regional majority has not applied the exact seal: confirmed=%d required=%d last=%v", confirmed, required, last)
}

func (g *Grower) advance(ctx context.Context, view rootstate.RegionProgress, nodes []uint64) error {
	if view.PendingTask != nil {
		if view.PendingTask.State == rootstate.GroupChangePending {
			return nil
		}
		return g.confirmCandidate(ctx, view)
	}
	observationNodes := view.Current.Nodes
	if view.Group.Epoch > view.Current.GroupEpoch {
		// An explicit maintenance removal leaves the immutable cohort with the
		// fenced identity, but the live Group is its stable subset. Start the
		// recovery handoff from live members; the old fixed denominator is still
		// enforced separately by observeOldSealMajority.
		observationNodes = memberIDs(view.Group.Members)
	}
	target, first, err := g.observeAny(ctx, observationNodes, view.Group.GroupID)
	if err != nil {
		return err
	}
	fenceMatchesGroup := slices.Equal(first.View.Record.Fence.Members, sortedMembers(view.Group.Members))
	// After an explicit maintenance removal, the immutable old fence still
	// names the retired Node. The live Group is allowed to be its stable subset
	// while a fresh Node is enrolled; it must never be treated as a new fence.
	if !fenceMatchesGroup && view.Group.Epoch > view.Current.GroupEpoch {
		fenceMatchesGroup = fenceContainsMembers(first.View.Record.Fence.Members, view.Group.Members)
	}
	if !progressObservation(view, first) || !recordConfirms(view.Current, first.View.Record) || !fenceMatchesGroup {
		return errors.New("regional growth differs from confirmed current fence")
	}
	h := first.View.Handoff
	sealedNext := view.Pending != nil && h != nil && h.Activation == nil && reflect.DeepEqual(h.Intent.Next, *view.Pending) && reflect.DeepEqual(h.Intent.Previous, first.View.Record)
	if !sealedNext {
		if view.Current.Generation > 1 {
			if !handoffAuthorized(h, view.CurrentGrowth) || !h.Intent.Accepts(first.View.Record, h.SealIndex) {
				return errors.New("regional current generation lacks authorized handoff")
			}
			if h.Activation == nil {
				if err := g.revalidate(ctx, view); err != nil {
					return err
				}
				activation := regionfence.Activation{Candidate: first.View.Record, Confirmed: view.Current}
				return g.proposeHandoff(ctx, target, view.Group.GroupID, regionfence.HandoffCommand{Action: regionfence.Activate, Activation: &activation})
			}
			if !reflect.DeepEqual(h.Activation.Confirmed, view.Current) || h.ActivationIndex > first.View.AppliedIndex {
				return errors.New("regional activation differs from current Root publication")
			}
		} else if h != nil {
			return errors.New("initial regional generation has unexpected handoff")
		}
	}
	if view.Pending == nil {
		if !growthCandidate(nodes, view.Current.Nodes, view.Group.Members) {
			return nil
		}
		if err := g.revalidate(ctx, view); err != nil {
			return err
		}
		return submitRoot(ctx, g.host, g.root, rootstate.Command{Type: rootstate.PlanRegionCohort, RegionPlan: &rootstate.RegionCohortPlan{RegionID: view.Current.RegionID, GroupID: view.Group.GroupID, ExpectedGeneration: view.Current.Generation, Nodes: nodes}})
	}
	if !sealedNext {
		if err := g.revalidate(ctx, view); err != nil {
			return err
		}
		intent := regionfence.HandoffIntent{Previous: first.View.Record, Next: *view.Pending, Recovery: view.Group.Epoch > view.Current.GroupEpoch}
		return g.proposeHandoff(ctx, target, view.Group.GroupID, regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &intent})
	}
	if h.Validate() != nil || h.SealIndex > first.View.AppliedIndex {
		return errors.New("invalid regional seal observation")
	}
	if err := g.observeOldSealMajority(ctx, view, first.View.Record, h); err != nil {
		return err
	}
	oldRoles := make(map[uint64]string, len(view.Group.Members))
	for _, member := range view.Group.Members {
		oldRoles[member.RaftID] = member.Role
	}
	members := make([]rootstate.GroupMember, 0, len(view.Pending.Nodes))
	for _, id := range view.Pending.Nodes {
		role := oldRoles[id]
		if role == "" {
			role = rootstate.GroupMemberVoter
		}
		members = append(members, rootstate.GroupMember{RaftID: id, Role: role})
	}
	p := rootstate.RegionGrowthSeal{RegionID: view.Current.RegionID, GroupID: view.Group.GroupID, Generation: view.Pending.Generation, PlannedIndex: view.Pending.PlannedIndex, ExpectedEpoch: view.Group.Epoch,
		PreviousFenceRequestID: first.View.Record.RequestID, PreviousFenceIndex: first.View.Record.Index, PreviousMembershipIndex: first.View.Record.Fence.MembershipIndex,
		Recovery: view.Group.Epoch > view.Current.GroupEpoch, SealRequestID: h.SealRequestID, SealIndex: h.SealIndex, Members: members}
	p.ChangeID = "regional-growth-" + stableDigest([]byte(h.SealRequestID))
	if err := g.revalidate(ctx, view); err != nil {
		return err
	}
	return submitRoot(ctx, g.host, g.root, rootstate.Command{Type: rootstate.AuthorizeRegionGrowth, RegionGrowth: &p})
}

func sortedMembers(members []rootstate.GroupMember) []rootstate.GroupMember {
	members = slices.Clone(members)
	slices.SortFunc(members, func(a, b rootstate.GroupMember) int {
		if a.RaftID < b.RaftID {
			return -1
		}
		if a.RaftID > b.RaftID {
			return 1
		}
		return 0
	})
	return members
}

func stableDigest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func (g *Grower) proposeHandoff(ctx context.Context, target uint64, group string, h regionfence.HandoffCommand) error {
	data, err := routedstate.EncodeRegionHandoff("regional-handoff-intent", h)
	if err != nil {
		return err
	}
	data, err = routedstate.EncodeRegionHandoff("regional-handoff-"+stableDigest(data), h)
	if err != nil {
		return err
	}
	return g.participants.Propose(ctx, target, group, data)
}

func (g *Grower) confirmCandidate(ctx context.Context, view rootstate.RegionProgress) error {
	target, first, err := g.observeAny(ctx, view.Pending.Nodes, view.Group.GroupID)
	if err != nil {
		return err
	}
	v, h := first.View, first.View.Handoff
	if first.RootConfirmedIndex < view.AppliedIndex || first.GroupConfirmedIndex == 0 || v.AppliedIndex < first.GroupConfirmedIndex || !v.Found || v.Record.Validate() != nil || !handoffAuthorized(h, view.PendingGrowth) || h.Activation != nil || !reflect.DeepEqual(h.Intent.Next, *view.Pending) || !recordConfirms(view.Current, h.Intent.Previous) || h.SealIndex > v.AppliedIndex {
		return errors.New("candidate lacks exact authorized seal")
	}
	fence := regionfence.Descriptor{RegionID: view.Current.RegionID, GroupID: view.Group.GroupID, Generation: view.Pending.Generation, PlannedIndex: view.Pending.PlannedIndex, GroupEpoch: view.Group.Epoch, MembershipIndex: v.MembershipIndex, Members: sortedMembers(view.Group.Members)}
	if fence.Validate() != nil || !fence.Matches(v.Conf) || v.MembershipIndex <= h.SealIndex || v.MembershipIndex > v.AppliedIndex {
		return errors.New("candidate membership has not completed after seal")
	}
	if !reflect.DeepEqual(v.Record.Fence, fence) {
		if !reflect.DeepEqual(v.Record, h.Intent.Previous) {
			return errors.New("unexpected regional candidate fence")
		}
		if err := g.revalidate(ctx, view); err != nil {
			return err
		}
		data, err := routedstate.EncodeRegionFence("regional-growth-candidate", fence)
		if err != nil {
			return err
		}
		data, err = routedstate.EncodeRegionFence("regional-candidate-"+stableDigest(data), fence)
		if err != nil {
			return err
		}
		return g.participants.Propose(ctx, target, view.Group.GroupID, data)
	}
	// A permanently unavailable old member must not prevent a new member from
	// preheating its Pack. This has no publication authority: the full candidate
	// observation and Root revalidation below still gate confirmation.
	for _, id := range view.Pending.Nodes {
		if slices.Contains(view.Current.Nodes, id) {
			continue
		}
		if err := g.repair.FillPending(ctx, id, *h); err != nil {
			return err
		}
	}
	for _, id := range view.Pending.Nodes {
		o, err := g.participants.Observe(ctx, id, view.Group.GroupID)
		if err != nil {
			return err
		}
		if !progressObservation(view, o) || !reflect.DeepEqual(o.View.Record, v.Record) || !reflect.DeepEqual(o.View.Handoff, h) || !h.Intent.Accepts(o.View.Record, h.SealIndex) {
			return errors.New("regional participant has not applied exact candidate")
		}
	}
	var verifier uint64
	for _, id := range view.Pending.Nodes {
		if !slices.Contains(view.Current.Nodes, id) {
			verifier = id
			break
		}
	}
	if verifier == 0 {
		return errors.New("regional candidate has no newly added verification Node")
	}
	if err := g.repair.VerifyPending(ctx, verifier, *h); err != nil {
		return err
	}
	if err := g.revalidate(ctx, view); err != nil {
		return err
	}
	return submitRoot(ctx, g.host, g.root, rootstate.Command{Type: rootstate.ConfirmRegionCohort, RegionConfirmation: &rootstate.RegionCohortConfirmation{RegionID: view.Current.RegionID, GroupID: view.Group.GroupID, Generation: view.Pending.Generation, GroupEpoch: view.Group.Epoch, GroupFenceRequestID: v.Record.RequestID, GroupFenceIndex: v.Record.Index}})
}
