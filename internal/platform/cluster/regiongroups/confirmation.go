package regiongroups

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"slices"
	"sort"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
)

type Observation struct {
	RootConfirmedIndex  uint64
	GroupConfirmedIndex uint64
	View                enginestate.RegionFenceView
}

// Participants supplies authenticated, direct-to-node observations. Propose may
// forward to the Group leader, but observations MUST NOT redirect to that leader.
// Local participants require the same real Root/Group read barriers as remotes.
type Participants interface {
	Observe(context.Context, uint64, string) (Observation, error)
	Propose(context.Context, uint64, string, []byte) error
}

type Confirmer struct {
	host         Host
	root         *rootstate.State
	participants Participants
	cursor       string
}

func NewConfirmer(host Host, root *rootstate.State, participants Participants) (*Confirmer, error) {
	if host == nil || root == nil || participants == nil {
		return nil, errors.New("regional confirmation requires Host, Root and participants")
	}
	return &Confirmer{host: host, root: root, participants: participants}, nil
}

// Reconcile runs serially. Each round attempts one Region, without allowing an
// unavailable Region to monopolize the next round. Only initial formation is
// enabled: expansion needs a separate active/pending data-generation handoff.
func (c *Confirmer) Reconcile(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	local, err := c.host.Status(raft.RootGroupID)
	if err != nil {
		return err
	}
	if local.Role != etcdraft.StateLeader || local.LeaderID == 0 || local.LeaderID != local.RaftID {
		return nil
	}
	if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	groups, err := c.root.Groups()
	if err != nil {
		return err
	}
	var ready []rootstate.RegionConfirmationView
	for _, group := range groups {
		if group.GroupID == raft.RootGroupID {
			continue
		}
		view, found, err := c.root.PendingRegionConfirmation(group.GroupID)
		if err != nil {
			return err
		}
		if found && (view.Cohort.Formation || view.Cohort.Reformation) {
			ready = append(ready, view)
		}
	}
	if len(ready) == 0 {
		return nil
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].Group.GroupID < ready[j].Group.GroupID })
	selected := ready[0]
	for _, view := range ready {
		if view.Group.GroupID > c.cursor {
			selected = view
			break
		}
	}
	c.cursor = selected.Group.GroupID
	attempt, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	err = c.confirm(attempt, selected)
	if pendingInspection(err) && ctx.Err() == nil {
		return nil
	}
	return err
}

func (c *Confirmer) confirm(ctx context.Context, plan rootstate.RegionConfirmationView) error {
	group := plan.Group.GroupID
	target := plan.Cohort.Nodes[0]
	first, err := c.participants.Observe(ctx, target, group)
	if err != nil {
		return err
	}
	members := slices.Clone(plan.Group.Members)
	slices.SortFunc(members, func(a, b rootstate.GroupMember) int {
		if a.RaftID < b.RaftID {
			return -1
		}
		if a.RaftID > b.RaftID {
			return 1
		}
		return 0
	})
	fence := regionfence.Descriptor{RegionID: plan.Cohort.RegionID, GroupID: group, Generation: plan.Cohort.Generation,
		Reformation: plan.Cohort.Reformation, PlannedIndex: plan.Cohort.PlannedIndex, GroupEpoch: plan.Group.Epoch, MembershipIndex: first.View.MembershipIndex, Members: members}
	if !observationMatches(plan, fence, target, first) {
		return errors.New("regional participant differs from current Root plan")
	}
	if first.View.Found && (!first.View.Active || !reflect.DeepEqual(first.View.Record.Fence, fence)) {
		// A stale observation must never overwrite a newer local generation.
		if !fence.Advances(first.View.Record.Fence) {
			return errors.New("regional fence cannot advance the observed generation")
		}
	}
	command, err := routedstate.EncodeRegionFence("regional-fence-intent", fence)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(command)
	requestID := "regional-fence-" + hex.EncodeToString(digest[:])
	if first.View.Active && reflect.DeepEqual(first.View.Record.Fence, fence) {
		requestID = first.View.Record.RequestID // Adopt an already applied identical fence after failover.
	} else {
		if err := c.revalidate(ctx, plan); err != nil {
			return err
		}
		command, err = routedstate.EncodeRegionFence(requestID, fence)
		if err != nil {
			return err
		}
		if err := c.participants.Propose(ctx, target, group, command); err != nil {
			return err
		}
	}
	var record regionfence.Record
	for _, id := range plan.Cohort.Nodes {
		observed, err := c.participants.Observe(ctx, id, group)
		if err != nil {
			return err
		}
		v := observed.View
		if !observationMatches(plan, fence, id, observed) || !v.Found || !v.Active || v.Record.Validate() != nil || !reflect.DeepEqual(v.Record.Fence, fence) || v.Record.RequestID != requestID || v.Record.Index > v.AppliedIndex {
			return errors.New("regional participant has not applied the exact planned fence")
		}
		if record.Index != 0 && !reflect.DeepEqual(record, v.Record) {
			return errors.New("regional participants disagree on fence record")
		}
		record = v.Record
	}
	if err := c.revalidate(ctx, plan); err != nil {
		return err
	}
	return submitRoot(ctx, c.host, c.root, rootstate.Command{Type: rootstate.ConfirmRegionCohort, RegionConfirmation: &rootstate.RegionCohortConfirmation{
		RegionID: fence.RegionID, GroupID: group, Generation: fence.Generation, GroupEpoch: fence.GroupEpoch, GroupFenceRequestID: record.RequestID, GroupFenceIndex: record.Index}})
}

func observationMatches(plan rootstate.RegionConfirmationView, fence regionfence.Descriptor, target uint64, o Observation) bool {
	v := o.View
	return v.Handoff == nil && o.RootConfirmedIndex >= plan.AppliedIndex && o.GroupConfirmedIndex != 0 && v.AppliedIndex >= o.GroupConfirmedIndex && v.MembershipIndex > 0 && v.MembershipIndex <= v.AppliedIndex && v.MembershipIndex == fence.MembershipIndex && fence.Matches(v.Conf) && slices.Contains(fence.Nodes(), target)
}

func (c *Confirmer) revalidate(ctx context.Context, original rootstate.RegionConfirmationView) error {
	if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	now, found, err := c.root.PendingRegionConfirmation(original.Group.GroupID)
	if err != nil {
		return err
	}
	if !found || !reflect.DeepEqual(now.Cohort, original.Cohort) || !reflect.DeepEqual(now.Group, original.Group) {
		return errors.New("regional confirmation plan changed during observation")
	}
	return ctx.Err()
}
