// Package noderemoval builds the ordinary, operator-requested membership
// changes for one Node. It deliberately has no replacement workflow or
// background task: callers submit each returned Maintenance plan through the
// existing Group membership reconciler, then remove the Root member and fence
// the old identity after the plans complete.
package noderemoval

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	ErrNodeNotRegistered        = errors.New("Node is not registered")
	ErrRemovalHasPendingWork    = errors.New("Node has pending membership or join work")
	ErrRemovalWouldLoseMajority = errors.New("Node removal would lose the current voter majority")
	// ErrRegionGroupNeedsRebuild is retained for non-Region Groups whose last
	// voter would otherwise be removed. A confirmed Region Group is retired
	// automatically as part of the explicit operator request instead.
	ErrRegionGroupNeedsRebuild = errors.New("non-Region Group cannot remove its last voter")
)

// GroupPlan is one ordinary member-maintenance plan. The plans are
// independent durable GroupChange records and can be retried separately.
type GroupPlan struct {
	GroupID string
	Plan    rootstate.GroupChangePlan
	// Retire is set only for a confirmed Region Group whose sole voter is
	// being explicitly removed. Retirement clears the old Region's current
	// responsibility so the normal Root removal/Fence path can continue; a
	// future same-name Region forms through ordinary Node join.
	Retire bool
}

// Plan is a read-only Root cut for one manual Node removal request. It does
// not change Root state and does not bind a future Node; a new Node joins via
// the normal enrollment path after the old member is gone.
type Plan struct {
	Node           rootstate.Node
	RootMember     bool
	AlreadyFenced  bool
	RootMembership *raftpb.ConfState
	Groups         []GroupPlan
}

// Build reads a registered Node and derives maintenance plans for every
// current data Group containing it. The caller must obtain a Root ReadIndex
// before calling Build. All descriptors, responsibilities and active tasks
// are captured under a single Root state lock.
func Build(root *rootstate.State, raftID uint64) (Plan, error) {
	if root == nil || raftID == 0 {
		return Plan{}, errors.New("Node removal requires Root state and a RaftID")
	}
	view, found, err := root.NodeRemovalView(raftID)
	if err != nil {
		return Plan{}, err
	}
	if !found {
		return Plan{}, ErrNodeNotRegistered
	}
	if len(view.Responsibilities.PendingRootJoins) != 0 {
		return Plan{}, ErrRemovalHasPendingWork
	}
	if err := CheckRootRemoval(view.Membership, raftID); err != nil {
		return Plan{}, err
	}
	return buildPlan(root, view)
}

func buildPlan(root *rootstate.State, view rootstate.NodeRemovalView) (Plan, error) {
	node, responsibilities, groups := view.Node, view.Responsibilities, view.Groups
	plans := make([]GroupPlan, 0, len(responsibilities.CurrentGroups))
	wanted := make(map[string]bool, len(responsibilities.CurrentGroups))
	pending := make(map[string]bool, len(responsibilities.PendingGroups))
	activeTasks := make(map[string]rootstate.GroupChange, len(view.PendingChanges))
	for _, task := range view.PendingChanges {
		activeTasks[task.GroupID] = task
	}
	for _, groupID := range responsibilities.CurrentGroups {
		wanted[groupID] = true
	}
	for _, groupID := range responsibilities.PendingGroups {
		pending[groupID] = true
	}
	for _, group := range groups {
		if !wanted[group.GroupID] {
			continue
		}
		desired := make([]rootstate.GroupMember, 0, len(group.Members)-1)
		voters := 0
		desiredVoters := 0
		removed := false
		for _, member := range group.Members {
			if member.Role == rootstate.GroupMemberVoter {
				voters++
			}
			if member.RaftID == node.RaftID {
				removed = true
				continue
			}
			desired = append(desired, member)
			if member.Role == rootstate.GroupMemberVoter {
				desiredVoters++
			}
		}
		if !removed {
			return Plan{}, fmt.Errorf("Root responsibility lists Node %d in Group %q but descriptor does not", node.RaftID, group.GroupID)
		}
		if desiredVoters < voters/2+1 {
			if voters == 1 && desiredVoters == 0 {
				if _, regional, regionErr := root.RegionProgress(group.GroupID); regionErr != nil {
					return Plan{}, regionErr
				} else if regional {
					plans = append(plans, GroupPlan{GroupID: group.GroupID, Retire: true})
					continue
				}
				return Plan{}, fmt.Errorf("Group %q: %w", group.GroupID, ErrRegionGroupNeedsRebuild)
			}
			return Plan{}, fmt.Errorf("Group %q: %w", group.GroupID, ErrRemovalWouldLoseMajority)
		}
		change := rootstate.GroupChangePlan{
			ChangeID: deterministicChangeID(node, group), GroupID: group.GroupID, ExpectedEpoch: group.Epoch,
			Members: desired, Maintenance: true,
		}
		if pending[group.GroupID] {
			active, found := activeTasks[group.GroupID]
			if !found || active.ChangeID != change.ChangeID || active.ExpectedEpoch != change.ExpectedEpoch || active.State != rootstate.GroupChangePending || !slices.Equal(active.Members, change.Members) || !active.Maintenance {
				return Plan{}, ErrRemovalHasPendingWork
			}
		}
		plans = append(plans, GroupPlan{GroupID: group.GroupID, Plan: change})
	}
	for groupID := range pending {
		if !wanted[groupID] {
			return Plan{}, ErrRemovalHasPendingWork
		}
	}
	slices.SortFunc(plans, func(a, b GroupPlan) int { return cmpString(a.GroupID, b.GroupID) })
	return Plan{Node: node, RootMember: responsibilities.RootMember, AlreadyFenced: view.Fenced, RootMembership: view.Membership, Groups: plans}, nil
}

func deterministicChangeID(node rootstate.Node, group rootstate.Group) string {
	sum := sha256.Sum256([]byte(node.NodeID + "\x00" + group.GroupID + "\x00" + fmt.Sprint(group.Epoch)))
	return "node-remove-" + hex.EncodeToString(sum[:])
}

// CheckRootRemoval runs before any data Group mutation. Root quorum loss must
// not be discovered only after the data memberships have already changed.
func CheckRootRemoval(conf *raftpb.ConfState, raftID uint64) error {
	if conf == nil || raftID == 0 || len(conf.GetVoters()) == 0 {
		return errors.New("invalid Root membership for Node removal")
	}
	if len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return errors.New("Root membership is in joint consensus; retry after it settles")
	}
	if slices.Contains(conf.GetVoters(), raftID) && len(conf.GetVoters())-1 < len(conf.GetVoters())/2+1 {
		return ErrRemovalWouldLoseMajority
	}
	return nil
}

func cmpString(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
