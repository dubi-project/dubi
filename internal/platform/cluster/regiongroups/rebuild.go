package regiongroups

import (
	"errors"
	"fmt"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

// RebuildPlan is a side-effect-free plan for reconstructing a retired Region
// Group. It is deliberately not a membership task: callers must first persist
// an explicit retirement/recovery intent and prove module-owned data recovery.
// The plan only fixes the old authority cut and the entirely new voter set.
type RebuildPlan struct {
	GroupID             string
	RegionID            string
	PreviousGeneration  uint64
	PreviousGroupEpoch  uint64
	PreviousFenceIndex  uint64
	PreviousNodes       []uint64
	TargetNodes         []rootstate.Node
	RequiredLocalVoters int
}

// PlanRebuild validates the immutable boundary for a future empty-Group
// reconstruction. The caller must derive Target Nodes from the same current
// Root cut; this function checks that they are in one Region, use fresh
// identities, and are disjoint from the retired cohort. A one-Node target is
// valid for Geo-only operation; it is never LocalReady.
func PlanRebuild(progress rootstate.RegionProgress, target []rootstate.Node) (RebuildPlan, error) {
	current := progress.Current
	if !progress.Eligible || progress.Pending != nil || progress.Head.PendingGeneration != 0 ||
		current.Validate() != nil || current.State != rootstate.RegionCohortConfirmed ||
		progress.Head.CurrentGeneration != current.Generation || progress.Head.RegionID != current.RegionID ||
		progress.Group.GroupID != current.GroupID || progress.Group.Epoch != current.GroupEpoch ||
		len(target) == 0 {
		return RebuildPlan{}, errors.New("Region Group rebuild requires an exact confirmed current cut")
	}
	old := make(map[uint64]bool, len(current.Nodes))
	for _, id := range current.Nodes {
		old[id] = true
	}
	seenRaft := make(map[uint64]bool, len(target))
	seenNode := make(map[string]bool, len(target))
	for i, node := range target {
		if node.RaftID == 0 || node.NodeID == "" || node.Address == "" || node.RegionID != current.RegionID ||
			seenRaft[node.RaftID] || seenNode[node.NodeID] || old[node.RaftID] ||
			(i > 0 && target[i-1].RaftID >= node.RaftID) {
			return RebuildPlan{}, errors.New("Region Group rebuild target must contain sorted fresh Nodes in one Region")
		}
		seenRaft[node.RaftID], seenNode[node.NodeID] = true, true
	}
	return RebuildPlan{GroupID: current.GroupID, RegionID: current.RegionID,
		PreviousGeneration: current.Generation, PreviousGroupEpoch: current.GroupEpoch,
		PreviousFenceIndex: current.GroupFenceIndex, PreviousNodes: slices.Clone(current.Nodes),
		TargetNodes: slices.Clone(target), RequiredLocalVoters: len(target)/2 + 1}, nil
}

func (p RebuildPlan) Validate() error {
	if raft.ValidateGroupID(p.GroupID) != nil || p.GroupID == raft.RootGroupID || rootstate.ValidateRangeID(p.RegionID) != nil ||
		p.PreviousGeneration == 0 || p.PreviousGroupEpoch < 2 ||
		p.PreviousFenceIndex == 0 || len(p.PreviousNodes) == 0 || len(p.TargetNodes) == 0 ||
		p.RequiredLocalVoters != len(p.TargetNodes)/2+1 {
		return errors.New("invalid Region Group rebuild plan")
	}
	for i, id := range p.PreviousNodes {
		if id == 0 || (i > 0 && p.PreviousNodes[i-1] >= id) {
			return errors.New("rebuild previous Nodes are not sorted")
		}
	}
	for i, node := range p.TargetNodes {
		if node.RaftID == 0 || node.NodeID == "" || node.RegionID != p.RegionID ||
			(i > 0 && p.TargetNodes[i-1].RaftID >= node.RaftID) {
			return errors.New("rebuild target Nodes are not sorted or differ from Region")
		}
		if slices.Contains(p.PreviousNodes, node.RaftID) {
			return fmt.Errorf("rebuild target Node %d belongs to the retired cohort", node.RaftID)
		}
	}
	return nil
}
