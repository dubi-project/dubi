// Package placement derives durable data thresholds from the persistent
// topology. An observed online set is never a substitute for that topology.
package placement

import (
	"errors"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

// LocalPlan fixes the Region generation and the exact independent Nodes whose
// physical holdings may satisfy its local data majority. It is not a Raft
// quorum, a remote-Region receipt, or evidence that any bytes are held.
type LocalPlan struct {
	RegionID   string
	GroupID    string
	Generation uint64
	GroupEpoch uint64
	FenceIndex uint64
	Required   int
	nodes      []rootstate.Node
}

// Holding is a caller-verified physical observation for one exact RecordID.
// StoreID identifies the observed Store but never supplies an additional vote.
type Holding struct {
	RaftID   uint64
	NodeID   string
	StoreID  string
	RecordID string
}

// PlanLocal uses the confirmed Root cohort, not a liveness-filtered Node list.
// Pending growth blocks new plans until its generation is activated. Local
// foreground plans require three Nodes; small Regions may only be planned by
// the cross-Region path, never silently used by WriteLocal.
func PlanLocal(progress rootstate.RegionProgress, members []rootstate.Node) (LocalPlan, error) {
	if len(progress.Current.Nodes) < 3 {
		return LocalPlan{}, errors.New("local foreground placement requires at least three confirmed Nodes")
	}
	return planRegion(progress, members)
}

// planRegion describes a strict regional majority, including small Regions.
// This is a component of GeoPlan, not independent LocalReady authority.
func planRegion(progress rootstate.RegionProgress, members []rootstate.Node) (LocalPlan, error) {
	c := progress.Current
	if !progress.Eligible || progress.Pending != nil || progress.Head.PendingGeneration != 0 ||
		c.Validate() != nil || c.State != rootstate.RegionCohortConfirmed ||
		progress.Head.CurrentGeneration != c.Generation || progress.Head.RegionID != c.RegionID ||
		progress.Group.GroupID != c.GroupID || progress.Group.Epoch != c.GroupEpoch ||
		len(members) != len(c.Nodes) || len(progress.Group.Members) != len(c.Nodes) || len(c.Nodes) == 0 {
		return LocalPlan{}, errors.New("local placement requires an exact, confirmed Region cohort")
	}
	identities := make(map[string]bool, len(members))
	roles := make(map[uint64]string, len(progress.Group.Members))
	for _, member := range progress.Group.Members {
		if member.RaftID == 0 || roles[member.RaftID] != "" {
			return LocalPlan{}, errors.New("local placement has duplicate Group members")
		}
		roles[member.RaftID] = member.Role
	}
	for i, node := range members {
		if node.RaftID != c.Nodes[i] || node.NodeID == "" || node.Address == "" || node.RegionID != c.RegionID {
			return LocalPlan{}, errors.New("local placement members differ from the confirmed Region cohort")
		}
		if identities[node.NodeID] {
			return LocalPlan{}, errors.New("local placement has duplicate Node identities")
		}
		identities[node.NodeID] = true
		if roles[node.RaftID] != rootstate.GroupMemberVoter {
			return LocalPlan{}, errors.New("local placement differs from the current Group voters")
		}
	}
	return LocalPlan{RegionID: c.RegionID, GroupID: c.GroupID, Generation: c.Generation,
		GroupEpoch: c.GroupEpoch, FenceIndex: c.GroupFenceIndex,
		Required: len(c.Nodes)/2 + 1, nodes: slices.Clone(members)}, nil
}

func (p LocalPlan) Nodes() []rootstate.Node { return slices.Clone(p.nodes) }

// Count only accepts observations for the exact RecordID from distinct,
// current cohort Nodes. The caller must authenticate the observations and
// recheck Root/Group authority after physical IO. Invalid evidence fails
// closed instead of being silently discarded or counted twice.
func (p LocalPlan) Count(recordID string, holdings []Holding) (int, error) {
	if recordID == "" || p.Required < 1 || len(p.nodes) < p.Required {
		return 0, errors.New("invalid local placement plan or RecordID")
	}
	known := make(map[uint64]string, len(p.nodes))
	for _, node := range p.nodes {
		known[node.RaftID] = node.NodeID
	}
	seen := make(map[uint64]bool, len(holdings))
	stores := make(map[string]bool, len(holdings))
	for _, holding := range holdings {
		if holding.RaftID == 0 || known[holding.RaftID] == "" || known[holding.RaftID] != holding.NodeID ||
			holding.StoreID == "" || holding.RecordID != recordID || seen[holding.RaftID] || stores[holding.StoreID] {
			return 0, errors.New("holding differs from the local placement plan or repeats a Node")
		}
		seen[holding.RaftID] = true
		stores[holding.StoreID] = true
	}
	return len(seen), nil
}

func (p LocalPlan) Matches(other LocalPlan) bool {
	return p.RegionID == other.RegionID && p.GroupID == other.GroupID &&
		p.Generation == other.Generation && p.GroupEpoch == other.GroupEpoch && p.FenceIndex == other.FenceIndex &&
		p.Required == other.Required && slices.Equal(p.nodes, other.nodes)
}
