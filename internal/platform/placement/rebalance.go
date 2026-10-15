package placement

import (
	"errors"
	"fmt"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

// RepairScope describes why a target was selected. It is a placement decision,
// not an instruction to copy bytes yet.
type RepairScope string

const (
	RepairOnNode        RepairScope = "same_node"
	RepairInRegion      RepairScope = "same_region"
	RepairInOtherRegion RepairScope = "other_region"
)

// RegionalHolding is an authenticated observation for one exact record. A
// StoreID never supplies an extra vote; Region and Node are both checked.
type RegionalHolding struct {
	RegionID string
	Holding  Holding
}

// NodeAvailability is a fresh operational observation. It can influence a
// repair target, but it never changes the durable Region/Node denominator.
type NodeAvailability struct {
	Node        rootstate.Node
	Healthy     bool
	HasCapacity bool
}

type RepairTarget struct {
	Scope    RepairScope
	RegionID string
	Nodes    []rootstate.Node
}

// SelectRepairTarget chooses enough distinct Nodes in one confirmed Region to
// restore that Region's strict majority. The preferred Region is tried first;
// if it has no usable capacity, another Region may be selected. Volumes are
// deliberately absent from the result: multiple Volumes on one Node cannot
// satisfy a second vote.
func (p GeoPlan) SelectRepairTarget(recordID, preferredRegion string, preferredNode uint64, holdings []RegionalHolding, availability []NodeAvailability) (RepairTarget, error) {
	if recordID == "" || len(p.regions) == 0 {
		return RepairTarget{}, errors.New("repair target selection requires a GeoPlan and RecordID")
	}
	known := make(map[string]LocalPlan, len(p.regions))
	nodes := make(map[uint64]rootstate.Node)
	for _, region := range p.regions {
		known[region.RegionID] = region
		for _, node := range region.nodes {
			nodes[node.RaftID] = node
		}
	}
	counts := make(map[string]int, len(p.regions))
	seenNode := make(map[uint64]bool)
	seenStore := make(map[string]bool)
	for _, observed := range holdings {
		plan, found := known[observed.RegionID]
		if !found || observed.Holding.RecordID != recordID || seenNode[observed.Holding.RaftID] || seenStore[observed.Holding.StoreID] {
			return RepairTarget{}, errors.New("repair holding differs from the confirmed GeoPlan or repeats a vote")
		}
		if _, found := nodes[observed.Holding.RaftID]; !found {
			return RepairTarget{}, errors.New("repair holding references a foreign Node")
		}
		if _, err := plan.Count(recordID, []Holding{observed.Holding}); err != nil {
			return RepairTarget{}, err
		}
		seenNode[observed.Holding.RaftID] = true
		seenStore[observed.Holding.StoreID] = true
		counts[observed.RegionID]++
	}
	available := make(map[string][]rootstate.Node)
	seenAvailability := make(map[uint64]bool)
	var preferredCandidate *rootstate.Node
	for _, candidate := range availability {
		if candidate.Node.RaftID == 0 || candidate.Node.NodeID == "" {
			continue
		}
		knownNode, found := nodes[candidate.Node.RaftID]
		if !found || knownNode.NodeID != candidate.Node.NodeID || knownNode.RegionID != candidate.Node.RegionID || seenNode[candidate.Node.RaftID] {
			continue
		}
		if seenAvailability[candidate.Node.RaftID] {
			return RepairTarget{}, errors.New("repair availability repeats a Node")
		}
		seenAvailability[candidate.Node.RaftID] = true
		if !candidate.Healthy || !candidate.HasCapacity {
			continue
		}
		if candidate.Node.RaftID == preferredNode {
			copy := candidate.Node
			preferredCandidate = &copy
		}
		available[candidate.Node.RegionID] = append(available[candidate.Node.RegionID], candidate.Node)
	}
	if preferredCandidate != nil {
		return RepairTarget{Scope: RepairOnNode, RegionID: preferredCandidate.RegionID, Nodes: []rootstate.Node{*preferredCandidate}}, nil
	}
	for region := range available {
		slices.SortFunc(available[region], func(a, b rootstate.Node) int {
			if a.RaftID < b.RaftID {
				return -1
			}
			if a.RaftID > b.RaftID {
				return 1
			}
			return 0
		})
	}
	order := make([]string, 0, len(p.regions))
	if preferredRegion != "" {
		if _, found := known[preferredRegion]; found {
			order = append(order, preferredRegion)
		}
	}
	for _, region := range p.persistentRegionOrder() {
		if region != preferredRegion {
			order = append(order, region)
		}
	}
	for _, regionID := range order {
		plan := known[regionID]
		need := plan.Required - counts[regionID]
		if need <= 0 {
			continue
		}
		candidates := available[regionID]
		if len(candidates) < need {
			continue
		}
		selected := slices.Clone(candidates[:need])
		scope := RepairInOtherRegion
		if regionID == preferredRegion {
			scope = RepairInRegion
			if len(selected) == 1 && selected[0].RaftID == 0 {
				scope = RepairOnNode
			}
		}
		return RepairTarget{Scope: scope, RegionID: regionID, Nodes: selected}, nil
	}
	return RepairTarget{}, fmt.Errorf("no healthy capacity can restore a Region majority for RecordID %q", recordID)
}

// SelectRepairCandidates returns the exact missing Nodes needed to restore a
// Region majority without making a capacity claim. It is used by durable
// background retry: each target write is attempted independently, and the
// caller must re-observe the full GeoPlan before deleting its retry task.
func (p GeoPlan) SelectRepairCandidates(recordID, preferredRegion string, holdings []RegionalHolding) (RepairTarget, error) {
	available := make([]NodeAvailability, 0)
	seen := make(map[uint64]bool)
	for _, observed := range holdings {
		seen[observed.Holding.RaftID] = true
	}
	for _, region := range p.regions {
		for _, node := range region.nodes {
			if !seen[node.RaftID] {
				available = append(available, NodeAvailability{Node: node, Healthy: true, HasCapacity: true})
			}
		}
	}
	return p.SelectRepairTarget(recordID, preferredRegion, 0, holdings, available)
}

func (p GeoPlan) persistentRegionOrder() []string {
	order := make([]string, 0, len(p.regions))
	for _, region := range p.regions {
		order = append(order, region.RegionID)
	}
	slices.Sort(order)
	return order
}
