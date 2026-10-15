package recordrepair

import (
	"context"
	"errors"
	"fmt"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// RepairSelectedTargets executes a previously validated Geo placement choice.
// It deliberately does not discover a new denominator, publish a logical
// reference, or claim GeoReady. The caller must observe the returned holdings
// again and run GeoPlan.Assess/AssessPublication after the writes.
//
// Each target owns its local Pack write. RepairExisting is attempted first so
// a damaged existing location is replaced without changing its logical index;
// if the target has no index yet, the operation falls back to a normal local
// immutable-record Put on that target.
func RepairSelectedTargets(ctx context.Context, plan placement.GeoPlan, target placement.RepairTarget,
	source rootstate.Node, group string, d records.Descriptor, client RemoteRepairer) ([]records.Holding, error) {
	if ctx == nil || client == nil || len(target.Nodes) == 0 || target.RegionID == "" ||
		source.RaftID == 0 || source.NodeID == "" || group == "" || d.GroupID != group {
		return nil, errors.New("Geo repair requires a selected target, source and descriptor")
	}
	if target.Scope != placement.RepairOnNode && target.Scope != placement.RepairInRegion && target.Scope != placement.RepairInOtherRegion {
		return nil, errors.New("Geo repair target has an invalid scope")
	}
	if target.Scope == placement.RepairOnNode && len(target.Nodes) != 1 {
		return nil, errors.New("single-Node Geo repair scope requires exactly one target")
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	recordID, err := d.ID()
	if err != nil {
		return nil, err
	}
	knownRegions := plan.Regions()
	regionFound := false
	known := make(map[uint64]rootstate.Node)
	for _, region := range knownRegions {
		if region.RegionID == target.RegionID {
			regionFound = true
		}
		for _, node := range region.Nodes() {
			known[node.RaftID] = node
		}
	}
	if !regionFound {
		return nil, errors.New("Geo repair target Region is not in the confirmed plan")
	}
	if node, found := known[source.RaftID]; !found || node.NodeID != source.NodeID {
		return nil, errors.New("Geo repair source is not in the confirmed plan")
	}
	if target.Scope == placement.RepairInOtherRegion && target.RegionID == source.RegionID {
		return nil, errors.New("cross-Region Geo repair target matches the source Region")
	}
	seen := make(map[uint64]bool, len(target.Nodes))
	holdings := make([]records.Holding, 0, len(target.Nodes))
	var failures []error
	remaining := len(target.Nodes)
	for _, destination := range target.Nodes {
		if destination.RaftID == 0 || destination.NodeID == "" || destination.RaftID == source.RaftID ||
			seen[destination.RaftID] {
			return nil, errors.New("Geo repair targets repeat the source or a Node")
		}
		planned, found := known[destination.RaftID]
		if !found || planned != destination || destination.RegionID != target.RegionID {
			return nil, errors.New("Geo repair target differs from the confirmed plan")
		}
		seen[destination.RaftID] = true
		attempt, cancel := sourceAttemptContext(ctx, remaining)
		remaining--
		holding, repairErr := client.RepairRecordOnNode(attempt, destination, source, group, d)
		cancel()
		if repairErr != nil {
			fetchAttempt, fetchCancel := sourceAttemptContext(ctx, remaining+1)
			holding, repairErr = client.FetchRecordOnNode(fetchAttempt, destination, source, group, d)
			fetchCancel()
		}
		if repairErr != nil {
			failures = append(failures, fmt.Errorf("target Node %d: %w", destination.RaftID, repairErr))
			continue
		}
		actual, idErr := holding.Descriptor.ID()
		if idErr != nil || actual != recordID || holding.RecordID != recordID || holding.StoreID == "" || holding.VolumeID == "" {
			failures = append(failures, fmt.Errorf("target Node %d returned an inconsistent holding", destination.RaftID))
			continue
		}
		holdings = append(holdings, holding)
	}
	if len(holdings) == 0 {
		return nil, errors.Join(append([]error{errors.New("no selected Geo repair target accepted the record")}, failures...)...)
	}
	return holdings, errors.Join(failures...)
}
