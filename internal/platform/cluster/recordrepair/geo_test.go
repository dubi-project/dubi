package recordrepair

import (
	"context"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

func geoRepairPlan(t *testing.T) placement.GeoPlan {
	t.Helper()
	progress := func(region, group string, ids []uint64) rootstate.RegionProgress {
		members := make([]rootstate.GroupMember, 0, len(ids))
		for _, id := range ids {
			members = append(members, rootstate.GroupMember{RaftID: id, Role: rootstate.GroupMemberVoter})
		}
		return rootstate.RegionProgress{AppliedIndex: 7, Eligible: true,
			Head: rootstate.RegionCohortHead{RegionID: region, CurrentGeneration: 1},
			Current: rootstate.RegionCohort{RegionID: region, GroupID: group, Generation: 1, Nodes: ids,
				State: rootstate.RegionCohortConfirmed, PlannedIndex: 1, ConfirmedIndex: 5,
				GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 4},
			Group: rootstate.Group{GroupID: group, Epoch: 2, Members: members}}
	}
	west := []rootstate.Node{{RaftID: 1, NodeID: "node_00000000000000000000000000000001", RegionID: "west", Address: "w1"}, {RaftID: 2, NodeID: "node_00000000000000000000000000000002", RegionID: "west", Address: "w2"}, {RaftID: 3, NodeID: "node_00000000000000000000000000000003", RegionID: "west", Address: "w3"}}
	east := []rootstate.Node{{RaftID: 4, NodeID: "node_00000000000000000000000000000004", RegionID: "east", Address: "e1"}, {RaftID: 5, NodeID: "node_00000000000000000000000000000005", RegionID: "east", Address: "e2"}, {RaftID: 6, NodeID: "node_00000000000000000000000000000006", RegionID: "east", Address: "e3"}}
	view := rootstate.RegionPlacementView{AppliedIndex: 7, Regions: []rootstate.RegionPlacement{
		{Progress: progress("west", "region-west", []uint64{1, 2, 3}), Nodes: west},
		{Progress: progress("east", "region-east", []uint64{4, 5, 6}), Nodes: east},
	}}
	plan, err := placement.PlanGeo(view)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestRepairSelectedTargetsWritesOnlyConfirmedGeoTargets(t *testing.T) {
	plan := geoRepairPlan(t)
	regions := plan.Regions()
	var west, east placement.LocalPlan
	for _, region := range regions {
		if region.RegionID == "west" {
			west = region
		} else if region.RegionID == "east" {
			east = region
		}
	}
	d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "search", DatasetID: "items"},
		"region-west", []byte("geo-key"), []byte("geo-body"))
	if err != nil {
		t.Fatal(err)
	}
	fake := &remoteRepairFake{repairSourceFake: &repairSourceFake{nodes: append(west.Nodes(), east.Nodes()...)}}
	holdings, err := RepairSelectedTargets(context.Background(), plan, placement.RepairTarget{Scope: placement.RepairInOtherRegion,
		RegionID: "east", Nodes: []rootstate.Node{east.Nodes()[0]}}, west.Nodes()[0], d.GroupID, d, fake)
	if err != nil || len(holdings) != 1 || len(fake.remoteTargets) != 1 || fake.remoteTargets[0] != 4 {
		t.Fatal(holdings, fake.remoteTargets, err)
	}
	if _, err := RepairSelectedTargets(context.Background(), plan, placement.RepairTarget{RegionID: "east", Nodes: []rootstate.Node{{RaftID: 99, NodeID: "node_00000000000000000000000000000099", RegionID: "east"}}}, west.Nodes()[0], d.GroupID, d, fake); err == nil {
		t.Fatal("foreign Geo target was accepted")
	}
}
