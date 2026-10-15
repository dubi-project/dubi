package placement

import (
	"slices"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

func rebalancePlanFixture(t *testing.T) GeoPlan {
	t.Helper()
	westProgress, westNodes := localFixture()
	west, err := planRegion(westProgress, westNodes)
	if err != nil {
		t.Fatal(err)
	}
	eastProgress := westProgress
	eastProgress.Current = westProgress.Current
	eastProgress.Current.RegionID = "east"
	eastProgress.Current.GroupID = "region-east"
	eastProgress.Current.Nodes = []uint64{4, 5, 6}
	eastProgress.Head.RegionID = "east"
	eastProgress.Group.GroupID = "region-east"
	eastProgress.Group.Members = []rootstate.GroupMember{{RaftID: 4, Role: rootstate.GroupMemberVoter}, {RaftID: 5, Role: rootstate.GroupMemberVoter}, {RaftID: 6, Role: rootstate.GroupMemberVoter}}
	eastNodes := []rootstate.Node{{RaftID: 4, NodeID: "node_00000000000000000000000000000004", RegionID: "east", Address: "e1:7443"}, {RaftID: 5, NodeID: "node_00000000000000000000000000000005", RegionID: "east", Address: "e2:7443"}, {RaftID: 6, NodeID: "node_00000000000000000000000000000006", RegionID: "east", Address: "e3:7443"}}
	east, err := planRegion(eastProgress, eastNodes)
	if err != nil {
		t.Fatal(err)
	}
	return GeoPlan{regions: []LocalPlan{west, east}}
}

func TestSelectRepairTargetPrefersPreferredRegionAndNeverCountsVolumes(t *testing.T) {
	plan := rebalancePlanFixture(t)
	target, err := plan.SelectRepairTarget("record-one", "west", 3, []RegionalHolding{{RegionID: "west", Holding: Holding{RaftID: 1, NodeID: "node_00000000000000000000000000000001", StoreID: "store-1", RecordID: "record-one"}}, {RegionID: "west", Holding: Holding{RaftID: 2, NodeID: "node_00000000000000000000000000000002", StoreID: "store-2", RecordID: "record-one"}}}, []NodeAvailability{{Node: plan.regions[0].nodes[2], Healthy: true, HasCapacity: true}, {Node: plan.regions[1].nodes[0], Healthy: true, HasCapacity: true}})
	if err != nil || target.Scope != RepairOnNode || target.RegionID != "west" || !slices.Equal([]uint64{target.Nodes[0].RaftID}, []uint64{3}) {
		t.Fatal(target, err)
	}
	if _, err := plan.SelectRepairTarget("record-one", "west", 0, []RegionalHolding{{RegionID: "west", Holding: Holding{RaftID: 1, NodeID: "node_00000000000000000000000000000001", StoreID: "store-1", RecordID: "record-one"}}, {RegionID: "west", Holding: Holding{RaftID: 2, NodeID: "node_00000000000000000000000000000002", StoreID: "store-2", RecordID: "record-one"}}, {RegionID: "west", Holding: Holding{RaftID: 3, NodeID: "node_00000000000000000000000000000003", StoreID: "store-3", RecordID: "record-one"}}}, []NodeAvailability{{Node: plan.regions[0].nodes[0], Healthy: true, HasCapacity: true}}); err == nil {
		t.Fatal("duplicate same-Region holding did not leave the planner with no repair debt")
	}
}

func TestSelectRepairTargetFallsBackToOtherRegionOnlyWithCapacity(t *testing.T) {
	plan := rebalancePlanFixture(t)
	holdings := []RegionalHolding{{RegionID: "west", Holding: Holding{RaftID: 1, NodeID: "node_00000000000000000000000000000001", StoreID: "store-1", RecordID: "record-one"}}, {RegionID: "west", Holding: Holding{RaftID: 2, NodeID: "node_00000000000000000000000000000002", StoreID: "store-2", RecordID: "record-one"}}}
	available := []NodeAvailability{{Node: plan.regions[0].nodes[2], Healthy: false, HasCapacity: true}, {Node: plan.regions[1].nodes[0], Healthy: true, HasCapacity: true}, {Node: plan.regions[1].nodes[1], Healthy: true, HasCapacity: true}}
	target, err := plan.SelectRepairTarget("record-one", "west", 0, holdings, available)
	if err != nil || target.Scope != RepairInOtherRegion || target.RegionID != "east" || len(target.Nodes) != 2 {
		t.Fatal(target, err)
	}
	if _, err := plan.SelectRepairTarget("record-one", "west", 0, holdings, []NodeAvailability{{Node: plan.regions[1].nodes[0], Healthy: true, HasCapacity: true}}); err == nil {
		t.Fatal("one target Node in other Region was treated as a Region majority")
	}
}

func TestSelectRepairTargetRejectsRepeatedAvailabilityAndForeignIdentity(t *testing.T) {
	plan := rebalancePlanFixture(t)
	base := []RegionalHolding{{RegionID: "west", Holding: Holding{RaftID: 1, NodeID: plan.regions[0].nodes[0].NodeID, StoreID: "store-1", RecordID: "record-one"}}}
	node := plan.regions[0].nodes[1]
	if _, err := plan.SelectRepairTarget("record-one", "west", 0, base, []NodeAvailability{{Node: node, Healthy: true, HasCapacity: true}, {Node: node, Healthy: true, HasCapacity: true}}); err == nil {
		t.Fatal("repeated availability was accepted")
	}
	spoof := node
	spoof.NodeID = "node_ffffffffffffffffffffffffffffffff"
	if _, err := plan.SelectRepairTarget("record-one", "west", 0, base, []NodeAvailability{{Node: spoof, Healthy: true, HasCapacity: true}}); err == nil {
		t.Fatal("foreign Node identity was accepted as repair capacity")
	}
}
