package placement

import (
	"slices"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

func localFixture() (rootstate.RegionProgress, []rootstate.Node) {
	nodes := []rootstate.Node{
		{RaftID: 1, NodeID: "node_00000000000000000000000000000001", RegionID: "west", Address: "n1:7443"},
		{RaftID: 2, NodeID: "node_00000000000000000000000000000002", RegionID: "west", Address: "n2:7443"},
		{RaftID: 3, NodeID: "node_00000000000000000000000000000003", RegionID: "west", Address: "n3:7443"},
	}
	current := rootstate.RegionCohort{RegionID: "west", GroupID: "region-west", Generation: 1,
		Nodes: []uint64{1, 2, 3}, State: rootstate.RegionCohortConfirmed,
		PlannedIndex: 1, ConfirmedIndex: 5, GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 4}
	return rootstate.RegionProgress{Eligible: true,
		Head:    rootstate.RegionCohortHead{RegionID: "west", CurrentGeneration: 1},
		Current: current, Group: rootstate.Group{GroupID: "region-west", Epoch: 2, Members: []rootstate.GroupMember{
			{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter},
		}}}, nodes
}

func TestPlanLocalCountsIndependentStableNodes(t *testing.T) {
	progress, members := localFixture()
	plan, err := PlanLocal(progress, members)
	if err != nil || plan.Required != 2 || !slices.Equal(plan.Nodes(), members) {
		t.Fatal(plan, err)
	}
	// A caller cannot change the plan by modifying the input or returned copy.
	members[0].NodeID = "changed"
	returned := plan.Nodes()
	returned[0].NodeID = "also-changed"
	first := Holding{RaftID: 1, NodeID: plan.Nodes()[0].NodeID, StoreID: "store-one", RecordID: "record-one"}
	second := Holding{RaftID: 2, NodeID: plan.Nodes()[1].NodeID, StoreID: "store-two", RecordID: "record-one"}
	if count, err := plan.Count("record-one", []Holding{first}); err != nil || count != 1 || count >= plan.Required {
		t.Fatal("one Node met a stable majority", count, err)
	}
	if count, err := plan.Count("record-one", []Holding{first, second}); err != nil || count != plan.Required {
		t.Fatal("two independent Nodes did not meet majority", count, err)
	}
	// Multiple Volumes or duplicate observations from one Node never add votes.
	for _, bad := range []Holding{
		{RaftID: 1, NodeID: first.NodeID, StoreID: "another-volume", RecordID: "record-one"},
		{RaftID: 9, NodeID: first.NodeID, StoreID: "store-other", RecordID: "record-one"},
		{RaftID: 2, NodeID: second.NodeID, StoreID: "store-two", RecordID: "another-record"},
		{RaftID: 2, NodeID: "replaced-node", StoreID: "store-two", RecordID: "record-one"},
		{RaftID: 2, NodeID: second.NodeID, StoreID: "store-one", RecordID: "record-one"},
	} {
		if _, err := plan.Count("record-one", []Holding{first, bad}); err == nil {
			t.Fatal("counted invalid physical holding", bad)
		}
	}
}

func TestPlanLocalNeverShrinksForHealthOrPendingGrowth(t *testing.T) {
	progress, members := localFixture()
	if _, err := PlanLocal(progress, members[:2]); err == nil {
		t.Fatal("offline Node shrank the durable denominator")
	}
	progress.Pending = &rootstate.RegionCohort{Generation: 2}
	if _, err := PlanLocal(progress, members); err == nil {
		t.Fatal("pending generation admitted a new placement plan")
	}
	progress.Pending = nil
	progress.Eligible = false
	if _, err := PlanLocal(progress, members); err == nil {
		t.Fatal("ineligible Region admitted a plan")
	}
	progress.Eligible = true
	plan, err := PlanLocal(progress, members)
	if err != nil {
		t.Fatal(err)
	}
	progress.Current.Generation++
	progress.Head.CurrentGeneration++
	progress.Current.PlannedIndex++
	progress.Current.ConfirmedIndex++
	progress.Current.GroupFenceIndex++
	progress.Current.GroupFenceRequestID = "next-fence"
	newPlan, err := PlanLocal(progress, members)
	if err != nil || plan.Matches(newPlan) {
		t.Fatal("old and new Region generations were treated as one plan", err)
	}
	progress, members = localFixture()
	progress.Current.GroupEpoch++
	progress.Current.GroupFenceIndex++
	progress.Current.GroupFenceRequestID = "same-generation-refence"
	progress.Group.Epoch++
	refenced, err := PlanLocal(progress, members)
	if err != nil || plan.Matches(refenced) {
		t.Fatal("new Group fence within one Region generation reused the old physical cut", err)
	}
}

func TestPlanLocalAcceptsSmallConfirmedCohorts(t *testing.T) {
	progress, members := localFixture()
	progress.Current.Nodes = []uint64{1, 2}
	progress.Group.Members = progress.Group.Members[:2]
	if _, err := PlanLocal(progress, members[:2]); err == nil {
		t.Fatal("two-Node Region accepted a local foreground plan")
	}
	plan, err := planRegion(progress, members[:2])
	if err != nil || plan.Required != 2 {
		t.Fatalf("two-Node Region was not planned with an all-Node majority: %+v, %v", plan, err)
	}
	progress.Current.Nodes = []uint64{1}
	progress.Group.Members = progress.Group.Members[:1]
	if _, err := PlanLocal(progress, members[:1]); err == nil {
		t.Fatal("one-Node Region accepted a local foreground plan")
	}
	plan, err = planRegion(progress, members[:1])
	if err != nil || plan.Required != 1 {
		t.Fatalf("one-Node Region was not planned with a one-Node majority: %+v, %v", plan, err)
	}
	if count, err := plan.Count("record-one", []Holding{{RaftID: 1, NodeID: members[0].NodeID, StoreID: "store-one", RecordID: "record-one"}}); err != nil || count != 1 {
		t.Fatalf("one-Node Region did not count its only holding: %d, %v", count, err)
	}

	progress, members = localFixture()
	members[1].RegionID = "east"
	if _, err := PlanLocal(progress, members); err == nil {
		t.Fatal("cross-Region Node counted as local")
	}
	progress, members = localFixture()
	members[1].RaftID = members[0].RaftID
	if _, err := PlanLocal(progress, members); err == nil {
		t.Fatal("duplicate RaftID counted twice")
	}
}

func TestPlanLocalUsesNewDenominatorOnlyAfterConfirmedGrowth(t *testing.T) {
	progress, members := localFixture()
	progress.Current.Generation = 2
	progress.Current.Nodes = []uint64{1, 2, 3, 4}
	progress.Current.PlannedIndex = 6
	progress.Current.ConfirmedIndex = 10
	progress.Current.GroupEpoch = 3
	progress.Current.GroupFenceRequestID = "growth-fence"
	progress.Current.GroupFenceIndex = 9
	progress.Head.CurrentGeneration = 2
	progress.Group.Epoch = 3
	progress.Group.Members = append(progress.Group.Members,
		rootstate.GroupMember{RaftID: 4, Role: rootstate.GroupMemberVoter})
	members = append(members, rootstate.Node{RaftID: 4,
		NodeID: "node_00000000000000000000000000000004", RegionID: "west", Address: "n4:7443"})
	plan, err := PlanLocal(progress, members)
	if err != nil || plan.Required != 3 {
		t.Fatal("confirmed four-Node generation did not require three holdings", plan, err)
	}
	progress.Group.Members[0], progress.Group.Members[3] = progress.Group.Members[3], progress.Group.Members[0]
	if reordered, err := PlanLocal(progress, members); err != nil || !plan.Matches(reordered) {
		t.Fatal("Group member order changed a stable placement plan", reordered, err)
	}
}
