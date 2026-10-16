package recordwrite

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type geoStageSender struct {
	fail map[uint64]error
}

func (s *geoStageSender) PutRecord(_ context.Context, node rootstate.Node, d records.Descriptor, _ []byte) (raftrpc.HoldingObservation, error) {
	if err := s.fail[node.RaftID]; err != nil {
		return raftrpc.HoldingObservation{}, err
	}
	id, _ := d.ID()
	return raftrpc.HoldingObservation{RaftID: node.RaftID, NodeID: node.NodeID, RootConfirmedIndex: 7,
		Holding: records.Holding{RecordID: id, Descriptor: d, StoreID: fmt.Sprintf("store-%d", node.RaftID), VolumeID: fmt.Sprintf("volume-%d", node.RaftID)}}, nil
}

func geoStagePlan(t *testing.T) (placement.GeoPlan, router.Target, records.Descriptor) {
	return geoStagePlanWithSizes(t, 3, 3)
}

func geoStagePlanWithSizes(t *testing.T, westSize, eastSize int) (placement.GeoPlan, router.Target, records.Descriptor) {
	t.Helper()
	west := []rootstate.Node{
		{RaftID: 1, NodeID: "node_00000000000000000000000000000001", RegionID: "west", Address: "w1:7443"},
		{RaftID: 2, NodeID: "node_00000000000000000000000000000002", RegionID: "west", Address: "w2:7443"},
		{RaftID: 3, NodeID: "node_00000000000000000000000000000003", RegionID: "west", Address: "w3:7443"},
	}
	east := []rootstate.Node{
		{RaftID: 4, NodeID: "node_00000000000000000000000000000004", RegionID: "east", Address: "e1:7443"},
		{RaftID: 5, NodeID: "node_00000000000000000000000000000005", RegionID: "east", Address: "e2:7443"},
		{RaftID: 6, NodeID: "node_00000000000000000000000000000006", RegionID: "east", Address: "e3:7443"},
	}
	progress := func(region, group string, nodes []rootstate.Node) rootstate.RegionProgress {
		ids := make([]uint64, len(nodes))
		members := make([]rootstate.GroupMember, len(nodes))
		for i, node := range nodes {
			ids[i] = node.RaftID
			members[i] = rootstate.GroupMember{RaftID: node.RaftID, Role: rootstate.GroupMemberVoter}
		}
		current := rootstate.RegionCohort{RegionID: region, GroupID: group, Generation: 1, Nodes: ids,
			State: rootstate.RegionCohortConfirmed, PlannedIndex: 1, ConfirmedIndex: 5,
			GroupEpoch: 2, GroupFenceRequestID: "fence-1", GroupFenceIndex: 4}
		return rootstate.RegionProgress{AppliedIndex: 7, Eligible: true, Head: rootstate.RegionCohortHead{RegionID: region, CurrentGeneration: 1},
			Current: current, Group: rootstate.Group{GroupID: group, Epoch: 2, Members: members}}
	}
	west, east = west[:westSize], east[:eastSize]
	view := rootstate.RegionPlacementView{AppliedIndex: 7, Regions: []rootstate.RegionPlacement{
		{Progress: progress("west", "region-west", west), Nodes: west},
		{Progress: progress("east", "region-east", east), Nodes: east},
	}}
	plan, err := placement.PlanGeo(view)
	if err != nil {
		t.Fatal(err)
	}
	target := router.Target{Keyspace: rootstate.Keyspace{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items"},
		RangeID: "range-a", OwnerGroupID: "region-west", Epoch: 3, RoutingKey: []byte("key")}
	descriptor, err := records.Describe(target.Keyspace, "origin-group", []byte("key"), []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	return plan, target, descriptor
}

func TestStageGeoSmallRegionsKeepExactLocalThreshold(t *testing.T) {
	for _, westSize := range []int{1, 2} {
		t.Run(fmt.Sprintf("west-%d", westSize), func(t *testing.T) {
			plan, _, descriptor := geoStagePlanWithSizes(t, westSize, 1)
			staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, descriptor, []byte("body"))
			if err != nil || len(staged.Evidence) != 2 {
				t.Fatalf("small-Region staging = %+v, %v", staged, err)
			}
			for _, evidence := range staged.Evidence {
				if len(evidence.Holdings) != len(evidence.Nodes)/2+1 {
					t.Fatalf("missing strict regional majority: %+v", evidence)
				}
			}
			sender := &geoStageSender{fail: map[uint64]error{4: status.Error(codes.Unavailable, "offline")}}
			if _, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, sender, descriptor, []byte("body")); err == nil {
				t.Fatal("single remaining Region was treated as cross-Region majority")
			}
			if westSize == 2 {
				sender.fail = map[uint64]error{2: status.Error(codes.Unavailable, "offline")}
				if _, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, sender, descriptor, []byte("body")); err == nil {
					t.Fatal("two-Node Region denominator was shrunk to one")
				}
			}
		})
	}
}

func TestStageGeoReachesRegionMajority(t *testing.T) {
	plan, _, descriptor := geoStagePlan(t)
	staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, descriptor, []byte("body"))
	if err != nil || len(staged.Evidence) != 2 || staged.RecordID == "" {
		t.Fatal(staged, err)
	}
	coverage, err := plan.Assess(staged.RecordID, staged.Evidence)
	if err != nil || !coverage.HasMajority() || len(coverage.CoveredRegions) != 2 {
		t.Fatal(coverage, err)
	}
}

func TestStageGeoUsesFixedRegionMajorityWhenOneRegionIsOffline(t *testing.T) {
	regions := []struct {
		name string
		node rootstate.Node
	}{
		{name: "west", node: rootstate.Node{RaftID: 1, NodeID: "node_00000000000000000000000000000001", RegionID: "west", Address: "w1:7443"}},
		{name: "east", node: rootstate.Node{RaftID: 2, NodeID: "node_00000000000000000000000000000002", RegionID: "east", Address: "e1:7443"}},
		{name: "north", node: rootstate.Node{RaftID: 3, NodeID: "node_00000000000000000000000000000003", RegionID: "north", Address: "n1:7443"}},
	}
	progress := func(region, group string, node rootstate.Node) rootstate.RegionPlacement {
		current := rootstate.RegionCohort{RegionID: region, GroupID: group, Generation: 1, Nodes: []uint64{node.RaftID},
			State: rootstate.RegionCohortConfirmed, PlannedIndex: 1, ConfirmedIndex: 5,
			GroupEpoch: 2, GroupFenceRequestID: "fence-1", GroupFenceIndex: 4}
		return rootstate.RegionPlacement{Progress: rootstate.RegionProgress{AppliedIndex: 7,
			Eligible: true, Head: rootstate.RegionCohortHead{RegionID: region, CurrentGeneration: 1},
			Current: current, Group: rootstate.Group{GroupID: group, Epoch: 2,
				Members: []rootstate.GroupMember{{RaftID: node.RaftID, Role: rootstate.GroupMemberVoter}}}}, Nodes: []rootstate.Node{node}}
	}
	view := rootstate.RegionPlacementView{AppliedIndex: 7}
	for _, region := range regions {
		view.Regions = append(view.Regions, progress(region.name, "region-"+region.name, region.node))
	}
	plan, err := placement.PlanGeo(view)
	if err != nil {
		t.Fatal(err)
	}
	_, _, descriptor := geoStagePlanWithSizes(t, 1, 1)
	// East is unavailable, but West + North are the fixed two-Region majority.
	sender := &geoStageSender{fail: map[uint64]error{2: status.Error(codes.Unavailable, "offline")}}
	staged, err := StageGeo(context.Background(), plan, 1, regions[0].node.NodeID, &testStore{}, sender, descriptor, []byte("body"))
	if err != nil || len(staged.Evidence) != 2 {
		t.Fatalf("geo majority staging = %+v, %v", staged, err)
	}
	coverage, err := plan.Assess(staged.RecordID, staged.Evidence)
	if err != nil || !coverage.HasMajority() || len(coverage.CoveredRegions) != 2 {
		t.Fatalf("geo majority coverage = %+v, %v", coverage, err)
	}
	if !slices.Equal(coverage.CoveredRegions, []string{"north", "west"}) {
		t.Fatalf("unexpected covered Regions = %v", coverage.CoveredRegions)
	}
}

func TestStageGeoDoesNotShrinkOfflineRegionDenominator(t *testing.T) {
	plan, _, descriptor := geoStagePlan(t)
	sender := &geoStageSender{fail: map[uint64]error{4: status.Error(codes.Unavailable, "offline"), 5: status.Error(codes.Unavailable, "offline"), 6: status.Error(codes.Unavailable, "offline")}}
	if _, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, sender, descriptor, []byte("body")); err == nil {
		t.Fatal("offline Region was silently removed from geo denominator")
	}
}
