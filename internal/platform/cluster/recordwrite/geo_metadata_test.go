package recordwrite

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type geoMirrorTransport struct {
	failed   map[uint64]bool
	attempts []uint64
	mirrors  []routedstate.GeoMetadataMirror
}

func (t *geoMirrorTransport) MirrorGeo(_ context.Context, node uint64, _ string, encoded []byte) (capstate.Result, error) {
	t.attempts = append(t.attempts, node)
	if t.failed[node] {
		return capstate.Result{}, errors.New("node unavailable")
	}
	command, err := routedstate.DecodeCommand(encoded)
	if err != nil || command.Type != routedstate.MirrorGeo {
		return capstate.Result{}, err
	}
	t.mirrors = append(t.mirrors, *command.GeoMetadata)
	return capstate.Result{RequestID: command.RequestID, Index: node, Applied: true}, nil
}

func geoThreeRegionPlan(t *testing.T) (placement.GeoPlan, router.Target, records.Descriptor) {
	t.Helper()
	regions := []struct {
		region string
		group  string
		node   rootstate.Node
	}{
		{region: "west", group: "region-west", node: rootstate.Node{RaftID: 1, NodeID: "node_00000000000000000000000000000001", RegionID: "west", Address: "w1:7443"}},
		{region: "east", group: "region-east", node: rootstate.Node{RaftID: 2, NodeID: "node_00000000000000000000000000000002", RegionID: "east", Address: "e1:7443"}},
		{region: "north", group: "region-north", node: rootstate.Node{RaftID: 3, NodeID: "node_00000000000000000000000000000003", RegionID: "north", Address: "n1:7443"}},
	}
	view := rootstate.RegionPlacementView{AppliedIndex: 7}
	for _, item := range regions {
		current := rootstate.RegionCohort{RegionID: item.region, GroupID: item.group, Generation: 1, Nodes: []uint64{item.node.RaftID},
			State: rootstate.RegionCohortConfirmed, PlannedIndex: 1, ConfirmedIndex: 5,
			GroupEpoch: 2, GroupFenceRequestID: "fence-1", GroupFenceIndex: 4}
		view.Regions = append(view.Regions, rootstate.RegionPlacement{Progress: rootstate.RegionProgress{AppliedIndex: 7,
			Eligible: true, Head: rootstate.RegionCohortHead{RegionID: item.region, CurrentGeneration: 1}, Current: current,
			Group: rootstate.Group{GroupID: item.group, Epoch: 2,
				Members: []rootstate.GroupMember{{RaftID: item.node.RaftID, Role: rootstate.GroupMemberVoter}}}}, Nodes: []rootstate.Node{item.node}})
	}
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

func TestReplicateGeoMetadataRetriesAnotherVoter(t *testing.T) {
	plan, target, descriptor := geoStagePlan(t)
	staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, descriptor, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	recordID, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: "publish-1", Index: 9,
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:     target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, err := GeoMetadataMirrors(target, descriptor, publication, plan, staged.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	transport := &geoMirrorTransport{failed: map[uint64]bool{1: true, 4: true}}
	results, err := ReplicateGeoMetadata(context.Background(), transport, plan, mirrors)
	if err != nil || len(results) != 2 {
		t.Fatalf("replication results = %+v, err %v", results, err)
	}
	if len(transport.attempts) != 4 || transport.attempts[0] != 4 || transport.attempts[1] != 5 ||
		transport.attempts[2] != 1 || transport.attempts[3] != 2 {
		t.Fatalf("voter retry order = %v", transport.attempts)
	}
	if err := mirrors[0].Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicateGeoMetadataIdentifiesRetryableRegionDebt(t *testing.T) {
	plan, target, descriptor := geoStagePlan(t)
	staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, descriptor, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	recordID, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: "publish-2", Index: 10,
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:     target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, err := GeoMetadataMirrors(target, descriptor, publication, plan, staged.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	transport := &geoMirrorTransport{failed: map[uint64]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true}}
	_, err = ReplicateGeoMetadata(context.Background(), transport, plan, mirrors)
	var debt *GeoMetadataReplicationError
	if !errors.As(err, &debt) || !debt.Retryable || debt.RegionID == "" || debt.Mirror.RegionID != debt.RegionID {
		t.Fatal("retryable Region debt was not identified", err)
	}
}

func TestGeoMetadataMirrorsFollowCoveredRegionMajority(t *testing.T) {
	plan, target, descriptor := geoThreeRegionPlan(t)
	staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{},
		&geoStageSender{fail: map[uint64]error{2: status.Error(codes.Unavailable, "east offline")}}, descriptor, []byte("body"))
	if err != nil || len(staged.Evidence) != 2 {
		t.Fatalf("partial geo staging = %+v, err=%v", staged, err)
	}
	recordID, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: "publish-covered", Index: 13,
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:     target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, err := GeoMetadataMirrors(target, descriptor, publication, plan, staged.Evidence)
	if err != nil || len(mirrors) != 2 {
		t.Fatalf("covered Region mirrors = %+v, err=%v", mirrors, err)
	}
	for _, mirror := range mirrors {
		if mirror.RegionID == "east" {
			t.Fatal("offline Region received a synchronous metadata mirror")
		}
	}
	results, err := ReplicateGeoMetadata(context.Background(), &geoMirrorTransport{}, plan, mirrors)
	if err != nil || len(results) != 2 {
		t.Fatalf("covered Region mirror delivery = %+v, err=%v", results, err)
	}
}

func TestGeoReceiptMatchesCurrentRegionGeneration(t *testing.T) {
	plan, target, descriptor := geoStagePlan(t)
	staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, descriptor, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range staged.Evidence {
		for j := range staged.Evidence[i].Holdings {
			staged.Evidence[i].Holdings[j].StoreID = fmt.Sprintf("store_%032x", staged.Evidence[i].Holdings[j].RaftID)
		}
	}
	recordID, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: "geo-reconfirm-publish", Index: 17,
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:     target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	coverage, err := plan.AssessPublication(publication, staged.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := GeoConfirmationClaim(target, descriptor.Key, coverage, staged.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	ref := capstate.RecordReference{Version: 2, Descriptor: descriptor, OwnerGroupID: target.OwnerGroupID,
		RangeID: target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey,
		RequestID: publication.RequestID, CommandHash: publication.CommandHash, Index: publication.Index}
	receipt := capstate.GeoCoverageRecord{Version: 1, Claim: claim, ReferenceCommandHash: publication.CommandHash,
		RequestID: "geo-reconfirm-receipt", Index: 18}
	if !GeoReceiptMatchesPlan(receipt, ref, target, plan) {
		t.Fatal("current Geo receipt was rejected")
	}
	fresh, _, _ := geoStagePlanWithSizes(t, 2, 3)
	if GeoReceiptMatchesPlan(receipt, ref, target, fresh) {
		t.Fatal("Geo receipt survived a changed Region generation/cohort")
	}
}

func TestGeoMetadataRetryStoreSurvivesAndClearsDeliveryDebt(t *testing.T) {
	plan, target, descriptor := geoStagePlan(t)
	staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, descriptor, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	recordID, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: "publish-3", Index: 11,
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:     target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, err := GeoMetadataMirrors(target, descriptor, publication, plan, staged.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenGeoMetadataRetryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	transport := &geoMirrorTransport{failed: map[uint64]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true}}
	if _, err := ReplicateGeoMetadataWithRetryStore(context.Background(), transport, plan, mirrors, store); err == nil {
		t.Fatal("unreachable mirror delivery was reported successful")
	}
	pending, err := store.List()
	if err != nil || len(pending) != len(mirrors) {
		t.Fatal("retry ledger did not retain all mirror debt", pending, err)
	}
	transport.failed = nil
	if _, err := ReplicateGeoMetadataWithRetryStore(context.Background(), transport, plan, pending, store); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.List(); err != nil || len(pending) != 0 {
		t.Fatal("completed mirror debt was not cleared", pending, err)
	}
}

func TestRetryPendingGeoMetadataKeepsStalePlanDebt(t *testing.T) {
	plan, target, descriptor := geoStagePlan(t)
	staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, descriptor, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	recordID, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: "publish-4", Index: 12,
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:     target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, err := GeoMetadataMirrors(target, descriptor, publication, plan, staged.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenGeoMetadataRetryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, mirror := range mirrors {
		if err := store.Save(mirror); err != nil {
			t.Fatal(err)
		}
	}
	transport := &geoMirrorTransport{}
	report, err := RetryPendingGeoMetadata(context.Background(), transport, store, func(context.Context, routedstate.GeoMetadataMirror) (placement.LocalPlan, error) {
		return placement.LocalPlan{}, errors.New("Root plan generation changed")
	})
	if err != nil || report.Attempted != 2 || report.Completed != 0 || report.Pending != 2 {
		t.Fatal(report, err)
	}
	if pending, err := store.List(); err != nil || len(pending) != 2 {
		t.Fatal("stale mirror debt was dropped", pending, err)
	}
}

func TestRetryPendingGeoMetadataRebindsSameNameRegionGeneration(t *testing.T) {
	plan, target, descriptor := geoStagePlanWithSizes(t, 1, 1)
	staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, descriptor, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	recordID, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: "publish-reformed", Index: 14,
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:     target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, err := GeoMetadataMirrors(target, descriptor, publication, plan, staged.Evidence)
	if err != nil || len(mirrors) != 2 {
		t.Fatal(mirrors, err)
	}
	store, err := OpenGeoMetadataRetryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, mirror := range mirrors {
		if err := store.Save(mirror); err != nil {
			t.Fatal(err)
		}
	}

	// The west Region keeps its name and deterministic GroupID, but has a
	// confirmed generation-2 cohort after the old Region was retired.
	view := rootstate.RegionPlacementView{AppliedIndex: 8}
	for _, region := range plan.Regions() {
		nodes := region.Nodes()
		generation := region.Generation
		reformation := false
		if region.RegionID == "west" {
			nodes = []rootstate.Node{{RaftID: 7, NodeID: "node_00000000000000000000000000000007", RegionID: "west", Address: "w-new"}}
			generation = 2
			reformation = true
		}
		ids := make([]uint64, len(nodes))
		members := make([]rootstate.GroupMember, len(nodes))
		for i, node := range nodes {
			ids[i] = node.RaftID
			members[i] = rootstate.GroupMember{RaftID: node.RaftID, Role: rootstate.GroupMemberVoter}
		}
		current := rootstate.RegionCohort{RegionID: region.RegionID, GroupID: region.GroupID, Generation: generation, Reformation: reformation, Nodes: ids,
			State: rootstate.RegionCohortConfirmed, PlannedIndex: 1, ConfirmedIndex: 5,
			GroupEpoch: region.GroupEpoch, GroupFenceRequestID: "fence-1", GroupFenceIndex: region.FenceIndex}
		view.Regions = append(view.Regions, rootstate.RegionPlacement{Progress: rootstate.RegionProgress{AppliedIndex: 8,
			Eligible: true, Head: rootstate.RegionCohortHead{RegionID: region.RegionID, CurrentGeneration: generation}, Current: current,
			Group: rootstate.Group{GroupID: region.GroupID, Epoch: region.GroupEpoch, Members: members}}, Nodes: nodes})
	}
	fresh, err := placement.PlanGeo(view)
	if err != nil {
		t.Fatal(err)
	}
	transport := &geoMirrorTransport{}
	resolve := func(_ context.Context, mirror routedstate.GeoMetadataMirror) (placement.LocalPlan, error) {
		for _, region := range fresh.Regions() {
			if region.RegionID == mirror.RegionID {
				return region, nil
			}
		}
		return placement.LocalPlan{}, errors.New("Region disappeared")
	}
	report, err := RetryPendingGeoMetadata(context.Background(), transport, store, resolve)
	if err != nil || report.Completed != 2 || report.Pending != 0 || len(transport.mirrors) != 2 {
		t.Fatal(report, transport.mirrors, err)
	}
	for _, mirror := range transport.mirrors {
		if mirror.RegionID == "west" && (mirror.Generation != 2 || mirror.GroupEpoch != 2 || mirror.FenceIndex != 4) {
			t.Fatalf("west mirror was not rebound to the current cohort: %+v", mirror)
		}
	}
	if pending, err := store.List(); err != nil || len(pending) != 0 {
		t.Fatal("rebound mirror debt was not cleared", pending, err)
	}
}
