package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

func TestEmptyGeoMirrorRetryDoesNotWaitForRoot(t *testing.T) {
	ledger, err := recordwrite.OpenGeoMetadataRetryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	worker := &geoMetadataRepair{runtime: &node.Runtime{}, client: &raftrpc.GroupClient{}, store: ledger}
	// This runtime cannot obtain a Root quorum. An empty durable queue must
	// still finish without touching it, allowing record discovery to proceed.
	report, err := worker.step(context.Background())
	if err != nil || report.Attempted != 0 || report.Completed != 0 || report.Pending != 0 {
		t.Fatal("empty retry queue attempted remote authority", report, err)
	}
	key := []byte("key")
	target := router.Target{Keyspace: rootstate.Keyspace{TenantID: "tenant", EngineID: "record", ModuleID: "items"},
		RangeID: "range-items", OwnerGroupID: "region-west", Epoch: 1, RoutingKey: key}
	descriptor, err := records.Describe(target.Keyspace, "origin-group", key, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	id, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	mirror := routedstate.GeoMetadataMirror{Target: target, Key: key, DestinationGroupID: "region-west",
		RegionID: "west", Generation: 1, GroupEpoch: 2, FenceIndex: 4, RecordID: id,
		ReferenceRequestID: "publication", ReferenceIndex: 7, ReferenceCommandHash: strings.Repeat("0", 64), Descriptor: descriptor}
	if err := ledger.Save(mirror); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.step(context.Background()); err == nil {
		t.Fatal("nonempty queue skipped Root authorization")
	}
	if count, err := ledger.Count(); err != nil || count != 1 {
		t.Fatal("failed Root authorization lost durable mirror debt", count, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := worker.step(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("empty queue ignored caller cancellation", err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.step(context.Background()); err == nil {
		t.Fatal("unreadable retry ledger was treated as empty")
	}
}

func TestGeoDiscoveryIncludesFirstReferenceOfEveryRangeAndPage(t *testing.T) {
	view := rootstate.RegionPlacementView{AppliedIndex: 7}
	for regionIndex, region := range []string{"east", "west"} {
		group := "region-" + region
		var nodes []rootstate.Node
		var ids []uint64
		var members []rootstate.GroupMember
		for i := 1; i <= 3; i++ {
			id := uint64(regionIndex*3 + i)
			nodes = append(nodes, rootstate.Node{RaftID: id, NodeID: fmt.Sprintf("node_%032x", id), RegionID: region, Address: fmt.Sprintf("n%d:12443", id)})
			ids = append(ids, id)
			members = append(members, rootstate.GroupMember{RaftID: id, Role: rootstate.GroupMemberVoter})
		}
		progress := rootstate.RegionProgress{AppliedIndex: 7, Eligible: true,
			Head: rootstate.RegionCohortHead{RegionID: region, CurrentGeneration: 1},
			Current: rootstate.RegionCohort{RegionID: region, GroupID: group, Generation: 1, Nodes: ids,
				State: rootstate.RegionCohortConfirmed, PlannedIndex: 1, ConfirmedIndex: 5,
				GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 4},
			Group: rootstate.Group{GroupID: group, Epoch: 2, Members: members}}
		view.Regions = append(view.Regions, rootstate.RegionPlacement{Progress: progress, Nodes: nodes})
	}
	confirmed, err := placement.PlanGeo(view)
	if err != nil {
		t.Fatal(err)
	}
	for page := 0; page < 2; page++ {
		plans := make(map[string]placement.GeoPlan)
		calls, visited := 0, 0
		for _, rangeID := range []string{"range-a", "range-a", "range-b", "range-b"} {
			plan, err := cachedGeoDiscoveryPlan(plans, rangeID, func() (placement.GeoPlan, error) {
				calls++
				return confirmed, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Regions()) >= 2 {
				visited++
			}
		}
		if visited != 4 || calls != 2 {
			t.Fatalf("page %d skipped first references: eligible=%d, Root resolutions=%d", page, visited, calls)
		}
	}
	plans := make(map[string]placement.GeoPlan)
	if _, err := cachedGeoDiscoveryPlan(plans, "range-a", func() (placement.GeoPlan, error) {
		return placement.GeoPlan{}, errors.New("Root placement temporarily unavailable")
	}); err == nil || len(plans) != 0 {
		t.Fatal("failed discovery was cached as a successful plan", err)
	}
	if plan, err := cachedGeoDiscoveryPlan(plans, "range-a", func() (placement.GeoPlan, error) {
		return confirmed, nil
	}); err != nil || len(plan.Regions()) != 2 {
		t.Fatal("discovery did not recover after the Root lookup succeeded", plan, err)
	}
}

func TestGeoRecordNeedsRepairWhenCoverageReceiptIsMissing(t *testing.T) {
	if !geoRecordNeedsRepair(false, enginestate.GeoCoverageRecord{}, enginestate.RecordReference{},
		router.Target{}, placement.GeoPlan{}) {
		t.Fatal("local-majority record without a Geo receipt was not queued for background repair")
	}
}

func TestGeoDiscoveryContinuesOnlyAfterAdvancingAPage(t *testing.T) {
	worker := &geoMetadataRepair{}
	if worker.nextDelay() != geoMetadataRepairInterval {
		t.Fatal("idle discovery lost its normal polling interval")
	}
	worker.geoCursors = map[string][]byte{"region-west": []byte("unfinished")}
	if worker.nextDelay() != geoMetadataRepairInterval {
		t.Fatal("an old cursor alone caused fast retries without progress")
	}
	worker.discoveryAdvanced = true
	if worker.nextDelay() != geoDiscoveryContinuationDelay {
		t.Fatal("a successful unfinished page waited for the idle interval")
	}
	// A round that cannot start discovery must not retain the previous delay.
	_, _ = worker.stepRecords(nil)
	if worker.nextDelay() != geoMetadataRepairInterval {
		t.Fatal("failed discovery retained the fast continuation delay")
	}
}

func TestGeoRecordNeedsRepairWhenCoverageReceiptIsStale(t *testing.T) {
	if !geoRecordNeedsRepair(true, enginestate.GeoCoverageRecord{}, enginestate.RecordReference{},
		router.Target{}, placement.GeoPlan{}) {
		t.Fatal("stale Geo receipt was not queued for background repair")
	}
}

// A physical retry may already have retired its byte hint before metadata
// reconfirmation discovers an unavailable Owner. That failure must survive a
// restart instead of disappearing until the next full discovery sweep.
func TestGeoReconfirmationRestoresDurableHintsWhenOwnerIsUnavailable(t *testing.T) {
	path := t.TempDir()
	ledger, err := recordrepair.OpenGeoRecordRepairStore(path)
	if err != nil {
		t.Fatal(err)
	}
	var refs []enginestate.RecordReference
	for i := 0; i < 3; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "record", ModuleID: "items"}, "region-west", key, []byte("body"))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, enginestate.RecordReference{Version: 2, Descriptor: d, OwnerGroupID: "region-west", RangeID: "range-items", RangeEpoch: 1, RoutingKey: key, RequestID: fmt.Sprintf("publication-%d", i), Index: 7, CommandHash: strings.Repeat("0", 64)})
	}
	worker := &geoMetadataRepair{runtime: &node.Runtime{}, records: ledger}
	pending, err := worker.reconfirmStaleGeoRecords(context.Background(), refs)
	if err == nil || len(pending) != len(refs) {
		t.Fatal("unavailable Owner lost pending references", pending, err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	ledger, err = recordrepair.OpenGeoRecordRepairStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	kept, err := ledger.List()
	if err != nil || len(kept) != len(refs) {
		t.Fatal("reconfirmation debt did not survive restart", kept, err)
	}
}
