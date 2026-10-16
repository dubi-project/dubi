package recordrepair

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

type geoRetryTransport struct {
	held map[uint64]bool
}

func (t *geoRetryTransport) RepairRecordOnNode(_ context.Context, target, _ rootstate.Node, _ string, d records.Descriptor) (records.Holding, error) {
	t.held[target.RaftID] = true
	id, _ := d.ID()
	return records.Holding{RecordID: id, Descriptor: d, StoreID: "store-remote", VolumeID: "volume-remote"}, nil
}

func (t *geoRetryTransport) FetchRecordOnNode(context.Context, rootstate.Node, rootstate.Node, string, records.Descriptor) (records.Holding, error) {
	return records.Holding{}, context.Canceled
}

func geoRepairReference(t *testing.T) enginestate.RecordReference {
	t.Helper()
	d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "search", DatasetID: "items"},
		"region-west", []byte("geo-repair"), []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	return enginestate.RecordReference{Version: 2, Descriptor: d, OwnerGroupID: "region-west", RangeID: "range-a",
		RangeEpoch: 3, RoutingKey: []byte("geo-repair"), RequestID: "geo-repair-request", Index: 7,
		CommandHash: hex.EncodeToString(make([]byte, 32))}
}

func TestGeoRecordRepairStorePersistsAndDeletesReferenceCut(t *testing.T) {
	store, err := OpenGeoRecordRepairStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ref := geoRepairReference(t)
	if err := store.Save(ref); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ref); err != nil {
		t.Fatal("idempotent task save failed", err)
	}
	items, err := store.List()
	if err != nil || len(items) != 1 || items[0].RequestID != ref.RequestID || items[0].Descriptor.Keyspace != ref.Descriptor.Keyspace {
		t.Fatal(items, err)
	}
	if err := store.Delete(ref); err != nil {
		t.Fatal(err)
	}
	items, err = store.List()
	if err != nil || len(items) != 0 {
		t.Fatal(items, err)
	}
}

func TestGeoRecordRepairStoreRejectsInvalidReference(t *testing.T) {
	store, err := OpenGeoRecordRepairStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ref := geoRepairReference(t)
	ref.RangeEpoch = 0
	if err := store.Save(ref); err == nil {
		t.Fatal("invalid reference was accepted")
	}
}

func TestRetryPendingGeoRecordsDeletesOnlyAfterFreshGeoMajority(t *testing.T) {
	store, err := OpenGeoRecordRepairStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ref := geoRepairReference(t)
	if err := store.Save(ref); err != nil {
		t.Fatal(err)
	}
	plan := geoRepairPlan(t)
	transport := &geoRetryTransport{held: map[uint64]bool{1: true, 2: true}}
	observer := func(_ context.Context, node rootstate.Node, d records.Descriptor) (raftrpc.HoldingObservation, error) {
		if !transport.held[node.RaftID] {
			return raftrpc.HoldingObservation{}, context.Canceled
		}
		id, _ := d.ID()
		return raftrpc.HoldingObservation{RaftID: node.RaftID, NodeID: node.NodeID, RootConfirmedIndex: 9,
			Holding: records.Holding{RecordID: id, Descriptor: d, StoreID: "store-" + node.NodeID, VolumeID: "volume-" + node.NodeID}}, nil
	}
	resolve := func(context.Context, enginestate.RecordReference) (placement.GeoPlan, string, error) {
		return plan, "west", nil
	}
	report, err := RetryPendingGeoRecords(context.Background(), store, resolve, observer, transport)
	if err != nil || report.Completed != 1 || report.Pending != 0 || !transport.held[4] || !transport.held[5] {
		t.Fatal(report, transport.held, err)
	}
	if refs, err := store.List(); err != nil || len(refs) != 0 {
		t.Fatal(refs, err)
	}
}

func TestRetryPendingGeoRecordsRepairsReformedSingleNodeRegion(t *testing.T) {
	store, err := OpenGeoRecordRepairStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ref := geoRepairReference(t)
	if err := store.Save(ref); err != nil {
		t.Fatal(err)
	}

	// The west Region has been explicitly retired and formed again with one
	// fresh Node. Its Region name and deterministic GroupID are unchanged, but
	// the confirmed generation and physical Node set are new. East still has a
	// local majority and is the only available source for the missing west copy.
	progress := func(region, group string, generation uint64, reformation bool, ids []uint64) rootstate.RegionProgress {
		members := make([]rootstate.GroupMember, 0, len(ids))
		for _, id := range ids {
			members = append(members, rootstate.GroupMember{RaftID: id, Role: rootstate.GroupMemberVoter})
		}
		return rootstate.RegionProgress{AppliedIndex: 7, Eligible: true,
			Head: rootstate.RegionCohortHead{RegionID: region, CurrentGeneration: generation},
			Current: rootstate.RegionCohort{RegionID: region, GroupID: group, Generation: generation, Reformation: reformation, Nodes: ids,
				State: rootstate.RegionCohortConfirmed, PlannedIndex: 1, ConfirmedIndex: 5,
				GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 4},
			Group: rootstate.Group{GroupID: group, Epoch: 2, Members: members}}
	}
	west := []rootstate.Node{{RaftID: 7, NodeID: "node_00000000000000000000000000000007", RegionID: "west", Address: "w-new"}}
	east := []rootstate.Node{
		{RaftID: 4, NodeID: "node_00000000000000000000000000000004", RegionID: "east", Address: "e1"},
		{RaftID: 5, NodeID: "node_00000000000000000000000000000005", RegionID: "east", Address: "e2"},
		{RaftID: 6, NodeID: "node_00000000000000000000000000000006", RegionID: "east", Address: "e3"},
	}
	view := rootstate.RegionPlacementView{AppliedIndex: 7, Regions: []rootstate.RegionPlacement{
		{Progress: progress("west", "region-west", 2, true, []uint64{7}), Nodes: west},
		{Progress: progress("east", "region-east", 1, false, []uint64{4, 5, 6}), Nodes: east},
	}}
	plan, err := placement.PlanGeo(view)
	if err != nil {
		t.Fatal(err)
	}
	transport := &geoRetryTransport{held: map[uint64]bool{4: true, 5: true}}
	observer := func(_ context.Context, node rootstate.Node, d records.Descriptor) (raftrpc.HoldingObservation, error) {
		if !transport.held[node.RaftID] {
			return raftrpc.HoldingObservation{}, context.Canceled
		}
		id, _ := d.ID()
		return raftrpc.HoldingObservation{RaftID: node.RaftID, NodeID: node.NodeID, RootConfirmedIndex: 9,
			Holding: records.Holding{RecordID: id, Descriptor: d, StoreID: "store-" + node.NodeID, VolumeID: "volume-" + node.NodeID}}, nil
	}
	resolve := func(context.Context, enginestate.RecordReference) (placement.GeoPlan, string, error) {
		return plan, "west", nil
	}
	report, err := RetryPendingGeoRecords(context.Background(), store, resolve, observer, transport)
	if err != nil || report.Completed != 1 || report.Pending != 0 || !transport.held[7] {
		t.Fatal(report, transport.held, err)
	}
	if refs, err := store.List(); err != nil || len(refs) != 0 {
		t.Fatal(refs, err)
	}
}
