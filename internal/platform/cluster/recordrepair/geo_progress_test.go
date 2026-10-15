package recordrepair

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
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

func geoRepairReference(t *testing.T) capstate.RecordReference {
	t.Helper()
	d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", CapabilityID: "search", DatasetID: "items"},
		"region-west", []byte("geo-repair"), []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	return capstate.RecordReference{Version: 2, Descriptor: d, OwnerGroupID: "region-west", RangeID: "range-a",
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
	resolve := func(context.Context, capstate.RecordReference) (placement.GeoPlan, string, error) {
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
