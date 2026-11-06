package recordrepair

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

type geoRetryTransport struct {
	held      map[uint64]bool
	repairErr error
}

func (t *geoRetryTransport) RepairRecordOnNode(_ context.Context, target, _ rootstate.Node, _ string, d records.Descriptor) (records.Holding, error) {
	if t.repairErr != nil {
		return records.Holding{}, t.repairErr
	}
	t.held[target.RaftID] = true
	id, _ := d.ID()
	return records.Holding{RecordID: id, Descriptor: d, StoreID: "store-remote", VolumeID: "volume-remote"}, nil
}

func (t *geoRetryTransport) FetchRecordOnNode(context.Context, rootstate.Node, rootstate.Node, string, records.Descriptor) (records.Holding, error) {
	if t.repairErr != nil {
		return records.Holding{}, t.repairErr
	}
	return records.Holding{}, context.Canceled
}

func geoRepairReference(t *testing.T) enginestate.RecordReference {
	t.Helper()
	d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "search", ModuleID: "items"},
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

func TestGeoRetryRetiresOnlyConfirmedSupersededPublications(t *testing.T) {
	for _, test := range []struct {
		name    string
		cut     uint64
		err     error
		retired bool
	}{
		{"overwritten-or-deleted", 9, fmt.Errorf("Owner confirmed overwrite: %w", ErrGeoReferenceSuperseded), true},
		{"no-confirmed-Root-cut", 0, ErrGeoReferenceSuperseded, false},
		{"unavailable-Owner", 9, errors.New("Owner unavailable"), false},
		{"authorization-failure", 9, errors.New("Owner authorization failed"), false},
		{"deadline", 9, context.DeadlineExceeded, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store, err := OpenGeoRecordRepairStore(root)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			ref := geoRepairReference(t)
			if err := store.Save(ref); err != nil {
				t.Fatal(err)
			}
			report, retryErr := RetryPendingGeoRecords(context.Background(), store,
				func(context.Context, enginestate.RecordReference) (placement.GeoPlan, string, uint64, error) {
					return placement.GeoPlan{}, "", test.cut, test.err
				}, func(context.Context, rootstate.Node, records.Descriptor) (raftrpc.HoldingObservation, error) {
					t.Fatal("unresolved or superseded work attempted a physical observation")
					return raftrpc.HoldingObservation{}, nil
				}, &geoRetryTransport{})
			if report.Completed != 0 || report.Attempted != 1 {
				t.Fatal("superseded work was counted as physical completion", report)
			}
			if test.retired {
				if retryErr != nil || report.Superseded != 1 || report.Pending != 0 {
					t.Fatal(report, retryErr)
				}
			} else if retryErr == nil || report.Superseded != 0 || report.Pending != 1 {
				t.Fatal("unconfirmed/error debt was discarded", report, retryErr)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = OpenGeoRecordRepairStore(root)
			if err != nil {
				t.Fatal(err)
			}
			refs, err := store.List()
			if err != nil || len(refs) != int(report.Pending) {
				t.Fatal("retirement/debt did not survive restart", refs, err)
			}
		})
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
	observer := func(ctx context.Context, node rootstate.Node, d records.Descriptor) (raftrpc.HoldingObservation, error) {
		if cut, ok := router.ConfirmedRootIndex(ctx); !ok || cut != 9 {
			return raftrpc.HoldingObservation{}, errors.New("confirmed Root cut was not propagated to holding observation")
		}
		if !transport.held[node.RaftID] {
			return raftrpc.HoldingObservation{}, context.Canceled
		}
		id, _ := d.ID()
		return raftrpc.HoldingObservation{RaftID: node.RaftID, NodeID: node.NodeID, RootConfirmedIndex: 9,
			Holding: records.Holding{RecordID: id, Descriptor: d, StoreID: "store-" + node.NodeID, VolumeID: "volume-" + node.NodeID}}, nil
	}
	resolve := func(context.Context, enginestate.RecordReference) (placement.GeoPlan, string, uint64, error) {
		return plan, "west", 9, nil
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
	resolve := func(context.Context, enginestate.RecordReference) (placement.GeoPlan, string, uint64, error) {
		return plan, "west", 9, nil
	}
	report, err := RetryPendingGeoRecords(context.Background(), store, resolve, observer, transport)
	if err != nil || report.Completed != 1 || report.Pending != 0 || !transport.held[7] {
		t.Fatal(report, transport.held, err)
	}
	if refs, err := store.List(); err != nil || len(refs) != 0 {
		t.Fatal(refs, err)
	}
}

func TestRetryPendingGeoRecordsReportsRepairFailureAndKeepsDebt(t *testing.T) {
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
	transport := &geoRetryTransport{held: map[uint64]bool{1: true, 2: true}, repairErr: errors.New("test target unavailable")}
	observer := func(_ context.Context, node rootstate.Node, d records.Descriptor) (raftrpc.HoldingObservation, error) {
		if !transport.held[node.RaftID] {
			return raftrpc.HoldingObservation{}, errors.New("record not found")
		}
		id, _ := d.ID()
		return raftrpc.HoldingObservation{RaftID: node.RaftID, NodeID: node.NodeID, RootConfirmedIndex: 9,
			Holding: records.Holding{RecordID: id, Descriptor: d, StoreID: "store-" + node.NodeID, VolumeID: "volume-" + node.NodeID}}, nil
	}
	report, err := RetryPendingGeoRecords(context.Background(), store,
		func(context.Context, enginestate.RecordReference) (placement.GeoPlan, string, uint64, error) {
			return plan, "east", 9, nil
		},
		observer, transport)
	if err == nil || !strings.Contains(err.Error(), "test target unavailable") || report.Pending != 1 {
		t.Fatalf("report=%+v err=%v; want pending debt and repair failure", report, err)
	}
	if refs, listErr := store.List(); listErr != nil || len(refs) != 1 {
		t.Fatalf("pending repair reference was lost: refs=%d err=%v", len(refs), listErr)
	}
}

func TestObserveGeoRecordProbesNodesConcurrently(t *testing.T) {
	plan := geoRepairPlan(t)
	ref := geoRepairReference(t)
	recordID, err := ref.Descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 6)
	release := make(chan struct{})
	observer := func(_ context.Context, _ rootstate.Node, _ records.Descriptor) (raftrpc.HoldingObservation, error) {
		started <- struct{}{}
		<-release
		return raftrpc.HoldingObservation{}, context.Canceled
	}
	done := make(chan struct{})
	go func() {
		_, _, _, _ = observeGeoRecord(context.Background(), plan, ref.Descriptor, recordID, observer)
		close(done)
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			<-done
			t.Fatal("Geo holding observations were serialized")
		}
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Geo holding observations did not finish")
	}
}

func TestObserveGeoRecordReturnsContextDeadline(t *testing.T) {
	plan := geoRepairPlan(t)
	ref := geoRepairReference(t)
	recordID, err := ref.Descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, err = observeGeoRecord(ctx, plan, ref.Descriptor, recordID,
		func(ctx context.Context, _ rootstate.Node, _ records.Descriptor) (raftrpc.HoldingObservation, error) {
			return raftrpc.HoldingObservation{}, ctx.Err()
		})
	if err != context.Canceled {
		t.Fatalf("canceled observation error = %v, want context.Canceled", err)
	}
}

func TestGeoRepairSourceForOtherRegionExcludesTargetRegion(t *testing.T) {
	plan := geoRepairPlan(t)
	nodes := make(map[uint64]rootstate.Node)
	for _, region := range plan.Regions() {
		for _, node := range region.Nodes() {
			nodes[node.RaftID] = node
		}
	}
	// Deliberately put a holding from the target Region first. Concurrent
	// observations must not make the selected source violate cross-Region repair.
	holdings := []placement.RegionalHolding{
		{RegionID: "east", Holding: placement.Holding{RaftID: 4, NodeID: nodes[4].NodeID}},
		{RegionID: "west", Holding: placement.Holding{RaftID: 1, NodeID: nodes[1].NodeID}},
	}
	source, found := geoRepairSource(placement.RepairTarget{Scope: placement.RepairInOtherRegion, RegionID: "east"}, holdings, nodes)
	if !found || source.RegionID != "west" || source.RaftID != 1 {
		t.Fatalf("source=%+v found=%t; want a source outside target Region east", source, found)
	}
}
