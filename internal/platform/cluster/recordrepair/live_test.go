package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type repairSourceFake struct {
	nodes        []rootstate.Node
	body         []byte
	observeErr   map[uint64]error
	fetchErr     map[uint64]error
	blockFetch   map[uint64]bool
	spoofNodeID  map[uint64]string
	observed     []uint64
	fetchedFrom  []uint64
	repairedFrom []uint64
}

type remoteRepairFake struct {
	*repairSourceFake
	remoteTargets []uint64
}

func (f *remoteRepairFake) RepairRecordOnNode(_ context.Context, target, _ rootstate.Node, _ string, d records.Descriptor) (records.Holding, error) {
	f.remoteTargets = append(f.remoteTargets, target.RaftID)
	id, err := d.ID()
	if err != nil {
		return records.Holding{}, err
	}
	return records.Holding{RecordID: id, Descriptor: d, StoreID: "store_remote", VolumeID: "volume_remote"}, nil
}

func (f *remoteRepairFake) FetchRecordOnNode(context.Context, rootstate.Node, rootstate.Node, string, records.Descriptor) (records.Holding, error) {
	return records.Holding{}, errors.New("fetch fallback should not be needed")
}

func (f *repairSourceFake) ObserveRecordHolding(_ context.Context, raftID uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	f.observed = append(f.observed, raftID)
	if err := f.observeErr[raftID]; err != nil {
		return raftrpc.HoldingObservation{}, err
	}
	id, err := d.ID()
	if err != nil {
		return raftrpc.HoldingObservation{}, err
	}
	for _, node := range f.nodes {
		if node.RaftID == raftID {
			if claimed := f.spoofNodeID[raftID]; claimed != "" {
				node.NodeID = claimed
			}
			return raftrpc.HoldingObservation{RaftID: raftID, NodeID: node.NodeID, RootConfirmedIndex: 5,
				Holding: records.Holding{Descriptor: d, RecordID: id,
					StoreID: fmt.Sprintf("store_%032x", raftID)}}, nil
		}
	}
	return raftrpc.HoldingObservation{}, errors.New("unknown test Node")
}

func (f *repairSourceFake) FetchRecord(ctx context.Context, source rootstate.Node, d records.Descriptor,
	destination *records.Store) (raftrpc.RecordFetchResult, error) {
	f.fetchedFrom = append(f.fetchedFrom, source.RaftID)
	if f.blockFetch[source.RaftID] {
		<-ctx.Done()
		return raftrpc.RecordFetchResult{}, ctx.Err()
	}
	if err := f.fetchErr[source.RaftID]; err != nil {
		return raftrpc.RecordFetchResult{}, err
	}
	_, err := destination.Put(ctx, d, f.body)
	return raftrpc.RecordFetchResult{}, err
}

func (f *repairSourceFake) RepairRecord(ctx context.Context, source rootstate.Node, d records.Descriptor,
	destination *records.Store) (raftrpc.RecordFetchResult, error) {
	f.repairedFrom = append(f.repairedFrom, source.RaftID)
	if f.blockFetch[source.RaftID] {
		<-ctx.Done()
		return raftrpc.RecordFetchResult{}, ctx.Err()
	}
	if err := f.fetchErr[source.RaftID]; err != nil {
		return raftrpc.RecordFetchResult{}, err
	}
	_, err := destination.RepairExisting(ctx, d, f.body)
	return raftrpc.RecordFetchResult{}, err
}

func TestRemoteRepairUsesAnotherCurrentCohortNode(t *testing.T) {
	plan := repairTestPlan(t)
	d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "search", DatasetID: "items"},
		"origin-group", []byte("remote-key"), []byte("remote-body"))
	if err != nil {
		t.Fatal(err)
	}
	fake := &remoteRepairFake{repairSourceFake: &repairSourceFake{nodes: plan.Nodes()}}
	if err := remoteRepair(context.Background(), plan, 1, plan.Nodes()[1], d.GroupID, d, fake); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fake.remoteTargets, []uint64{3}) {
		t.Fatal("remote repair selected the wrong target", fake.remoteTargets)
	}
}

func repairTestStore(t *testing.T) *records.Store {
	t.Helper()
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	volumes, err := volume.Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = volumes.Close() })
	packs, err := pack.OpenPool(volumes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = packs.Close() })
	store, err := records.OpenNodeStore(packs, volumes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func repairTestPlan(t *testing.T) placement.LocalPlan {
	t.Helper()
	nodes := []rootstate.Node{
		{RaftID: 1, NodeID: "node_00000000000000000000000000000001", RegionID: "west", Address: "n1:7443"},
		{RaftID: 2, NodeID: "node_00000000000000000000000000000002", RegionID: "west", Address: "n2:7443"},
		{RaftID: 3, NodeID: "node_00000000000000000000000000000003", RegionID: "west", Address: "n3:7443"},
	}
	cohort := rootstate.RegionCohort{RegionID: "west", GroupID: "region-west", Generation: 1,
		Nodes: []uint64{1, 2, 3}, State: rootstate.RegionCohortConfirmed,
		PlannedIndex: 1, ConfirmedIndex: 5, GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 4}
	progress := rootstate.RegionProgress{Eligible: true,
		Head: rootstate.RegionCohortHead{RegionID: "west", CurrentGeneration: 1}, Current: cohort,
		Group: rootstate.Group{GroupID: "region-west", Epoch: 2, Members: []rootstate.GroupMember{
			{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter},
			{RaftID: 3, Role: rootstate.GroupMemberVoter},
		}}}
	plan, err := placement.PlanLocal(progress, nodes)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestRepairReferenceFallbackAndRetryWithoutOverwritingLocalDamage(t *testing.T) {
	plan := repairTestPlan(t)
	store := repairTestStore(t)
	body := []byte("repair-from-another-confirmed-Node")
	d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "search", DatasetID: "items"},
		"origin-group", []byte("key"), body)
	if err != nil {
		t.Fatal(err)
	}
	ref := enginestate.RecordReference{Descriptor: d}
	fake := &repairSourceFake{nodes: plan.Nodes(), body: body,
		observeErr: map[uint64]error{2: status.Error(codes.NotFound, "source lost bytes")}}
	fetched, err := repairReference(context.Background(), plan, ref, 1, store, fake)
	if err != nil || !fetched || !slices.Equal(fake.observed, []uint64{2, 3}) ||
		!slices.Equal(fake.fetchedFrom, []uint64{3}) {
		t.Fatal("repair did not try the next confirmed source", fetched, fake, err)
	}
	if _, err := store.Verify(context.Background(), d); err != nil {
		t.Fatal("fallback did not persist verified local Pack bytes", err)
	}
	if fetched, err := repairReference(context.Background(), plan, ref, 1, store, fake); err != nil || fetched || len(fake.observed) != 2 {
		t.Fatal("already held bytes triggered redundant remote repair", fetched, fake, err)
	}
	// A failed first source fetch is also retryable against the next Node.
	d2, err := records.Describe(d.Keyspace, d.GroupID, []byte("second"), body)
	if err != nil {
		t.Fatal(err)
	}
	fake.observeErr = nil
	fake.fetchErr = map[uint64]error{2: status.Error(codes.DataLoss, "source Pack damaged")}
	if fetched, err := repairReference(context.Background(), plan, enginestate.RecordReference{Descriptor: d2}, 1, store, fake); err != nil || !fetched ||
		!slices.Equal(fake.fetchedFrom, []uint64{3, 2, 3}) {
		t.Fatal("corrupt source prevented alternate source repair", fetched, fake, err)
	}
	// If all sources are unavailable, retry later; never fabricate a holding.
	d3, err := records.Describe(d.Keyspace, d.GroupID, []byte("third"), body)
	if err != nil {
		t.Fatal(err)
	}
	fake.observeErr = map[uint64]error{2: status.Error(codes.Unavailable, "offline"), 3: status.Error(codes.NotFound, "missing")}
	if fetched, err := repairReference(context.Background(), plan, enginestate.RecordReference{Descriptor: d3}, 1, store, fake); err == nil || fetched {
		t.Fatal("all sources missing was counted as repaired", fetched, err)
	}
	if _, err := store.Verify(context.Background(), d3); !errors.Is(err, records.ErrNotHeld) {
		t.Fatal("failed repair fabricated local data", err)
	}
	if err := processRepairReference(context.Background(), plan,
		enginestate.RecordReference{RequestID: "missing", Descriptor: d3}, 1, store, fake, 0, &LivePageReport{}); err == nil {
		t.Fatal("one-shot full-cut repair falsely completed over missing data")
	}
	page := LivePageReport{}
	if err := processRepairReference(context.Background(), plan,
		enginestate.RecordReference{RequestID: "missing", Descriptor: d3}, 1, store, fake, 2, &page); err != nil {
		t.Fatal("bounded page must retain retryable debt and advance", err)
	}
	fake.observeErr = nil
	d4, err := records.Describe(d.Keyspace, d.GroupID, []byte("fourth"), body)
	if err != nil {
		t.Fatal(err)
	}
	if err := processRepairReference(context.Background(), plan,
		enginestate.RecordReference{RequestID: "later", Descriptor: d4}, 1, store, fake, 2, &page); err != nil ||
		page.Unrepaired != 1 || page.Fetched != 1 || len(page.Failures) != 1 || len(page.References) != 1 ||
		page.References[0].RequestID != "later" {
		t.Fatal("failed first reference starved a later repair in one bounded page", page, err)
	}
	fake.observeErr = nil
	fake.spoofNodeID = map[uint64]string{2: "node_000000000000000000000000000000ff"}
	beforeFetch := len(fake.fetchedFrom)
	if fetched, err := repairReference(context.Background(), plan, enginestate.RecordReference{Descriptor: d3}, 1, store, fake); err == nil || fetched || len(fake.fetchedFrom) != beforeFetch {
		t.Fatal("source outside confirmed Node identity was used", fetched, fake, err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	page = LivePageReport{}
	if err := processRepairReference(cancelled, plan,
		enginestate.RecordReference{RequestID: "cancelled", Descriptor: d3}, 1, store, fake, 2, &page); err == nil || page.Unrepaired != 0 {
		t.Fatal("cancellation was mistaken for retryable page debt", page, err)
	}
	// A damaged/closed local Store is not the same as a missing record and must
	// never be overwritten from a remote source without an explicit recovery.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	fake.observeErr = nil
	fake.spoofNodeID = nil
	before := len(fake.observed)
	if fetched, err := repairReference(context.Background(), plan, enginestate.RecordReference{Descriptor: d3}, 1, store, fake); err == nil || fetched || len(fake.observed) != before {
		t.Fatal("unusable local Store was silently repaired", fetched, fake, err)
	}
}

func TestRepairReferenceRelocatesLostPackFromConfirmedSource(t *testing.T) {
	plan := repairTestPlan(t)
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	volumes, err := volume.Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = volumes.Close() })
	packs, err := pack.OpenPool(volumes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = packs.Close() })
	store, err := records.OpenNodeStore(packs, volumes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	body := []byte("recovered-from-confirmed-source")
	d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "search", DatasetID: "items"},
		"origin-group", []byte("lost-key"), body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), d, body); err != nil {
		t.Fatal(err)
	}
	oldPath := filepath.Join(root, "volume")
	if err := os.Rename(oldPath, oldPath+".offline"); err != nil {
		t.Fatal(err)
	}
	replacement := t.TempDir() // Explicit added Volumes cannot overlap Store metadata.
	if _, err := volumes.Add(replacement); err != nil {
		t.Fatal(err)
	}
	fake := &repairSourceFake{nodes: plan.Nodes(), body: body,
		observeErr: map[uint64]error{2: status.Error(codes.Unavailable, "offline"), 3: status.Error(codes.NotFound, "not held")}}
	if repaired, err := repairReference(context.Background(), plan, enginestate.RecordReference{Descriptor: d}, 1, store, fake); err == nil || repaired || len(fake.repairedFrom) != 0 {
		t.Fatal("unavailable confirmed sources fabricated a replacement", repaired, fake, err)
	}
	if _, err := store.Verify(context.Background(), d); !errors.Is(err, records.ErrUnavailable) {
		t.Fatal("unverified repair changed the old locator", err)
	}
	fake.observeErr = nil
	fetched, err := repairReference(context.Background(), plan, enginestate.RecordReference{Descriptor: d}, 1, store, fake)
	if err != nil || !fetched || !slices.Equal(fake.repairedFrom, []uint64{2}) || len(fake.fetchedFrom) != 0 {
		t.Fatal("confirmed source did not repair the existing unreadable location", fetched, fake, err)
	}
	if _, err := store.Verify(context.Background(), d); err != nil {
		t.Fatal("relocated record was not physically held", err)
	}
}

func TestRepairSourceUnavailableIsRetryableButAuthorityFailureIsNot(t *testing.T) {
	for _, tc := range []struct {
		err       error
		retryable bool
	}{
		{records.ErrUnavailable, true},
		{status.Error(codes.Unavailable, "source Store unavailable"), true},
		{status.Error(codes.FailedPrecondition, "source registration changed"), false},
		{status.Error(codes.PermissionDenied, "source identity changed"), false},
	} {
		if got := retryableRecordSource(tc.err); got != tc.retryable {
			t.Fatalf("retryableRecordSource(%v) = %t, want %t", tc.err, got, tc.retryable)
		}
	}
}

func TestInterruptedRepairRetriesTheSameReferenceFromConfirmedSources(t *testing.T) {
	plan := repairTestPlan(t)
	store := repairTestStore(t)
	body := []byte("repair-after-an-interrupted-source-stream")
	d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "search", DatasetID: "items"},
		"origin-group", []byte("interrupted"), body)
	if err != nil {
		t.Fatal(err)
	}
	ref := enginestate.RecordReference{RequestID: "interrupted-repair", Descriptor: d}
	fake := &repairSourceFake{nodes: plan.Nodes(), body: body,
		fetchErr: map[uint64]error{
			2: status.Error(codes.Unavailable, "stream interrupted"),
			3: status.Error(codes.DeadlineExceeded, "source timed out"),
		}}
	first := LivePageReport{}
	if err := processRepairReference(context.Background(), plan, ref, 1, store, fake, 1, &first); err != nil ||
		first.Unrepaired != 1 || first.Fetched != 0 || len(first.References) != 0 ||
		!slices.Equal(fake.fetchedFrom, []uint64{2, 3}) {
		t.Fatal("interrupted pass did not retain bounded repair debt", first, fake.fetchedFrom, err)
	}
	if _, err := store.Verify(context.Background(), d); !errors.Is(err, records.ErrNotHeld) {
		t.Fatal("interrupted pass fabricated local bytes", err)
	}
	delete(fake.fetchErr, 3)
	second := LivePageReport{}
	if err := processRepairReference(context.Background(), plan, ref, 1, store, fake, 1, &second); err != nil ||
		second.Unrepaired != 0 || second.Fetched != 1 || len(second.References) != 1 ||
		!slices.Equal(fake.fetchedFrom, []uint64{2, 3, 2, 3}) {
		t.Fatal("retry did not use the surviving confirmed source", second, fake.fetchedFrom, err)
	}
	if _, err := store.Verify(context.Background(), d); err != nil {
		t.Fatal("retry did not persist verified Pack bytes", err)
	}
}

func TestLiveRepairLeavesTimeForFallbackWhenFirstSourceStalls(t *testing.T) {
	plan := repairTestPlan(t)
	store := repairTestStore(t)
	body := []byte("fallback-after-stalled-source")
	d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "search", DatasetID: "items"},
		"origin-group", []byte("stalled"), body)
	if err != nil {
		t.Fatal(err)
	}
	fake := &repairSourceFake{nodes: plan.Nodes(), body: body, blockFetch: map[uint64]bool{2: true}}
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	fetched, err := repairReference(ctx, plan, enginestate.RecordReference{Descriptor: d}, 1, store, fake)
	if err != nil || !fetched || ctx.Err() != nil || !slices.Equal(fake.fetchedFrom, []uint64{2, 3}) {
		t.Fatal("stalled source consumed the whole repair round", fetched, fake.fetchedFrom, ctx.Err(), err)
	}
	if _, err := store.Verify(ctx, d); err != nil {
		t.Fatal("fallback bytes were not held locally", err)
	}
}
