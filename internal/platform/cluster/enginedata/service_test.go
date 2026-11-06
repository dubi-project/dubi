package enginedata

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	etcdraft "go.etcd.io/raft/v3"
)

func localGeoHintFixture(t *testing.T) recordwrite.LocalWrite {
	t.Helper()
	descriptor, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "search", ModuleID: "documents"},
		"region-west", []byte("document"), []byte("opaque engine bytes"))
	if err != nil {
		t.Fatal(err)
	}
	return recordwrite.LocalWrite{Reference: enginestate.RecordReference{Version: 2, Descriptor: descriptor,
		OwnerGroupID: "region-west", RangeID: "range-documents", RangeEpoch: 1, RoutingKey: descriptor.Key,
		RequestID: "publish-document", Index: 7, CommandHash: hex.EncodeToString(make([]byte, 32))},
		ConfirmationIndex: 8, RegionGeneration: 1}
}

func TestLocalReadyPersistsGeoHintBeforeReturningAndSurvivesReopen(t *testing.T) {
	root := t.TempDir()
	ledger, err := recordrepair.OpenGeoRecordRepairStore(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if ledger != nil {
			_ = ledger.Close()
		}
	})
	service := &Service{}
	service.SetGeoRetryStores(nil, ledger)
	ready := localGeoHintFixture(t)
	for attempt := 0; attempt < 2; attempt++ {
		result, err := service.completeLocalWrite(ready, true)
		if err != nil || result.State != string(WriteLocalReady) || !reflect.DeepEqual(result.Reference, ready.Reference) ||
			result.ConfirmationIndex != ready.ConfirmationIndex {
			t.Fatal("local confirmation changed while scheduling Geo repair", result, err)
		}
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	ledger, err = recordrepair.OpenGeoRecordRepairStore(root)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := ledger.List()
	if err != nil || len(pending) != 1 || !reflect.DeepEqual(pending[0], ready.Reference) {
		t.Fatal("successful/retried local write did not leave one durable module-neutral hint", pending, err)
	}
}

type geoHintSinkFailure struct {
	err   error
	calls int
}

func (sink *geoHintSinkFailure) Save(enginestate.RecordReference) error {
	sink.calls++
	return sink.err
}

func TestLocalReadyDoesNotAcknowledgeAnUnpersistedGeoHint(t *testing.T) {
	failure := errors.New("Geo ledger has no writable capacity")
	sink := &geoHintSinkFailure{err: failure}
	service := &Service{}
	service.SetGeoRetryStores(nil, sink)
	ready := localGeoHintFixture(t)
	result, err := service.completeLocalWrite(ready, true)
	if !errors.Is(err, failure) || sink.calls != 1 || !reflect.DeepEqual(result.Reference, ready.Reference) {
		t.Fatal("lost hint was acknowledged or the committed publication was hidden", result, err, sink.calls)
	}
	// A genuinely single-Region write does not acquire unnecessary Geo debt.
	if _, err := service.completeLocalWrite(ready, false); err != nil || sink.calls != 1 {
		t.Fatal("single-Region write touched the Geo queue", err, sink.calls)
	}
}

func TestIncompleteEngineDataEntryFailsClosed(t *testing.T) {
	if service, err := New(nil, nil); err == nil || service != nil {
		t.Fatal("incomplete Node constructed a engine data entry", service, err)
	}
	space := rootstate.Keyspace{TenantID: "tenant", EngineID: "example", ModuleID: "data"}
	var incomplete Service
	if result, err := incomplete.WriteLocal(context.Background(), WriteRequest{Keyspace: space,
		Key: []byte("key"), Publish: func(records.Descriptor) ([]byte, error) { return nil, nil }}); err == nil || result.State != WriteUnknown {
		t.Fatal("incomplete entry accepted a write", result, err)
	}
	if result, err := incomplete.ReadLocal(context.Background(), space, []byte("key")); err == nil || len(result.Body) != 0 {
		t.Fatal("incomplete entry accepted a read", result, err)
	}
}

type ownerReadBarrierLocalFake struct {
	index   uint64
	err     error
	status  raft.GroupStatus
	blocked bool
}

func (fake ownerReadBarrierLocalFake) ReadIndex(ctx context.Context, _ string) (uint64, error) {
	if fake.blocked {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return fake.index, fake.err
}

func (fake ownerReadBarrierLocalFake) Status(string) (raft.GroupStatus, error) {
	return fake.status, nil
}

type ownerReadBarrierClientFake struct {
	candidates []uint64
	indices    map[uint64]uint64
	errors     map[uint64]error
	calls      []uint64
}

func (fake *ownerReadBarrierClientFake) GroupCandidates(string) []uint64 {
	return append([]uint64(nil), fake.candidates...)
}

func (fake *ownerReadBarrierClientFake) ReadIndex(_ context.Context, target uint64, _ string) (uint64, error) {
	fake.calls = append(fake.calls, target)
	return fake.indices[target], fake.errors[target]
}

func TestReadOwnerGroupIndexUsesLocalBarrierFirst(t *testing.T) {
	client := &ownerReadBarrierClientFake{candidates: []uint64{11, 12}}
	index, err := readOwnerGroupIndex(context.Background(), ownerReadBarrierLocalFake{index: 7,
		status: raft.GroupStatus{RaftID: 10, LeaderID: 10, Role: etcdraft.StateLeader}}, client, 10, "region-a")
	if err != nil || index != 7 || len(client.calls) != 0 {
		t.Fatalf("local Owner read = %d, calls=%v, err=%v", index, client.calls, err)
	}
}

func TestReadOwnerGroupIndexFallsBackAfterStalledLocalRead(t *testing.T) {
	client := &ownerReadBarrierClientFake{candidates: []uint64{11, 12, 13},
		indices: map[uint64]uint64{12: 19}}
	local := ownerReadBarrierLocalFake{blocked: true, status: raft.GroupStatus{LeaderID: 12}}
	started := time.Now()
	index, err := readOwnerGroupIndex(context.Background(), local, client, 10, "region-a")
	if err != nil || index != 19 || !reflect.DeepEqual(client.calls, []uint64{12}) {
		t.Fatalf("remote Owner read = %d, calls=%v, err=%v", index, client.calls, err)
	}
	if elapsed := time.Since(started); elapsed >= ownerLocalReadIndexTimeout+ownerRemoteReadIndexTimeout {
		t.Fatalf("fallback waited for the whole user read budget: %s", elapsed)
	}
}

func TestRetryRouteAfterTransientRootCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	want := router.Target{RangeID: "after-new-root-cut"}
	got, err := retryRoute(ctx, 10*time.Millisecond, func(attempt context.Context) (router.Target, error) {
		calls++
		switch calls {
		case 1:
			<-attempt.Done()
			return router.Target{}, attempt.Err()
		case 2:
			return router.Target{}, raft.ErrNoLeader
		case 3:
			return router.Target{}, router.ErrCatalogBehind
		case 4:
			return router.Target{}, fmt.Errorf("confirm Root route: %w", errors.Join(status.Error(codes.Unavailable, "leader hint"), status.Error(codes.Unavailable, "old voter connection refused")))
		default:
			return want, nil
		}
	})
	if err != nil || got.RangeID != want.RangeID || calls != 5 {
		t.Fatal("transient Root cuts were not retried", got, calls, err)
	}
}

func TestRetryRouteDoesNotHideStableFailure(t *testing.T) {
	calls := 0
	_, err := retryRoute(context.Background(), time.Second, func(context.Context) (router.Target, error) {
		calls++
		return router.Target{}, router.ErrNoRoute
	})
	if !errors.Is(err, router.ErrNoRoute) || calls != 1 {
		t.Fatal("stable routing error was retried or rewritten", calls, err)
	}
}

func TestReadCandidateContextReservesTimeForNextOwner(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	first, done := readCandidateContext(parent, 3)
	defer done()
	parentDeadline, _ := parent.Deadline()
	firstDeadline, _ := first.Deadline()
	if gap := parentDeadline.Sub(firstDeadline); gap < 150*time.Millisecond {
		t.Fatal("first Owner read consumed time reserved for other candidates", gap)
	}
	noDeadline, release := readCandidateContext(context.Background(), 2)
	defer release()
	if deadline, ok := noDeadline.Deadline(); !ok || time.Until(deadline) > readAttemptTimeout {
		t.Fatal("unbounded Owner read attempt", deadline, ok)
	}
}

func TestWriteCandidateContextReservesTimeForNextOwner(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	first, done := writeCandidateContext(parent, 3)
	defer done()
	parentDeadline, _ := parent.Deadline()
	firstDeadline, _ := first.Deadline()
	if gap := parentDeadline.Sub(firstDeadline); gap < 150*time.Millisecond {
		t.Fatal("first Owner write consumed time reserved for other candidates", gap)
	}
	noDeadline, release := writeCandidateContext(context.Background(), 2)
	defer release()
	if deadline, ok := noDeadline.Deadline(); !ok || time.Until(deadline) > writeAttemptTimeout {
		t.Fatal("unbounded Owner write attempt", deadline, ok)
	}
}

func TestForwardWriteCandidateContextFailsOverPromptly(t *testing.T) {
	withoutDeadline, cancel := forwardWriteCandidateContext(context.Background(), 2)
	defer cancel()
	deadline, ok := withoutDeadline.Deadline()
	if !ok || time.Until(deadline) > writeForwardAttemptTimeout || time.Until(deadline) < writeForwardAttemptTimeout-time.Second {
		t.Fatal("forwarded Owner attempt did not use the short failover budget", deadline, ok)
	}

	parent, parentCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer parentCancel()
	first, done := forwardWriteCandidateContext(parent, 2)
	defer done()
	parentDeadline, _ := parent.Deadline()
	firstDeadline, _ := first.Deadline()
	if gap := parentDeadline.Sub(firstDeadline); gap < 100*time.Millisecond {
		t.Fatal("first forwarded Owner attempt consumed time reserved for the next candidate", gap)
	}
}

func TestProbeRemoteRegionQuorumRequiresAnIndependentRegionMajority(t *testing.T) {
	regions := []regionProbeTarget{{groupID: "region-b", raftIDs: []uint64{2, 3, 4}},
		{groupID: "region-c", raftIDs: []uint64{5, 6, 7}}}
	var calls atomic.Int32
	read := func(_ context.Context, raftID uint64, groupID string) (uint64, error) {
		calls.Add(1)
		if groupID == "region-c" && raftID == 6 {
			return 12, nil
		}
		return 0, errors.New("region is unavailable")
	}
	if err := probeRemoteRegionQuorum(context.Background(), regions, 1, read); err != nil {
		t.Fatal("a remote Region with a Raft majority was not recognized", err)
	}
	if calls.Load() == 0 {
		t.Fatal("remote Region availability was not probed")
	}
}

func TestProbeRemoteRegionQuorumFailsClosedWhenAllOtherRegionsAreDown(t *testing.T) {
	regions := []regionProbeTarget{{groupID: "region-b", raftIDs: []uint64{2, 3}},
		{groupID: "region-c", raftIDs: []uint64{4, 5}}}
	err := probeRemoteRegionQuorum(context.Background(), regions, 1,
		func(context.Context, uint64, string) (uint64, error) { return 0, errors.New("unavailable") })
	if err == nil {
		t.Fatal("local-only write was allowed in a multi-Region topology")
	}
}

func TestProbeRemoteRegionQuorumRejectsEmptyRemoteTopology(t *testing.T) {
	err := probeRemoteRegionQuorum(context.Background(), []regionProbeTarget{{groupID: "region-b"}}, 1,
		func(context.Context, uint64, string) (uint64, error) { return 1, nil })
	if err == nil {
		t.Fatal("remote Region without members was treated as available")
	}
}

func TestConfirmedClusterRegionTargetsUsesAllConfirmedRegions(t *testing.T) {
	groups := []rootstate.Group{{GroupID: "region-sg", StateKind: rootstate.GroupStateEngine},
		{GroupID: "region-ff", StateKind: rootstate.GroupStateEngine},
		{GroupID: "engine-without-region-cohort", StateKind: rootstate.GroupStateEngine}}
	progress := map[string]rootstate.RegionProgress{
		"region-sg": {AppliedIndex: 20, Current: rootstate.RegionCohort{GroupID: "region-sg", RegionID: "sg", Nodes: []uint64{1, 2, 3}}},
		"region-ff": {AppliedIndex: 20, Current: rootstate.RegionCohort{GroupID: "region-ff", RegionID: "ff", Nodes: []uint64{4, 5, 6}}},
	}
	localNodes, remote, regions, err := confirmedClusterRegionTargets(groups, "region-sg", 19,
		func(group string) (rootstate.RegionProgress, bool, error) {
			view, found := progress[group]
			return view, found, nil
		})
	if err != nil || localNodes != 3 || len(remote) != 1 || remote[0].groupID != "region-ff" ||
		!reflect.DeepEqual(remote[0].raftIDs, []uint64{4, 5, 6}) || !reflect.DeepEqual(regions, []string{"sg", "ff"}) {
		t.Fatal("cluster topology did not include the confirmed remote Region", localNodes, remote, regions, err)
	}
}

func TestConfirmedClusterRegionTargetsPreservesSingleRegionTopology(t *testing.T) {
	groups := []rootstate.Group{{GroupID: "region-sg", StateKind: rootstate.GroupStateEngine}}
	localNodes, remote, regions, err := confirmedClusterRegionTargets(groups, "region-sg", 5,
		func(group string) (rootstate.RegionProgress, bool, error) {
			return rootstate.RegionProgress{AppliedIndex: 5,
				Current: rootstate.RegionCohort{GroupID: group, RegionID: "sg", Nodes: []uint64{1, 2, 3}}}, true, nil
		})
	if err != nil || localNodes != 3 || len(remote) != 0 || !reflect.DeepEqual(regions, []string{"sg"}) {
		t.Fatal("single-Region cluster was not preserved", localNodes, remote, regions, err)
	}
}

func TestConfirmedClusterRegionTargetsFailsClosedForStaleRootView(t *testing.T) {
	groups := []rootstate.Group{{GroupID: "region-sg", StateKind: rootstate.GroupStateEngine}}
	_, _, _, err := confirmedClusterRegionTargets(groups, "region-sg", 9,
		func(group string) (rootstate.RegionProgress, bool, error) {
			return rootstate.RegionProgress{AppliedIndex: 8,
				Current: rootstate.RegionCohort{GroupID: group, RegionID: "sg", Nodes: []uint64{1, 2, 3}}}, true, nil
		})
	if err == nil {
		t.Fatal("stale confirmed Region view passed the Root read-cut check")
	}
}

func TestRootRouteRetryRejectsPermanentVoterFailuresInJoinedErrors(t *testing.T) {
	for _, fault := range []error{
		status.Error(codes.PermissionDenied, "removed Node"),
		errors.Join(status.Error(codes.Unavailable, "election"), status.Error(codes.PermissionDenied, "invalid membership")),
		errors.Join(context.DeadlineExceeded, errors.New("malformed Root reply")),
	} {
		calls := 0
		_, err := retryRoute(context.Background(), time.Second, func(context.Context) (router.Target, error) { calls++; return router.Target{}, fault })
		if err == nil || calls != 1 {
			t.Fatal("permanent Root failure was retried", calls, err)
		}
	}
}
