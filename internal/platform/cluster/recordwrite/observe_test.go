package recordwrite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type publishedBarrier struct{ ownerIndex uint64 }

func (b publishedBarrier) ReadIndex(_ context.Context, group string) (uint64, error) {
	switch group {
	case "root":
		return 5, nil
	case "region-west":
		return b.ownerIndex, nil
	default:
		return 0, errors.New("unexpected read barrier Group")
	}
}

type referenceHandler struct{ descriptor records.Descriptor }

func (h *referenceHandler) Apply(tx *enginestate.Transaction, target router.Target, _ []byte) (enginestate.Outcome, error) {
	return enginestate.Outcome{}, tx.SetRecordReference(target.RoutingKey, h.descriptor)
}

type testObserver struct {
	mu     sync.Mutex
	root   *testRoot
	errors map[uint64]error
	calls  []uint64
	delays map[uint64]time.Duration
	before func()
	badID  bool
}

func (o *testObserver) ObserveRecordHolding(ctx context.Context, id uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	o.mu.Lock()
	o.calls = append(o.calls, id)
	before := o.before
	o.before = nil
	delay := o.delays[id]
	o.mu.Unlock()
	if before != nil {
		before()
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return raftrpc.HoldingObservation{}, ctx.Err()
		case <-timer.C:
		}
	}
	if err := o.errors[id]; err != nil {
		return raftrpc.HoldingObservation{}, err
	}
	for _, node := range o.root.members {
		if node.RaftID != id {
			continue
		}
		holding := testHolding(d)
		holding.StoreID = "store-remote"
		if o.badID {
			holding.RecordID = "wrong"
		}
		return raftrpc.HoldingObservation{RaftID: id, NodeID: node.NodeID,
			RootConfirmedIndex: 5, Holding: holding}, nil
	}
	return raftrpc.HoldingObservation{}, errors.New("unknown Node")
}

func (o *testObserver) callCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.calls)
}

func publishedFixture(t *testing.T) (*testRoot, router.Target, records.Descriptor, *enginestate.State, *referenceHandler) {
	t.Helper()
	root, target, descriptor := stageFixture(t)
	handler := &referenceHandler{descriptor: descriptor}
	owner, err := enginestate.Open(t.TempDir(), target.OwnerGroupID, handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	rangeDescriptor := rootstate.Range{RangeID: target.RangeID, TenantID: target.Keyspace.TenantID,
		EngineID: target.Keyspace.EngineID, DatasetID: target.Keyspace.DatasetID,
		OwnerGroupID: target.OwnerGroupID, Epoch: target.Epoch, State: rootstate.RangeStateActive}
	if err := owner.ApplyRange(1, "activate", rangeDescriptor); err != nil {
		t.Fatal(err)
	}
	if err := owner.ApplyRouted(2, "published", target, []byte("publish")); err != nil {
		t.Fatal(err)
	}
	return root, target, descriptor, owner, handler
}

func TestObservePublishedLocalRechecksPublicationAndStablePhysicalMajority(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failures map[uint64]error
		want     int
		fail     bool
	}{
		{name: "local and first remote", want: 2},
		{name: "missing remote still forms majority", failures: map[uint64]error{2: status.Error(codes.NotFound, "missing")}, want: 2},
		{name: "offline majority fails", failures: map[uint64]error{2: status.Error(codes.Unavailable, "offline"), 3: status.Error(codes.Unavailable, "offline")}, want: 2, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, target, descriptor, owner, _ := publishedFixture(t)
			observer := &testObserver{root: root, errors: tc.failures}
			store := &testStore{held: true}
			got, err := ObservePublishedLocal(context.Background(), publishedBarrier{ownerIndex: 2}, root, owner,
				1, root.members[0].NodeID, store, observer, target, descriptor, "published", 2)
			if (err != nil) != tc.fail || (tc.fail && observer.callCount() != tc.want) {
				t.Fatal(got, err, observer.calls)
			}
			if tc.fail {
				if got.Reference.Index != 0 {
					t.Fatal("failed observation returned an accepted publication", got)
				}
				return
			}
			if got.Reference.Index != 2 || got.Plan.Required != 2 || len(got.Holdings) != 2 || root.reads != 2 {
				t.Fatal("publication, physical majority or final Root check missing", got, root.reads)
			}
		})
	}
}

func TestObservePublishedLocalUsesHealthyRemoteWithoutSerialTimeoutBudget(t *testing.T) {
	root, target, descriptor, owner, _ := publishedFixture(t)
	observer := &testObserver{root: root, delays: map[uint64]time.Duration{2: time.Second}}
	started := time.Now()
	got, err := ObservePublishedLocal(context.Background(), publishedBarrier{ownerIndex: 2}, root, owner,
		1, root.members[0].NodeID, &testStore{held: true}, observer, target, descriptor, "published", 2)
	if err != nil {
		t.Fatal(got, err)
	}
	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("healthy remote was blocked behind a slow cohort Node: %s", elapsed)
	}
	if got.Reference.Index != 2 || len(got.Holdings) != 2 || got.Holdings[0].RaftID != 1 || got.Holdings[1].RaftID != 3 {
		t.Fatal("parallel holding observation did not confirm a stable local majority", got, observer.callCount())
	}
}

func TestUnavailableHoldingDistinguishesStorageFromAuthorityFailure(t *testing.T) {
	if !unavailableHolding(records.ErrUnavailable) ||
		!unavailableHolding(status.Error(codes.Unavailable, "Store unavailable")) {
		t.Fatal("unavailable physical Store was not treated as an absent holding")
	}
	if unavailableHolding(status.Error(codes.FailedPrecondition, "Node registration changed")) ||
		unavailableHolding(status.Error(codes.PermissionDenied, "Node certificate rejected")) {
		t.Fatal("authority failure was mistaken for an absent physical holding")
	}
}

func TestObservePublishedLocalRejectsReplacedReferenceAndChangedCohort(t *testing.T) {
	root, target, descriptor, owner, handler := publishedFixture(t)
	observer := &testObserver{root: root}
	observer.before = func() {
		handler.descriptor = descriptor
		if err := owner.ApplyRouted(3, "replace", target, []byte("publish")); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := ObservePublishedLocal(context.Background(), publishedBarrier{ownerIndex: 2}, root, owner,
		1, root.members[0].NodeID, &testStore{held: true}, observer, target, descriptor, "published", 2); err == nil || got.Reference.Index != 0 {
		t.Fatal("replaced publication accepted", got, err)
	}

	root, target, descriptor, owner, _ = publishedFixture(t)
	root.afterFirst = func(view *rootstate.RegionProgress) {
		view.Current.Generation = 2
		view.Current.PlannedIndex = 2
		view.Current.ConfirmedIndex = 6
		view.Current.GroupEpoch = 3
		view.Current.GroupFenceRequestID = "new-fence"
		view.Current.GroupFenceIndex = 5
		view.Head.CurrentGeneration = 2
		view.Group.Epoch = 3
		view.AppliedIndex = 6
	}
	if got, err := ObservePublishedLocal(context.Background(), publishedBarrier{ownerIndex: 2}, root, owner,
		1, root.members[0].NodeID, &testStore{held: true}, &testObserver{root: root}, target, descriptor, "published", 2); err == nil || got.Reference.Index != 0 {
		t.Fatal("changed cohort accepted", got, err)
	}
}

func TestObservePublishedLocalRejectsWrongPublicationAndObservation(t *testing.T) {
	root, target, descriptor, owner, _ := publishedFixture(t)
	for _, request := range []string{"wrong", "published"} {
		index := uint64(2)
		if request == "published" {
			index = 3
		}
		observer := &testObserver{root: root}
		if got, err := ObservePublishedLocal(context.Background(), publishedBarrier{ownerIndex: 2}, root, owner,
			1, root.members[0].NodeID, &testStore{held: true}, observer, target, descriptor, request, index); err == nil || got.Reference.Index != 0 || observer.callCount() != 0 {
			t.Fatal("wrong publication reached physical observation", got, err)
		}
	}
	observer := &testObserver{root: root, badID: true}
	if got, err := ObservePublishedLocal(context.Background(), publishedBarrier{ownerIndex: 2}, root, owner,
		1, root.members[0].NodeID, &testStore{held: true}, observer, target, descriptor, "published", 2); err == nil || got.Reference.Index != 0 {
		t.Fatal("wrong remote RecordID accepted", got, err)
	}
}

func TestObservePublishedLocalRequiresCallerInConfirmedCohort(t *testing.T) {
	root, target, descriptor, owner, _ := publishedFixture(t)
	for _, caller := range []struct {
		id   uint64
		name string
	}{
		{id: 999, name: "outside-region"},
		{id: 1, name: "wrong-node-identity"},
	} {
		observer := &testObserver{root: root}
		got, err := ObservePublishedLocal(context.Background(), publishedBarrier{ownerIndex: 2}, root, owner,
			caller.id, caller.name, &testStore{held: true}, observer, target, descriptor, "published", 2)
		if err == nil || got.Reference.Index != 0 || observer.callCount() != 0 {
			t.Fatalf("caller %+v bypassed confirmed cohort membership: %+v, %v", caller, got, err)
		}
	}
}

func TestStagedPublicationReusesOnlyVerifiedStageEvidence(t *testing.T) {
	root, target, descriptor, owner, _ := publishedFixture(t)
	store := &testStore{}
	staged, err := StageLocal(context.Background(), testBarrier{}, root, 1,
		root.members[0].NodeID, store, &testSender{}, target, descriptor, []byte("body"))
	if err != nil {
		t.Fatal("stage physical majority", err)
	}
	for i := range staged.Holdings {
		staged.Holdings[i].StoreID = fmt.Sprintf("store_%032x", i+1)
	}
	request := WriteRequest{RequestID: "published", Target: target, Descriptor: descriptor}
	observed, err := stagedPublication(context.Background(), publishedBarrier{ownerIndex: 2}, owner, request, staged, 2)
	if err != nil || observed.Reference.Index != 2 || !staged.Plan.Matches(observed.Plan) || len(observed.Holdings) != 2 {
		t.Fatal("staged proof did not bind to the exact publication", observed, err)
	}
	claim, err := ConfirmationClaim(target, observed)
	if err != nil || claim.ReferenceIndex != 2 || len(claim.Holdings) != 2 {
		t.Fatal("staged proof did not produce a valid confirmation claim", claim, err)
	}

	staged.Holdings = staged.Holdings[:1]
	if _, err := stagedPublication(context.Background(), publishedBarrier{ownerIndex: 2}, owner, request, staged, 2); err == nil {
		t.Fatal("staged minority was accepted")
	}
}

func readCurrentFixture(t *testing.T) (*testRoot, router.Target, enginestate.RecordReference,
	*enginestate.State, routedstate.LocalConfirmation, string, placement.LocalPlan) {
	t.Helper()
	root, target, descriptor := stageFixture(t)
	root.view.Current.GroupFenceIndex = 3
	handler := &referenceHandler{descriptor: descriptor}
	owner, err := enginestate.Open(t.TempDir(), target.OwnerGroupID, handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := owner.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	rangeDescriptor := rootstate.Range{RangeID: target.RangeID, TenantID: target.Keyspace.TenantID,
		EngineID: target.Keyspace.EngineID, DatasetID: target.Keyspace.DatasetID,
		OwnerGroupID: target.OwnerGroupID, Epoch: target.Epoch, State: rootstate.RangeStateActive}
	if err := owner.ApplyRange(2, "activate", rangeDescriptor); err != nil {
		t.Fatal(err)
	}
	fence := regionfence.Descriptor{RegionID: "west", GroupID: target.OwnerGroupID, Generation: 1,
		PlannedIndex: 2, GroupEpoch: 2, MembershipIndex: 1,
		Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter},
			{RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}
	encodedFence, err := routedstate.EncodeRegionFence("fence", fence)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := routedstate.New(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Apply(3, encodedFence); err != nil {
		t.Fatal(err)
	}
	if err := owner.ApplyRouted(4, "published", target, []byte("publish")); err != nil {
		t.Fatal(err)
	}
	observed, err := ObservePublishedLocal(context.Background(), publishedBarrier{ownerIndex: 4}, root, owner,
		1, root.members[0].NodeID, &testStore{held: true}, &testObserver{root: root},
		target, descriptor, "published", 4)
	if err != nil {
		t.Fatal(err)
	}
	observed.Holdings[0].StoreID = "store_00000000000000000000000000000001"
	observed.Holdings[1].StoreID = "store_00000000000000000000000000000002"
	claim, err := ConfirmationClaim(target, observed)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.ApplyLocalConfirmation(owner.AppliedIndex()+1, "confirm", claim); err != nil {
		t.Fatal(err)
	}
	return root, target, observed.Reference, owner, claim, "confirm", observed.Plan
}

func TestCheckReadCurrentPreservesOwnerAndRegionFencesWithoutRemoteScans(t *testing.T) {
	root, target, ref, owner, claim, confirmationID, plan := readCurrentFixture(t)
	root.reads = 0
	rootReads := root.reads
	got, err := CheckReadCurrent(context.Background(), publishedBarrier{ownerIndex: owner.AppliedIndex()},
		root, owner, target, ref, confirmationID, claim, plan)
	if err != nil || !plan.Matches(got) {
		t.Fatal("current publication was rejected", got, err)
	}
	if root.reads-rootReads != 1 {
		t.Fatalf("final check should use one Root read after Owner currentness, got %d", root.reads-rootReads)
	}

	root, target, ref, owner, claim, confirmationID, plan = readCurrentFixture(t)
	if err := owner.ApplyRouted(owner.AppliedIndex()+1, "replacement", target, []byte("publish")); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckReadCurrent(context.Background(), publishedBarrier{ownerIndex: owner.AppliedIndex()},
		root, owner, target, ref, confirmationID, claim, plan); err == nil {
		t.Fatal("concurrently replaced Owner reference was accepted")
	}

	root, target, ref, owner, claim, confirmationID, plan = readCurrentFixture(t)
	root.reads = 1
	root.afterFirst = func(view *rootstate.RegionProgress) {
		view.Current.Generation++
		view.Current.PlannedIndex++
		view.Current.ConfirmedIndex++
		view.Current.GroupEpoch++
		view.Current.GroupFenceRequestID = "new-fence"
		view.Current.GroupFenceIndex++
		view.Head.CurrentGeneration++
		view.Group.Epoch++
		view.AppliedIndex++
	}
	if _, err := CheckReadCurrent(context.Background(), publishedBarrier{ownerIndex: owner.AppliedIndex()},
		root, owner, target, ref, confirmationID, claim, plan); err == nil {
		t.Fatal("concurrently changed Root Region plan was accepted")
	}
}

func TestWriterNeverPublishesWithoutStablePhysicalMajority(t *testing.T) {
	root, target, descriptor, owner, _ := publishedFixture(t)
	store := &testStore{}
	submissions := 0
	writer := Writer{Barrier: publishedBarrier{ownerIndex: 2}, Root: root, Owner: owner,
		LocalID: 1, LocalNodeID: root.members[0].NodeID, Store: store,
		Sender: &testSender{errors: map[uint64]error{
			2: status.Error(codes.Unavailable, "offline"), 3: status.Error(codes.Unavailable, "offline"),
		}}, Observer: &testObserver{root: root},
		Submit: func(context.Context, string, []byte) error { submissions++; return nil }}
	request := WriteRequest{RequestID: "new-publication", Target: target, Descriptor: descriptor,
		Body: []byte("body"), Payload: []byte("publish")}
	if got, err := writer.WriteLocal(context.Background(), request); err == nil || got.Reference.Index != 0 || submissions != 0 {
		t.Fatal("minority physical stage published a reference", got, submissions, err)
	}
}
