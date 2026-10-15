package recordwrite

import (
	"context"
	"errors"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
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

func (h *referenceHandler) Apply(tx *capstate.Transaction, target router.Target, _ []byte) (capstate.Outcome, error) {
	return capstate.Outcome{}, tx.SetRecordReference(target.RoutingKey, h.descriptor)
}

type testObserver struct {
	root   *testRoot
	errors map[uint64]error
	calls  []uint64
	before func()
	badID  bool
}

func (o *testObserver) ObserveRecordHolding(_ context.Context, id uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	o.calls = append(o.calls, id)
	if o.before != nil {
		o.before()
		o.before = nil
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

func publishedFixture(t *testing.T) (*testRoot, router.Target, records.Descriptor, *capstate.State, *referenceHandler) {
	t.Helper()
	root, target, descriptor := stageFixture(t)
	handler := &referenceHandler{descriptor: descriptor}
	owner, err := capstate.Open(t.TempDir(), target.OwnerGroupID, handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	rangeDescriptor := rootstate.Range{RangeID: target.RangeID, TenantID: target.Keyspace.TenantID,
		CapabilityID: target.Keyspace.CapabilityID, DatasetID: target.Keyspace.DatasetID,
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
		{name: "local and first remote", want: 1},
		{name: "missing first remote falls back", failures: map[uint64]error{2: status.Error(codes.NotFound, "missing")}, want: 2},
		{name: "offline majority fails", failures: map[uint64]error{2: status.Error(codes.Unavailable, "offline"), 3: status.Error(codes.Unavailable, "offline")}, want: 2, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, target, descriptor, owner, _ := publishedFixture(t)
			observer := &testObserver{root: root, errors: tc.failures}
			store := &testStore{held: true}
			got, err := ObservePublishedLocal(context.Background(), publishedBarrier{ownerIndex: 2}, root, owner,
				1, root.members[0].NodeID, store, observer, target, descriptor, "published", 2)
			if (err != nil) != tc.fail || len(observer.calls) != tc.want {
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
			1, root.members[0].NodeID, &testStore{held: true}, observer, target, descriptor, request, index); err == nil || got.Reference.Index != 0 || len(observer.calls) != 0 {
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
		if err == nil || got.Reference.Index != 0 || len(observer.calls) != 0 {
			t.Fatalf("caller %+v bypassed confirmed cohort membership: %+v, %v", caller, got, err)
		}
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
