package recordwrite

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testBarrier struct{}

func (testBarrier) ReadIndex(_ context.Context, group string) (uint64, error) {
	if group != "root" {
		return 0, errors.New("wrong barrier Group")
	}
	return 5, nil
}

type testRoot struct {
	view       rootstate.RegionProgress
	members    []rootstate.Node
	reads      int
	afterFirst func(*rootstate.RegionProgress)
}

func (r *testRoot) RegionProgress(group string) (rootstate.RegionProgress, bool, error) {
	if group != r.view.Current.GroupID {
		return rootstate.RegionProgress{}, false, nil
	}
	r.reads++
	if r.reads == 2 && r.afterFirst != nil {
		r.afterFirst(&r.view)
	}
	return r.view, true, nil
}
func (r *testRoot) RootMembers(ids ...uint64) ([]rootstate.Node, bool, error) {
	if len(ids) != len(r.members) {
		return nil, false, nil
	}
	for i, id := range ids {
		if id != r.members[i].RaftID {
			return nil, false, nil
		}
	}
	return append([]rootstate.Node(nil), r.members...), true, nil
}

type testStore struct {
	held bool
	puts int
}

func (s *testStore) Put(_ context.Context, d records.Descriptor, payload []byte) (records.Holding, error) {
	if !bytes.Equal(payload, []byte("body")) {
		return records.Holding{}, errors.New("wrong payload")
	}
	s.held, s.puts = true, s.puts+1
	return testHolding(d), nil
}
func (s *testStore) Verify(_ context.Context, d records.Descriptor) (records.Holding, error) {
	if !s.held {
		return records.Holding{}, records.ErrNotHeld
	}
	return testHolding(d), nil
}
func testHolding(d records.Descriptor) records.Holding {
	id, _ := d.ID()
	return records.Holding{RecordID: id, Descriptor: d, StoreID: "store-local", VolumeID: "volume-local"}
}

type testSender struct {
	errors map[uint64]error
	calls  []uint64
	badID  bool
	block  uint64
}

func (s *testSender) PutRecord(ctx context.Context, target rootstate.Node, d records.Descriptor, _ []byte) (raftrpc.HoldingObservation, error) {
	s.calls = append(s.calls, target.RaftID)
	if target.RaftID == s.block {
		<-ctx.Done()
		return raftrpc.HoldingObservation{}, ctx.Err()
	}
	if err := s.errors[target.RaftID]; err != nil {
		return raftrpc.HoldingObservation{}, err
	}
	holding := testHolding(d)
	holding.StoreID = "store-remote"
	if s.badID {
		holding.RecordID = "another-record"
	}
	return raftrpc.HoldingObservation{RaftID: target.RaftID, NodeID: target.NodeID, RootConfirmedIndex: 5, Holding: holding}, nil
}

func TestStageLocalSlowRemoteLeavesTimeForHealthyMajority(t *testing.T) {
	root, target, descriptor := stageFixture(t)
	store, sender := &testStore{}, &testSender{block: 2}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := StageLocal(ctx, testBarrier{}, root, 1, root.members[0].NodeID,
		store, sender, target, descriptor, []byte("body"))
	if err != nil || ctx.Err() != nil || !reflect.DeepEqual(sender.calls, []uint64{2, 3}) ||
		len(got.Holdings) != 2 || got.Plan.Required != 2 {
		t.Fatal("slow destination prevented a healthy physical majority", got, err, ctx.Err(), sender.calls)
	}
}

func stageFixture(t *testing.T) (*testRoot, router.Target, records.Descriptor) {
	t.Helper()
	members := []rootstate.Node{
		{RaftID: 1, NodeID: "node_00000000000000000000000000000001", RegionID: "west", Address: "n1:7443"},
		{RaftID: 2, NodeID: "node_00000000000000000000000000000002", RegionID: "west", Address: "n2:7443"},
		{RaftID: 3, NodeID: "node_00000000000000000000000000000003", RegionID: "west", Address: "n3:7443"},
	}
	current := rootstate.RegionCohort{RegionID: "west", GroupID: "region-west", Generation: 1,
		Nodes: []uint64{1, 2, 3}, State: rootstate.RegionCohortConfirmed,
		PlannedIndex: 1, ConfirmedIndex: 5, GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 4}
	root := &testRoot{members: members, view: rootstate.RegionProgress{AppliedIndex: 5, Eligible: true,
		Head:    rootstate.RegionCohortHead{RegionID: "west", CurrentGeneration: 1},
		Current: current, Group: rootstate.Group{GroupID: "region-west", Epoch: 2, Members: []rootstate.GroupMember{
			{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter},
		}}}}
	space := rootstate.Keyspace{TenantID: "tenant", CapabilityID: "record", DatasetID: "items"}
	// The immutable origin is not necessarily the current Range owner.
	d, err := records.Describe(space, "origin-group", []byte("key"), []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	return root, router.Target{Keyspace: space, RangeID: "range-west", OwnerGroupID: "region-west", Epoch: 1, RoutingKey: []byte("key")}, d
}

func TestStageLocalRequiresRealMajorityAndFallsBackWithinStableCohort(t *testing.T) {
	for _, tc := range []struct {
		name      string
		failures  map[uint64]error
		wantCalls []uint64
		wantError bool
	}{
		{name: "first remote", wantCalls: []uint64{2}},
		{name: "offline remote", failures: map[uint64]error{2: status.Error(codes.Unavailable, "offline")}, wantCalls: []uint64{2, 3}},
		{name: "remote Store unavailable", failures: map[uint64]error{2: status.Error(codes.Unavailable, "record Store cannot confirm durable holding")}, wantCalls: []uint64{2, 3}},
		{name: "no majority", failures: map[uint64]error{2: status.Error(codes.Unavailable, "offline"), 3: status.Error(codes.Unavailable, "offline")}, wantCalls: []uint64{2, 3}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, target, descriptor := stageFixture(t)
			store, sender := &testStore{}, &testSender{errors: tc.failures}
			got, err := StageLocal(context.Background(), testBarrier{}, root, 1, root.members[0].NodeID, store, sender, target, descriptor, []byte("body"))
			if (err != nil) != tc.wantError || !reflect.DeepEqual(sender.calls, tc.wantCalls) || store.puts != 1 {
				t.Fatal(got, err, sender.calls, store.puts)
			}
			if tc.wantError {
				if got.RecordID != "" {
					t.Fatal("partial holdings were returned as a completed stage")
				}
				return
			}
			if len(got.Holdings) != 2 || got.Plan.Required != 2 || root.reads != 2 {
				t.Fatal("stage did not recheck the exact Root cohort", got, root.reads)
			}
		})
	}
}

func TestStageLocalRejectsChangedCohortAndUntrustedObservations(t *testing.T) {
	root, target, descriptor := stageFixture(t)
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
	if got, err := StageLocal(context.Background(), testBarrier{}, root, 1, root.members[0].NodeID, &testStore{}, &testSender{}, target, descriptor, []byte("body")); err == nil || got.RecordID != "" {
		t.Fatal("changed cohort accepted", got, err)
	}
	root, target, descriptor = stageFixture(t)
	if got, err := StageLocal(context.Background(), testBarrier{}, root, 1, root.members[0].NodeID, &testStore{}, &testSender{badID: true}, target, descriptor, []byte("body")); err == nil || got.RecordID != "" {
		t.Fatal("wrong RecordID accepted", got, err)
	}
	root, target, descriptor = stageFixture(t)
	root.view.Pending = &rootstate.RegionCohort{Generation: 2}
	store := &testStore{}
	if _, err := StageLocal(context.Background(), testBarrier{}, root, 1, root.members[0].NodeID, store, &testSender{}, target, descriptor, []byte("body")); err == nil || store.puts != 0 {
		t.Fatal("pending generation admitted new physical staging", err, store.puts)
	}
}
