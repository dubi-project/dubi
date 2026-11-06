package recordwrite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"go.etcd.io/raft/v3/raftpb"
)

type pageReferenceHandler struct{}

func (pageReferenceHandler) Apply(tx *enginestate.Transaction, target router.Target, _ []byte) (enginestate.Outcome, error) {
	descriptor, err := records.Describe(target.Keyspace, "origin-group", target.RoutingKey, []byte("body"))
	if err != nil {
		return enginestate.Outcome{}, err
	}
	return enginestate.Outcome{}, tx.SetRecordReference(target.RoutingKey, descriptor)
}

type confirmationPageBarrier struct {
	owner         *enginestate.State
	rootCalls     int
	ownerCalls    int
	ownerIndex    uint64
	forceIndex    bool
	failFinalRoot bool
}

func (b *confirmationPageBarrier) ReadIndex(_ context.Context, group string) (uint64, error) {
	if group == "root" {
		b.rootCalls++
		if b.failFinalRoot && b.rootCalls == 2 {
			return 0, context.DeadlineExceeded
		}
		return 5, nil
	}
	if group != b.owner.GroupID() {
		return 0, errors.New("unexpected Owner group")
	}
	b.ownerCalls++
	if b.forceIndex {
		return b.ownerIndex, nil
	}
	return b.owner.AppliedIndex(), nil
}

func confirmationPageFixture(t *testing.T) (*testRoot, *enginestate.State, []enginestate.RecordReference) {
	t.Helper()
	root, target, _ := stageFixture(t)
	root.view.Current.GroupFenceIndex = 3
	owner, err := enginestate.Open(t.TempDir(), target.OwnerGroupID, pageReferenceHandler{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := owner.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	if err := owner.ApplyRange(2, "activate", rootstate.Range{RangeID: target.RangeID, TenantID: target.Keyspace.TenantID,
		EngineID: target.Keyspace.EngineID, ModuleID: target.Keyspace.ModuleID, OwnerGroupID: target.OwnerGroupID,
		Epoch: target.Epoch, State: rootstate.RangeStateActive}); err != nil {
		t.Fatal(err)
	}
	fence, err := routedstate.EncodeRegionFence("fence", regionfence.Descriptor{RegionID: "west", GroupID: target.OwnerGroupID,
		Generation: 1, PlannedIndex: 2, GroupEpoch: 2, MembershipIndex: 1, Members: root.view.Group.Members})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := routedstate.New(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Apply(3, fence); err != nil {
		t.Fatal(err)
	}
	var refs []enginestate.RecordReference
	for i := 0; i < 8; i++ {
		target.RoutingKey = []byte(fmt.Sprintf("item-%d", i))
		request := fmt.Sprintf("publish-%d", i)
		if err := owner.ApplyRouted(owner.AppliedIndex()+1, request, target, []byte("publish")); err != nil {
			t.Fatal(err)
		}
		descriptor, err := records.Describe(target.Keyspace, "origin-group", target.RoutingKey, []byte("body"))
		if err != nil {
			t.Fatal(err)
		}
		observed, err := ObservePublishedLocal(context.Background(), publishedBarrier{ownerIndex: owner.AppliedIndex()}, root, owner,
			1, root.members[0].NodeID, &testStore{held: true}, &testObserver{root: root}, target, descriptor, request, owner.AppliedIndex())
		if err != nil {
			t.Fatal(err)
		}
		for j := range observed.Holdings {
			observed.Holdings[j].StoreID = fmt.Sprintf("store_%032x", j+1)
		}
		claim, err := ConfirmationClaim(target, observed)
		if err != nil {
			t.Fatal(err)
		}
		if err := owner.ApplyLocalConfirmation(owner.AppliedIndex()+1, fmt.Sprintf("confirm-%d", i), claim); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, observed.Reference)
	}
	root.reads = 0
	return root, owner, refs
}

func TestLocalConfirmationPageChecksEightCurrentReceiptsWithThreeBarriers(t *testing.T) {
	root, owner, refs := confirmationPageFixture(t)
	barrier := &confirmationPageBarrier{owner: owner}
	pending, err := (Writer{Barrier: barrier, Root: root, Owner: owner}).PendingLocalConfirmations(context.Background(), refs)
	if err != nil || len(pending) != 0 || barrier.rootCalls != 2 || barrier.ownerCalls != 1 || root.reads != 2 {
		t.Fatal("healthy page repeated per-record barriers or queued reconfirmation", len(pending), barrier, root.reads, err)
	}
}

func TestLocalConfirmationPageQueuesOnlyCurrentMissingReceipt(t *testing.T) {
	root, owner, refs := confirmationPageFixture(t)
	for _, i := range []int{6, 7} {
		ref := refs[i]
		target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID,
			OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
		if err := owner.ApplyRouted(owner.AppliedIndex()+1, fmt.Sprintf("overwrite-%d", i), target, []byte("publish")); err != nil {
			t.Fatal(err)
		}
		if i == 7 {
			_, err := owner.ReadRouted(context.Background(), liveOwnerBarrier{owner}, target, func(view enginestate.ReadView) error {
				var err error
				refs[i], _, err = view.RecordReference(ref.Descriptor.Key)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	barrier := &confirmationPageBarrier{owner: owner}
	pending, err := (Writer{Barrier: barrier, Root: root, Owner: owner}).PendingLocalConfirmations(context.Background(), refs)
	if err != nil || len(pending) != 1 || pending[0].RequestID != "overwrite-7" || barrier.rootCalls != 2 || barrier.ownerCalls != 1 {
		t.Fatal("replaced reference or healthy receipt became reconfirmation work", pending, barrier, err)
	}
}

func TestLocalConfirmationPageRejectsUnconfirmedCutsAndChangedCohort(t *testing.T) {
	for _, scenario := range []string{"empty-owner-cut", "owner-behind", "final-root-failure", "changed-cohort", "foreign-reference"} {
		t.Run(scenario, func(t *testing.T) {
			root, owner, refs := confirmationPageFixture(t)
			barrier := &confirmationPageBarrier{owner: owner}
			switch scenario {
			case "empty-owner-cut":
				barrier.forceIndex = true
			case "owner-behind":
				barrier.forceIndex, barrier.ownerIndex = true, owner.AppliedIndex()+1
			case "final-root-failure":
				barrier.failFinalRoot = true
			case "changed-cohort":
				root.afterFirst = func(view *rootstate.RegionProgress) {
					view.Current.Generation++
					view.Current.PlannedIndex++
					view.Current.ConfirmedIndex++
					view.Current.GroupEpoch++
					view.Current.GroupFenceRequestID = "next-fence"
					view.Current.GroupFenceIndex++
					view.Head.CurrentGeneration++
					view.Group.Epoch++
					view.AppliedIndex++
				}
			case "foreign-reference":
				refs[3].OwnerGroupID = "region-other"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			pending, err := (Writer{Barrier: barrier, Root: root, Owner: owner}).PendingLocalConfirmations(ctx, refs)
			if err == nil || pending != nil {
				t.Fatal("failed barrier or changed ownership was accepted as a healthy page", pending, err)
			}
			if scenario == "changed-cohort" && !strings.Contains(err.Error(), "Region plan changed") {
				t.Fatal("fixture did not exercise two different valid Region plans", err)
			}
		})
	}
}
