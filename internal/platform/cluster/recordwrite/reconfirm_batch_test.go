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
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type repairBatchBarrier struct {
	mu                    sync.Mutex
	owner                 *enginestate.State
	rootCalls, ownerCalls int
	final                 func() error
}

func (b *repairBatchBarrier) ReadIndex(ctx context.Context, group string) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if group == raft.RootGroupID {
		b.rootCalls++
		if b.rootCalls >= 2 && b.final != nil {
			if err := b.final(); err != nil {
				return 0, err
			}
		}
		return 5, nil
	}
	if group != b.owner.GroupID() {
		return 0, errors.New("wrong Owner")
	}
	b.ownerCalls++
	return b.owner.AppliedIndex(), nil
}

type repairBatchStore struct{ testStore }

func (s *repairBatchStore) Verify(ctx context.Context, d records.Descriptor) (records.Holding, error) {
	h, err := s.testStore.Verify(ctx, d)
	h.StoreID = fmt.Sprintf("store_%032x", 1)
	return h, err
}

type repairBatchObserver struct{ base *testObserver }

func (o repairBatchObserver) ObserveRecordHolding(ctx context.Context, id uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	if cut, ok := router.ConfirmedRootIndex(ctx); !ok || cut != 5 {
		return raftrpc.HoldingObservation{}, errors.New("physical observation lacks this page's confirmed Root cut")
	}
	h, err := o.base.ObserveRecordHolding(ctx, id, d)
	h.Holding.StoreID = fmt.Sprintf("store_%032x", id)
	return h, err
}

func repairBatchFixture(t *testing.T, count int) (Writer, []enginestate.RecordReference, *repairBatchBarrier, *testRoot, *testObserver) {
	t.Helper()
	root, target, _, owner, handler := publishedFixture(t)
	if err := owner.ApplyMembership(3, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	fence := regionfence.Descriptor{RegionID: "west", GroupID: target.OwnerGroupID, Generation: 1, PlannedIndex: 1, GroupEpoch: 2, MembershipIndex: 3, Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}
	encoded, err := routedstate.EncodeRegionFence("fence", fence)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := routedstate.New(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Apply(4, encoded); err != nil {
		t.Fatal(err)
	}
	var refs []enginestate.RecordReference
	for i := 0; i < count; i++ {
		child := target
		child.RoutingKey = []byte(fmt.Sprintf("page-key-%03d", i))
		d, err := records.Describe(target.Keyspace, "origin-group", child.RoutingKey, []byte("body"))
		if err != nil {
			t.Fatal(err)
		}
		handler.descriptor = d
		if err := owner.ApplyRouted(owner.AppliedIndex()+1, fmt.Sprintf("page-publication-%d", i), child, []byte("publish")); err != nil {
			t.Fatal(err)
		}
		_, err = owner.ReadRouted(context.Background(), liveOwnerBarrier{owner}, child, func(view enginestate.ReadView) error {
			ref, found, err := view.RecordReference(child.RoutingKey)
			if err != nil {
				return err
			}
			if !found {
				return errors.New("missing fixture publication")
			}
			refs = append(refs, ref)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	barrier := &repairBatchBarrier{owner: owner}
	observer := &testObserver{root: root}
	var submitMu sync.Mutex
	w := Writer{Barrier: barrier, Root: root, Owner: owner, LocalID: 1, LocalNodeID: root.members[0].NodeID, Store: &repairBatchStore{testStore{held: true}}, Observer: repairBatchObserver{observer}, Submit: func(ctx context.Context, group string, b []byte) error {
		submitMu.Lock()
		defer submitMu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if group != owner.GroupID() {
			return errors.New("wrong submission Owner")
		}
		return adapter.Apply(owner.AppliedIndex()+1, b)
	}}
	return w, refs, barrier, root, observer
}

func TestLocalConfirmationRepairOverlapsRaftWaitsAndJoinsCanceledWorkers(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelEarly), func(t *testing.T) {
			w, refs, _, _, _ := repairBatchFixture(t, 8)
			submit := w.Submit
			entered := make(chan struct{}, 8)
			release := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w.Submit = func(ctx context.Context, group string, encoded []byte) error {
				entered <- struct{}{}
				<-release // Deliberately retain the worker after cancellation.
				return submit(ctx, group, encoded)
			}
			done := make(chan error, 1)
			go func() {
				failures, err := w.RefreshLocalConfirmationPage(ctx, refs)
				if err == nil && len(failures) != 0 {
					err = errors.Join(failures...)
				}
				done <- err
			}()
			released := false
			defer func() {
				if !released {
					cancel()
					close(release)
					select {
					case <-done:
					case <-time.After(5 * time.Second):
					}
				}
			}()
			for range geoConfirmationPageConcurrency {
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("local confirmation serialized independent Raft waits")
				}
			}
			select {
			case <-entered:
				t.Fatal("local confirmation exceeded the shared concurrency bound")
			default:
			}
			if cancelEarly {
				cancel()
				select {
				case <-done:
					t.Fatal("cancellation returned with an owned worker still running")
				case <-time.After(25 * time.Millisecond):
				}
			}
			close(release)
			released = true
			select {
			case err := <-done:
				if cancelEarly && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if !cancelEarly && err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("local confirmation workers did not finish")
			}
		})
	}
}

func TestConfirmationRepairPageAmortizesRootAndKeepsDurablePerRowProof(t *testing.T) {
	w, refs, b, _, observer := repairBatchFixture(t, 64)
	failures, err := w.RefreshLocalConfirmationPage(context.Background(), refs)
	if err != nil || len(failures) != 0 || b.rootCalls != 2 || b.ownerCalls == 0 || observer.callCount() < 64 {
		t.Fatal(failures, err, b.rootCalls, b.ownerCalls, observer.callCount())
	}
	for _, ref := range refs {
		target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID, OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
		_, err := w.Owner.ReadRouted(context.Background(), liveOwnerBarrier{w.Owner}, target, func(v enginestate.ReadView) error {
			receipt, found, err := v.LocalConfirmation(ref.Descriptor.Key)
			if err != nil {
				return err
			}
			if !found || receipt.Claim.ReferenceIndex != ref.Index || receipt.Claim.Validate() != nil {
				return errors.New("row lacks durable complete confirmation")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	calls := observer.callCount()
	failures, err = w.RefreshLocalConfirmationPage(context.Background(), refs)
	if err != nil || len(failures) != 0 || observer.callCount() != calls || b.rootCalls != 4 {
		t.Fatal("current receipts were republished", failures, err, b.rootCalls, observer.callCount())
	}
}

func TestConfirmationRepairPageCannotCompleteAfterRootChangeOrQuorumLoss(t *testing.T) {
	for _, mode := range []string{"quorum-loss", "plan-change", "physical-minority", "too-many"} {
		t.Run(mode, func(t *testing.T) {
			w, refs, b, root, observer := repairBatchFixture(t, 3)
			switch mode {
			case "quorum-loss":
				b.final = func() error { return errors.New("Root quorum lost") }
			case "plan-change":
				b.final = func() error { root.view.Current.Generation++; root.view.Head.CurrentGeneration++; return nil }
			case "physical-minority":
				observer.errors = map[uint64]error{2: status.Error(codes.Unavailable, "offline"), 3: status.Error(codes.Unavailable, "offline")}
			case "too-many":
				for len(refs) <= localConfirmationRepairPageLimit {
					refs = append(refs, refs[0])
				}
			}
			failures, err := w.RefreshLocalConfirmationPage(context.Background(), refs)
			if mode == "physical-minority" {
				if err != nil || len(failures) != 3 {
					t.Fatal("physical minority cleared row debt", failures, err)
				}
				return
			}
			if err == nil {
				t.Fatal("unsafe page accepted", failures, b.rootCalls)
			}
			if mode == "too-many" && b.rootCalls != 0 {
				t.Fatal("oversized page entered runtime")
			}
		})
	}
}
