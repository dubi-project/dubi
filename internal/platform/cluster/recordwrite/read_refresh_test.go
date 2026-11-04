package recordwrite

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Advance the committed Owner fence and Root plan, leaving the publication's
// old confirmation intact. Same-generation membership ABA also needs refresh.
func advanceReadFixtureFence(t *testing.T, root *testRoot, owner *enginestate.State) {
	t.Helper()
	view, err := owner.RegionFence()
	if err != nil {
		t.Fatal(err)
	}
	fence := view.Record.Fence
	fence.GroupEpoch++
	request := "read-refresh-fence"
	index := owner.AppliedIndex() + 1
	if err := owner.ApplyRegionFence(index, request, fence); err != nil {
		t.Fatal(err)
	}
	result, found, err := owner.ResultForRegionFence(request, fence)
	if err != nil || !found || !result.Applied {
		t.Fatal("fixture fence was not committed", result, err)
	}
	root.view.Current.GroupEpoch = fence.GroupEpoch
	root.view.Group.Epoch = fence.GroupEpoch
	root.view.Current.GroupFenceIndex = index
	root.view.Current.GroupFenceRequestID = request
}

func TestReadLocalRefreshesOnlyStaleCohortAndRechecksAllGates(t *testing.T) {
	for _, scenario := range []string{"refresh", "already current", "skipped", "callback ACK only", "submit ACK only", "refresh error", "lost majority", "overwrite", "overwrite before refresh", "changed cohort", "Root unavailable after refresh", "initial no majority", "observation denied", "unconfirmed members", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			root, target, ref, owner, _, _, _ := readCurrentFixture(t)
			if scenario != "already current" {
				advanceReadFixtureFence(t, root, owner)
			}
			local, remote := readTestStore(t), readTestStore(t)
			for _, store := range []*records.Store{local, remote} {
				if _, err := store.Put(ctx, ref.Descriptor, []byte("body")); err != nil {
					t.Fatal(err)
				}
			}
			var lost atomic.Bool
			if scenario == "initial no majority" {
				lost.Store(true)
			}
			observer := readObserverFunc(func(ctx context.Context, id uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
				if scenario == "observation denied" {
					return raftrpc.HoldingObservation{}, status.Error(codes.PermissionDenied, "denied")
				}
				if id != 2 || lost.Load() {
					return raftrpc.HoldingObservation{}, status.Error(codes.Unavailable, "offline")
				}
				holding, err := remote.Verify(ctx, d)
				return raftrpc.HoldingObservation{RaftID: id, NodeID: root.members[1].NodeID, RootConfirmedIndex: 5, Holding: holding}, err
			})
			adapter, err := routedstate.New(owner)
			if err != nil {
				t.Fatal(err)
			}
			submits, refreshes := 0, 0
			writer := Writer{Barrier: liveOwnerBarrier{owner}, Root: root, Owner: owner, LocalID: 1, LocalNodeID: root.members[0].NodeID,
				Store: local, Observer: observer, Submit: func(ctx context.Context, group string, command []byte) error {
					submits++
					if group != owner.GroupID() {
						t.Fatal("foreign Owner submission")
					}
					if scenario == "submit ACK only" {
						return nil
					}
					return adapter.Apply(owner.AppliedIndex()+1, command)
				}}
			reader := Reader{Barrier: liveOwnerBarrier{owner}, Root: root, Owner: owner, LocalID: 1, LocalNodeID: root.members[0].NodeID,
				Store: local, Observer: observer, Fetcher: readFetcherFunc(nil)}
			marker := errors.New("refresh rejected")
			reader.RefreshLocal = func(call context.Context, current enginestate.RecordReference) (bool, error) {
				refreshes++
				if current.CommandHash != ref.CommandHash || current.Index != ref.Index {
					t.Fatal("different publication refreshed")
				}
				switch scenario {
				case "skipped":
					return false, nil
				case "callback ACK only":
					return true, nil
				case "refresh error":
					return true, marker
				case "canceled":
					cancel()
				case "overwrite before refresh":
					if err := owner.ApplyRouted(owner.AppliedIndex()+1, "overwrite-before-refresh", target, []byte("publish")); err != nil {
						t.Fatal(err)
					}
				case "submit ACK only":
					var done context.CancelFunc
					call, done = context.WithTimeout(call, 40*time.Millisecond)
					defer done()
				}
				_, err := writer.EnsureCurrentLocalConfirmation(call, current)
				if err != nil {
					return true, err
				}
				switch scenario {
				case "lost majority":
					lost.Store(true)
				case "overwrite":
					if err := owner.ApplyRouted(owner.AppliedIndex()+1, "overwrite-after-refresh", target, []byte("publish")); err != nil {
						t.Fatal(err)
					}
				case "changed cohort":
					root.view.Current.GroupEpoch++
					root.view.Group.Epoch++
				case "Root unavailable after refresh":
					root.members = root.members[:2]
				}
				return true, nil
			}
			if scenario == "unconfirmed members" {
				root.members = root.members[:2]
			}
			got, err := reader.ReadLocal(ctx, target, ref.Descriptor.Key)
			wantSuccess := scenario == "refresh" || scenario == "already current"
			if wantSuccess {
				if err != nil || string(got.Body) != "body" || got.Reference.CommandHash != ref.CommandHash {
					t.Fatal("safe read failed", got, err)
				}
			} else if err == nil || len(got.Body) != 0 {
				t.Fatal("unsafe read accepted", got, err)
			}
			wantRefresh := 1
			if scenario == "already current" || scenario == "initial no majority" || scenario == "observation denied" || scenario == "unconfirmed members" {
				wantRefresh = 0
			}
			if refreshes != wantRefresh {
				t.Fatal("refresh repeated or triggered on an unrelated failure", refreshes, err)
			}
			if scenario == "refresh" && submits != 1 {
				t.Fatal("confirmation not submitted exactly once", submits)
			}
			if wantRefresh == 0 && submits != 0 {
				t.Fatal("unexpected submission", submits)
			}
			if scenario == "overwrite before refresh" && submits != 0 {
				t.Fatal("stale publication was submitted", submits)
			}
			if scenario == "refresh error" && !errors.Is(err, marker) {
				t.Fatal("refresh failure masked", err)
			}
			if scenario == "submit ACK only" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("submission ACK treated as committed result", err)
			}
			if scenario == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation ignored", err)
			}
		})
	}
}
