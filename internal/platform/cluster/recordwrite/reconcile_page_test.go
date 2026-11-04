package recordwrite

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReconcileLocalPageSharesRootCutsAndKeepsEveryDurableReceipt(t *testing.T) {
	w, refs, b, _, observer := repairBatchFixture(t, 64)
	var submissions atomic.Int32
	submit := w.Submit
	w.Submit = func(ctx context.Context, g string, data []byte) error {
		submissions.Add(1)
		return submit(ctx, g, data)
	}
	failures, err := w.ReconcileLocalConfirmationPage(context.Background(), refs)
	if err != nil || len(failures) != 0 || b.rootCalls != 2 || submissions.Load() != 64 || observer.callCount() < 64 {
		t.Fatal("page lost proof or repeated Root barriers", failures, err, b.rootCalls, submissions.Load(), observer.callCount())
	}
	for _, ref := range refs {
		target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID, OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
		_, err := w.Owner.ReadRouted(context.Background(), liveOwnerBarrier{w.Owner}, target, func(v enginestate.ReadView) error {
			receipt, found, err := v.LocalConfirmation(ref.Descriptor.Key)
			if err != nil {
				return err
			}
			if !found || receipt.Claim.Validate() != nil || receipt.Claim.ReferenceIndex != ref.Index || receipt.ReferenceCommandHash != ref.CommandHash {
				return errors.New("missing exact durable local confirmation")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	calls, ownerCalls := observer.callCount(), b.ownerCalls
	failures, err = w.ReconcileLocalConfirmationPage(context.Background(), refs)
	if err != nil || len(failures) != 0 || b.rootCalls != 4 || b.ownerCalls != ownerCalls+1 || submissions.Load() != 64 || observer.callCount() != calls {
		t.Fatal("current page repeated IO, mutation or per-row Owner barriers", failures, err, b.rootCalls, b.ownerCalls-ownerCalls, submissions.Load(), observer.callCount()-calls)
	}
}

func TestReconcileLocalPageCannotClearDebtAcrossAuthorityOrPhysicalFailure(t *testing.T) {
	for _, mode := range []string{"quorum-loss", "plan-change", "physical-minority", "too-many", "unapplied-root", "cancel-at-final"} {
		t.Run(mode, func(t *testing.T) {
			w, refs, b, root, observer := repairBatchFixture(t, 8)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "quorum-loss":
				b.final = func() error { return errors.New("Root quorum lost") }
			case "plan-change":
				b.final = func() error { root.view.Current.Generation++; root.view.Head.CurrentGeneration++; return nil }
			case "physical-minority":
				observer.errors = map[uint64]error{2: status.Error(codes.Unavailable, "offline"), 3: status.Error(codes.Unavailable, "offline")}
			case "too-many":
				for len(refs) <= 64 {
					refs = append(refs, refs[0])
				}
			case "unapplied-root":
				root.view.AppliedIndex = 4
			case "cancel-at-final":
				b.final = func() error { cancel(); return nil }
			}
			failures, err := w.ReconcileLocalConfirmationPage(ctx, refs)
			if mode == "physical-minority" {
				if err != nil || len(failures) != 8 {
					t.Fatal("physical minority may clear page debt", failures, err)
				}
			} else if err == nil || failures != nil {
				t.Fatal("unsafe page returned completed results", mode, failures, err)
			}
			if mode == "too-many" && b.rootCalls != 0 {
				t.Fatal("oversized page entered Root")
			}
			if mode == "unapplied-root" && observer.callCount() != 0 {
				t.Fatal("unapplied Root cut reached physical IO")
			}
		})
	}
}

func TestReconcileLocalPageRefreshesOnlyStaleRows(t *testing.T) {
	w, refs, _, _, _ := repairBatchFixture(t, 16)
	if failures, err := w.ReconcileLocalConfirmationPage(context.Background(), refs[:8]); err != nil || len(failures) != 0 {
		t.Fatal(failures, err)
	}
	var submissions atomic.Int32
	submit := w.Submit
	w.Submit = func(ctx context.Context, g string, data []byte) error {
		submissions.Add(1)
		return submit(ctx, g, data)
	}
	if failures, err := w.ReconcileLocalConfirmationPage(context.Background(), refs); err != nil || len(failures) != 0 || submissions.Load() != 8 {
		t.Fatal(fmt.Sprint(failures), err, submissions.Load())
	}
}
