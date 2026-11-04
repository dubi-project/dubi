package recordwrite

import (
	"context"
	"errors"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
)

func withDiscoveryBatchTransport(w Writer) Writer {
	base := w.MirrorGeo
	w.MirrorGeo = batchMirrorFunc{single: base.MirrorGeo,
		batch: func(ctx context.Context, target uint64, group string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
			rows := make([]raftrpc.GeoMirrorBatchResult, len(commands))
			for i, command := range commands {
				rows[i].Result, rows[i].Err = base.MirrorGeo(ctx, target, group, command)
			}
			return rows, nil
		}}
	return w
}

func TestGeoDiscoveryCutKeepsDurableProofAndFreshFinalBarrier(t *testing.T) {
	for _, mode := range []string{"single", "batch"} {
		t.Run(mode, func(t *testing.T) {
			w, refs, b, _, observer, _ := geoBatchFixture(t, 64)
			if mode == "batch" {
				w = withDiscoveryBatchTransport(w)
			}
			ctx := context.Background()
			cut, err := b.ReadIndex(ctx, raft.RootGroupID) // Discovery admission.
			if err != nil {
				t.Fatal(err)
			}
			for page := 0; page < 2; page++ {
				var results []GeoConfirmationRefresh
				if page == 0 {
					results, err = w.RefreshGeoConfirmationPageAtCut(ctx, refs[:32], 3, 3, cut)
				} else {
					results, err = w.RefreshGeoConfirmationPage(ctx, refs[32:], 3, 3)
				}
				if err != nil || len(results) != 32 || b.rootCalls != 2+page*2 {
					t.Fatal("discovery cut or final barrier was not scoped to its page", err, len(results), b.rootCalls)
				}
				for i, result := range results {
					if !result.Confirmed || result.Err != nil {
						t.Fatal(page, i, result)
					}
				}
			}
			if observer.calls != 128 {
				t.Fatal("physical observations were omitted", observer.calls)
			}
			for _, ref := range refs {
				target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID,
					OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
				_, err := w.Owner.ReadRouted(ctx, liveOwnerBarrier{w.Owner}, target, func(v enginestate.ReadView) error {
					receipt, found, err := v.GeoCoverage(ref.Descriptor.Key)
					if err != nil {
						return err
					}
					if !found || receipt.Claim.Validate() != nil || receipt.Claim.ReferenceIndex != ref.Index ||
						receipt.ReferenceCommandHash != ref.CommandHash {
						return errors.New("exact durable publication receipt missing")
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			assertLocalGeoMirrors(t, w, refs)
			t.Logf("discovery + two pages: 4 Root cuts (previously 5), 64 durable receipts, %d physical observations", observer.calls)
		})
	}
}

func TestGeoDiscoveryCutFailsClosed(t *testing.T) {
	for _, mode := range []string{"zero", "unapplied", "quorum-loss", "cohort-change", "route-change", "canceled", "cancel-at-final", "physical-minority", "mirror-rejection"} {
		t.Run(mode, func(t *testing.T) {
			w, refs, b, root, observer, mirrors := geoBatchFixture(t, 32)
			w = withDiscoveryBatchTransport(w)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cut, err := b.ReadIndex(ctx, raft.RootGroupID)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "zero":
				cut = 0
			case "unapplied":
				root.view.AppliedIndex = cut - 1
			case "quorum-loss":
				b.final = func() error { return errors.New("Root quorum lost after discovery") }
			case "cohort-change":
				b.final = func() error {
					root.view.Regions[0].Progress.Current.Generation++
					root.view.Regions[0].Progress.Head.CurrentGeneration++
					return nil
				}
			case "route-change":
				b.final = func() error { root.mapping.RangeEpoch++; return nil }
			case "canceled":
				cancel()
			case "cancel-at-final":
				b.final = func() error { cancel(); return nil }
			case "physical-minority":
				observer.absent = true
			case "mirror-rejection":
				mirrors.failed = map[uint64]bool{4: true}
			}
			results, err := w.RefreshGeoConfirmationPageAtCut(ctx, refs, 3, 3, cut)
			if mode == "physical-minority" || mode == "mirror-rejection" {
				if err != nil || len(results) != len(refs) {
					t.Fatal(err, results)
				}
				for _, result := range results {
					if result.Confirmed || result.Err == nil {
						t.Fatal("failed row could clear debt", result)
					}
				}
				return
			}
			if err == nil || results != nil {
				t.Fatal("unsafe page returned debt-clearing results", err, results)
			}
			if (mode == "zero" || mode == "unapplied" || mode == "canceled") && (observer.calls != 0 || b.rootCalls != 1) {
				t.Fatal("invalid admission reached IO or another Root barrier", observer.calls, b.rootCalls)
			}
		})
	}
}
