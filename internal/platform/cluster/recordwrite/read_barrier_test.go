package recordwrite

import (
	"context"
	"errors"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

type readGateBarrier func(context.Context, string) (uint64, error)

func (f readGateBarrier) ReadIndex(ctx context.Context, group string) (uint64, error) {
	return f(ctx, group)
}

func TestReaderCombinesFinalRootCheckWithoutLosingCurrentness(t *testing.T) {
	for _, scenario := range []string{"current", "no admission", "overwrite", "receipt replaced", "Root unavailable", "cohort changed", "canceled", "stale receipt Root unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			root, target, ref, owner, claim, _, _ := readCurrentFixture(t)
			root.reads = 0
			local := readTestStore(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario != "no admission" {
				ctx = router.WithConfirmedRootIndex(ctx, 5)
			}
			if _, err := local.Put(ctx, ref.Descriptor, []byte("body")); err != nil {
				t.Fatal(err)
			}
			if scenario == "stale receipt Root unavailable" {
				advanceReadFixtureFence(t, root, owner)
			}
			rootCalls, ownerCalls, refreshes := 0, 0, 0
			marker := errors.New("Root quorum unavailable")
			barrier := readGateBarrier(func(ctx context.Context, group string) (uint64, error) {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
				if group == "root" {
					rootCalls++
					switch scenario {
					case "Root unavailable", "stale receipt Root unavailable":
						return 0, marker
					case "cohort changed":
						root.view.Current.Generation++
					}
					return 5, nil
				}
				ownerCalls++
				// Owner reads: initial receipt, pre-IO reference, post-IO reference,
				// then final exact receipt/reference. Inject after the physical scan.
				if ownerCalls == 4 {
					switch scenario {
					case "overwrite":
						if err := owner.ApplyRouted(owner.AppliedIndex()+1, "changed", target, []byte("publish")); err != nil {
							t.Fatal(err)
						}
					case "receipt replaced":
						if err := owner.ApplyLocalConfirmation(owner.AppliedIndex()+1, "changed-receipt", claim); err != nil {
							t.Fatal(err)
						}
					case "canceled":
						cancel()
						return 0, ctx.Err()
					}
				}
				return owner.AppliedIndex(), nil
			})
			reader := Reader{Barrier: barrier, Root: root, Owner: owner, LocalID: 1, LocalNodeID: root.members[0].NodeID,
				Store: local, Observer: validReadObserver{observer: &testObserver{root: root}}, Fetcher: readFetcherFunc(nil)}
			reader.RefreshLocal = func(context.Context, enginestate.RecordReference) (bool, error) { refreshes++; return false, nil }
			got, err := reader.ReadLocal(ctx, target, ref.Descriptor.Key)
			success := scenario == "current" || scenario == "no admission"
			if success {
				if err != nil || string(got.Body) != "body" || got.Reference.CommandHash != ref.CommandHash {
					t.Fatal(got, err)
				}
				want := 1
				if scenario == "no admission" {
					want = 2
				}
				if rootCalls != want {
					t.Fatalf("Root barriers=%d want=%d", rootCalls, want)
				}
			} else if err == nil || len(got.Body) != 0 {
				t.Fatal("unsafe read returned bytes", got, err)
			}
			if refreshes != 0 {
				t.Fatal("provisional observation triggered refresh", refreshes)
			}
			if (scenario == "Root unavailable" || scenario == "stale receipt Root unavailable") && !errors.Is(err, marker) {
				t.Fatal("lost quorum error", err)
			}
		})
	}
}
