package recordwrite

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readObserverFunc func(context.Context, uint64, records.Descriptor) (raftrpc.HoldingObservation, error)

func (f readObserverFunc) ObserveRecordHolding(ctx context.Context, id uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	return f(ctx, id, d)
}

type readFetcherFunc func(context.Context, rootstate.Node, records.Descriptor, *records.Store) (raftrpc.RecordFetchResult, error)

func (f readFetcherFunc) FetchRecord(ctx context.Context, n rootstate.Node, d records.Descriptor, s *records.Store) (raftrpc.RecordFetchResult, error) {
	return f(ctx, n, d, s)
}

func readTestStore(t *testing.T) *records.Store {
	t.Helper()
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	volumes, err := volume.Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = volumes.Close() })
	packs, err := pack.OpenPool(volumes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = packs.Close() })
	store, err := records.OpenNodeStore(packs, volumes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestReadLocalRepairsMissingCopyWithoutWeakeningMajority(t *testing.T) {
	for _, scenario := range []string{"surviving source", "silent failed peer", "source lost after copy", "overwrite during copy", "region changed during copy", "fetch denied", "observation denied", "incomplete fetch"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			if scenario == "silent failed peer" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				defer cancel()
			}
			root, target, ref, owner, _, _, _ := readCurrentFixture(t)
			local, source := readTestStore(t), readTestStore(t)
			if _, err := source.Put(ctx, ref.Descriptor, []byte("body")); err != nil {
				t.Fatal(err)
			}
			var withdrawn atomic.Bool
			var fetches atomic.Int32
			observer := readObserverFunc(func(ctx context.Context, id uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
				if scenario == "silent failed peer" && id == 3 {
					<-ctx.Done()
					return raftrpc.HoldingObservation{}, ctx.Err()
				}
				if scenario == "observation denied" {
					return raftrpc.HoldingObservation{}, status.Error(codes.PermissionDenied, "denied")
				}
				if id != 2 || withdrawn.Load() {
					return raftrpc.HoldingObservation{}, status.Error(codes.Unavailable, "offline")
				}
				holding, err := source.Verify(ctx, d)
				return raftrpc.HoldingObservation{RaftID: 2, NodeID: root.members[1].NodeID, RootConfirmedIndex: 5, Holding: holding}, err
			})
			fetcher := readFetcherFunc(func(ctx context.Context, n rootstate.Node, d records.Descriptor, dst *records.Store) (raftrpc.RecordFetchResult, error) {
				fetches.Add(1)
				if scenario == "fetch denied" {
					return raftrpc.RecordFetchResult{}, status.Error(codes.PermissionDenied, "denied")
				}
				if n.RaftID != 2 {
					return raftrpc.RecordFetchResult{}, status.Error(codes.Unavailable, "offline")
				}
				if scenario == "incomplete fetch" {
					return raftrpc.RecordFetchResult{}, nil // No durable bytes: an ACK alone is insufficient.
				}
				body, _, err := source.Read(ctx, d)
				if err == nil {
					_, err = dst.Put(ctx, d, body)
				}
				if scenario == "source lost after copy" {
					withdrawn.Store(true)
				}
				if scenario == "overwrite during copy" {
					if applyErr := owner.ApplyRouted(owner.AppliedIndex()+1, "overwrite", target, []byte("overwrite")); applyErr != nil {
						t.Fatal(applyErr)
					}
				}
				if scenario == "region changed during copy" {
					root.view.Current.Generation++
				}
				return raftrpc.RecordFetchResult{}, err
			})
			reader := Reader{Barrier: liveOwnerBarrier{owner: owner}, Root: root, Owner: owner,
				LocalID: 1, LocalNodeID: root.members[0].NodeID, Store: local, Observer: observer, Fetcher: fetcher}
			result, err := reader.ReadLocal(ctx, target, ref.Descriptor.Key)
			if scenario == "surviving source" || scenario == "silent failed peer" {
				if err != nil || string(result.Body) != "body" || fetches.Load() != 1 {
					t.Fatalf("read did not repair and freshly prove majority: %+v, %v, fetches=%d", result, err, fetches.Load())
				}
			} else if err == nil || len(result.Body) != 0 {
				t.Fatalf("unsafe read accepted: %+v, %v", result, err)
			}
			if scenario == "fetch denied" && (fetches.Load() != 1 || status.Code(err) != codes.PermissionDenied) {
				t.Fatalf("fetch authorization failure was retried or masked: %v", err)
			}
			if scenario == "incomplete fetch" && !errors.Is(err, records.ErrNotHeld) {
				t.Fatalf("partial transfer was not verified: %v", err)
			}
		})
	}
}
