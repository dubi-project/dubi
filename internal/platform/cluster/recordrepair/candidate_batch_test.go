package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func candidateBatchFixture(t *testing.T, count int) (enginestate.RecordCut, []enginestate.RecordReference, []rootstate.Node) {
	t.Helper()
	cut, base, nodes := candidateFixture(t)
	refs := make([]enginestate.RecordReference, count)
	cut.Count, cut.PayloadBytes = uint64(count), 0
	for i := range refs {
		refs[i] = base[0]
		refs[i].Descriptor.Key = []byte(fmt.Sprintf("row-%04d", i))
		cut.PayloadBytes += refs[i].Descriptor.Size
	}
	return cut, refs, nodes
}

func candidateBatchHolding(d records.Descriptor, node uint64) records.Holding {
	id, _ := d.ID()
	return records.Holding{Descriptor: d, RecordID: id, StoreID: fmt.Sprintf("store-%d", node)}
}

func candidateBatchObservation(ctx context.Context, node uint64, d records.Descriptor) raftrpc.HoldingObservation {
	index, _ := router.ConfirmedRootIndex(ctx)
	return raftrpc.HoldingObservation{RaftID: node, NodeID: fmt.Sprintf("node-%d", node), RootConfirmedIndex: index,
		Holding: candidateBatchHolding(d, node)}
}

func TestCandidateBatchPreservesEveryMajorityAndRootRowBudget(t *testing.T) {
	for _, batching := range []bool{false, true} {
		t.Run(fmt.Sprint(batching), func(t *testing.T) {
			cut, refs, nodes := candidateBatchFixture(t, 256)
			roots, rpcs, remoteRows, localRows, rowsAtCut := 0, 0, 0, 0, 0
			barrier := candidateBarrierFunc(func(context.Context, string) (uint64, error) {
				roots++
				rowsAtCut = 0
				return cut.Seal.Intent.Next.PlannedIndex + uint64(roots), nil
			})
			observe := func(ctx context.Context, node uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
				rowsAtCut++
				remoteRows++
				index, ok := router.ConfirmedRootIndex(ctx)
				if !ok || index != cut.Seal.Intent.Next.PlannedIndex+uint64(roots) || rowsAtCut > 64 {
					t.Fatal("Root cut exceeded its physical row budget", index, rowsAtCut)
				}
				return candidateBatchObservation(ctx, node, d), nil
			}
			single := func(ctx context.Context, node uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
				rpcs++
				time.Sleep(time.Millisecond)
				return observe(ctx, node, d)
			}
			batch := func(ctx context.Context, node uint64, ds []records.Descriptor) ([]raftrpc.RecordHoldingResult, error) {
				rpcs++
				time.Sleep(time.Millisecond)
				rows := make([]raftrpc.RecordHoldingResult, len(ds))
				for i, d := range ds {
					rows[i].Observation, rows[i].Err = observe(ctx, node, d)
				}
				return rows, nil
			}
			verify := func(_ context.Context, d records.Descriptor) (records.Holding, error) {
				localRows++
				return candidateBatchHolding(d, 4), nil
			}
			start := time.Now()
			var report Report
			var err error
			if batching {
				report, err = verifyCandidateReferencesBatched(context.Background(), cut, nodes, 4, verify, barrier, single, batch, testWalk(refs))
			} else {
				report, err = verifyCandidateReferences(context.Background(), cut, nodes, 4, verify,
					candidatePageObserver(barrier, cut.Seal.Intent.Next.PlannedIndex, single), testWalk(refs))
			}
			wantRPC := 512
			if batching {
				wantRPC = 16
			}
			if err != nil || report.References != 256 || report.ReferencedBytes != cut.PayloadBytes || localRows != 256 || remoteRows != 512 || roots != 8 || rpcs != wantRPC {
				t.Fatal(report, err, localRows, remoteRows, roots, rpcs)
			}
			t.Logf("batch=%t RPC=%d Root=%d local=%d remote=%d elapsed=%s (injected 1ms/RPC, not WAN benchmark)", batching, rpcs, roots, localRows, remoteRows, time.Since(start))
		})
	}
}

func TestCandidateBatchFailuresRemainUnconfirmed(t *testing.T) {
	for _, mode := range []string{"partial-retry-peer", "minority", "bad-length", "wrong-node", "wrong-record", "duplicate-store", "old-root", "denied", "unsupported", "local-lost", "next-cut-failed", "cancel-in-batch", "manifest-error", "wrong-count"} {
		t.Run(mode, func(t *testing.T) {
			cut, refs, nodes := candidateBatchFixture(t, 65)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			roots, singles := 0, 0
			barrier := candidateBarrierFunc(func(context.Context, string) (uint64, error) {
				roots++
				if mode == "next-cut-failed" && roots > 1 {
					return 0, errors.New("Root unavailable")
				}
				return cut.Seal.Intent.Next.PlannedIndex, nil
			})
			single := func(ctx context.Context, n uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
				singles++
				return candidateBatchObservation(ctx, n, d), nil
			}
			batch := func(ctx context.Context, n uint64, ds []records.Descriptor) ([]raftrpc.RecordHoldingResult, error) {
				if mode == "unsupported" {
					return nil, status.Error(codes.Unimplemented, "old peer")
				}
				if mode == "denied" {
					return nil, status.Error(codes.PermissionDenied, "fenced")
				}
				if mode == "bad-length" {
					return nil, nil
				}
				if mode == "cancel-in-batch" {
					cancel()
				}
				rows := make([]raftrpc.RecordHoldingResult, len(ds))
				for i, d := range ds {
					o := candidateBatchObservation(ctx, n, d)
					if i == len(ds)-1 {
						switch mode {
						case "partial-retry-peer":
							if n == 1 {
								rows[i].Err = records.ErrNotHeld
							}
						case "minority":
							if n == 1 || n == 2 {
								rows[i].Err = records.ErrNotHeld
							}
						case "wrong-node":
							o.NodeID = "foreign"
						case "wrong-record":
							o.Holding.RecordID = "foreign"
						case "duplicate-store":
							o.Holding.StoreID = "store-4"
						case "old-root":
							o.RootConfirmedIndex--
						}
					}
					rows[i].Observation = o
				}
				return rows, nil
			}
			verify := func(_ context.Context, d records.Descriptor) (records.Holding, error) {
				if mode == "local-lost" {
					return records.Holding{}, records.ErrNotHeld
				}
				return candidateBatchHolding(d, 4), nil
			}
			walk := testWalk(refs)
			if mode == "manifest-error" {
				walk = func(visit func(enginestate.RecordReference) error) error {
					if err := testWalk(refs)(visit); err != nil {
						return err
					}
					return errors.New("manifest digest mismatch")
				}
			}
			if mode == "wrong-count" {
				cut.Count++
			}
			report, err := verifyCandidateReferencesBatched(ctx, cut, nodes, 4, verify, barrier, single, batch, walk)
			pass := mode == "partial-retry-peer" || mode == "unsupported"
			if (err == nil) != pass {
				t.Fatal(report, err)
			}
			if !pass && report != (Report{}) {
				t.Fatal("partial proof escaped", report)
			}
			if mode == "unsupported" && singles != 130 {
				t.Fatal("old peer omitted physical rows", singles)
			}
			if mode != "unsupported" && singles != 0 {
				t.Fatal("unsafe protocol fallback", singles)
			}
		})
	}
}

func TestCandidateBatchBoundsBytesAndOwnsWalkerFrames(t *testing.T) {
	cut, refs, nodes := candidateBatchFixture(t, 6)
	for i := range refs {
		refs[i].Descriptor.Size = 2 << 20
	}
	refs[3].Descriptor.Size = (4 << 20) + 1
	cut.PayloadBytes = 14<<20 + 1
	barrier := candidateBarrierFunc(func(context.Context, string) (uint64, error) { return cut.Seal.Intent.Next.PlannedIndex, nil })
	seen := map[string]int{}
	verify := func(_ context.Context, d records.Descriptor) (records.Holding, error) {
		seen[string(d.Key)]++
		return candidateBatchHolding(d, 4), nil
	}
	singles := 0
	single := func(ctx context.Context, n uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
		singles++
		if d.Size <= raftrpc.FetchRecordBatchBytes {
			t.Fatal("small row bypassed batch")
		}
		return candidateBatchObservation(ctx, n, d), nil
	}
	batch := func(ctx context.Context, n uint64, ds []records.Descriptor) ([]raftrpc.RecordHoldingResult, error) {
		var size uint64
		rows := make([]raftrpc.RecordHoldingResult, len(ds))
		for i, d := range ds {
			size += d.Size
			rows[i].Observation = candidateBatchObservation(ctx, n, d)
		}
		if len(ds) > 32 || size > raftrpc.FetchRecordBatchBytes {
			t.Fatal("batch exceeded bounds", len(ds), size)
		}
		return rows, nil
	}
	walk := func(visit func(enginestate.RecordReference) error) error {
		key := make([]byte, len(refs[0].Descriptor.Key))
		for _, ref := range refs {
			copy(key, ref.Descriptor.Key)
			ref.Descriptor.Key = key
			if err := visit(ref); err != nil {
				return err
			}
		}
		return nil
	}
	report, err := verifyCandidateReferencesBatched(context.Background(), cut, nodes, 4, verify, barrier, single, batch, walk)
	if err != nil || report.References != 6 || len(seen) != 6 || singles != 2 {
		t.Fatal(report, err, seen, singles)
	}
}
