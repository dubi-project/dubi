package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type candidateBarrierFunc func(context.Context, string) (uint64, error)

func (f candidateBarrierFunc) ReadIndex(ctx context.Context, g string) (uint64, error) {
	return f(ctx, g)
}

func TestCandidateObservationPagesRetainEveryPhysicalMajority(t *testing.T) {
	cut, base, nodes := candidateFixture(t)
	var refs []enginestate.RecordReference
	cut.Count, cut.PayloadBytes = 70, 0
	for i := 0; i < 70; i++ {
		r := base[0]
		r.Descriptor.Key = []byte(fmt.Sprint(i))
		refs = append(refs, r)
		cut.PayloadBytes += r.Descriptor.Size
	}
	barriers, calls := 0, 0
	barrier := candidateBarrierFunc(func(ctx context.Context, g string) (uint64, error) {
		if g != raft.RootGroupID {
			t.Fatal(g)
		}
		barriers++
		return cut.Seal.Intent.Next.PlannedIndex + uint64(barriers), nil
	})
	observe := func(ctx context.Context, target uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
		calls++
		index, ok := router.ConfirmedRootIndex(ctx)
		if !ok || index != cut.Seal.Intent.Next.PlannedIndex+uint64((calls-1)/64+1) {
			t.Fatal("stale/unconfirmed page cut", index, calls)
		}
		id, _ := d.ID()
		return raftrpc.HoldingObservation{RaftID: target, NodeID: fmt.Sprintf("node-%d", target), RootConfirmedIndex: index, Holding: records.Holding{Descriptor: d, RecordID: id, StoreID: fmt.Sprintf("store-%d", target)}}, nil
	}
	verify := func(ctx context.Context, d records.Descriptor) (records.Holding, error) {
		id, _ := d.ID()
		return records.Holding{Descriptor: d, RecordID: id, StoreID: "store-4"}, nil
	}
	report, err := verifyCandidateReferences(context.Background(), cut, nodes, 4, verify, candidatePageObserver(barrier, cut.Seal.Intent.Next.PlannedIndex, observe), testWalk(refs))
	if err != nil || report.References != 70 || report.ReferencedBytes != cut.PayloadBytes || calls != 140 || barriers != 3 {
		t.Fatal("coverage or page bound changed", report, err, calls, barriers)
	}
}

func TestCandidateObservationPageFailuresCannotReusePreviousConfirmation(t *testing.T) {
	for _, mode := range []string{"no-quorum", "zero", "behind-plan", "revoked", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			cut, refs, _ := candidateFixture(t)
			barriers, calls := 0, 0
			barrier := candidateBarrierFunc(func(ctx context.Context, g string) (uint64, error) {
				barriers++
				if barriers == 1 {
					return cut.Seal.Intent.Next.PlannedIndex, nil
				}
				switch mode {
				case "no-quorum":
					return 0, errors.New("Root quorum unavailable")
				case "zero":
					return 0, nil
				case "behind-plan":
					return cut.Seal.Intent.Next.PlannedIndex - 1, nil
				}
				return cut.Seal.Intent.Next.PlannedIndex + 1, nil
			})
			observe := func(ctx context.Context, target uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
				calls++
				if mode == "revoked" && calls == 65 {
					return raftrpc.HoldingObservation{}, status.Error(codes.PermissionDenied, "fenced")
				}
				if err := ctx.Err(); err != nil {
					return raftrpc.HoldingObservation{}, err
				}
				return raftrpc.HoldingObservation{}, nil
			}
			page := candidatePageObserver(barrier, cut.Seal.Intent.Next.PlannedIndex, observe)
			for i := 0; i < 64; i++ {
				if _, err := page(context.Background(), 1, refs[0].Descriptor); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			if mode == "canceled" {
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			if _, err := page(ctx, 1, refs[0].Descriptor); err == nil {
				t.Fatal("reused a stale or denied confirmation", mode)
			}
			if (mode == "no-quorum" || mode == "zero" || mode == "behind-plan") && calls != 64 {
				t.Fatal("observed without new Root cut", calls)
			}
		})
	}
}
