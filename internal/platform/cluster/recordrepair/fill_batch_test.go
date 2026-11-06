package recordrepair

import (
	"context"
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFillBatchBoundsPagesAndReverifiesPhysicalBytes(t *testing.T) {
	for _, mode := range []string{"normal", "disjoint-sources", "partial-destination", "unverified-success", "revoked", "large-record"} {
		t.Run(mode, func(t *testing.T) {
			var refs []enginestate.RecordReference
			for i := 0; i < 70; i++ {
				d, err := records.Describe(rootstate.Keyspace{TenantID: "t", EngineID: "record", ModuleID: "d"}, "region-west", []byte(fmt.Sprint(i)), []byte("data"))
				if err != nil {
					t.Fatal(err)
				}
				if mode == "large-record" && i == 34 {
					d.Size = raftrpc.FetchRecordBatchBytes + 1
				}
				refs = append(refs, enginestate.RecordReference{Descriptor: d})
			}
			held := map[string]bool{}
			var batches, individual int
			verify := func(d records.Descriptor) error {
				if held[string(d.Key)] {
					return nil
				}
				return records.ErrNotHeld
			}
			fetch := func(_ context.Context, source rootstate.Node, d records.Descriptor) error {
				individual++
				if mode == "disjoint-sources" && ((len(d.Key)%2 == 0) != (source.RaftID == 1)) {
					return status.Error(codes.NotFound, "on other member")
				}
				held[string(d.Key)] = true
				return nil
			}
			batch := func(_ context.Context, source rootstate.Node, ds []records.Descriptor) error {
				batches++
				var bytes uint64
				for _, d := range ds {
					bytes += d.Size
				}
				if len(ds) > raftrpc.FetchRecordBatchLimit || bytes > raftrpc.FetchRecordBatchBytes {
					t.Fatal("unbounded batch", len(ds), bytes)
				}
				switch mode {
				case "disjoint-sources":
					return status.Error(codes.NotFound, "no complete source")
				case "revoked":
					return status.Error(codes.PermissionDenied, "fenced")
				case "unverified-success":
					return nil
				case "partial-destination":
					if source.RaftID == 1 {
						held[string(ds[0].Key)] = true
						return status.Error(codes.Unavailable, "local I/O interrupted")
					}
				}
				for _, d := range ds {
					held[string(d.Key)] = true
				}
				return nil
			}
			report, err := fillModeBatched(context.Background(), []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, testSources(), false, testWalk(refs), verify, fetch, batch)
			if mode == "revoked" || mode == "unverified-success" {
				if err == nil || report != (Report{}) || individual != 0 || batches != 1 {
					t.Fatal("unsafe batch succeeded or retried", report, err, batches, individual)
				}
				return
			}
			if err != nil || report.References != 70 || report.Fetched != 70 || len(held) != 70 {
				t.Fatal(report, err, len(held))
			}
			switch mode {
			case "normal":
				if batches != 3 || individual != 0 {
					t.Fatal("per-record fallback on healthy source", batches, individual)
				}
			case "disjoint-sources":
				if batches != 9 || individual < 70 {
					t.Fatal("disjoint sources not preserved", batches, individual)
				}
			case "partial-destination":
				if batches != 6 || individual != 0 {
					t.Fatal("retry failed", batches, individual)
				}
			case "large-record":
				if individual != 1 {
					t.Fatal("oversized record did not use bounded single transfer", individual)
				}
			}
		})
	}
}
