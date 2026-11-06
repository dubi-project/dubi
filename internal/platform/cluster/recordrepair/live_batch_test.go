package recordrepair

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type liveBatchFake struct {
	*repairSourceFake
	bodies         map[string][]byte
	mode           string
	calls, repairs int
	counts         []int
	bytes          []uint64
}

func (f *liveBatchFake) batch(ctx context.Context, source rootstate.Node, ds []records.Descriptor, store *records.Store, repair bool) error {
	f.calls++
	if repair {
		f.repairs++
	}
	f.counts = append(f.counts, len(ds))
	var size uint64
	for _, d := range ds {
		size += d.Size
	}
	f.bytes = append(f.bytes, size)
	if f.mode == "revoked" {
		return status.Error(codes.PermissionDenied, "source revoked")
	}
	if f.mode == "disjoint" {
		return status.Error(codes.NotFound, "source lacks one row")
	}
	if f.mode == "lying" {
		return nil
	}
	if f.mode == "capacity" {
		return volume.ErrNoWritableVolume
	}
	for i, d := range ds {
		id, _ := d.ID()
		var err error
		if repair {
			_, err = store.RepairExisting(ctx, d, f.bodies[id])
		} else {
			_, err = store.Put(ctx, d, f.bodies[id])
		}
		if err != nil {
			return err
		}
		if f.mode == "partial" && source.RaftID == 2 && i == 0 {
			return status.Error(codes.Unavailable, "reply lost after local write")
		}
	}
	return nil
}
func (f *liveBatchFake) FetchRecords(ctx context.Context, source rootstate.Node, ds []records.Descriptor, store *records.Store) error {
	return f.batch(ctx, source, ds, store, false)
}
func (f *liveBatchFake) RepairRecords(ctx context.Context, source rootstate.Node, ds []records.Descriptor, store *records.Store) error {
	return f.batch(ctx, source, ds, store, true)
}
func (f *liveBatchFake) FetchRecord(ctx context.Context, source rootstate.Node, d records.Descriptor, store *records.Store) (raftrpc.RecordFetchResult, error) {
	f.fetchedFrom = append(f.fetchedFrom, source.RaftID)
	id, _ := d.ID()
	h, err := store.Put(ctx, d, f.bodies[id])
	return raftrpc.RecordFetchResult{Local: h}, err
}
func (f *liveBatchFake) RepairRecord(ctx context.Context, source rootstate.Node, d records.Descriptor, store *records.Store) (raftrpc.RecordFetchResult, error) {
	f.repairedFrom = append(f.repairedFrom, source.RaftID)
	id, _ := d.ID()
	h, err := store.RepairExisting(ctx, d, f.bodies[id])
	return raftrpc.RecordFetchResult{Local: h}, err
}

func liveBatchFixture(t *testing.T, count, payloadBytes int, relocate bool) (*records.Store, []enginestate.RecordReference, *liveBatchFake) {
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
	fake := &liveBatchFake{repairSourceFake: &repairSourceFake{nodes: repairTestPlan(t).Nodes()}, bodies: make(map[string][]byte)}
	refs := make([]enginestate.RecordReference, count)
	for i := range refs {
		body := bytes.Repeat([]byte{byte(i + 1)}, payloadBytes)
		d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "record", ModuleID: "items"}, "origin", []byte(fmt.Sprintf("row-%03d", i)), body)
		if err != nil {
			t.Fatal(err)
		}
		id, _ := d.ID()
		fake.bodies[id] = body
		refs[i] = enginestate.RecordReference{Descriptor: d, RequestID: fmt.Sprintf("row-%d", i)}
		if relocate {
			if _, err := store.Put(context.Background(), d, body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if relocate {
		if err := os.Rename(filepath.Join(root, "volume"), filepath.Join(root, "volume.offline")); err != nil {
			t.Fatal(err)
		}
		if _, err := volumes.Add(t.TempDir()); err != nil {
			t.Fatal(err)
		}
	}
	return store, refs, fake
}

func TestLiveBatchRepairsLostVolumeWithBoundedTransfers(t *testing.T) {
	for _, relocate := range []bool{false, true} {
		t.Run(fmt.Sprint(relocate), func(t *testing.T) {
			store, refs, fake := liveBatchFixture(t, 64, 256, relocate)
			filled, err := prefillRepairPage(context.Background(), repairTestPlan(t), refs, 1, store, fake)
			if err != nil || len(filled) != 64 || fake.calls != 2 || len(fake.observed) != 0 {
				t.Fatal(len(filled), fake.calls, err)
			}
			if relocate && fake.repairs != 2 {
				t.Fatal("lost indexes were not relocated", fake.repairs)
			}
			for _, ref := range refs {
				if _, err := store.Verify(context.Background(), ref.Descriptor); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestLiveBatchByteLimitAndFailureFallback(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		store, refs, fake := liveBatchFixture(t, 5, 2<<20, false)
		filled, err := prefillRepairPage(context.Background(), repairTestPlan(t), refs, 1, store, fake)
		if err != nil || len(filled) != 5 || fake.calls != 3 {
			t.Fatal(len(filled), fake.calls, err)
		}
		for i, n := range fake.bytes {
			if n > raftrpc.FetchRecordBatchBytes || fake.counts[i] > raftrpc.FetchRecordBatchLimit {
				t.Fatal("unbounded batch", n, fake.counts[i])
			}
		}
	})
	for _, mode := range []string{"disjoint", "lying", "partial", "revoked", "closed", "canceled", "capacity"} {
		t.Run(mode, func(t *testing.T) {
			store, refs, fake := liveBatchFixture(t, 4, 32, true)
			fake.mode = mode
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "closed" {
				_ = store.Close()
			}
			if mode == "canceled" {
				cancel()
			}
			filled, err := prefillRepairPage(ctx, repairTestPlan(t), refs, 1, store, fake)
			switch mode {
			case "revoked":
				if status.Code(err) != codes.PermissionDenied || fake.calls != 1 {
					t.Fatal(err, fake.calls)
				}
				return
			case "canceled":
				if !errors.Is(err, context.Canceled) || fake.calls != 0 {
					t.Fatal(err, fake.calls)
				}
				return
			case "closed":
				if err != nil || len(filled) != 0 || fake.calls != 0 {
					t.Fatal(err, len(filled), fake.calls)
				}
				return
			case "partial":
				if err != nil || len(filled) != len(refs) || fake.calls != 2 {
					t.Fatal(err, len(filled), fake.calls)
				}
			default:
				if err != nil || len(filled) != 0 {
					t.Fatal(err, len(filled))
				}
			}
			var report LivePageReport
			for _, ref := range refs {
				if err := processRepairReference(ctx, repairTestPlan(t), ref, 1, store, fake, 64, &report); err != nil {
					t.Fatal(err)
				}
			}
			if report.Unrepaired != 0 || len(report.References) != len(refs) {
				t.Fatal(report)
			}
		})
	}
}
