package recordwrite

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
)

func TestGeoCompletionBatchRestartAndExactKeys(t *testing.T) {
	mirrors := retryBatchMirrors(t, 64)
	dir := t.TempDir()
	s, e := OpenGeoMetadataRetryStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = s.Close() }()
	if e = s.saveMirrors(mirrors); e != nil {
		t.Fatal(e)
	}
	completed := &geoMirrorCompletions{limit: 32}
	for _, m := range mirrors[:32] {
		if e = completed.add(m); e != nil {
			t.Fatal(e)
		}
	}
	if e = completed.add(mirrors[32]); e == nil {
		t.Fatal("completion exceeded page bound")
	}
	// Losing the in-memory list before cleanup leaves all durable work intact.
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = OpenGeoMetadataRetryStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	if n, e := s.Count(); n != 64 || e != nil {
		t.Fatal(n, e)
	}
	newer := mirrors[0]
	newer.Generation++
	if e = s.Save(newer); e != nil {
		t.Fatal(e)
	}
	if e = s.deleteMirrors(append(completed.mirrors, completed.mirrors[0])); e != nil {
		t.Fatal(e)
	}
	if e = s.deleteMirrors(completed.mirrors); e != nil {
		t.Fatal(e)
	}
	if n, e := s.Count(); n != 33 || e != nil {
		t.Fatal(n, e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = OpenGeoMetadataRetryStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	rows, e := s.List()
	if e != nil || len(rows) != 33 {
		t.Fatal(len(rows), e)
	}
	expected := map[string]bool{}
	for _, m := range append(mirrors[32:], newer) {
		id, _ := mirrorRequestID(m)
		expected[id] = true
	}
	for _, m := range rows {
		id, _ := mirrorRequestID(m)
		if !expected[id] {
			t.Fatal("wrong generation/publication removed or retained", id)
		}
	}
}

func TestGeoCompletionBatchSyncFailureDoesNotLoseUnconfirmed(t *testing.T) {
	mirrors := retryBatchMirrors(t, 34)
	dir := t.TempDir()
	fs := &retrySyncFS{FS: vfs.Default}
	db, e := pebble.Open(filepath.Join(dir, "dubi-geo-metadata-retry"), &pebble.Options{FS: fs, Logger: pebbleutil.QuietLogger{}})
	if e != nil {
		t.Fatal(e)
	}
	s := &GeoMetadataRetryStore{db: db}
	if e = s.saveMirrors(mirrors); e != nil {
		t.Fatal(e)
	}
	fs.fail.Store(true)
	if e = s.deleteMirrors(mirrors[:32]); e == nil {
		t.Fatal("failed cleanup sync reported success")
	}
	if _, e = s.Count(); e == nil {
		t.Fatal("uncertain ledger remains usable")
	}
	fs.fail.Store(false)
	_ = s.Close()
	s, e = OpenGeoMetadataRetryStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	rows, e := s.List()
	if e != nil {
		t.Fatal(e)
	}
	got := map[string]bool{}
	for _, m := range rows {
		id, _ := mirrorRequestID(m)
		got[id] = true
	}
	for _, m := range mirrors[32:] {
		id, _ := mirrorRequestID(m)
		if !got[id] {
			t.Fatal("unconfirmed debt lost after sync failure", id)
		}
	}
	if n, e := s.Count(); e != nil || n != len(rows) {
		t.Fatal("reopened count not reconstructed", n, len(rows), e)
	}
}

func TestGeoCompletionBatchInvalidInputDoesNotPartlyDelete(t *testing.T) {
	rows := retryBatchMirrors(t, 2)
	s, e := OpenGeoMetadataRetryStore(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.saveMirrors(rows); e != nil {
		t.Fatal(e)
	}
	if e = s.deleteMirrors([]routedstate.GeoMetadataMirror{rows[0], {}}); e == nil {
		t.Fatal("invalid mirror accepted")
	}
	if n, e := s.Count(); n != 2 || e != nil {
		t.Fatal(n, e)
	}
}

func TestGeoCompletionBatchPagePartialSuccessAndCleanupFailure(t *testing.T) {
	for _, mode := range []string{"success", "rejected", "cleanup-failure", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			w, refs, _, _, _, _ := geoBatchFixture(t, 32)
			s, fs := openRetrySyncStore(t)
			defer s.Close()
			w.MirrorRetry = s
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			original := w.MirrorGeo
			var confirmations atomic.Int64
			deliver := func(c context.Context, n uint64, g string, b []byte) (enginestate.Result, error) {
				cmd, e := routedstate.DecodeCommand(b)
				if e != nil {
					return enginestate.Result{}, e
				}
				if mode == "rejected" && cmd.GeoMetadata.ReferenceRequestID == refs[0].RequestID {
					return enginestate.Result{RequestID: cmd.RequestID, Index: 10, Error: "durable rejection"}, nil
				}
				return original.MirrorGeo(c, n, g, b)
			}
			w.MirrorGeo = batchMirrorFunc{single: deliver, batch: func(c context.Context, n uint64, g string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
				rows := make([]raftrpc.GeoMirrorBatchResult, len(commands))
				for i, b := range commands {
					rows[i].Result, rows[i].Err = deliver(c, n, g, b)
				}
				return rows, nil
			}}
			submit := w.Submit
			w.Submit = func(c context.Context, g string, b []byte) error {
				e := submit(c, g, b)
				if e != nil {
					return e
				}
				cmd, e := routedstate.DecodeCommand(b)
				if e != nil {
					return e
				}
				if cmd.Geo != nil {
					n := confirmations.Add(1)
					if mode == "cleanup-failure" && n == 32 {
						fs.fail.Store(true)
					}
					if mode == "cancel" && n == 1 {
						cancel()
					}
				}
				return nil
			}
			before := fs.syncs.Load()
			rows, e := w.RefreshGeoConfirmationPage(ctx, refs, 3, 3)
			fs.fail.Store(false)
			if mode == "cleanup-failure" {
				if e == nil || len(rows) != 0 {
					t.Fatal("page returned success after failed cleanup", rows, e)
				}
				if _, err := s.Count(); err == nil {
					t.Fatal("failed ledger usable")
				}
				return
			}
			if mode == "cancel" {
				if !errors.Is(e, context.Canceled) || len(rows) != 0 {
					t.Fatal(rows, e)
				}
				if n, err := s.Count(); err != nil || n > 62 {
					t.Fatal("known successful mirrors not cleaned on cancellation", n, err)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			for i, r := range rows {
				if mode == "rejected" && i == 0 {
					if r.Err == nil || r.Confirmed {
						t.Fatal("rejected mirror confirmed", r)
					}
				} else if r.Err != nil || !r.Confirmed {
					t.Fatal(i, r)
				}
			}
			if mode == "rejected" {
				pending, e := s.List()
				if e != nil || len(pending) != 1 || pending[0].ReferenceRequestID != refs[0].RequestID {
					t.Fatal(pending, e)
				}
			} else {
				if n, e := s.Count(); n != 0 || e != nil {
					t.Fatal(n, e)
				}
				// 64 saves in two batches, 64 deletes in two batches; no per-row sync.
				if syncs := fs.syncs.Load() - before; syncs != 4 {
					t.Fatal("page did not batch both persistence stages", syncs)
				}
			}
		})
	}
}

func TestGeoCompletionBatchCancelledDeliveryRetainsFailedAfterReopen(t *testing.T) {
	w, refs, _, _, _, _ := geoBatchFixture(t, 1)
	ready, e := w.prepareGeoConfirmation(context.Background(), refs[0], 3, 3)
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	s, e := OpenGeoMetadataRetryStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = s.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	completed := &geoMirrorCompletions{limit: 2}
	calls := 0
	tr := batchMirrorFunc{single: func(_ context.Context, _ uint64, _ string, b []byte) (enginestate.Result, error) {
		calls++
		if calls == 2 {
			cancel()
			return enginestate.Result{}, context.Canceled
		}
		cmd, e := routedstate.DecodeCommand(b)
		return enginestate.Result{RequestID: cmd.RequestID, Index: 1, Applied: true}, e
	}}
	if _, e = replicateGeoMetadataWithCompletion(ctx, tr, ready.plan, ready.mirrors, s, completed.add); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if len(completed.mirrors) != 1 {
		t.Fatal(len(completed.mirrors))
	}
	if e = s.deleteMirrors(completed.mirrors); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = OpenGeoMetadataRetryStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	rows, e := s.List()
	if e != nil || len(rows) != 1 || rows[0].RegionID != ready.mirrors[1].RegionID {
		t.Fatal(rows, e)
	}
}

func TestGeoCompletionBatchDiskDiagnostic(t *testing.T) {
	rows := retryBatchMirrors(t, 64)
	for _, mode := range []string{"single", "batch"} {
		s, fs := openRetrySyncStore(t)
		if e := s.saveMirrors(rows); e != nil {
			t.Fatal(e)
		}
		before := fs.syncs.Load()
		start := time.Now()
		if mode == "single" {
			for _, r := range rows {
				if e := s.Delete(r); e != nil {
					t.Fatal(e)
				}
			}
		} else {
			if e := s.deleteMirrors(rows); e != nil {
				t.Fatal(e)
			}
		}
		t.Logf("mode=%s deletes=64 elapsed_us=%d syncs=%d", mode, time.Since(start).Microseconds(), fs.syncs.Load()-before)
		want := int64(64)
		if mode == "batch" {
			want = 2
		}
		if got := fs.syncs.Load() - before; got != want {
			t.Fatal(got, want)
		}
		if e := s.Close(); e != nil {
			t.Fatal(e)
		}
	}
}
