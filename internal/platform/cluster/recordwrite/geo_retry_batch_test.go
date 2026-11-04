package recordwrite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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

func retryBatchMirrors(t *testing.T, count int) []routedstate.GeoMetadataMirror {
	t.Helper()
	w, refs, _, _, _, _ := geoBatchFixture(t, 1)
	ready, err := w.prepareGeoConfirmation(context.Background(), refs[0], 3, 3)
	if err != nil || ready == nil {
		t.Fatal(ready, err)
	}
	mirrors := make([]routedstate.GeoMetadataMirror, count)
	for i := range mirrors {
		mirrors[i] = ready.mirrors[0]
		mirrors[i].ReferenceRequestID = fmt.Sprintf("retry-batch-%04d", i)
	}
	return mirrors
}

func TestGeoRetryBatchPersistsAndSkipsIdenticalDebt(t *testing.T) {
	rows := retryBatchMirrors(t, 70)
	dir := t.TempDir()
	s, err := OpenGeoMetadataRetryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err = s.saveMirrors(append(rows, rows[0])); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Count(); n != 70 || err != nil {
		t.Fatal(n, err)
	}
	before := s.db.Metrics().WAL.BytesIn
	if err = s.saveMirrors(rows); err != nil {
		t.Fatal(err)
	}
	if got := s.db.Metrics().WAL.BytesIn; got != before {
		t.Fatal("identical debt rewrote WAL", before, got)
	}
	if err = s.Delete(rows[0]); err != nil {
		t.Fatal(err)
	}
	if err = s.Save(rows[0]); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenGeoMetadataRetryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := s.List()
	if err != nil || len(pending) != 70 {
		t.Fatal(len(pending), err)
	}
	// Concurrent delete/re-save is serialized by the same lock; no stale in-memory
	// deduplication may skip debt that was actually removed from Pebble.
	var workers sync.WaitGroup
	for i := range rows {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if e := s.Delete(rows[i]); e != nil {
				t.Error(e)
			}
			if e := s.Save(rows[i]); e != nil {
				t.Error(e)
			}
		}()
	}
	workers.Wait()
	if n, err := s.Count(); n != 70 || err != nil {
		t.Fatal(n, err)
	}
}

func TestGeoRetryBatchRejectsInvalidOrCorruptRows(t *testing.T) {
	rows := retryBatchMirrors(t, 2)
	s, err := OpenGeoMetadataRetryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.saveMirrors([]routedstate.GeoMetadataMirror{rows[0], {}}); err == nil {
		t.Fatal("invalid row accepted")
	}
	if n, _ := s.Count(); n != 0 {
		t.Fatal("invalid first batch partly persisted", n)
	}
	id, _ := mirrorRequestID(rows[1])
	if err := s.db.Set(geoRetryKey(id), []byte("corrupt"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.saveMirrors(rows); err == nil {
		t.Fatal("corrupt existing debt silently overwritten")
	}
	id, _ = mirrorRequestID(rows[0])
	if _, closer, err := s.db.Get(geoRetryKey(id)); err == nil {
		closer.Close()
		t.Fatal("conflicting batch partly applied")
	} else if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatal(err)
	}
}

type retrySyncFS struct {
	vfs.FS
	fail  atomic.Bool
	syncs atomic.Int64
}
type retrySyncFile struct {
	vfs.File
	fs *retrySyncFS
}

func (f *retrySyncFS) Create(name string) (vfs.File, error) {
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(name, ".log") {
		return &retrySyncFile{File: file, fs: f}, nil
	}
	return file, nil
}
func (f *retrySyncFile) Sync() error {
	f.fs.syncs.Add(1)
	if f.fs.fail.Load() {
		return errors.New("injected retry WAL sync failure")
	}
	return f.File.Sync()
}
func (f *retrySyncFile) SyncData() error            { return f.Sync() }
func (f *retrySyncFile) SyncTo(int64) (bool, error) { return true, f.Sync() }
func openRetrySyncStore(t *testing.T) (*GeoMetadataRetryStore, *retrySyncFS) {
	t.Helper()
	fs := &retrySyncFS{FS: vfs.Default}
	db, err := pebble.Open(t.TempDir(), &pebble.Options{FS: fs, Logger: pebbleutil.QuietLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	return &GeoMetadataRetryStore{db: db}, fs
}
func TestGeoRetryBatchUsesOneSyncAndFailsClosed(t *testing.T) {
	rows := retryBatchMirrors(t, 32)
	s, fs := openRetrySyncStore(t)
	defer s.Close()
	before := fs.syncs.Load()
	if err := s.saveMirrors(rows); err != nil {
		t.Fatal(err)
	}
	if got := fs.syncs.Load() - before; got != 1 {
		t.Fatal("batch sync count", got)
	}
	before = fs.syncs.Load()
	if err := s.saveMirrors(rows); err != nil {
		t.Fatal(err)
	}
	if got := fs.syncs.Load() - before; got != 0 {
		t.Fatal("duplicate batch synced", got)
	}
	fs.fail.Store(true)
	rows[0].ReferenceRequestID = "sync-failure"
	if err := s.saveMirrors(rows[:1]); err == nil {
		t.Fatal("failed sync reported success")
	}
	fs.fail.Store(false)
	if _, err := s.Count(); err == nil {
		t.Fatal("uncertain queue remained usable")
	}
	if err := s.Save(rows[1]); err == nil {
		t.Fatal("duplicate bypassed failed-store guard")
	}
	if err := s.Delete(rows[1]); err == nil {
		t.Fatal("failed-store allowed deletion")
	}
}

func TestGeoRetryBatchPagePersistsBeforeDelivery(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			w, refs, _, _, _, _ := geoBatchFixture(t, 32)
			s, fs := openRetrySyncStore(t)
			defer s.Close()
			w.MirrorRetry = s
			single := w.MirrorGeo
			var first sync.Once
			var calls atomic.Int64
			check := func() {
				first.Do(func() {
					n, e := s.Count()
					if e != nil || n != 64 {
						t.Errorf("page delivery preceded full durable debt: %d %v", n, e)
					}
				})
			}
			submit := w.Submit
			w.Submit = func(c context.Context, g string, b []byte) error { calls.Add(1); check(); return submit(c, g, b) }
			w.MirrorGeo = batchMirrorFunc{single: func(c context.Context, n uint64, g string, b []byte) (enginestate.Result, error) {
				calls.Add(1)
				check()
				return single.MirrorGeo(c, n, g, b)
			}, batch: func(c context.Context, n uint64, g string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
				calls.Add(1)
				check()
				rows := make([]raftrpc.GeoMirrorBatchResult, len(commands))
				for i, b := range commands {
					rows[i].Result, rows[i].Err = single.MirrorGeo(c, n, g, b)
				}
				return rows, nil
			}}
			fs.fail.Store(fail)
			rows, err := w.RefreshGeoConfirmationPage(context.Background(), refs, 3, 3)
			fs.fail.Store(false)
			if fail {
				if err == nil || calls.Load() != 0 {
					t.Fatal("delivery after failed ledger sync", err, calls.Load())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range rows {
				if !r.Confirmed || r.Err != nil {
					t.Fatal(r)
				}
			}
			if n, e := s.Count(); n != 0 || e != nil {
				t.Fatal(n, e)
			}
		})
	}
}

// Real disk diagnostic only; no environment-dependent latency assertion.
func TestGeoRetryBatchDiskDiagnostic(t *testing.T) {
	rows := retryBatchMirrors(t, 64)
	for _, mode := range []string{"sequential", "batch"} {
		s, fs := openRetrySyncStore(t)
		before := fs.syncs.Load()
		start := time.Now()
		if mode == "batch" {
			if e := s.saveMirrors(rows); e != nil {
				t.Fatal(e)
			}
		} else {
			for _, r := range rows {
				if e := s.Save(r); e != nil {
					t.Fatal(e)
				}
			}
		}
		t.Logf("mode=%s rows=%d elapsed_us=%d syncs=%d", mode, len(rows), time.Since(start).Microseconds(), fs.syncs.Load()-before)
		if e := s.Close(); e != nil {
			t.Fatal(e)
		}
	}
}

func TestGeoRetryBatchDeleteAndCursorSyncFailuresFenceStore(t *testing.T) {
	for _, op := range []string{"delete", "cursor"} {
		t.Run(op, func(t *testing.T) {
			rows := retryBatchMirrors(t, 1)
			s, fs := openRetrySyncStore(t)
			defer s.Close()
			if err := s.Save(rows[0]); err != nil {
				t.Fatal(err)
			}
			fs.fail.Store(true)
			var err error
			if op == "delete" {
				err = s.Delete(rows[0])
			} else {
				err = s.MarkAttempt(rows[0])
			}
			fs.fail.Store(false)
			if err == nil {
				t.Fatal("failed sync reported success")
			}
			if _, e := s.Count(); e == nil {
				t.Fatal("uncertain store still usable")
			}
			if _, _, e := s.NextPage(8); e == nil {
				t.Fatal("uncertain debt offered for retry")
			}
		})
	}
}

func TestGeoRetryBatchDoesNotHideProgrammingPanics(t *testing.T) {
	s := new(GeoMetadataRetryStore)
	defer func() {
		if r := recover(); r != "programming fault" {
			t.Fatalf("unexpected recovered value: %v", r)
		}
		if s.failed != nil {
			t.Fatal("programming panic converted to storage error")
		}
	}()
	var err error
	defer s.recoverWriteFatal(&err)
	panic("programming fault")
}
