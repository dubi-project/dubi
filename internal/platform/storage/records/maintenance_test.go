package records

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

type checkpointBlockingPool struct {
	*pack.Pool
	captured, resume chan struct{}
}

func (p *checkpointBlockingPool) SnapshotPacks(ctx context.Context) ([]pack.SnapshotPack, error) {
	packs, err := p.Pool.SnapshotPacks(ctx)
	if err != nil {
		return nil, err
	}
	close(p.captured)
	<-p.resume
	return packs, nil
}

func TestCheckpointPinsMatchingIndexThroughConcurrentWriteAndRewrite(t *testing.T) {
	ctx := context.Background()
	root := createPoolRoot(t)
	r, p := realPool(t, root)
	s, err := OpenNodeStore(p, r)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	d := descriptor(t, "checkpoint-old", "old-body")
	if _, err := s.Put(ctx, d, []byte("old-body")); err != nil {
		t.Fatal(err)
	}
	blocking := &checkpointBlockingPool{Pool: p, captured: make(chan struct{}), resume: make(chan struct{})}
	s.packs = blocking
	type result struct {
		packs []pack.SnapshotPack
		image []byte
		err   error
	}
	done := make(chan result, 1)
	go func() { packs, image, err := s.SnapshotWithPacks(ctx); done <- result{packs, image, err} }()
	<-blocking.captured
	later := descriptor(t, "checkpoint-new", "later-body")
	writeDone := make(chan error, 1)
	go func() { _, err := s.Put(ctx, later, []byte("later-body")); writeDone <- err }()
	select {
	case err := <-writeDone:
		t.Fatal("write escaped the checkpoint lock", err)
	case <-time.After(15 * time.Millisecond):
	}
	close(blocking.resume)
	captured := <-done
	if captured.err != nil {
		t.Fatal(captured.err)
	}
	if p.PinnedPacks() != int64(len(captured.packs)) {
		t.Fatal("checkpoint pin accounting missing")
	}
	defer func() {
		for i := range captured.packs {
			_ = captured.packs[i].Close()
		}
	}()
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	s.packs = p
	var index IndexSnapshot
	if err := json.Unmarshal(captured.image, &index); err != nil || len(index.Entries) != 1 {
		t.Fatal("checkpoint captured a later index with an older Pack list", index, err)
	}
	if _, err := s.RewriteCompaction(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(captured.packs[0].Path); !os.IsNotExist(err) {
		t.Fatal("test did not reclaim the original Pack", err)
	}
	// Restore the pinned, now-unlinked Pack and captured index into a new Store.
	newRoot := createPoolRoot(t)
	newRegistry, newPool := realPool(t, newRoot)
	newStore, err := OpenNodeStore(newPool, newRegistry)
	if err != nil {
		t.Fatal(err)
	}
	defer newStore.Close()
	report, err := newRegistry.Status()
	if err != nil {
		t.Fatal(err)
	}
	target := report.Volumes[0].ID
	for _, item := range captured.packs {
		reader, err := item.Open()
		if err != nil {
			t.Fatal(err)
		}
		err = newPool.ImportSealed(ctx, target, item.PackID, reader, item.Size)
		_ = reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := newStore.RestoreIndex(ctx, captured.image, func(loc pack.Location) (pack.Location, error) {
		loc.StoreID = newPool.StoreID()
		loc.VolumeID = target
		return loc, nil
	}); err != nil {
		t.Fatal(err)
	}
	if body, _, err := newStore.Read(ctx, d); err != nil || string(body) != "old-body" {
		t.Fatal("GC destroyed checkpoint bytes", string(body), err)
	}
	if _, _, err := newStore.Read(ctx, later); err != ErrNotHeld {
		t.Fatal("future write leaked into older checkpoint", err)
	}
	copyOfPin := captured.packs[0]
	if err := captured.packs[0].Close(); err != nil {
		t.Fatal(err)
	}
	if err := copyOfPin.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := copyOfPin.Open(); err == nil {
		t.Fatal("copied checkpoint handle reopened a released pin")
	}
	if p.PinnedPacks() != 0 {
		t.Fatal("checkpoint handles leaked pins", p.PinnedPacks())
	}

}

func TestVolumeBalanceResumesWithoutDrainingAndReclaimsSelectedPack(t *testing.T) {
	ctx := context.Background()
	root := createPoolRoot(t)
	r, p := realPool(t, root)
	s, err := OpenNodeStore(p, r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	descriptors := []Descriptor{descriptor(t, "balance-a", "a"), descriptor(t, "balance-b", "b")}
	for i, d := range descriptors {
		if _, err := s.Put(ctx, d, []byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	report, err := r.Status()
	if err != nil {
		t.Fatal(err)
	}
	source := report.Volumes[0].ID
	target, err := r.Add(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	plan := volume.BalancePlan{Source: source, Target: target.ID, MaximumBytes: 64 << 20}
	started, err := s.BeginVolumeBalance(ctx, plan)
	if err != nil || !started {
		t.Fatal(started, err)
	}
	moved, done, err := s.MigrateVolumeStep(ctx, source)
	if err != nil || done || moved != 1 {
		t.Fatal(moved, done, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenNodeStore(p, r)
	if err != nil {
		t.Fatal(err)
	}
	status, err := s.VolumeMigration()
	if err != nil || !status.Active || status.TargetVolumeID != target.ID || status.SourcePackID == "" || status.Migrated != 1 {
		t.Fatal(status, err)
	}
	if _, err := s.MigrateVolume(ctx, source); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenNodeStore(p, r)
	if err != nil {
		t.Fatal(err)
	}
	pendingGC, err := s.VolumeMigration()
	if err != nil || !pendingGC.Active || pendingGC.SourcePackID == "" {
		t.Fatal("copy completion lost durable collection debt", pendingGC, err)
	}
	if moved, done, err := s.MigrateVolumeStep(ctx, source); err != nil || moved != 0 || !done {
		t.Fatal("restart did not resume collection phase", moved, done, err)
	}
	if _, err := s.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteVolumeBalance(source, target.ID, pendingGC.SourcePackID); err != nil {
		t.Fatal(err)
	}
	report, err = r.Status()
	if err != nil {
		t.Fatal(err)
	}
	if report.Volumes[0].State != volume.Active {
		t.Fatal("capacity balance drained the source", report)
	}
	for _, d := range descriptors {
		if location(t, s, d).VolumeID != target.ID {
			t.Fatal("balance resumed to wrong target")
		}
		if _, err := s.Verify(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	// An empty source must not generate an endless migration task.
	if started, err := s.BeginVolumeBalance(ctx, plan); err != nil || started {
		t.Fatal(started, err)
	}
}

func TestThrottledRewriteYieldsStoreLockToForeground(t *testing.T) {
	root := createPoolRoot(t)
	r, p := realPool(t, root)
	s, err := OpenNodeStore(p, r)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	d := descriptor(t, "yield", "payload")
	if _, err := s.Put(context.Background(), d, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	scheduler := resource.NewMaintenance(1)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	ctx, finish, err := scheduler.TryStart(ctx, "compaction")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	rewritten := make(chan error, 1)
	go func() { _, err := s.RewriteCompaction(ctx); rewritten <- err }()
	deadline := time.Now().Add(time.Second)
	for scheduler.Status().AccountedBytes == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	readDone := make(chan error, 1)
	go func() { _, err := s.Verify(context.Background(), d); readDone <- err }()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("throttled compaction held Store lock")
	}
	if err := <-rewritten; err == nil {
		t.Fatal("tiny IO budget failed to bound compaction")
	}
}
