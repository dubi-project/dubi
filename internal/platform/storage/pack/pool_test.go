package pack

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

func poolAppend(t *testing.T, p *Pool, engine, key string) Location {
	t.Helper()
	loc, err := p.Append(context.Background(), Record{EngineID: engine, TenantID: "tenant", Key: []byte(key), Payload: []byte("data")})
	if errors.Is(err, volume.ErrInsufficientSpace) {
		t.Skip("integration test requires disk headroom above the safety floor")
	}
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func TestPoolSelectsVolumesAndPreservesMixedPacks(t *testing.T) {
	r, v := setup(t)
	other, err := r.Add(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := OpenPool(r)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	a := poolAppend(t, p, "file", "a")
	b := poolAppend(t, p, "message", "b")
	c := poolAppend(t, p, "search", "c")
	if a.VolumeID != v.ID || b.VolumeID != other.ID || c.VolumeID != v.ID || c.PackID != a.PackID {
		t.Fatal("selection/physical sharing", a, b, c)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = OpenPool(r)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i, loc := range []Location{a, b, c} {
		record, err := p.Read(loc)
		if err != nil || record.EngineID != []string{"file", "message", "search"}[i] {
			t.Fatal(record, err)
		}
	}
	if err := os.Rename(v.Path, v.Path+".offline"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Read(a); err == nil {
		t.Fatal("read silently substituted another Volume")
	}
	if loc := poolAppend(t, p, "cache", "healthy"); loc.VolumeID != other.ID {
		t.Fatal("selected missing Volume")
	}
	if _, err := os.Stat(v.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing Volume recreated")
	}
}

func TestPoolDrainSealsWritesButPreservesPackReads(t *testing.T) {
	r, v := setup(t)
	p, err := OpenPool(r)
	if err != nil {
		t.Fatal(err)
	}
	loc := poolAppend(t, p, "file", "before-drain")
	if err := p.DrainVolume(context.Background(), v.ID); err != nil {
		t.Fatal("drain failed", err)
	}
	if _, err := p.Append(context.Background(), Record{EngineID: "file", TenantID: "tenant", Key: []byte("after-drain"), Payload: []byte("data")}); err == nil {
		t.Fatal("draining Volume accepted a new Pack write")
	}
	if record, err := p.Read(loc); err != nil || string(record.Key) != "before-drain" {
		t.Fatal("draining Volume lost a committed Pack read", record, err)
	}
	if err := p.RemoveDrained(context.Background(), v.ID); !errors.Is(err, volume.ErrVolumeHasData) {
		t.Fatal("non-empty Volume removal was not rejected", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPoolNoCrossVolumeRetryAfterWriteError(t *testing.T) {
	r, v := setup(t)
	other, err := r.Add(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := OpenPool(r)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	a := poolAppend(t, p, "file", "a")
	b := poolAppend(t, p, "message", "b")
	if a.VolumeID != v.ID || b.VolumeID != other.ID {
		t.Fatal("unexpected selection")
	}
	before := p.collections[b.VolumeID].active.state
	state := filepath.Join(v.Path, packDirectory, a.PackID+".state")
	if err := os.Rename(state, state+".lost"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append(context.Background(), Record{EngineID: "file", TenantID: "t", Key: []byte("uncertain")}); err == nil {
		t.Fatal("damaged write accepted")
	}
	if len(p.WriteErrors()) != 1 || p.WriteErrors()[v.ID] == "" {
		t.Fatal("failed Volume not write-blocked")
	}
	if p.collections[b.VolumeID].active.state != before {
		t.Fatal("failed request retried on healthy Volume")
	}
	if next := poolAppend(t, p, "search", "next-request"); next.VolumeID != b.VolumeID {
		t.Fatal("new request did not avoid write-blocked Volume")
	}
	if _, err := p.Read(b); err != nil {
		t.Fatal("healthy data became unreadable", err)
	}
}

func TestPoolReadNeedsNoSpaceAndDefersPendingCreation(t *testing.T) {
	r, v, m := collection(t, headerSize+1)
	loc, err := m.Append(Record{EngineID: "file", TenantID: "t", Key: []byte("confirmed")})
	if err != nil {
		t.Fatal(err)
	}
	id, err := randomID()
	if err != nil {
		t.Fatal(err)
	}
	next := m.catalog
	next.Pending = id
	if err := m.save(next); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	// Occupy the admission budget in memory, without filling the real disk.
	report, err := r.Status()
	if err != nil {
		t.Fatal(err)
	}
	s := report.Volumes[0].Capacity
	floor := max(volume.MinFreeBytes, s.TotalBytes/20)
	if s.AvailableBytes <= floor+(64<<20) {
		t.Skip("requires test filesystem headroom")
	}
	lease, err := r.Reserve(s.AvailableBytes-floor-(1<<20), []string{v.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	p, err := OpenPool(r)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Append(context.Background(), Record{EngineID: "record", TenantID: "t", Key: []byte("no-space")}); !errors.Is(err, volume.ErrInsufficientSpace) {
		t.Fatal("reservation did not block append", err)
	}
	if _, err := p.Read(loc); err != nil {
		t.Fatal("low-space read failed", err)
	}
	if p.collections[v.ID].catalog.Pending != id {
		t.Fatal("read consumed Pending intent")
	}
	if _, err := os.Stat(filepath.Join(v.Path, packDirectory, id+".pack")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("read created Pending Pack", err)
	}
	lease.Release()
	if next := poolAppend(t, p, "message", "after-space-release"); next.PackID != id {
		t.Fatal("budgeted write did not resume pending ID")
	}
}

func TestPoolCancellationValidationAndClose(t *testing.T) {
	r, v := setup(t)
	p, err := OpenPool(r)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Append(ctx, Record{EngineID: "file", TenantID: "t", Key: []byte("k")}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := p.Append(context.Background(), Record{}); err == nil {
		t.Fatal("invalid record accepted")
	}
	if _, err := os.Stat(filepath.Join(v.Path, packDirectory)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cancelled/invalid append created files")
	}
	if appendBudget(Record{EngineID: "file", TenantID: "t", Key: []byte("k")}) != (4<<20)+(64<<10) {
		t.Fatal("unexpected append budget")
	}
	loc := poolAppend(t, p, "record", "ok")
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Read(loc); err == nil {
		t.Fatal("read through closed Pool")
	}
	if _, err := p.Append(context.Background(), Record{EngineID: "file", TenantID: "t", Key: []byte("k")}); err == nil {
		t.Fatal("append through closed Pool")
	}
	// Collection locks are released; the registry remains usable.
	m, err := OpenCollection(r, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err := m.Read(loc); err != nil {
		t.Fatal(err)
	}
}
