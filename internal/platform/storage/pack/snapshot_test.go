package pack

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
)

func TestPoolSnapshotPacksSealsAndEnumeratesLocalFiles(t *testing.T) {
	r, _ := setup(t)
	p, err := OpenPool(r)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	loc := poolAppend(t, p, "file", "checkpoint")
	items, err := p.SnapshotPacks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].StoreID != loc.StoreID || items[0].VolumeID != loc.VolumeID || items[0].PackID != loc.PackID {
		t.Fatalf("unexpected snapshot packs: %+v", items)
	}
	info, err := os.Stat(items[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != items[0].Size || info.Size() <= 0 {
		t.Fatalf("snapshot size = %d, recorded %d", info.Size(), items[0].Size)
	}
	reader, err := items[0].Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Compact(context.Background(), func(Location) bool { return false }); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(items[0].Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("compaction did not unlink the sealed Pack: %v", err)
	}
	data, readErr := io.ReadAll(reader)
	if closeErr := reader.Close(); readErr != nil || closeErr != nil || !bytes.Contains(data, []byte("DPR2")) {
		t.Fatalf("snapshot reader failed: read=%v close=%v", readErr, closeErr)
	}
	if p.collections[loc.VolumeID].active != nil {
		t.Fatal("snapshot left an active Pack")
	}
	if err := items[0].Close(); err != nil {
		t.Fatal(err)
	}
}
