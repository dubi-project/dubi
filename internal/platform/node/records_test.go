package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

func TestRuntimeRecordsInitializeRestartAndRelease(t *testing.T) {
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	if err := InitializeRoot(root); err != nil {
		t.Fatal(err)
	}
	options := Options{StoreRoot: root, TickInterval: time.Hour, CapabilityMux: nodeTestMux(t)}
	r, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if r.Records == nil {
		t.Fatal("missing records")
	}
	d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", CapabilityID: "search", DatasetID: "dataset"}, "origin-group", []byte("key"), []byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	h, err := r.Records.Put(context.Background(), d, []byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	if h.StoreID != r.Packs.StoreID() {
		t.Fatal("records did not share Node Pack pool")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Records.Verify(context.Background(), d); err == nil {
		t.Fatal("closed runtime leaked record handle")
	}
	// Repeated explicit init validates the same mandatory child; it does not
	// erase records or require the originating metadata Group to be hosted.
	if err := InitializeRoot(root); err != nil {
		t.Fatal(err)
	}
	r, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	value, next, err := r.Records.Read(context.Background(), d)
	if err != nil || string(value) != "data" || next.RecordID != h.RecordID || next.StoreID != h.StoreID {
		t.Fatal(next, err)
	}
	b, err := r.Volumes.Resource(volume.ImmutableRecordsResource)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CheckActive(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeRecordsLostResourceRejectsInitAndOpen(t *testing.T) {
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	if err := InitializeRoot(root); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"DUBI_RECORDS", "records"} {
		path := filepath.Join(root, name)
		if err := os.Rename(path, path+".lost"); err != nil {
			t.Fatal(err)
		}
	}
	if err := InitializeRoot(root); err == nil {
		t.Fatal("repeat init recreated active records")
	}
	options := Options{StoreRoot: root, TickInterval: time.Hour, CapabilityMux: nodeTestMux(t)}
	for i := 0; i < 2; i++ {
		if r, err := Open(options); err == nil {
			r.Close()
			t.Fatal("startup recreated active records")
		}
	}
	for _, name := range []string{"DUBI_RECORDS", "records"} {
		path := filepath.Join(root, name)
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("resource recreated", err)
		}
		if err := os.Rename(path+".lost", path); err != nil {
			t.Fatal(err)
		}
	}
	r, err := Open(options)
	if err != nil {
		t.Fatal("failed startup leaked resource ownership", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}
