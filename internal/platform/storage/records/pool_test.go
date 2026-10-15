package records

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

func createPoolRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	r, err := volume.Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return root
}
func realPool(t *testing.T, root string) (*volume.Registry, *pack.Pool) {
	t.Helper()
	r, err := volume.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	p, err := pack.OpenPool(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	return r, p
}

func TestRecordSharedPackReopenAndLostVolume(t *testing.T) {
	root := createPoolRoot(t)
	r, p := realPool(t, root)
	s, err := OpenNodeStore(p, r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	var descriptors []Descriptor
	var locations []pack.Location
	for i, capa := range []string{"file", "cache", "message", "search"} {
		d := descriptor(t, "same-key", "data")
		d.Keyspace.CapabilityID = capa
		if i%2 == 0 {
			d.Keyspace.TenantID = "other-tenant"
		}
		put(t, s, d, "data")
		descriptors = append(descriptors, d)
		locations = append(locations, location(t, s, d))
	}
	for _, l := range locations {
		if l.PackID != locations[0].PackID || l.VolumeID != locations[0].VolumeID {
			t.Fatal("split Pack by logical scope", locations)
		}
	}
	if err := errors.Join(s.Close(), p.Close(), r.Close()); err != nil {
		t.Fatal(err)
	}
	r, p = realPool(t, root)
	s, err = OpenNodeStore(p, r)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range descriptors {
		put(t, s, d, "data")
		if _, err := s.Verify(context.Background(), d); err != nil {
			t.Fatal(err)
		}
	}
	for i, d := range descriptors {
		if got := location(t, s, d); got != locations[i] {
			t.Fatal("reopen appended duplicate", got, locations[i])
		}
	}
	report, err := r.Status()
	if err != nil {
		t.Fatal(err)
	}
	path := report.Volumes[0].Path
	if err := os.Rename(path, path+".offline"); err != nil {
		t.Fatal(err)
	}
	for _, d := range descriptors {
		if h, err := s.Verify(context.Background(), d); !errors.Is(err, ErrUnavailable) || h.RecordID != "" {
			t.Fatal("missing Volume produced holding", h, err)
		}
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("lost Volume recreated", err)
	}
	if err := os.Rename(path+".offline", path); err != nil {
		t.Fatal(err)
	}
	for _, d := range descriptors {
		if _, err := s.Verify(context.Background(), d); err != nil {
			t.Fatal("restored Volume was not physically reverified", err)
		}
	}
}

func TestRecordRelocatesFromLostVolumeToAddedVolume(t *testing.T) {
	root := createPoolRoot(t)
	r, p := realPool(t, root)
	s, err := OpenNodeStore(p, r)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close() // Capture this handle; a reopened handle is closed separately.
	d := descriptor(t, "lost-volume", "replacement-data")
	put(t, s, d, "replacement-data")
	old := location(t, s, d)
	status, err := r.Status()
	if err != nil || len(status.Volumes) != 1 {
		t.Fatal(status, err)
	}
	oldPath := status.Volumes[0].Path
	if err := os.Rename(oldPath, oldPath+".offline"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Verify(context.Background(), d); !errors.Is(err, ErrUnavailable) {
		t.Fatal("lost Volume remained a holding", err)
	}
	newPath := t.TempDir() // Additional Volumes live outside StoreRoot.
	newVolume, err := r.Add(newPath)
	if err != nil {
		t.Fatal("adding a replacement Volume failed while the old one was missing", err)
	}
	h, err := s.RepairExisting(context.Background(), d, []byte("replacement-data"))
	if err != nil || h.VolumeID != newVolume.ID {
		t.Fatal("repair did not relocate onto the healthy Volume", h, err)
	}
	if got := location(t, s, d); got == old || got.VolumeID != newVolume.ID {
		t.Fatal("old location remained authoritative", got)
	}
	if err := errors.Join(s.Close(), p.Close(), r.Close()); err != nil {
		t.Fatal(err)
	}
	r, p = realPool(t, root)
	s, err = OpenNodeStore(p, r)
	if err != nil {
		t.Fatal("reopening with the old Volume still missing failed", err)
	}
	defer s.Close()
	if _, err := s.Verify(context.Background(), d); err != nil {
		t.Fatal("replacement did not survive restart", err)
	}
}

func TestVolumeMigrationPersistsNewLocationsAndEmptiesSource(t *testing.T) {
	root := createPoolRoot(t)
	r, p := realPool(t, root)
	s, err := OpenNodeStore(p, r)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	descriptors := []Descriptor{descriptor(t, "migrate-a", "a"), descriptor(t, "migrate-b", "b")}
	for _, d := range descriptors {
		if _, err := s.Put(context.Background(), d, []byte(string(d.Key[len(d.Key)-1:]))); errors.Is(err, volume.ErrInsufficientSpace) {
			t.Skip("migration test requires disk headroom above the safety floor")
		} else if err != nil {
			t.Fatal(err)
		}
	}
	report, err := r.Status()
	if err != nil || len(report.Volumes) != 1 {
		t.Fatal(report, err)
	}
	source := report.Volumes[0]
	if _, err := r.Add(t.TempDir()); err != nil {
		t.Fatal("add replacement Volume", err)
	}
	if err := p.DrainVolume(context.Background(), source.ID); err != nil {
		t.Fatal("drain source", err)
	}
	if err := s.BeginVolumeMigration(source.ID); err != nil {
		t.Fatal(err)
	}
	moved, err := s.MigrateVolume(context.Background(), source.ID)
	if err != nil || moved != uint64(len(descriptors)) {
		t.Fatal("migration did not move every record", moved, err)
	}
	for _, d := range descriptors {
		loc := location(t, s, d)
		if loc.VolumeID == source.ID {
			t.Fatal("index still points at drained Volume", loc)
		}
		if _, err := s.Verify(context.Background(), d); err != nil {
			t.Fatal("migrated record is not readable", err)
		}
	}
	if status, err := s.VolumeMigration(); err != nil || status.Active {
		t.Fatal("migration task was not cleared after completion", status, err)
	}
	if _, err := s.Compact(context.Background()); err != nil {
		t.Fatal("collect source Pack", err)
	}
	if err := p.RemoveDrained(context.Background(), source.ID); err != nil {
		t.Fatal("remove empty drained Volume", err)
	}
}

func TestRecordProcessCrashRecovery(t *testing.T) {
	if root := os.Getenv("DUBI_RECORD_CRASH_ROOT"); root != "" {
		_, p := realPool(t, root)
		stage := os.Getenv("DUBI_RECORD_CRASH_STAGE")
		hook := func(got string) error {
			if got == stage {
				os.Exit(73)
			}
			return nil
		}
		s, err := open(root, p, true, hook)
		if err != nil {
			t.Fatal(err)
		}
		if stage == "before-index-sync" || stage == "after-index-sync" {
			s.commit = func(b *pebble.Batch) error {
				if stage == "after-index-sync" {
					if err := b.Commit(pebble.Sync); err != nil {
						t.Fatal(err)
					}
				}
				os.Exit(73)
				return nil
			}
			put(t, s, descriptor(t, "crash-key", "durable-data"), "durable-data")
		}
		t.Fatal("crash point not reached")
	}
	for _, stage := range []string{"after-directory", "after-intent", "after-index-open", "after-index-identity", "after-active", "before-index-sync", "after-index-sync"} {
		t.Run(stage, func(t *testing.T) {
			root := createPoolRoot(t)
			cmd := exec.Command(os.Args[0], "-test.run=^TestRecordProcessCrashRecovery$", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), "DUBI_RECORD_CRASH_ROOT="+root, "DUBI_RECORD_CRASH_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 73 {
				t.Fatalf("child did not crash at %s: %v\n%s", stage, err, out)
			}
			_, p := realPool(t, root)
			s, err := open(root, p, stage == "after-directory", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			d := descriptor(t, "crash-key", "durable-data")
			_, err = s.Verify(context.Background(), d)
			if stage == "after-index-sync" {
				if err != nil {
					t.Fatal("acknowledged index lost", err)
				}
			} else if !errors.Is(err, ErrNotHeld) {
				t.Fatal("unpublished record became held", err)
			}
			put(t, s, d, "durable-data")
			if _, err := s.Verify(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(root, registrationFile)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
