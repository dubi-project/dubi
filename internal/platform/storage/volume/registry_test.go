package volume

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/identity"
)

func testStore(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	root, err := canonicalDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func status(t *testing.T, r *Registry) Report {
	t.Helper()
	report, err := r.Status()
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func closeRegistry(t *testing.T, r *Registry) {
	t.Helper()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultVolumePersistsAndAddKeepsIt(t *testing.T) {
	root := testStore(t)
	r, err := Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	first := status(t, r)
	if len(first.Volumes) != 1 || first.Volumes[0].Path != filepath.Join(root, "volume") ||
		first.Volumes[0].State != Active || first.Volumes[0].Health != "healthy" {
		t.Fatalf("default: %+v", first)
	}
	if _, err := Open(root); err == nil {
		t.Fatal("second registry acquired the same Store")
	}
	second, err := r.Add(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if second.ID == first.Volumes[0].ID {
		t.Fatal("volume IDs were reused")
	}
	again, err := r.Add(second.Path)
	if err != nil || again != second {
		t.Fatalf("retry: %+v %v", again, err)
	}
	expanded := status(t, r)
	if len(expanded.Volumes) != 2 || expanded.Volumes[0].Record != first.Volumes[0].Record {
		t.Fatal("add changed default volume")
	}
	if expanded.Volumes[0].Device == expanded.Volumes[1].Device && expanded.Capacity.TotalBytes != expanded.Volumes[0].Capacity.TotalBytes {
		t.Fatal("same filesystem capacity counted twice")
	}
	if _, err := Inspect(root); err != nil {
		t.Fatalf("live read-only inspection: %v", err)
	}
	closeRegistry(t, r)
	r, err = Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	recovered := status(t, r)
	if recovered.StoreID != expanded.StoreID || len(recovered.Volumes) != 2 || recovered.Volumes[1].Record != second {
		t.Fatalf("restart changed identity/set: %+v", recovered)
	}
}

func TestVolumeDrainStopsWritesAndNeverDeletesPackData(t *testing.T) {
	root := testStore(t)
	r, err := Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	volumeID := status(t, r).Volumes[0].ID
	draining, err := r.BeginDrain(volumeID)
	if err != nil || draining.State != Draining {
		t.Fatal(draining, err)
	}
	if err := r.WithVolume(volumeID, false, func(*os.Root, string) error { return nil }); err != nil {
		t.Fatal("draining volume lost read access", err)
	}
	if err := r.WithVolume(volumeID, true, func(*os.Root, string) error { return nil }); err == nil {
		t.Fatal("draining volume remained writable", err)
	}
	if _, err := r.RemoveDrained(volumeID); err != nil {
		t.Fatal("empty volume should be removable", err)
	}
	if got := status(t, r).Volumes[0].State; got != Removed {
		t.Fatal("volume did not persist Removed state", got)
	}
}

func TestVolumeRemoveAllowsExplicitRemovalOfMissingDrainedDisk(t *testing.T) {
	root := testStore(t)
	r, err := Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	record := status(t, r).Volumes[0].Record
	if _, err := r.BeginDrain(record.ID); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(record.Path, record.Path+"-failed"); err != nil {
		t.Fatal(err)
	}
	removed, err := r.RemoveDrained(record.ID)
	if err != nil || removed.State != Removed {
		t.Fatal("explicit removal of failed drained disk was rejected", removed, err)
	}
}

func TestExplicitVolumesNeverCreateDefault(t *testing.T) {
	root := testStore(t)
	paths := []string{t.TempDir(), t.TempDir()}
	r, err := Initialize(root, paths)
	if err != nil {
		t.Fatal(err)
	}
	report := status(t, r)
	closeRegistry(t, r)
	for _, retryPaths := range [][]string{nil, {paths[1], paths[0]}} {
		r, err = Initialize(root, retryPaths)
		if err != nil {
			t.Fatal(err)
		}
		if status(t, r).StoreID != report.StoreID {
			t.Fatal("StoreID changed")
		}
		closeRegistry(t, r)
		if _, err := os.Lstat(filepath.Join(root, "volume")); !os.IsNotExist(err) {
			t.Fatalf("default directory created: %v", err)
		}
	}
	if _, err := Initialize(root, []string{t.TempDir()}); err == nil {
		t.Fatal("init replaced volume set")
	}
}

func TestMissingOrWrongVolumeIsNotRecreated(t *testing.T) {
	for _, fault := range []string{"directory", "empty-mountpoint", "marker", "wrong-marker", "symlink"} {
		t.Run(fault, func(t *testing.T) {
			root := testStore(t)
			r, err := Initialize(root, nil)
			if err != nil {
				t.Fatal(err)
			}
			record := status(t, r).Volumes[0].Record
			closeRegistry(t, r)
			switch fault {
			case "directory", "empty-mountpoint", "symlink":
				if err := os.Rename(record.Path, record.Path+"-offline"); err != nil {
					t.Fatal(err)
				}
				if fault == "symlink" {
					if err := os.Symlink(record.Path+"-offline", record.Path); err != nil {
						t.Fatal(err)
					}
				} else if fault == "empty-mountpoint" {
					if err := os.Mkdir(record.Path, 0o750); err != nil {
						t.Fatal(err)
					}
				}
			case "marker":
				if err := os.Remove(filepath.Join(record.Path, markerName)); err != nil {
					t.Fatal(err)
				}
			case "wrong-marker":
				if err := os.WriteFile(filepath.Join(record.Path, markerName), []byte("{}"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			r, err = Initialize(root, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			report := status(t, r)
			if report.Volumes[0].Health == "healthy" || report.Capacity.TotalBytes != 0 {
				t.Fatalf("fault accepted: %+v", report)
			}
			wantHealth := "invalid"
			if fault == "directory" {
				wantHealth = "missing"
			}
			if report.Volumes[0].Health != wantHealth {
				t.Fatalf("wrong fault classification: got %q, want %q", report.Volumes[0].Health, wantHealth)
			}
			if report.Volumes[0].Record != record {
				t.Fatal("health failure changed lifecycle or identity")
			}
			if fault == "directory" {
				if _, err := os.Lstat(record.Path); !os.IsNotExist(err) {
					t.Fatal("missing directory recreated")
				}
			}
			if fault == "marker" {
				if _, err := os.Lstat(filepath.Join(record.Path, markerName)); !os.IsNotExist(err) {
					t.Fatal("missing marker recreated")
				}
			}
		})
	}
}

func TestRemovedDefaultStaysRemoved(t *testing.T) {
	root := testStore(t)
	r, err := Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Add(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	// Simulate a future completed Drain. No public removal API is exposed
	// before the data migration and clearance implementation is available.
	r.manifest.Volumes[0].State = Removed
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
	path := r.manifest.Volumes[0].Path
	closeRegistry(t, r)
	if err := os.Rename(path, path+"-removed"); err != nil {
		t.Fatal(err)
	}
	r, err = Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if status(t, r).Volumes[0].State != Removed {
		t.Fatal("removed default reactivated")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("removed default recreated")
	}
}

func TestPendingRegistrationRecovery(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-marker", true: "after-marker"}[published], func(t *testing.T) {
			root := testStore(t)
			r, err := Initialize(root, nil)
			if err != nil {
				t.Fatal(err)
			}
			record, err := r.prepare(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			r.manifest.Volumes = append(r.manifest.Volumes, record)
			if err := r.save(); err != nil {
				t.Fatal(err)
			}
			if published {
				if err := writeJSON(filepath.Join(record.Path, markerName), ".test-", marker{owner: r.manifest.owner, VolumeID: record.ID}, true); err != nil {
					t.Fatal(err)
				}
			}
			closeRegistry(t, r)
			r, err = Open(root)
			if err != nil {
				t.Fatal(err)
			}
			if status(t, r).Volumes[1].Health != "pending" {
				t.Fatal("open silently activated pending volume")
			}
			closeRegistry(t, r)
			r, err = Initialize(root, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got := status(t, r).Volumes[1]
			if got.ID != record.ID || got.State != Active || got.Health != "healthy" {
				t.Fatalf("pending resume: %+v", got)
			}
		})
	}
}

func TestPendingRegistrationRejectsReplacedDirectory(t *testing.T) {
	root := testStore(t)
	r, err := Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	record, err := r.prepare(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r.manifest.Volumes = append(r.manifest.Volumes, record)
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
	closeRegistry(t, r)
	if err := os.Rename(record.Path, record.Path+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(record.Path, 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := Initialize(root, nil); err == nil {
		t.Fatal("replacement pending directory accepted")
	}
	if _, err := os.Stat(filepath.Join(record.Path, markerName)); !os.IsNotExist(err) {
		t.Fatal("replacement got a marker")
	}
}

func TestInvalidPathsDoNotRegister(t *testing.T) {
	root := testStore(t)
	r, err := Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	nonempty := t.TempDir()
	if err := os.WriteFile(filepath.Join(nonempty, "user-file"), []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	inside := filepath.Join(root, "metadata")
	if err := os.Mkdir(inside, 0o750); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "volume", "nested")
	if err := os.Mkdir(nested, 0o750); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(t.TempDir(), alias); err != nil {
		t.Fatal(err)
	}
	before := r.manifest
	for _, path := range []string{"", root, filepath.Dir(root), inside, nested, nonempty, alias, filepath.Join(t.TempDir(), "missing")} {
		if _, err := r.Add(path); err == nil {
			t.Fatalf("accepted %q", path)
		}
	}
	if !reflect.DeepEqual(before, r.manifest) {
		t.Fatal("invalid paths changed registry")
	}
}

func TestRegistryLossCorruptionAndForeignIdentityAreRejected(t *testing.T) {
	for _, fault := range []string{"missing", "corrupt", "foreign", "unknown-field"} {
		t.Run(fault, func(t *testing.T) {
			root := testStore(t)
			r, err := Initialize(root, []string{t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			closeRegistry(t, r)
			path := filepath.Join(root, manifestName)
			switch fault {
			case "missing":
				err = os.Remove(path)
			case "corrupt":
				err = os.WriteFile(path, []byte("{"), 0o600)
			case "foreign":
				r.manifest.NodeID = "node_00000000000000000000000000000000"
				err = r.save()
			case "unknown-field":
				encoded, marshalErr := json.Marshal(r.manifest)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				encoded = append(encoded[:len(encoded)-1], []byte(",\"unknown\":true}")...)
				err = os.WriteFile(path, encoded, 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Initialize(root, nil); err == nil {
				t.Fatal("invalid registry recreated or accepted")
			}
			if _, err := os.Lstat(filepath.Join(root, "volume")); !os.IsNotExist(err) {
				t.Fatal("fallback default created")
			}
		})
	}
}

func TestClonedNodeCannotOpenSamePhysicalVolume(t *testing.T) {
	root := testStore(t)
	r, err := Initialize(root, []string{t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	clone := t.TempDir()
	for _, name := range []string{"DUBI_NODE_IDENTITY", manifestName, storageName} {
		encoded, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(clone, name), encoded, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if second, err := Open(clone); err == nil {
		_ = second.Close()
		t.Fatal("cloned node opened the same volume")
	}
}
