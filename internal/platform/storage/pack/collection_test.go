package pack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

func collection(t *testing.T, target int64) (*volume.Registry, volume.Status, *Collection) {
	t.Helper()
	r, v := setup(t)
	m, err := InitializeCollection(r, v.ID, target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return r, v, m
}

func TestCollectionRolloverRestartAndScope(t *testing.T) {
	r, v, m := collection(t, headerSize+100)
	var locations []Location
	for i := range 11 {
		loc, err := m.Append(Record{EngineID: "record", TenantID: fmt.Sprint("tenant-", i%2), Key: []byte("same-key"), Payload: bytes.Repeat([]byte{byte(i)}, 30)})
		if err != nil {
			t.Fatal(err)
		}
		locations = append(locations, loc)
	}
	if len(m.catalog.Packs) != 6 || m.active == nil {
		t.Fatal("wrong rollover count or active Pack", len(m.catalog.Packs))
	}
	id := m.catalog.CollectionID
	if other, err := OpenCollection(r, v.ID); err == nil {
		_ = other.Close()
		t.Fatal("concurrent collection opened")
	}
	if p, err := Create(r, v.ID, 0); err == nil {
		_ = p.Close()
		t.Fatal("standalone Create bypassed collection")
	}
	if p, err := Open(r, v.ID, locations[0].PackID); err == nil {
		_ = p.Close()
		t.Fatal("standalone Open bypassed collection")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := InitializeCollection(r, v.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.catalog.CollectionID != id || m.catalog.TargetSize != headerSize+100 {
		t.Fatal("identity/target changed")
	}
	var scanned []Location
	if err := m.Scan(func(loc Location, record Record) error {
		if len(record.Payload) != 30 || record.Payload[0] != byte(len(scanned)) {
			t.Fatal("scan reordered records")
		}
		scanned = append(scanned, loc)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(scanned, locations) {
		t.Fatal("scan changed stable locations")
	}
	for i, loc := range locations {
		record, err := m.Read(loc)
		if err != nil || record.Payload[0] != byte(i) {
			t.Fatal("read", err)
		}
	}
	otherVolume, err := r.Add(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	other, err := InitializeCollection(r, otherVolume.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Read(locations[0]); err == nil {
		t.Fatal("cross-volume location accepted")
	}
	bad := locations[0]
	bad.PackID = "pack_00000000000000000000000000000000"
	if _, err := m.Read(bad); err == nil {
		t.Fatal("unlisted Pack accepted")
	}
}

func TestPoolImportSealedRewritesScopeAndPreservesLocations(t *testing.T) {
	sourceRegistry, sourceVolume := setup(t)
	sourcePool, err := OpenPool(sourceRegistry)
	if err != nil {
		t.Fatal(err)
	}
	loc, err := sourcePool.Append(context.Background(), Record{EngineID: "file", TenantID: "tenant", Key: []byte("key"), Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	items, err := sourcePool.SnapshotPacks(context.Background())
	if err != nil || len(items) != 1 {
		t.Fatal("snapshot", err, len(items))
	}
	defer items[0].Close()
	image, err := items[0].Open()
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(image)
	_ = image.Close()
	if err != nil {
		t.Fatal(err)
	}

	destinationRegistry, destinationVolume := setup(t)
	destinationPool, err := OpenPool(destinationRegistry)
	if err != nil {
		t.Fatal(err)
	}
	if err := destinationPool.ImportSealed(context.Background(), destinationVolume.ID, loc.PackID, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal("import", err)
	}
	if loc.StoreID == destinationPool.StoreID() || sourceVolume.ID == destinationVolume.ID {
		t.Fatal("test did not create distinct restore identities")
	}
	restored := loc
	restored.StoreID = destinationPool.StoreID()
	restored.VolumeID = destinationVolume.ID
	got, err := destinationPool.Read(restored)
	if err != nil || string(got.Payload) != "payload" {
		t.Fatalf("restored Pack read = %+v, %v", got, err)
	}
	if err := destinationPool.ImportSealed(context.Background(), destinationVolume.ID, loc.PackID, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal("idempotent Pack import failed", err)
	}
	_ = sourcePool.Close()
	_ = destinationPool.Close()
}

func TestCollectionExplicitSealAndInvalidAppend(t *testing.T) {
	r, v, m := collection(t, 0)
	if err := m.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Append(Record{}); err == nil {
		t.Fatal("invalid append accepted")
	}
	if len(m.catalog.Packs) != 0 || m.catalog.Pending != "" {
		t.Fatal("empty/invalid operation created a Pack")
	}
	loc, err := m.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("k")})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := m.Seal(); err != nil {
		t.Fatal(err)
	}
	if m.active != nil || len(m.catalog.Packs) != 1 {
		t.Fatal("Seal created successor or kept FD")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := InitializeCollection(r, v.ID, headerSize+1); err == nil {
		_ = other.Close()
		t.Fatal("target changed on retry")
	}
	m, err = OpenCollection(r, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	next, err := m.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("next")})
	if err != nil {
		t.Fatal(err)
	}
	if next.PackID == loc.PackID {
		t.Fatal("appended into sealed Pack")
	}
}

func TestCollectionCompactRemovesOnlyUnreachableSealedPacks(t *testing.T) {
	r, v, m := collection(t, headerSize+1)
	first, err := m.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("old"), Payload: []byte("old")})
	if err != nil {
		t.Fatal(err)
	}
	live, err := m.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("live"), Payload: []byte("live")})
	if err != nil {
		t.Fatal(err)
	}
	if first.PackID == live.PackID || m.active != nil {
		t.Fatal("test did not create sealed Packs")
	}
	if _, err := r.BeginDrain(v.ID); err != nil {
		t.Fatal("drain test volume", err)
	}
	report, err := m.Compact(context.Background(), func(loc Location) bool { return loc == live })
	if err != nil || report.ScannedPacks != 2 || report.RemovedPacks != 1 || report.ReclaimedBytes <= 0 {
		t.Fatal("unexpected compact report", report, err)
	}
	if _, err := m.Read(first); err == nil {
		t.Fatal("unreachable Pack record remained readable")
	}
	if got, err := m.Read(live); err != nil || string(got.Payload) != "live" {
		t.Fatal("live Pack record was removed", got, err)
	}
	if _, err := os.Stat(filepath.Join(v.Path, "packs", first.PackID+".pack")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("old Pack file remained", err)
	}
	_ = r
}

func TestCollectionMissingMetadataNeverReinitialized(t *testing.T) {
	for _, what := range []string{"catalog", "marker", "directory", "lock", "pack", "checkpoint", "volume"} {
		t.Run(what, func(t *testing.T) {
			r, v, m := collection(t, headerSize+1)
			loc, err := m.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("k")})
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(v.Path, "packs")
			paths := map[string]string{"catalog": filepath.Join(dir, catalogName), "marker": filepath.Join(v.Path, collectionMarkerName), "directory": dir, "lock": filepath.Join(dir, collectionLockName), "pack": filepath.Join(dir, loc.PackID+".pack"), "checkpoint": filepath.Join(dir, loc.PackID+".state"), "volume": v.Path}
			missing := paths[what]
			if err := os.Rename(missing, missing+".offline"); err != nil {
				t.Fatal(err)
			}
			if _, err := m.Read(loc); err == nil {
				t.Fatal("read through lost resource")
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			for _, initialize := range []bool{false, true} {
				other, err := openCollection(r, v.ID, 0, initialize)
				if err == nil {
					_ = other.Close()
					t.Fatal("silently accepted loss", initialize)
				}
			}
			if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("missing resource recreated", err)
			}
			if what == "directory" {
				if p, err := Create(r, v.ID, 0); err == nil {
					_ = p.Close()
					t.Fatal("standalone recreated missing collection")
				}
				if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("standalone recreated directory")
				}
			}
		})
	}
}

func TestCollectionDoesNotAdoptOrphans(t *testing.T) {
	r, v := setup(t)
	p := create(t, r, v, 0)
	appendRecord(t, p, "standalone")
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if m, err := InitializeCollection(r, v.ID, 0); err == nil {
		_ = m.Close()
		t.Fatal("adopted standalone Pack")
	}
	// In an already initialized collection, unlisted files are retained but never
	// scanned or admitted as data. Maintenance/GC needs a separate proof protocol.
	otherVolume, err := r.Add(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m, err := InitializeCollection(r, otherVolume.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	orphan, err := open(r, otherVolume.ID, "pack_00000000000000000000000000000001", DefaultTargetSize, openMode{create: true, managed: true})
	if err != nil {
		t.Fatal(err)
	}
	loc := appendRecord(t, orphan, "orphan")
	if err := orphan.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Read(loc); err == nil {
		t.Fatal("read orphan")
	}
	count := 0
	if err := m.Scan(func(Location, Record) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("scanned orphan")
	}
}

func TestCollectionCorruptionFailsClosed(t *testing.T) {
	for _, what := range []string{"catalog-crc", "duplicate", "active-middle", "pending-with-active", "marker", "record"} {
		t.Run(what, func(t *testing.T) {
			r, v, m := collection(t, 0)
			loc, err := m.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("k")})
			if err != nil {
				t.Fatal(err)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			dirPath := filepath.Join(v.Path, "packs")
			switch what {
			case "catalog-crc":
				flip(t, filepath.Join(dirPath, catalogName), 20)
			case "marker":
				flip(t, filepath.Join(v.Path, collectionMarkerName), 20)
			case "record":
				flip(t, filepath.Join(dirPath, loc.PackID+".pack"), loc.Offset+loc.Length-1)
			default:
				c := m.catalog
				switch what {
				case "duplicate":
					c.Packs = append(c.Packs, loc.PackID)
				case "active-middle":
					c.Packs = append(c.Packs, "pack_00000000000000000000000000000001")
				case "pending-with-active":
					c.Pending = "pack_00000000000000000000000000000001"
				}
				dir, err := os.OpenRoot(dirPath)
				if err != nil {
					t.Fatal(err)
				}
				if err := writeDocument(dir, catalogName, "DUBICS02", c, false); err != nil {
					t.Fatal(err)
				}
				_ = dir.Close()
			}
			if other, err := OpenCollection(r, v.ID); err == nil {
				_ = other.Close()
				t.Fatal("accepted corrupt collection")
			}
		})
	}
}

func TestCollectionConcurrentRollover(t *testing.T) {
	r, v, m := collection(t, headerSize+1)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			loc, err := m.Append(Record{EngineID: "record", TenantID: "t", Key: []byte(fmt.Sprint(i))})
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := m.Read(loc); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if len(m.catalog.Packs) != 20 || m.active != nil {
		t.Fatal("lost records or leaked active handle")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := OpenCollection(r, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	count := 0
	if err := m.Scan(func(Location, Record) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 20 {
		t.Fatal("restart lost records")
	}
}

func TestPendingPackUnexpectedDataIsNeverTruncated(t *testing.T) {
	for _, kind := range []string{"without-checkpoint", "uncommitted-tail", "committed-record", "foreign-header"} {
		t.Run(kind, func(t *testing.T) {
			r, v, m := collection(t, 0)
			id, err := randomID()
			if err != nil {
				t.Fatal(err)
			}
			next := m.catalog
			next.Pending = id
			if err := m.save(next); err != nil {
				t.Fatal(err)
			}
			if err := m.access(func(dir *os.Root) error { return m.initializePending(dir, id) }); err != nil {
				t.Fatal(err)
			}
			dataPath := filepath.Join(v.Path, "packs", id+".pack")
			statePath := filepath.Join(v.Path, "packs", id+".state")
			switch kind {
			case "committed-record":
				p, err := open(r, v.ID, id, 0, openMode{managed: true})
				if err != nil {
					t.Fatal(err)
				}
				appendRecord(t, p, "illegal-pending-record")
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
			case "foreign-header":
				if err := os.Remove(statePath); err != nil {
					t.Fatal(err)
				}
				h := header{Scope: Scope{StoreID: m.catalog.StoreID, VolumeID: "foreign-volume", PackID: id}, Version: 2, TargetSize: DefaultTargetSize}
				b, err := encodeDocument("DUBIPK02", headerSize, h)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(dataPath, b, 0o600); err != nil {
					t.Fatal(err)
				}
			default:
				if kind == "without-checkpoint" {
					if err := os.Remove(statePath); err != nil {
						t.Fatal(err)
					}
				}
				f, err := os.OpenFile(dataPath, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write([]byte("unexpected-data")); err != nil {
					t.Fatal(err)
				}
				_ = f.Close()
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(dataPath)
			if err != nil {
				t.Fatal(err)
			}
			if other, err := OpenCollection(r, v.ID); err == nil {
				_ = other.Close()
				t.Fatal("accepted data in pending Pack")
			}
			after, err := os.ReadFile(dataPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("recovery modified unexpected pending bytes")
			}
		})
	}
}

func TestCollectionInitialPublicationResume(t *testing.T) {
	r, v, m := collection(t, 0)
	id := m.catalog.CollectionID
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	// Empty catalog published, sentinel not yet published: Initialize may finish
	// this phase, but Open must not silently initialize missing metadata.
	if err := os.Remove(filepath.Join(v.Path, collectionMarkerName)); err != nil {
		t.Fatal(err)
	}
	if other, err := OpenCollection(r, v.ID); err == nil {
		_ = other.Close()
		t.Fatal("Open recreated identity")
	}
	m, err := InitializeCollection(r, v.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.catalog.CollectionID != id {
		t.Fatal("initial publication changed identity")
	}
	loc, err := m.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("k")})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(v.Path, "packs", catalogName), filepath.Join(v.Path, "packs", catalogName+".lost")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("next")}); err == nil {
		t.Fatal("write through lost catalog")
	}
	if _, err := m.Read(loc); err == nil {
		t.Fatal("read through lost catalog")
	}
}

func TestCollectionCrashRecovery(t *testing.T) {
	if root := os.Getenv("DUBI_COLLECTION_CRASH_ROOT"); root != "" {
		r, err := volume.Open(root)
		if err != nil {
			t.Fatal(err)
		}
		report, err := r.Status()
		if err != nil {
			t.Fatal(err)
		}
		m, err := InitializeCollection(r, report.Volumes[0].ID, headerSize+1)
		if err != nil {
			t.Fatal(err)
		}
		loc, err := m.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("confirmed")})
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
		phase := os.Getenv("DUBI_COLLECTION_CRASH_PHASE")
		if phase == "header" {
			if err := m.access(func(dir *os.Root) error {
				f, err := dir.OpenFile(id+".pack", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
				if err != nil {
					return err
				}
				_, err = f.Write([]byte("DUBIPK02"))
				return errors.Join(err, f.Sync(), f.Close(), syncDirectory(dir))
			}); err != nil {
				t.Fatal(err)
			}
		}
		if phase == "files" {
			if err := m.access(func(dir *os.Root) error { return m.initializePending(dir, id) }); err != nil {
				t.Fatal(err)
			}
		}
		if phase == "catalog" || phase == "response-lost" {
			if err := m.finishPending(); err != nil {
				t.Fatal(err)
			}
		}
		if phase == "response-lost" {
			if _, err := m.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("uncertain-success")}); err != nil {
				t.Fatal(err)
			}
		}
		if err := json.NewEncoder(os.Stdout).Encode(struct {
			Location Location
			Pending  string
		}{loc, id}); err != nil {
			t.Fatal(err)
		}
		os.Exit(29) // No Close: process termination releases the actual file locks.
	}
	for _, phase := range []string{"intent", "header", "files", "catalog", "response-lost"} {
		t.Run(phase, func(t *testing.T) {
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
			cmd := exec.Command(os.Args[0], "-test.run=^TestCollectionCrashRecovery$")
			cmd.Env = append(os.Environ(), "DUBI_COLLECTION_CRASH_ROOT="+root, "DUBI_COLLECTION_CRASH_PHASE="+phase)
			out, err := cmd.Output()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 29 {
				t.Fatalf("child %s %v", out, err)
			}
			var result struct {
				Location Location
				Pending  string
			}
			if err := json.Unmarshal(bytes.TrimSpace(out), &result); err != nil {
				t.Fatal(err, string(out))
			}
			r, err = volume.Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			m, err := OpenCollection(r, result.Location.VolumeID)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			if len(m.catalog.Packs) != 2 || m.catalog.Packs[1] != result.Pending || m.catalog.Pending != "" {
				t.Fatal("pending identity not resumed", m.catalog)
			}
			if _, err := m.Read(result.Location); err != nil {
				t.Fatal(err)
			}
			count := 0
			if err := m.Scan(func(Location, Record) error { count++; return nil }); err != nil {
				t.Fatal(err)
			}
			expected := 1
			if phase == "response-lost" {
				expected = 2
			}
			if count != expected {
				t.Fatal("wrong committed record count", count, expected)
			}
			if _, err := m.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("after-crash")}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
