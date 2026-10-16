package pack

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

func setup(t *testing.T) (*volume.Registry, volume.Status) {
	t.Helper()
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	r, err := volume.Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	report, err := r.Status()
	if err != nil {
		t.Fatal(err)
	}
	return r, report.Volumes[0]
}

func create(t *testing.T, r *volume.Registry, v volume.Status, target int64) *Pack {
	t.Helper()
	p, err := Create(r, v.ID, target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	return p
}

func appendRecord(t *testing.T, p *Pack, key string) Location {
	t.Helper()
	loc, err := p.Append(Record{EngineID: "record", TenantID: "tenant-a", Key: []byte(key), Payload: []byte("value-" + key)})
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func path(v volume.Status, p *Pack, name string) string {
	return filepath.Join(v.Path, p.directoryName(), name)
}

func TestAppendScanRestartAndSeal(t *testing.T) {
	r, v := setup(t)
	p := create(t, r, v, 0)
	if p.header.TargetSize != DefaultTargetSize {
		t.Fatal("wrong default target")
	}
	records := []Record{
		{EngineID: "record", TenantID: "tenant-a", Key: []byte("duplicate"), Payload: []byte("one")},
		{EngineID: "record", TenantID: "tenant-b", Key: []byte("duplicate"), Payload: []byte("two")},
		{EngineID: "record", TenantID: "tenant-a", Key: []byte("binary\x00key"), Payload: []byte{}},
	}
	var locations []Location
	for _, record := range records {
		loc, err := p.Append(record)
		if err != nil {
			t.Fatal(err)
		}
		locations = append(locations, loc)
	}
	if _, err := Open(r, v.ID, p.Scope().PackID); err == nil {
		t.Fatal("concurrent handle accepted")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := Open(r, v.ID, locations[0].PackID)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i, loc := range locations {
		got, err := p.Read(loc)
		if err != nil || !reflect.DeepEqual(got, records[i]) {
			t.Fatalf("read: %+v %v", got, err)
		}
	}
	var scanned []Location
	if err := p.Scan(func(loc Location, record Record) error {
		if !reflect.DeepEqual(record, records[len(scanned)]) {
			t.Fatal("scan changed module bytes")
		}
		scanned = append(scanned, loc)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(locations, scanned) {
		t.Fatal("scan lost append order or location")
	}
	if err := p.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := p.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append(records[0]); !errors.Is(err, ErrSealed) {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = Open(r, v.ID, locations[0].PackID)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Append(records[0]); !errors.Is(err, ErrSealed) {
		t.Fatal("seal lost on restart", err)
	}
	if _, err := p.Read(locations[0]); err != nil {
		t.Fatal(err)
	}
}

func TestWholeRecordMayCrossTarget(t *testing.T) {
	r, v := setup(t)
	p := create(t, r, v, headerSize+30)
	loc := appendRecord(t, p, "large")
	if p.state.End <= p.header.TargetSize || !p.state.Sealed {
		t.Fatal("record was split or pack not sealed")
	}
	if _, err := p.Read(loc); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("k")}); !errors.Is(err, ErrSealed) {
		t.Fatal(err)
	}
	next := create(t, r, v, 0)
	if next.Scope().PackID == p.Scope().PackID {
		t.Fatal("reused PackID")
	}
	appendRecord(t, next, "next")
	if _, err := next.Read(loc); err == nil {
		t.Fatal("cross-Pack location accepted")
	}
}

func TestRecoveryDiscardsOnlyUncommittedTail(t *testing.T) {
	for _, tail := range [][]byte{[]byte("DPR"), bytes.Repeat([]byte{0xff}, 80), mustEncode(t, Record{EngineID: "record", TenantID: "t", Key: []byte("unacked"), Payload: []byte("whole")})} {
		t.Run(fmt.Sprint(len(tail)), func(t *testing.T) {
			r, v := setup(t)
			p := create(t, r, v, 0)
			loc := appendRecord(t, p, "acked")
			end := p.state.End
			// Simulate data reaching disk without publication of the checkpoint.
			if _, err := p.file.WriteAt(tail, end); err != nil {
				t.Fatal(err)
			}
			if err := p.file.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			p, err := Open(r, v.ID, loc.PackID)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			info, err := p.file.Stat()
			if err != nil || info.Size() != end {
				t.Fatal("uncommitted tail not truncated", err)
			}
			if _, err := p.Read(loc); err != nil {
				t.Fatal(err)
			}
			newLoc := appendRecord(t, p, "after-recovery")
			if newLoc.Offset != end {
				t.Fatal("incorrect append position")
			}
		})
	}
}

func TestCommittedCorruptionNeverTruncated(t *testing.T) {
	for _, kind := range []string{"header", "record-header", "tenant", "payload", "short", "state", "missing-state", "count", "sealed-tail"} {
		t.Run(kind, func(t *testing.T) {
			r, v := setup(t)
			p := create(t, r, v, 0)
			loc := appendRecord(t, p, "acked")
			if kind == "sealed-tail" {
				if err := p.Seal(); err != nil {
					t.Fatal(err)
				}
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			dataPath, statePath := path(v, p, p.dataName()), path(v, p, p.stateName())
			switch kind {
			case "state":
				flip(t, statePath, 20)
			case "missing-state":
				if err := os.Remove(statePath); err != nil {
					t.Fatal(err)
				}
			case "count":
				state := p.state
				state.Count = 0
				b, err := encodeDocument("DUBIPC01", stateSize, state)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(statePath, b, 0o600); err != nil {
					t.Fatal(err)
				}
			case "short":
				if err := os.Truncate(dataPath, p.state.End-1); err != nil {
					t.Fatal(err)
				}
			case "sealed-tail":
				f, err := os.OpenFile(dataPath, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write([]byte("unexplained")); err != nil {
					t.Fatal(err)
				}
				_ = f.Close()
			default:
				offset := int64(20)
				if kind == "record-header" {
					offset = loc.Offset + 12
				}
				if kind == "tenant" {
					offset = loc.Offset + recordHeaderSize
				}
				if kind == "payload" {
					offset = loc.Offset + loc.Length - 1
				}
				flip(t, dataPath, offset)
			}
			before, err := os.ReadFile(dataPath)
			if err != nil {
				t.Fatal(err)
			}
			opened, err := Open(r, v.ID, loc.PackID)
			if err == nil {
				_ = opened.Close()
				t.Fatal("damaged committed Pack opened")
			}
			after, err := os.ReadFile(dataPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("recovery modified committed corruption")
			}
		})
	}
}

func TestReadDetectsDamageAndRejectsBadLocations(t *testing.T) {
	r, v := setup(t)
	p := create(t, r, v, 0)
	loc := appendRecord(t, p, "data")
	for _, field := range []string{"tenant-scope", "offset", "length", "checksum"} {
		bad := loc
		switch field {
		case "tenant-scope":
			bad.VolumeID = "foreign-volume"
		case "offset":
			bad.Offset = -1
		case "length":
			bad.Length++
		case "checksum":
			bad.CRC32C++
		}
		if _, err := p.Read(bad); err == nil {
			t.Fatal("accepted invalid location", field)
		}
	}
	flip(t, path(v, p, p.dataName()), loc.Offset+loc.Length-1)
	if _, err := p.Read(loc); !errors.Is(err, ErrCorrupt) {
		t.Fatal("unchecked data", err)
	}
}

func TestVolumeAndPackLossNeverRecreate(t *testing.T) {
	for _, kind := range []string{"volume", "marker", "data", "state", "directory", "closed-registry"} {
		t.Run(kind, func(t *testing.T) {
			r, v := setup(t)
			p := create(t, r, v, 0)
			loc := appendRecord(t, p, "one")
			missing := ""
			switch kind {
			case "volume":
				missing = v.Path
			case "marker":
				missing = filepath.Join(v.Path, "DUBI_VOLUME")
			case "data":
				missing = path(v, p, p.dataName())
			case "state":
				missing = path(v, p, p.stateName())
			case "directory":
				missing = filepath.Join(v.Path, p.directoryName())
			case "closed-registry":
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if missing != "" {
				if err := os.Rename(missing, missing+".offline"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := p.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("k")}); err == nil {
				t.Fatal("write to lost resource")
			}
			if _, err := p.Read(loc); err == nil {
				t.Fatal("read through stale resource")
			}
			if err := p.Seal(); err == nil {
				t.Fatal("seal through stale resource")
			}
			if kind == "volume" || kind == "marker" || kind == "closed-registry" {
				if extra, err := Create(r, v.ID, 0); err == nil {
					_ = extra.Close()
					t.Fatal("created on missing volume")
				}
			}
			if missing != "" {
				if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("resource recreated", err)
				}
			}
		})
	}
}

func TestValidationAndScopeBinding(t *testing.T) {
	r, v := setup(t)
	for _, engine := range []string{"../escape", "", "a/b"} {
		if _, err := encodeRecord(Record{EngineID: engine, TenantID: "t", Key: []byte("k")}); err == nil {
			t.Fatal("accepted engine", engine)
		}
	}
	for _, target := range []int64{-1, headerSize, MaxTargetSize + 1} {
		if p, err := Create(r, v.ID, target); err == nil {
			_ = p.Close()
			t.Fatal("accepted target", target)
		}
	}
	p := create(t, r, v, 0)
	for _, record := range []Record{{Key: []byte("k")}, {EngineID: "record", TenantID: "t"}, {EngineID: "record", TenantID: "t", Key: make([]byte, MaxKeySize+1)}, {EngineID: "record", TenantID: string(make([]byte, MaxTenantSize+1)), Key: []byte("k")}} {
		if _, err := p.Append(record); err == nil {
			t.Fatal("invalid record accepted")
		}
	}
	loc := appendRecord(t, p, "one")
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	// Moving bytes to another Volume does not change their durable owner.
	otherVolume, err := r.Add(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(v.Path, "packs"), filepath.Join(otherVolume.Path, "packs")); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(r, otherVolume.ID, loc.PackID); err == nil {
		_ = other.Close()
		t.Fatal("cross-Volume header accepted")
	}
}

func TestConcurrentAppend(t *testing.T) {
	r, v := setup(t)
	p := create(t, r, v, 0)
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Go(func() {
			loc, err := p.Append(Record{EngineID: "record", TenantID: "t", Key: []byte(fmt.Sprint(i)), Payload: []byte("data")})
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := p.Read(loc); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	count := 0
	if err := p.Scan(func(Location, Record) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	if count != 24 {
		t.Fatal("lost append", count)
	}
}

// A subprocess exits without closing Pack or Registry. This exercises lock
// release and real process-boundary recovery, including a fully fsynced but
// unpublished record. It is not a simulation of hardware lying about fsync.
func TestProcessCrash(t *testing.T) {
	if root := os.Getenv("DUBI_PACK_CRASH_ROOT"); root != "" {
		r, err := volume.Open(root)
		if err != nil {
			t.Fatal(err)
		}
		report, err := r.Status()
		if err != nil {
			t.Fatal(err)
		}
		p, err := Create(r, report.Volumes[0].ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		loc := appendRecord(t, p, "confirmed")
		if os.Getenv("DUBI_PACK_CRASH_PHASE") == "published" {
			// Storage completed the second append, but its caller never receives
			// a response/location. Recovery must retain this uncertain outcome.
			appendRecord(t, p, "response-lost")
		} else {
			tail := mustEncode(t, Record{EngineID: "record", TenantID: "t", Key: []byte("unconfirmed")})
			if _, err := p.file.WriteAt(tail, p.state.End); err != nil {
				t.Fatal(err)
			}
			if err := p.file.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if err := json.NewEncoder(os.Stdout).Encode(loc); err != nil {
			t.Fatal(err)
		}
		os.Exit(23)
	}
	for _, phase := range []string{"unpublished", "published"} {
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
			cmd := exec.Command(os.Args[0], "-test.run=^TestProcessCrash$")
			cmd.Env = append(os.Environ(), "DUBI_PACK_CRASH_ROOT="+root, "DUBI_PACK_CRASH_PHASE="+phase)
			out, err := cmd.Output()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 23 {
				t.Fatalf("child: %s %v", out, err)
			}
			var loc Location
			if err := json.Unmarshal(bytes.TrimSpace(out), &loc); err != nil {
				t.Fatal(err, string(out))
			}
			r, err = volume.Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			p, err := Open(r, loc.VolumeID, loc.PackID)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err := p.Read(loc); err != nil {
				t.Fatal(err)
			}
			expected := uint64(1)
			if phase == "published" {
				expected = 2
			}
			if p.state.Count != expected {
				t.Fatal("recovered wrong commit boundary", p.state.Count, expected)
			}
			appendRecord(t, p, "after-crash")
		})
	}
}

func TestReplacedPackAndSymlinkDirectoryRejected(t *testing.T) {
	r, v := setup(t)
	p := create(t, r, v, 0)
	loc := appendRecord(t, p, "one")
	dataPath := path(v, p, p.dataName())
	b, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dataPath, dataPath+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Read(loc); err == nil {
		t.Fatal("replaced Pack file accepted")
	}
	if _, err := p.Append(Record{EngineID: "record", TenantID: "t", Key: []byte("k")}); err == nil {
		t.Fatal("wrote after file replacement")
	}
	otherVolume, err := r.Add(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(otherVolume.Path, "packs")); err != nil {
		t.Fatal(err)
	}
	if extra, err := Create(r, otherVolume.ID, 0); err == nil {
		_ = extra.Close()
		t.Fatal("symlink Pack directory accepted")
	}
}

func flip(t *testing.T, path string, offset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], offset); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], offset); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

func mustEncode(t *testing.T, r Record) []byte {
	t.Helper()
	b, err := encodeRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func FuzzRecordHeader(f *testing.F) {
	b, _ := encodeRecord(Record{EngineID: "record", TenantID: "tenant", Key: []byte("key"), Payload: []byte("value")})
	f.Add(b[:recordHeaderSize])
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, recordHeaderSize))
	f.Fuzz(func(t *testing.T, b []byte) {
		engine, tenant, key, payload, _, err := decodeRecordHeader(b)
		if err == nil && (engine <= 0 || engine > MaxEngineSize || tenant <= 0 || tenant > MaxTenantSize || key <= 0 || key > MaxKeySize || payload < 0 || payload > MaxPayloadSize) {
			t.Fatal("unchecked lengths")
		}
	})
}
