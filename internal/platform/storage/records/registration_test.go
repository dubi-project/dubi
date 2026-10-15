package records

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
)

var interrupted = errors.New("interrupted registration")

func interruptAt(want string) func(string) error {
	return func(stage string) error {
		if stage == want {
			return interrupted
		}
		return nil
	}
}

func TestRecordRegistrationInterruptedCreation(t *testing.T) {
	for _, stage := range []string{"after-directory", "after-intent", "after-index-open", "after-index-identity", "after-active"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			p := newMemoryPacks()
			if s, err := open(root, p, true, interruptAt(stage)); s != nil || !errors.Is(err, interrupted) {
				t.Fatal(s, err)
			}
			initialize := stage == "after-directory"
			if initialize {
				if s, err := open(root, p, false, nil); err == nil {
					s.Close()
					t.Fatal("missing registration recreated")
				}
			}
			s, err := open(root, p, initialize, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r, err := readRegistration(root)
			if err != nil || r.State != "active" || r.Device != 0 || r.Inode != 0 {
				t.Fatal(r, err)
			}
			put(t, s, descriptor(t, "recovered", "data"), "data")
		})
	}
}

func TestRecordRegistrationExclusiveAndExplicitCreation(t *testing.T) {
	root := t.TempDir()
	p := newMemoryPacks()
	if s, err := open(root, p, false, nil); err == nil {
		s.Close()
		t.Fatal("open created unregistered index")
	}
	if _, err := os.Stat(filepath.Join(root, directory)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("open created directory", err)
	}
	s, err := open(root, p, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, initialize := range []bool{false, true} {
		if other, err := open(root, p, initialize, nil); err == nil {
			other.Close()
			t.Fatal("second owner accepted")
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := open(root, p, true, nil); err == nil {
		other.Close()
		t.Fatal("initialize reused active registration")
	}
	foreign := newMemoryPacks()
	foreign.id = "store_11111111111111111111111111111111"
	if other, err := open(root, foreign, false, nil); err == nil {
		other.Close()
		t.Fatal("foreign Store accepted")
	}
	if _, err := OpenNodeStore(nil, nil); err == nil {
		t.Fatal("nil pool")
	}
}

func TestRecordRegistrationLossCannotBecomeEmptyStore(t *testing.T) {
	for _, part := range []string{"marker", "directory", "database", "whole-resource", "empty-database"} {
		t.Run(part, func(t *testing.T) {
			s, p := testStore(t)
			root := s.root
			put(t, s, descriptor(t, "held", "data"), "data")
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			paths := []string{}
			switch part {
			case "marker":
				paths = []string{filepath.Join(root, registrationFile)}
			case "directory":
				paths = []string{filepath.Join(root, directory)}
			case "database", "empty-database":
				paths = []string{filepath.Join(root, directory, "index")}
			case "whole-resource":
				paths = []string{filepath.Join(root, registrationFile), filepath.Join(root, directory)}
			}
			for _, path := range paths {
				if err := os.Rename(path, path+".lost"); err != nil {
					t.Fatal(err)
				}
			}
			if part == "empty-database" {
				if err := os.Mkdir(paths[0], 0700); err != nil {
					t.Fatal(err)
				}
			}
			before := treeDigest(t, root)
			if other, err := open(root, p, false, nil); err == nil {
				other.Close()
				t.Fatal("lost resource recreated")
			}
			if after := treeDigest(t, root); !reflect.DeepEqual(before, after) {
				t.Fatal("rejected reopen modified files", before, after)
			}
		})
	}
}

func treeDigest(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	out := map[string][32]byte{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			v, err := os.Readlink(path)
			out[path] = sha256.Sum256([]byte(v))
			return err
		}
		b, err := os.ReadFile(path)
		out[path] = sha256.Sum256(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRecordRegistrationRejectsForeignDatabaseBeforeWritableOpen(t *testing.T) {
	left, p := testStore(t)
	right, _ := testStore(t)
	put(t, left, descriptor(t, "left", "data"), "data")
	put(t, right, descriptor(t, "right", "data"), "data")
	if err := errors.Join(left.Close(), right.Close()); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(left.root, directory, "index")
	source := filepath.Join(right.root, directory, "index")
	if err := os.Rename(target, target+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(source, target); err != nil {
		t.Fatal(err)
	}
	before := treeDigest(t, left.root)
	if s, err := open(left.root, p, false, nil); err == nil {
		s.Close()
		t.Fatal("different resource with same StoreID accepted")
	}
	if after := treeDigest(t, left.root); !reflect.DeepEqual(before, after) {
		t.Fatal("foreign database modified")
	}
}

func TestRecordRegistrationRejectsUnidentifiedDatabase(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "pending"}[pending], func(t *testing.T) {
			root := t.TempDir()
			p := newMemoryPacks()
			if pending {
				if _, err := open(root, p, true, interruptAt("after-index-open")); !errors.Is(err, interrupted) {
					t.Fatal(err)
				}
			} else {
				s, err := open(root, p, true, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			db, err := pebbleutil.Open(filepath.Join(root, directory, "index"), &pebble.Options{Logger: pebbleutil.QuietLogger{}})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Delete(identityKey, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			if pending {
				if err := db.Set([]byte("foreign"), []byte("data"), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before := treeDigest(t, root)
			if s, err := open(root, p, false, nil); err == nil {
				s.Close()
				t.Fatal("unidentified database accepted")
			}
			if after := treeDigest(t, root); !reflect.DeepEqual(before, after) {
				t.Fatal("unidentified database modified")
			}
		})
	}
}

func TestRecordPendingDirectoryReplacementAndUnregisteredData(t *testing.T) {
	root := t.TempDir()
	p := newMemoryPacks()
	if _, err := open(root, p, true, interruptAt("after-intent")); !errors.Is(err, interrupted) {
		t.Fatal(err)
	}
	dir := filepath.Join(root, directory)
	if err := os.Rename(dir, dir+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if s, err := open(root, p, false, nil); err == nil {
		s.Close()
		t.Fatal("pending directory identity ignored")
	}
	other := t.TempDir()
	dir = filepath.Join(other, directory)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "unregistered-data"), []byte("bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := open(other, p, true, nil); err == nil {
		s.Close()
		t.Fatal("adopted unknown content")
	}
	if _, err := os.Stat(filepath.Join(other, registrationFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("published intent over unknown content", err)
	}
}

func TestRecordResourceSymlinksRejected(t *testing.T) {
	for _, part := range []string{"root", "lock", "registration", "directory", "database"} {
		t.Run(part, func(t *testing.T) {
			s, p := testStore(t)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			path := s.root
			switch part {
			case "lock":
				path = filepath.Join(path, ".DUBI_RECORDS.lock")
			case "registration":
				path = filepath.Join(path, registrationFile)
			case "directory":
				path = filepath.Join(path, directory)
			case "database":
				path = filepath.Join(path, directory, "index")
			}
			if err := os.Rename(path, path+".real"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(path+".real", path); err != nil {
				t.Fatal(err)
			}
			if part == "root" {
				t.Cleanup(func() { os.Remove(path); os.Rename(path+".real", path) })
			}
			if other, err := open(s.root, p, false, nil); err == nil {
				other.Close()
				t.Fatal("symlink accepted")
			}
		})
	}
}

func TestRecordRegistrationStrictDocument(t *testing.T) {
	for _, kind := range []string{"invalid", "oversized", "unknown", "trailing", "state", "resource", "store", "active-device"} {
		t.Run(kind, func(t *testing.T) {
			s, p := testStore(t)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			r := s.identity
			switch kind {
			case "state":
				r.State = "other"
			case "resource":
				r.ResourceID = strings.Repeat("A", 32)
			case "store":
				r.StoreID = "store_wrong"
			case "active-device":
				r.Device = 1
			}
			data, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "invalid":
				data = []byte(`{`)
			case "oversized":
				data = bytes.Repeat([]byte{' '}, 4097)
			case "unknown":
				data = append([]byte(`{"extra":1,`), data[1:]...)
			case "trailing":
				data = append(data, []byte(`{}`)...)
			}
			if err := os.WriteFile(filepath.Join(s.root, registrationFile), data, 0600); err != nil {
				t.Fatal(err)
			}
			if other, err := open(s.root, p, false, nil); err == nil {
				other.Close()
				t.Fatal("bad document accepted")
			}
		})
	}
}
