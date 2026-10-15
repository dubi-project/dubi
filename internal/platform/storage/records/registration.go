package records

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"golang.org/x/sys/unix"
)

const directory = volume.ImmutableRecordsDirectory
const registrationFile = volume.ImmutableRecordsMarker

type resourceIdentity struct {
	Version    int    `json:"version"`
	StoreID    string `json:"store_id"`
	ResourceID string `json:"resource_id"`
}
type registration struct {
	resourceIdentity
	State  string `json:"state"`
	Device uint64 `json:"pending_device,omitempty"`
	Inode  uint64 `json:"pending_inode,omitempty"`
}

func validToken(s string) bool {
	if len(s) != 32 {
		return false
	}
	v, e := hex.DecodeString(s)
	return e == nil && len(v) == 16 && hex.EncodeToString(v) == s
}
func (r registration) validate() error {
	if r.Version != 1 || !strings.HasPrefix(r.StoreID, "store_") || !validToken(strings.TrimPrefix(r.StoreID, "store_")) || !validToken(r.ResourceID) || (r.State != "pending" && r.State != "active") || r.State == "active" && (r.Device != 0 || r.Inode != 0) {
		return errors.New("invalid immutable record resource registration")
	}
	return nil
}

func open(root string, packs packBackend, initialize bool, hook func(string) error) (_ *Store, returnErr error) {
	return openBound(root, packs, initialize, hook, nil)
}

func openBound(root string, packs packBackend, initialize bool, hook func(string) error, binding *volume.ResourceBinding) (_ *Store, returnErr error) {
	if root == "" || packs == nil {
		return nil, errors.New("records require Node Store root and shared Packs")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	rootInfo, err := os.Lstat(absolute)
	if err != nil || !rootInfo.IsDir() {
		return nil, errors.Join(err, errors.New("record Node Store must be a physical directory"))
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, err
	}
	lease, err := os.OpenFile(filepath.Join(absolute, ".DUBI_RECORDS.lock"), os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o640)
	if err != nil {
		return nil, err
	}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, lease.Close())
		}
	}()
	info, err := lease.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.Join(err, errors.New("record resource lock must be regular"))
	}
	if err := unix.Flock(int(lease.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, err
	}
	stage := func(name string) error {
		if hook != nil {
			return hook(name)
		}
		return nil
	}
	reg, err := readRegistration(absolute)
	dir := filepath.Join(absolute, directory)
	if initialize && err == nil {
		return nil, errors.New("record resource already registered; reopen it")
	}
	if errors.Is(err, os.ErrNotExist) {
		if !initialize {
			return nil, errors.New("record resource registration missing; refusing empty recreation")
		}
		if err := os.Mkdir(dir, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if err := emptyDirectory(dir); err != nil {
			return nil, err
		}
		if err := syncDirectory(absolute); err != nil {
			return nil, err
		}
		if err := stage("after-directory"); err != nil {
			return nil, err
		}
		dev, ino, err := directoryIdentity(dir)
		if err != nil {
			return nil, err
		}
		resourceID := ""
		if binding != nil {
			resourceID = binding.ID()
		} else {
			var token [16]byte
			if _, err := rand.Read(token[:]); err != nil {
				return nil, err
			}
			resourceID = hex.EncodeToString(token[:])
		}
		reg = registration{resourceIdentity: resourceIdentity{Version: 1, StoreID: packs.StoreID(), ResourceID: resourceID}, State: "pending", Device: dev, Inode: ino}
		if err := reg.validate(); err != nil {
			return nil, err
		}
		if err := writeRegistration(absolute, reg); err != nil {
			return nil, err
		}
		if err := stage("after-intent"); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if reg.StoreID != packs.StoreID() {
		return nil, errors.New("immutable record index belongs to another Store")
	}
	if binding != nil && reg.ResourceID != binding.ID() {
		return nil, errors.New("record resource differs from parent Store intent")
	}
	if err := syncDirectory(absolute); err != nil {
		return nil, err
	}
	dirInfo, err := os.Lstat(dir)
	if err != nil || !dirInfo.IsDir() {
		return nil, errors.Join(err, errors.New("registered record directory missing or replaced"))
	}
	if reg.State == "pending" {
		dev, ino, err := directoryIdentity(dir)
		if err != nil {
			return nil, err
		}
		if dev != reg.Device || ino != reg.Inode {
			return nil, errors.New("pending record resource directory replaced")
		}
	}
	dbPath := filepath.Join(dir, "index")
	dbDirectoryExists := false
	if info, err := os.Lstat(dbPath); err == nil {
		if !info.IsDir() {
			return nil, errors.New("record index must be a physical directory")
		}
		dbDirectoryExists = true
	} else if !errors.Is(err, os.ErrNotExist) || reg.State == "active" {
		return nil, err
	}
	// Inspect before opening writable: even ErrorIfNotExists creates LOCK in
	// an empty replacement, and a writable open can replay/compact foreign WALs.
	description := &pebble.DBDesc{}
	if dbDirectoryExists {
		description, err = pebble.Peek(dbPath, vfs.Default)
		if err != nil {
			return nil, err
		}
	}
	if !description.Exists {
		if reg.State == "active" {
			return nil, errors.New("registered record index metadata missing")
		}
		if dbDirectoryExists {
			if err := emptyDirectory(dbPath); err != nil {
				return nil, err
			}
		}
	} else {
		probe, err := pebbleutil.Open(dbPath, &pebble.Options{Logger: pebbleutil.QuietLogger{}, ReadOnly: true, ErrorIfNotExists: true})
		if err != nil {
			return nil, err
		}
		_, identityErr := checkIdentity(probe, reg)
		if err := errors.Join(identityErr, probe.Close()); err != nil {
			return nil, err
		}
	}
	db, err := pebbleutil.Open(dbPath, &pebble.Options{Logger: pebbleutil.QuietLogger{}, ErrorIfNotExists: reg.State == "active"})
	if err != nil {
		return nil, err
	}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, db.Close())
		}
	}()
	if err := stage("after-index-open"); err != nil {
		return nil, err
	}
	present, err := checkIdentity(db, reg)
	if err != nil {
		return nil, err
	}
	if !present {
		encoded, err := json.Marshal(reg.resourceIdentity)
		if err != nil {
			return nil, err
		}
		if err := db.Set(identityKey, encoded, pebble.Sync); err != nil {
			return nil, err
		}
	} else if reg.State == "pending" {
		// A prior Sync may have returned an uncertain result. Readable identity
		// bytes alone do not complete its durability barrier.
		if err := db.LogData(nil, pebble.Sync); err != nil {
			return nil, err
		}
	}
	if err := syncDirectory(dir); err != nil {
		return nil, err
	}
	if err := stage("after-index-identity"); err != nil {
		return nil, err
	}
	if reg.State == "pending" {
		reg.State, reg.Device, reg.Inode = "active", 0, 0
		if err := writeRegistration(absolute, reg); err != nil {
			return nil, err
		}
	}
	if err := stage("after-active"); err != nil {
		return nil, err
	}
	dbInfo, err := os.Lstat(dbPath)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, packs: packs, root: absolute, identity: reg, rootInfo: rootInfo, dirInfo: dirInfo, dbInfo: dbInfo, lease: lease}
	if err := s.usable(); err != nil {
		return nil, err
	}
	return s, nil
}

// Missing identity is recoverable only in an empty pending database. Both the
// read-only preflight and writable open use this same validation.
func checkIdentity(db *pebble.DB, reg registration) (bool, error) {
	encoded, closer, err := db.Get(identityKey)
	if errors.Is(err, pebble.ErrNotFound) {
		if reg.State != "pending" {
			return false, errors.New("active record index has lost its identity")
		}
		iter, err := db.NewIter(nil)
		if err != nil {
			return false, err
		}
		nonempty := iter.First()
		err = errors.Join(iter.Error(), iter.Close())
		if err != nil || nonempty {
			return false, errors.Join(err, errors.New("unidentified record index is not empty"))
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var identity resourceIdentity
	valid := len(encoded) <= 4096 && strictJSON(encoded, &identity) == nil && identity == reg.resourceIdentity
	err = closer.Close()
	if err != nil || !valid {
		return false, errors.Join(err, errors.New("record index identity differs from registered resource"))
	}
	return true, nil
}

func readRegistration(root string) (registration, error) {
	var r registration
	f, err := os.OpenFile(filepath.Join(root, registrationFile), os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return r, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 4096 {
		return r, errors.Join(err, errors.New("invalid record registration file"))
	}
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return r, err
	}
	if len(data) > 4096 || strictJSON(data, &r) != nil {
		return r, errors.New("invalid record registration JSON")
	}
	return r, r.validate()
}
func writeRegistration(root string, r registration) (returnErr error) {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(root, ".dubi-records-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer func() { f.Close(); os.Remove(name) }()
	if err := f.Chmod(0o640); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, filepath.Join(root, registrationFile)); err != nil {
		return err
	}
	return syncDirectory(root)
}
func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
func emptyDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() {
		return errors.Join(err, errors.New("record directory is not physical"))
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	entries, err := f.ReadDir(1)
	if len(entries) != 0 || !errors.Is(err, io.EOF) {
		return errors.Join(err, errors.New("unregistered record directory is not empty"))
	}
	return nil
}
func directoryIdentity(path string) (uint64, uint64, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &stat); err != nil {
		return 0, 0, err
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}
