package logstore

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
	"golang.org/x/sys/unix"
)

const logDirectory = "dubi-raft"
const logRegistrationFile = "DUBI_RAFT_STORAGE"
const logRegistrationLimit = 4096

var logIdentityKey = []byte("meta/storage-identity")

type logRegistration struct {
	Version  int    `json:"version"`
	Identity string `json:"identity"`
	State    string `json:"state"`
	Device   uint64 `json:"pending_device,omitempty"`
	Inode    uint64 `json:"pending_inode,omitempty"`
}

func (r logRegistration) validate() error {
	id, err := hex.DecodeString(r.Identity)
	if r.Version != 1 || err != nil || len(id) != 16 || hex.EncodeToString(id) != r.Identity ||
		(r.State != "pending" && r.State != "active") || (r.State == "active" && (r.Device != 0 || r.Inode != 0)) {
		return errors.New("invalid Raft storage registration")
	}
	return nil
}

// openRegisteredLog refuses to reinterpret a previously active log as fresh.
// The external registration survives loss of the database directory; an inner
// synced identity rejects replacement by another (including empty) database.
// This registers the log only; loss of the whole Group directory additionally
// needs the parent Group catalog. No legacy database is silently adopted.
func openRegisteredLog(path, expectedIdentity string, hook func(string) error) (_ *pebble.DB, release func() error, returnErr error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil, errors.New("Raft storage parent is empty")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, nil, err
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.IsDir() {
		return nil, nil, errors.Join(err, errors.New("Raft storage parent must be an existing physical directory"))
	}
	root, err := os.OpenRoot(absolute)
	if err != nil {
		return nil, nil, err
	}
	lock, err := root.OpenFile(".DUBI_RAFT_STORAGE.lock", os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o640)
	if err != nil {
		return nil, nil, errors.Join(err, root.Close())
	}
	closeLease := func() error { return errors.Join(lock.Close(), root.Close()) }
	lockInfo, err := lock.Stat()
	if err != nil || !lockInfo.Mode().IsRegular() {
		return nil, nil, errors.Join(err, closeLease(), errors.New("Raft storage lock is not a regular file"))
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, nil, errors.Join(err, closeLease(), errors.New("Raft storage is already opening or open"))
	}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, closeLease())
		}
	}()
	stage := func(name string) error {
		if hook != nil {
			return hook(name)
		}
		return nil
	}
	registration, err := readLogRegistration(root)
	if expectedIdentity != "" && (err != nil || registration.Identity != expectedIdentity || registration.State != "active") {
		return nil, nil, errors.Join(err, errors.New("Raft log registration differs from parent Group catalog"))
	}
	if errors.Is(err, os.ErrNotExist) {
		if err := root.Mkdir(logDirectory, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, nil, err
		}
		child, err := physicalLogDirectory(root)
		if err != nil {
			return nil, nil, err
		}
		entries, readErr := child.ReadDir(1)
		var stat unix.Stat_t
		statErr := unix.Fstat(int(child.Fd()), &stat)
		closeErr := child.Close()
		if len(entries) != 0 || !errors.Is(readErr, io.EOF) || statErr != nil || closeErr != nil {
			return nil, nil, errors.Join(readErr, statErr, closeErr, errors.New("unregistered Raft database is not empty; refusing adoption"))
		}
		if err := syncLogRoot(root); err != nil {
			return nil, nil, err
		}
		if err := stage("after-directory"); err != nil {
			return nil, nil, err
		}
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			return nil, nil, err
		}
		registration = logRegistration{Version: 1, Identity: hex.EncodeToString(token[:]), State: "pending", Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
		if err := writeLogRegistration(root, registration, true); err != nil {
			return nil, nil, err
		}
		if err := stage("after-intent"); err != nil {
			return nil, nil, err
		}
	} else if err != nil {
		return nil, nil, err
	}
	if err := registration.validate(); err != nil {
		return nil, nil, err
	}
	child, err := physicalLogDirectory(root)
	if err != nil {
		return nil, nil, err
	}
	var stat unix.Stat_t
	statErr := unix.Fstat(int(child.Fd()), &stat)
	if err := errors.Join(statErr, child.Close()); err != nil {
		return nil, nil, err
	}
	if registration.State == "pending" && (registration.Device != uint64(stat.Dev) || registration.Inode != uint64(stat.Ino)) {
		return nil, nil, errors.New("pending Raft database directory was replaced")
	}
	// Active state must be durable again after an uncertain previous publication
	// before opening the database or returning a usable handle.
	if err := syncLogRoot(root); err != nil {
		return nil, nil, err
	}
	databasePath := filepath.Join(absolute, logDirectory)
	if registration.State == "active" {
		// ErrorIfNotExists alone still creates Pebble's LOCK in an empty
		// replacement. Reject absent metadata using its read-only inspection.
		description, err := pebble.Peek(databasePath, vfs.Default)
		if err != nil || !description.Exists {
			return nil, nil, errors.Join(err, errors.New("registered Raft database metadata is missing"))
		}
	}
	db, err := pebbleutil.Open(databasePath, &pebble.Options{Logger: pebbleutil.QuietLogger{}, ErrorIfNotExists: registration.State == "active"})
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, db.Close())
		}
	}()
	if err := stage("after-database"); err != nil {
		return nil, nil, err
	}
	value, closer, err := db.Get(logIdentityKey)
	if errors.Is(err, pebble.ErrNotFound) && registration.State == "pending" {
		iterator, err := db.NewIter(nil)
		if err != nil {
			return nil, nil, err
		}
		nonempty := iterator.First()
		if err := errors.Join(iterator.Error(), iterator.Close()); err != nil {
			return nil, nil, err
		}
		if nonempty {
			return nil, nil, errors.New("pending Raft database has records without its registered identity")
		}
		if err := db.Set(logIdentityKey, []byte(registration.Identity), pebble.Sync); err != nil {
			return nil, nil, err
		}
	} else if err != nil {
		return nil, nil, fmt.Errorf("Raft database identity missing: %w", err)
	} else {
		matches := string(value) == registration.Identity
		if err := closer.Close(); err != nil {
			return nil, nil, err
		}
		if !matches {
			return nil, nil, errors.New("Raft database identity differs from registration")
		}
		// Retry may observe the prior token after a Sync error. Re-sync rather
		// than treating readable bytes alone as durable publication evidence.
		if registration.State == "pending" {
			if err := db.LogData(nil, pebble.Sync); err != nil {
				return nil, nil, err
			}
		}
	}
	if err := stage("after-identity"); err != nil {
		return nil, nil, err
	}
	child, err = physicalLogDirectory(root)
	if err != nil {
		return nil, nil, err
	}
	if err := errors.Join(child.Sync(), child.Close()); err != nil {
		return nil, nil, err
	}
	if registration.State == "pending" {
		registration.State, registration.Device, registration.Inode = "active", 0, 0
		if err := writeLogRegistration(root, registration, false); err != nil {
			return nil, nil, err
		}
	}
	if err := stage("after-active"); err != nil {
		return nil, nil, err
	}
	return db, closeLease, nil
}

func physicalLogDirectory(root *os.Root) (*os.File, error) {
	file, err := root.OpenFile(logDirectory, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func readLogRegistration(root *os.Root) (logRegistration, error) {
	f, err := root.OpenFile(logRegistrationFile, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return logRegistration{}, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > logRegistrationLimit {
		return logRegistration{}, errors.Join(err, f.Close(), errors.New("invalid Raft registration file"))
	}
	data, err := io.ReadAll(io.LimitReader(f, logRegistrationLimit+1))
	if err := errors.Join(err, f.Close()); err != nil {
		return logRegistration{}, err
	}
	if len(data) > logRegistrationLimit {
		return logRegistration{}, errors.New("Raft registration exceeds limit")
	}
	var r logRegistration
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return logRegistration{}, err
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return logRegistration{}, errors.New("trailing Raft registration data")
	}
	return r, r.validate()
}

func writeLogRegistration(root *os.Root, r logRegistration, create bool) error {
	if err := r.validate(); err != nil {
		return err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	name := ".raft-registration-" + hex.EncodeToString(token[:])
	f, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	defer root.Remove(name)
	n, writeErr := f.Write(append(data, '\n'))
	if writeErr == nil && n != len(data)+1 {
		writeErr = io.ErrShortWrite
	}
	if err := errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		return err
	}
	if create {
		err = root.Link(name, logRegistrationFile)
	} else {
		err = root.Rename(name, logRegistrationFile)
	}
	if err != nil {
		return err
	}
	return syncLogRoot(root)
}

func syncLogRoot(root *os.Root) error {
	f, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
