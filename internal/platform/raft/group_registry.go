package raft

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

const groupRegistryFile = "DUBI_RAFT_GROUPS"
const maxRegisteredGroups = 4096
const maxGroupRegistryBytes = 2 << 20

// Host opens Groups outside its routing lock. Serialize only the short catalog
// publication, never log recovery, state replay or snapshot installation.
var groupRegistryMu sync.Mutex

type groupRegistration struct {
	ResourceID  string `json:"resource_id"`
	State       string `json:"state"`
	Device      uint64 `json:"pending_device,omitempty"`
	Inode       uint64 `json:"pending_inode,omitempty"`
	LogIdentity string `json:"log_identity,omitempty"`
}
type groupRegistry struct {
	Version int                          `json:"version"`
	Groups  map[string]groupRegistration `json:"groups"`
}

func validGroupResourceID(id string) bool {
	data, err := hex.DecodeString(id)
	return err == nil && len(data) == 16 && hex.EncodeToString(data) == id
}

func (r groupRegistry) validate() error {
	if r.Version != 1 || r.Groups == nil || len(r.Groups) > maxRegisteredGroups {
		return errors.New("invalid Raft Group registry")
	}
	seen := make(map[string]bool, len(r.Groups))
	for id, g := range r.Groups {
		if g.LogIdentity != "" && (g.State != "active" || !validGroupResourceID(g.LogIdentity)) {
			return errors.New("invalid Group log binding")
		}
		if ValidateGroupID(id) != nil || !validGroupResourceID(g.ResourceID) || seen[g.ResourceID] || (g.State != "pending" && g.State != "active") || (g.State == "active" && (g.Device != 0 || g.Inode != 0)) {
			return errors.New("invalid or duplicate registered Group resource")
		}
		seen[g.ResourceID] = true
	}
	return nil
}

func prepareRegisteredGroup(storeRoot, groupID string, hook func(string) error) (_ string, returnErr error) {
	if storeRoot == "" {
		return "", errors.New("Store root must not be empty")
	}
	if err := ValidateGroupID(groupID); err != nil {
		return "", err
	}
	absolute, err := filepath.Abs(storeRoot)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.IsDir() {
		return "", errors.Join(err, errors.New("Group Store must be an existing physical directory"))
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	groupRegistryMu.Lock()
	defer groupRegistryMu.Unlock()
	root, err := os.OpenRoot(absolute)
	if err != nil {
		return "", err
	}
	defer func() { returnErr = errors.Join(returnErr, root.Close()) }()
	lock, err := root.OpenFile(".DUBI_RAFT_GROUPS.lock", os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o640)
	if err != nil {
		return "", err
	}
	defer func() { returnErr = errors.Join(returnErr, lock.Close()) }()
	info, err = lock.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.Join(err, errors.New("Group registry lock is not a regular file"))
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return "", err
	}
	stage := func(at string) error {
		if hook != nil {
			return hook(at)
		}
		return nil
	}
	data, err := readGroupRegistryFile(root, groupRegistryFile, maxGroupRegistryBytes)
	var registry groupRegistry
	if errors.Is(err, os.ErrNotExist) {
		parent, err := physicalGroupDirectory(root, groupDirectory)
		if err == nil {
			entries, readErr := parent.ReadDir(1)
			closeErr := parent.Close()
			if len(entries) != 0 || !errors.Is(readErr, io.EOF) || closeErr != nil {
				return "", errors.Join(readErr, closeErr, errors.New("unregistered Group directory is not empty; refusing adoption"))
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		registry = groupRegistry{Version: 1, Groups: map[string]groupRegistration{}}
		if err := writeGroupRegistryFile(root, groupRegistryFile, registry, true); err != nil {
			return "", err
		}
		if err := stage("after-registry"); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	} else {
		d := json.NewDecoder(bytes.NewReader(data))
		d.DisallowUnknownFields()
		if err := d.Decode(&registry); err != nil {
			return "", err
		}
		if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
			return "", errors.New("trailing Group registry data")
		}
	}
	if err := registry.validate(); err != nil {
		return "", err
	}
	// A readable prior publication may have returned a directory Sync error.
	if err := syncGroupDirectory(root); err != nil {
		return "", err
	}
	parent, err := physicalGroupDirectory(root, groupDirectory)
	if errors.Is(err, os.ErrNotExist) && len(registry.Groups) == 0 {
		if err := root.Mkdir(groupDirectory, 0o750); err != nil {
			return "", err
		}
		if err := syncGroupDirectory(root); err != nil {
			return "", err
		}
		parent, err = physicalGroupDirectory(root, groupDirectory)
	}
	if err != nil {
		return "", err
	}
	parent.Close()
	parents, err := root.OpenRoot(groupDirectory)
	if err != nil {
		return "", err
	}
	defer parents.Close()
	registration, exists := registry.Groups[groupID]
	if !exists {
		if len(registry.Groups) >= maxRegisteredGroups {
			return "", errors.New("Raft Group registry exceeds internal limit")
		}
		if err := parents.Mkdir(groupID, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
			return "", err
		}
		child, err := physicalGroupDirectory(parents, groupID)
		if err != nil {
			return "", err
		}
		entries, readErr := child.ReadDir(1)
		var stat unix.Stat_t
		statErr := unix.Fstat(int(child.Fd()), &stat)
		closeErr := child.Close()
		if len(entries) != 0 || !errors.Is(readErr, io.EOF) || statErr != nil || closeErr != nil {
			return "", errors.Join(readErr, statErr, closeErr, errors.New("unregistered Group is not empty"))
		}
		if err := syncGroupDirectory(parents); err != nil {
			return "", err
		}
		if err := stage("after-directory"); err != nil {
			return "", err
		}
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			return "", err
		}
		registration = groupRegistration{ResourceID: hex.EncodeToString(token[:]), State: "pending", Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
		registry.Groups[groupID] = registration
		if err := writeGroupRegistryFile(root, groupRegistryFile, registry, false); err != nil {
			return "", err
		}
		if err := stage("after-intent"); err != nil {
			return "", err
		}
	}
	child, err := physicalGroupDirectory(parents, groupID)
	if err != nil {
		return "", err
	}
	var stat unix.Stat_t
	statErr := unix.Fstat(int(child.Fd()), &stat)
	if err := errors.Join(statErr, child.Close()); err != nil {
		return "", err
	}
	if registration.State == "pending" && (registration.Device != uint64(stat.Dev) || registration.Inode != uint64(stat.Ino)) {
		return "", errors.New("pending Group directory was replaced")
	}
	groupRoot, err := parents.OpenRoot(groupID)
	if err != nil {
		return "", err
	}
	defer groupRoot.Close()
	marker, err := readGroupRegistryFile(groupRoot, identityFile, 4096)
	if errors.Is(err, os.ErrNotExist) && registration.State == "pending" {
		if err := writeGroupRegistryFile(groupRoot, identityFile, diskIdentity{Version: identityVersion, GroupID: groupID, ResourceID: registration.ResourceID}, true); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	} else {
		identity, err := decodeIdentity(marker)
		if err != nil {
			return "", err
		}
		if identity.GroupID != groupID || identity.ResourceID != registration.ResourceID {
			return "", errors.New("Group directory identity differs from Store registry")
		}
	}
	if err := syncGroupDirectory(groupRoot); err != nil {
		return "", err
	}
	if err := stage("after-owner"); err != nil {
		return "", err
	}
	if registration.State == "pending" {
		registration.State, registration.Device, registration.Inode = "active", 0, 0
		registry.Groups[groupID] = registration
		if err := writeGroupRegistryFile(root, groupRegistryFile, registry, false); err != nil {
			return "", err
		}
	}
	if err := stage("after-active"); err != nil {
		return "", err
	}
	return filepath.Join(absolute, groupDirectory, groupID), nil
}

func physicalGroupDirectory(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
}

// Empty desired reads the persisted binding. A nonempty desired may publish
// exactly once, after the log's own registration and identity are durable and
// before any Raft bootstrap/Ready. Log recovery never holds this catalog lock.
func groupLogBinding(storeRoot, groupID, desired string) (_ string, returnErr error) {
	if ValidateGroupID(groupID) != nil || (desired != "" && !validGroupResourceID(desired)) {
		return "", errors.New("invalid Group log binding request")
	}
	groupRegistryMu.Lock()
	defer groupRegistryMu.Unlock()
	root, err := os.OpenRoot(storeRoot)
	if err != nil {
		return "", err
	}
	defer func() { returnErr = errors.Join(returnErr, root.Close()) }()
	lock, err := root.OpenFile(".DUBI_RAFT_GROUPS.lock", os.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return "", err
	}
	defer lock.Close()
	info, err := lock.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.Join(err, errors.New("invalid Group registry lock"))
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return "", err
	}
	data, err := readGroupRegistryFile(root, groupRegistryFile, maxGroupRegistryBytes)
	if err != nil {
		return "", err
	}
	var registry groupRegistry
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&registry); err != nil {
		return "", err
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return "", errors.New("trailing Group registry data")
	}
	if err := registry.validate(); err != nil {
		return "", err
	}
	registration, found := registry.Groups[groupID]
	if !found || registration.State != "active" {
		return "", errors.New("Group directory is not registered active")
	}
	if desired != "" {
		if registration.LogIdentity != "" && registration.LogIdentity != desired {
			return "", errors.New("registered Group log identity cannot change")
		}
		if registration.LogIdentity == "" {
			registration.LogIdentity = desired
			registry.Groups[groupID] = registration
			if err := writeGroupRegistryFile(root, groupRegistryFile, registry, false); err != nil {
				return "", err
			}
		}
	}
	if err := syncGroupDirectory(root); err != nil {
		return "", err
	}
	return registration.LogIdentity, nil
}
func syncGroupDirectory(root *os.Root) error {
	f, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func readGroupRegistryFile(root *os.Root, name string, limit int64) ([]byte, error) {
	f, err := root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > limit {
		return nil, errors.Join(err, f.Close(), errors.New("invalid Group registry file"))
	}
	data, readErr := io.ReadAll(io.LimitReader(f, limit+1))
	closeErr := f.Close()
	if int64(len(data)) > limit {
		return nil, errors.Join(closeErr, errors.New("Group registry exceeds limit"))
	}
	return data, errors.Join(readErr, closeErr)
}

func writeGroupRegistryFile(root *os.Root, name string, value any, create bool) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > maxGroupRegistryBytes {
		return errors.New("Group registry exceeds limit")
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	temp := ".group-registry-" + hex.EncodeToString(token[:])
	f, err := root.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	defer root.Remove(temp)
	n, writeErr := f.Write(append(data, '\n'))
	if writeErr == nil && n != len(data)+1 {
		writeErr = io.ErrShortWrite
	}
	if err := errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		return err
	}
	if create {
		err = root.Link(temp, name)
	} else {
		err = root.Rename(temp, name)
	}
	if err != nil {
		return err
	}
	return syncGroupDirectory(root)
}
