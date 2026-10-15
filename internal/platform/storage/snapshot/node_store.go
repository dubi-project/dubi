package snapshot

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

	"github.com/dubi-project/dubi/internal/platform/identity"
	"golang.org/x/sys/unix"
)

const (
	nodeStoreDirectory = "dubi-snapshots"
	nodeStoreRegistry  = "DUBI_SNAPSHOT_STORAGE"
	nodeStoreOwner     = "DUBI_SNAPSHOT_OWNER"
	maxOwnerBytes      = 4096
)

type nodeOwner struct {
	Version    int    `json:"version"`
	NodeID     string `json:"node_id"`
	RaftID     uint64 `json:"raft_id"`
	StoreID    string `json:"store_id"`
	ArtifactID string `json:"artifact_id"`
}

type nodeRegistration struct {
	Owner  nodeOwner `json:"owner"`
	State  string    `json:"state"`
	Device uint64    `json:"pending_device,omitempty"`
	Inode  uint64    `json:"pending_inode,omitempty"`
}

// OpenNodeStore automatically initializes or recovers the node-owned artifact
// directory under an EXISTING bound Store. The caller holds the node Store
// process lock for this Store's lifetime. No user-supplied artifact ID/path is
// needed. Once active, a missing directory/marker is never recreated.
//
// Only a new empty directory or the exact persisted pending registration can
// be initialized. Pending device/inode checks fence initialization races; an
// active directory uses its durable owner marker, not reboot-sensitive inode
// numbers. This registers artifacts only, not the full backup/GC resource set.
func OpenNodeStore(root, storeID string) (*Store, error) {
	return openNodeStore(root, storeID, nil)
}

func openNodeStore(root, storeID string, hook func(string) error) (*Store, error) {
	if strings.TrimSpace(root) == "" || !validStorageID(storeID, "store_") {
		return nil, errors.New("snapshot Store requires the node's registered StoreID")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.IsDir() {
		return nil, errors.Join(err, errors.New("snapshot node Store root must be an existing directory, not a symlink"))
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, err
	}
	id, err := identity.Load(absolute)
	if err != nil || id.ClusterID == "" {
		return nil, errors.Join(err, errors.New("snapshot node Store requires a bound Node identity"))
	}
	parent, err := os.OpenRoot(absolute)
	if err != nil {
		return nil, err
	}
	owned := false
	defer func() {
		if !owned {
			parent.Close()
		}
	}()
	stage := func(name string) error {
		if hook != nil {
			return hook(name)
		}
		return nil
	}
	var registration nodeRegistration
	err = readOwnerJSON(parent, nodeStoreRegistry, &registration)
	if errors.Is(err, os.ErrNotExist) {
		if err := parent.Mkdir(nodeStoreDirectory, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if err := syncRoot(parent); err != nil {
			return nil, err
		}
		child, err := openOwnedDirectory(parent)
		if err != nil {
			return nil, err
		}
		file, err := child.Open(".")
		child.Close()
		if err != nil {
			return nil, err
		}
		entries, readErr := file.ReadDir(1)
		var stat unix.Stat_t
		statErr := unix.Fstat(int(file.Fd()), &stat)
		closeErr := file.Close()
		if len(entries) != 0 || !errors.Is(readErr, io.EOF) || statErr != nil || closeErr != nil {
			return nil, errors.Join(readErr, statErr, closeErr, errors.New("unregistered snapshot directory is not empty; refusing adoption"))
		}
		if err := stage("after-directory"); err != nil {
			return nil, err
		}
		token, err := randomOwnerToken()
		if err != nil {
			return nil, err
		}
		registration = nodeRegistration{Owner: nodeOwner{Version: 1, NodeID: id.NodeID, RaftID: id.RaftID, StoreID: storeID,
			ArtifactID: "snapshots_" + token}, State: "pending", Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
		if err := writeOwnerJSON(parent, nodeStoreRegistry, registration, true); err != nil {
			return nil, err
		}
		if err := stage("after-intent"); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	owner := registration.Owner
	if owner.Version != 1 || owner.NodeID != id.NodeID || owner.RaftID != id.RaftID || owner.StoreID != storeID ||
		!validStorageID(owner.ArtifactID, "snapshots_") || (registration.State != "pending" && registration.State != "active") ||
		(registration.State == "active" && (registration.Device != 0 || registration.Inode != 0)) {
		return nil, errors.New("snapshot registration identity or lifecycle does not match Node Store")
	}
	child, err := openOwnedDirectory(parent)
	if err != nil {
		return nil, err
	}
	defer child.Close()
	if registration.State == "pending" {
		file, err := child.Open(".")
		if err != nil {
			return nil, err
		}
		var stat unix.Stat_t
		err = errors.Join(unix.Fstat(int(file.Fd()), &stat), file.Close())
		if err != nil || uint64(stat.Dev) != registration.Device || uint64(stat.Ino) != registration.Inode {
			return nil, errors.Join(err, errors.New("pending snapshot directory identity changed"))
		}
	}
	var marker nodeOwner
	err = readOwnerJSON(child, nodeStoreOwner, &marker)
	if errors.Is(err, os.ErrNotExist) && registration.State == "pending" {
		if err := writeOwnerJSON(child, nodeStoreOwner, owner, true); err != nil {
			return nil, err
		}
	} else if err != nil || marker != owner {
		return nil, errors.Join(err, errors.New("snapshot directory owner marker is missing or mismatched"))
	}
	if err := stage("after-owner"); err != nil {
		return nil, err
	}
	if registration.State == "pending" {
		registration.State, registration.Device, registration.Inode = "active", 0, 0
		if err := writeOwnerJSON(parent, nodeStoreRegistry, registration, false); err != nil {
			return nil, err
		}
	}
	if err := stage("after-active"); err != nil {
		return nil, err
	}
	store, err := OpenStore(filepath.Join(absolute, nodeStoreDirectory))
	if err != nil {
		return nil, err
	}
	store.nodeRoot = parent
	owned = true
	store.ownerCheck = func() error {
		var current nodeRegistration
		var marker nodeOwner
		if err := readOwnerJSON(parent, nodeStoreRegistry, &current); err != nil {
			return err
		}
		if err := readOwnerJSON(store.root, nodeStoreOwner, &marker); err != nil {
			return err
		}
		if current != registration || marker != owner {
			return errors.New("active snapshot resource ownership changed")
		}
		return nil
	}
	if err := store.check(); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return store, nil
}

func openOwnedDirectory(parent *os.Root) (*os.Root, error) {
	info, err := parent.Lstat(nodeStoreDirectory)
	if err != nil || !info.IsDir() {
		return nil, errors.Join(err, errors.New("snapshot directory is missing or is not a physical directory"))
	}
	child, err := parent.OpenRoot(nodeStoreDirectory)
	if err != nil {
		return nil, err
	}
	opened, err := child.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		child.Close()
		return nil, errors.Join(err, errors.New("snapshot directory changed while opening"))
	}
	return child, nil
}

func validStorageID(value, prefix string) bool {
	if len(value) != len(prefix)+32 || !strings.HasPrefix(value, prefix) || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value[len(prefix):])
	return err == nil
}

func randomOwnerToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(token[:]), nil
}

func readOwnerJSON(root *os.Root, name string, target any) (returnErr error) {
	info, err := root.Lstat(name)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 || info.Size() > maxOwnerBytes {
		return errors.New("invalid snapshot owner metadata file")
	}
	file, err := root.Open(name)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return errors.Join(err, errors.New("snapshot owner metadata changed while opening"))
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maxOwnerBytes+1))
	if err != nil || len(encoded) > maxOwnerBytes {
		return errors.Join(err, errors.New("snapshot owner metadata exceeds bound"))
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	canonical, err := json.Marshal(target)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return errors.Join(err, errors.New("non-canonical snapshot owner metadata"))
	}
	return nil
}

func writeOwnerJSON(root *os.Root, name string, value any, exclusive bool) error {
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > maxOwnerBytes {
		return errors.Join(err, errors.New("invalid snapshot ownership metadata"))
	}
	token, err := randomOwnerToken()
	if err != nil {
		return err
	}
	pending := ".snapshot-owner-" + token
	file, err := root.OpenFile(pending, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { file.Close(); _ = root.Remove(pending) }()
	if _, err := file.Write(encoded); err != nil {
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if exclusive {
		err = root.Link(pending, name)
	} else {
		err = root.Rename(pending, name)
	}
	if err != nil {
		return fmt.Errorf("publish snapshot ownership: %w", err)
	}
	return syncRoot(root)
}

func syncRoot(root *os.Root) error {
	file, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}
