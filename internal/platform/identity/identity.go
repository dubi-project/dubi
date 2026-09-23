// Package identity owns the durable identity of one DUBI node.
package identity

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	filename      = "DUBI_NODE_IDENTITY"
	formatVersion = 1
)

var (
	identityMu = new(sync.Mutex)
	nodeIDRE   = regexp.MustCompile(`^node_[0-9a-f]{32}$`)
	clusterRE  = regexp.MustCompile(`^cluster_[0-9a-f]{32}$`)
)

// Identity is immutable after creation except for binding a joining node to a Cluster.
type Identity struct {
	Version   int       `json:"version"`
	NodeID    string    `json:"node_id"`
	RaftID    uint64    `json:"raft_id"`
	ClusterID string    `json:"cluster_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Bootstrap creates a new Cluster and its first Node, or loads an existing bootstrap identity.
func Bootstrap(root string) (Identity, error) {
	identityMu.Lock()
	defer identityMu.Unlock()
	if err := prepareRoot(root); err != nil {
		return Identity{}, err
	}
	release, err := lock(root)
	if err != nil {
		return Identity{}, err
	}
	defer release()
	current, err := load(root)
	if err == nil {
		if current.ClusterID == "" {
			return Identity{}, fmt.Errorf("node %s was prepared to join and cannot bootstrap a new Cluster", current.NodeID)
		}
		return current, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return Identity{}, err
	}
	value, err := newIdentity(true)
	if err != nil {
		return Identity{}, err
	}
	if err := write(root, value); err != nil {
		return Identity{}, err
	}
	return value, nil
}

// PrepareJoin creates a stable Node identity without binding it to a Cluster.
func PrepareJoin(root string) (Identity, error) {
	identityMu.Lock()
	defer identityMu.Unlock()
	if err := prepareRoot(root); err != nil {
		return Identity{}, err
	}
	release, err := lock(root)
	if err != nil {
		return Identity{}, err
	}
	defer release()
	current, err := load(root)
	if err == nil {
		return current, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return Identity{}, err
	}
	value, err := newIdentity(false)
	if err != nil {
		return Identity{}, err
	}
	if err := write(root, value); err != nil {
		return Identity{}, err
	}
	return value, nil
}

func prepareRoot(root string) error {
	if root == "" {
		return errors.New("Node identity root is empty")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return fmt.Errorf("create Node identity root: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(err, errors.New("Node identity root is not a directory"))
	}
	return nil
}

// BindCluster binds a prepared Node once; rebinding it to another Cluster is forbidden.
func BindCluster(root, clusterID string) (Identity, error) {
	if !clusterRE.MatchString(clusterID) {
		return Identity{}, fmt.Errorf("invalid ClusterID %q", clusterID)
	}
	identityMu.Lock()
	defer identityMu.Unlock()
	release, err := lock(root)
	if err != nil {
		return Identity{}, err
	}
	defer release()
	value, err := load(root)
	if err != nil {
		return Identity{}, err
	}
	if value.ClusterID == clusterID {
		return value, nil
	}
	if value.ClusterID != "" {
		return Identity{}, fmt.Errorf("node %s is already bound to Cluster %s", value.NodeID, value.ClusterID)
	}
	value.ClusterID = clusterID
	if err := write(root, value); err != nil {
		return Identity{}, err
	}
	return value, nil
}

// Load reads and validates the durable Node identity.
func Load(root string) (Identity, error) {
	identityMu.Lock()
	defer identityMu.Unlock()
	release, err := lock(root)
	if err != nil {
		return Identity{}, err
	}
	defer release()
	return load(root)
}

func lock(root string) (func(), error) {
	file, err := os.OpenFile(filepath.Join(root, ".DUBI_NODE_IDENTITY.lock"), os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open Node identity lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock Node identity: %w", err)
	}
	return func() {
		_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
		_ = file.Close()
	}, nil
}

func load(root string) (Identity, error) {
	encoded, err := os.ReadFile(filepath.Join(root, filename))
	if err != nil {
		return Identity{}, err
	}
	var value Identity
	if err := json.Unmarshal(encoded, &value); err != nil {
		return Identity{}, fmt.Errorf("decode Node identity: %w", err)
	}
	if err := validate(value); err != nil {
		return Identity{}, err
	}
	return value, nil
}

func newIdentity(bootstrap bool) (Identity, error) {
	nodeID, err := randomID("node_", 16)
	if err != nil {
		return Identity{}, err
	}
	raftID, err := randomRaftID()
	if err != nil {
		return Identity{}, err
	}
	value := Identity{Version: formatVersion, NodeID: nodeID, RaftID: raftID, CreatedAt: time.Now().UTC()}
	if bootstrap {
		value.ClusterID, err = randomID("cluster_", 16)
		if err != nil {
			return Identity{}, err
		}
	}
	return value, nil
}

func randomID(prefix string, size int) (string, error) {
	random := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, random); err != nil {
		return "", fmt.Errorf("generate identity: %w", err)
	}
	return prefix + hex.EncodeToString(random), nil
}

func randomRaftID() (uint64, error) {
	var encoded [8]byte
	for {
		if _, err := io.ReadFull(rand.Reader, encoded[:]); err != nil {
			return 0, fmt.Errorf("generate RaftID: %w", err)
		}
		if id := binary.BigEndian.Uint64(encoded[:]); id != 0 {
			return id, nil
		}
	}
}

func validate(value Identity) error {
	if value.Version != formatVersion {
		return fmt.Errorf("unsupported Node identity version %d", value.Version)
	}
	if !nodeIDRE.MatchString(value.NodeID) {
		return fmt.Errorf("invalid NodeID %q", value.NodeID)
	}
	if value.RaftID == 0 {
		return errors.New("RaftID must not be zero")
	}
	if value.ClusterID != "" && !clusterRE.MatchString(value.ClusterID) {
		return fmt.Errorf("invalid ClusterID %q", value.ClusterID)
	}
	if value.CreatedAt.IsZero() {
		return errors.New("Node identity creation time is missing")
	}
	return nil
}

func write(root string, value Identity) error {
	if err := validate(value); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode Node identity: %w", err)
	}
	encoded = append(encoded, '\n')
	temporary, err := os.CreateTemp(root, ".DUBI_NODE_IDENTITY-*")
	if err != nil {
		return fmt.Errorf("create Node identity: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set Node identity permissions: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write Node identity: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync Node identity: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close Node identity: %w", err)
	}
	if err := os.Rename(temporaryPath, filepath.Join(root, filename)); err != nil {
		return fmt.Errorf("commit Node identity: %w", err)
	}
	directory, err := os.Open(root)
	if err != nil {
		return fmt.Errorf("open Node directory: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}
