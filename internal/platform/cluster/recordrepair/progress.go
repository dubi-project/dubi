package recordrepair

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
)

const repairProgressVersion = 1
const maxRepairProgressBytes = 1 << 20
const maxRepairProgressCursorBytes = 16 << 10

// Progress is a local, recoverable scan ledger. It is bound to one confirmed
// Region cohort and never grants placement or durability authority.
type Progress struct {
	Version    int      `json:"version"`
	GroupID    string   `json:"group_id"`
	RegionID   string   `json:"region_id"`
	Generation uint64   `json:"generation"`
	GroupEpoch uint64   `json:"group_epoch"`
	FenceIndex uint64   `json:"fence_index"`
	Nodes      []uint64 `json:"nodes"`
	Cursor     []byte   `json:"cursor,omitempty"`
	Unresolved uint64   `json:"unresolved,omitempty"`
	FirstError string   `json:"first_error,omitempty"`
	Done       bool     `json:"done"`
}

func (p Progress) validate() error {
	if p.Version != repairProgressVersion || raft.ValidateGroupID(p.GroupID) != nil || p.RegionID == "" ||
		p.Generation == 0 || p.GroupEpoch == 0 || p.FenceIndex == 0 || len(p.Nodes) == 0 || len(p.Nodes) > 4096 ||
		len(p.Cursor) > maxRepairProgressCursorBytes || len(p.FirstError) > 4096 {
		return errors.New("invalid repair progress")
	}
	for i, node := range p.Nodes {
		if node == 0 || (i > 0 && p.Nodes[i-1] >= node) {
			return errors.New("repair progress Nodes are not sorted and unique")
		}
	}
	if p.Done && len(p.Cursor) != 0 {
		return errors.New("completed repair progress has a cursor")
	}
	return nil
}

// Matches returns whether a saved scan belongs to the current Root cohort.
// The caller must still obtain and revalidate a fresh Root barrier before
// treating any scan result as useful.
func (p Progress) Matches(view rootstate.RegionProgress) bool {
	return p.validate() == nil && view.Current.Validate() == nil &&
		p.GroupID == view.Current.GroupID && p.RegionID == view.Current.RegionID &&
		p.Generation == view.Current.Generation && p.GroupEpoch == view.Current.GroupEpoch &&
		p.FenceIndex == view.Current.GroupFenceIndex && slices.Equal(p.Nodes, view.Current.Nodes)
}

type ProgressStore struct {
	mu     sync.Mutex
	db     *pebble.DB
	closed bool
}

func OpenProgressStore(root string) (*ProgressStore, error) {
	if root == "" {
		return nil, errors.New("repair progress directory is empty")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	db, err := pebbleutil.Open(filepath.Join(root, "dubi-repair-progress"), &pebble.Options{Logger: pebbleutil.QuietLogger{}})
	if err != nil {
		return nil, err
	}
	return &ProgressStore{db: db}, nil
}

func progressKey(groupID string) []byte { return []byte("progress/" + groupID) }

func (store *ProgressStore) Load(groupID string) (Progress, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.usable(); err != nil {
		return Progress{}, false, err
	}
	if raft.ValidateGroupID(groupID) != nil {
		return Progress{}, false, errors.New("invalid repair progress Group")
	}
	value, closer, err := store.db.Get(progressKey(groupID))
	if errors.Is(err, pebble.ErrNotFound) {
		return Progress{}, false, nil
	}
	if err != nil {
		return Progress{}, false, err
	}
	defer closer.Close()
	if len(value) == 0 || len(value) > maxRepairProgressBytes {
		return Progress{}, false, errors.New("repair progress exceeds size bound")
	}
	var progress Progress
	if err := json.Unmarshal(value, &progress); err != nil {
		return Progress{}, false, fmt.Errorf("decode repair progress: %w", err)
	}
	if progress.GroupID != groupID {
		return Progress{}, false, errors.New("repair progress key differs from Group")
	}
	if err := progress.validate(); err != nil {
		return Progress{}, false, err
	}
	progress.Nodes = slices.Clone(progress.Nodes)
	progress.Cursor = slices.Clone(progress.Cursor)
	return progress, true, nil
}

func (store *ProgressStore) Save(progress Progress) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.usable(); err != nil {
		return err
	}
	progress.Version = repairProgressVersion
	if err := progress.validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	if len(encoded) > maxRepairProgressBytes {
		return errors.New("repair progress exceeds size bound")
	}
	return store.db.Set(progressKey(progress.GroupID), encoded, pebble.Sync)
}

func (store *ProgressStore) Delete(groupID string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.usable(); err != nil {
		return err
	}
	if raft.ValidateGroupID(groupID) != nil {
		return errors.New("invalid repair progress Group")
	}
	return store.db.Delete(progressKey(groupID), pebble.Sync)
}

func (store *ProgressStore) usable() error {
	if store == nil || store.db == nil || store.closed {
		return errors.New("repair progress store is closed")
	}
	return nil
}

func (store *ProgressStore) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	return store.db.Close()
}
