// Package volume owns module-neutral, durable Node volume registration.
// WithVolume gates local storage access; Reserve provides shared filesystem
// byte admission. Drain is deliberately a two-step, write-stop boundary;
// Pack migration/compaction must empty a volume before final removal.
package volume

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/identity"
)

type State string

const (
	Registered State = "registered"
	Active     State = "active"
	Draining   State = "draining"
	Removed    State = "removed"
)

var (
	ErrVolumeNotActive   = errors.New("Volume is not active")
	ErrVolumeNotDraining = errors.New("Volume is not draining")
	ErrVolumeHasData     = errors.New("Volume still contains Pack data")
)

type Record struct {
	ID    string `json:"volume_id"`
	Path  string `json:"path"`
	State State  `json:"state"`
	// Only pending registration uses device/inode checks. Active volume
	// identity comes from its durable marker, not reboot-sensitive device IDs.
	Device uint64 `json:"pending_device,omitempty"`
	Inode  uint64 `json:"pending_inode,omitempty"`
}

type owner struct {
	Version int    `json:"version"`
	NodeID  string `json:"node_id"`
	RaftID  uint64 `json:"raft_id"`
	StoreID string `json:"store_id"`
}

type manifest struct {
	owner
	Volumes   []Record                  `json:"volumes"`
	Resources map[string]ResourceIntent `json:"resources"`
}

type marker struct {
	owner
	VolumeID string `json:"volume_id"`
}

type Registry struct {
	mu            sync.Mutex
	root          string
	manifest      manifest
	lock          *os.File
	markers       map[string]*os.File
	failed        error
	closed        bool
	reserved      map[uint64]uint64 // In-flight byte budgets shared by filesystem device.
	reserveCursor int
	probe         func(*os.Root) (diskSample, error) // nil uses real descriptor-based statfs.
	rootInfo      os.FileInfo
	publish       func(manifest) error // Test-only uncertain publication boundary.
}

// Initialize creates the initial volume set, or resumes the exact persisted
// registration intent. Explicit paths must already exist and be empty.
// No paths on a retry means reuse the registered set, never add a default.
func Initialize(root string, paths []string) (*Registry, error) {
	return open(root, paths, true)
}

// Open recovers registration without creating directories or markers.
// Missing volumes remain observable and never become new empty volumes.
func Open(root string) (*Registry, error) { return open(root, nil, false) }

func open(root string, paths []string, initialize bool) (*Registry, error) {
	root, err := canonicalDirectory(root)
	if err != nil {
		return nil, err
	}
	id, err := identity.Load(root)
	if err != nil {
		return nil, err
	}
	lock, err := lockFile(filepath.Join(root, lockName), true)
	if err != nil {
		return nil, err
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	r := &Registry{root: root, rootInfo: rootInfo, lock: lock, markers: make(map[string]*os.File)}
	fail := func(err error) (*Registry, error) { return nil, errors.Join(err, r.Close()) }
	err = readJSON(filepath.Join(root, manifestName), &r.manifest)
	if errors.Is(err, os.ErrNotExist) && initialize {
		if _, err := os.Lstat(filepath.Join(root, storageName)); !errors.Is(err, os.ErrNotExist) {
			return fail(errors.New("volume registry is missing from an initialized Store; refusing to recreate it"))
		}
		// Do not install a new parent identity over a surviving required child.
		for _, name := range []string{ImmutableRecordsDirectory, ImmutableRecordsMarker} {
			if _, err := os.Lstat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
				return fail(errors.New("Store parent catalog missing while immutable-record resource remains"))
			}
		}
		storeID, err := randomID("store_")
		if err != nil {
			return fail(err)
		}
		r.manifest.owner = owner{Version: 2, NodeID: id.NodeID, RaftID: id.RaftID, StoreID: storeID}
		resourceID, err := randomID("")
		if err != nil {
			return fail(err)
		}
		r.manifest.Resources = map[string]ResourceIntent{ImmutableRecordsResource: {ResourceID: resourceID, State: ResourcePending}}
		if len(paths) == 0 {
			path := filepath.Join(root, "volume")
			if err := os.Mkdir(path, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
				return fail(err)
			}
			if err := syncDirectory(root); err != nil {
				return fail(err)
			}
			paths = []string{path}
		}
		for _, path := range paths {
			record, err := r.prepare(path)
			if err != nil {
				return fail(err)
			}
			r.manifest.Volumes = append(r.manifest.Volumes, record)
		}
		if err := validatePaths(root, r.manifest.Volumes); err != nil {
			return fail(err)
		}
		if err := r.save(); err != nil {
			return fail(err)
		}
	} else if err != nil {
		return fail(fmt.Errorf("load volume registry (run init or complete join preparation): %w", err))
	}
	if err := validateManifest(root, r.manifest, id); err != nil {
		return fail(err)
	}
	if len(paths) != 0 {
		requested := make([]string, 0, len(paths))
		for _, path := range paths {
			canonical, err := canonicalDirectory(path)
			if err != nil {
				return fail(err)
			}
			requested = append(requested, canonical)
		}
		registered := make([]string, 0, len(r.manifest.Volumes))
		for _, record := range r.manifest.Volumes {
			registered = append(registered, record.Path)
		}
		slices.Sort(requested)
		slices.Sort(registered)
		if !slices.Equal(requested, registered) {
			return fail(errors.New("init cannot replace the persisted volume set; use volume add"))
		}
	}
	var saved owner
	err = readJSON(filepath.Join(root, storageName), &saved)
	if errors.Is(err, os.ErrNotExist) && initialize {
		for _, v := range r.manifest.Volumes {
			if v.State != Registered {
				return fail(errors.New("active Store identity marker missing; refusing recreation"))
			}
		}
		for _, resource := range r.manifest.Resources {
			if resource.State != ResourcePending {
				return fail(errors.New("active Store resource identity marker missing; refusing recreation"))
			}
		}
		if err := writeJSON(filepath.Join(root, storageName), ".dubi-storage-", r.manifest.owner, true); err != nil {
			return fail(err)
		}
	} else if err != nil || saved != r.manifest.owner {
		return fail(errors.Join(err, errors.New("Store identity marker is missing or does not match volume registry")))
	}
	// A previous manifest publication may have returned an uncertain directory
	// Sync result. Establish its durability before trusting active resources.
	if err := syncDirectory(root); err != nil {
		return fail(err)
	}
	for i := range r.manifest.Volumes {
		if initialize && r.manifest.Volumes[i].State == Registered {
			if err := r.activate(i); err != nil {
				return fail(err)
			}
		}
	}
	for _, record := range r.manifest.Volumes {
		if record.State == Removed || record.State == Registered {
			continue
		}
		if err := r.verify(record); err != nil {
			continue
		} // Degraded, never recreated.
		if err := r.hold(record); err != nil {
			return fail(err)
		}
	}
	return r, nil
}

func validateManifest(root string, m manifest, id identity.Identity) error {
	if m.Version != 2 {
		return errors.New("unsupported Store/Volume metadata format; v2 resource intent is required, no automatic migration")
	}
	if m.NodeID != id.NodeID || m.RaftID != id.RaftID || !validID(m.StoreID, "store_") {
		return errors.New("volume registry does not match durable Node identity")
	}
	if err := validateResources(m.Resources); err != nil {
		return err
	}
	return validatePaths(root, m.Volumes)
}

func (r *Registry) prepare(path string) (Record, error) {
	path, err := canonicalDirectory(path)
	if err != nil {
		return Record{}, err
	}
	id, err := randomID("volume_")
	if err != nil {
		return Record{}, err
	}
	record := Record{ID: id, Path: path, State: Registered}
	if err := validatePaths(r.root, append(slices.Clone(r.manifest.Volumes), record)); err != nil {
		return Record{}, err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return Record{}, err
	}
	if len(entries) != 0 {
		return Record{}, errors.New("new volume directory must be empty")
	}
	record.Device, record.Inode, err = directoryIdentity(path)
	return record, err
}

func (r *Registry) verify(record Record) error {
	canonical, err := canonicalDirectory(record.Path)
	if err != nil {
		return err
	}
	if canonical != record.Path {
		return errors.New("registered volume path resolves to a different directory")
	}
	var actual marker
	if err := readJSON(filepath.Join(record.Path, markerName), &actual); err != nil {
		return err
	}
	if actual != (marker{owner: r.manifest.owner, VolumeID: record.ID}) {
		return errors.New("volume marker does not match Node/Store/Volume identity")
	}
	if held := r.markers[record.ID]; held != nil {
		old, err := held.Stat()
		if err != nil {
			return err
		}
		current, err := os.Stat(filepath.Join(record.Path, markerName))
		if err != nil {
			return err
		}
		if !os.SameFile(old, current) {
			return errors.New("volume marker was replaced while open")
		}
	}
	return nil
}

func (r *Registry) hold(record Record) error {
	if r.markers[record.ID] != nil {
		return nil
	}
	file, err := lockFile(filepath.Join(record.Path, markerName), false)
	if err != nil {
		return err
	}
	r.markers[record.ID] = file
	return r.verify(record)
}

func (r *Registry) activate(index int) error {
	record := r.manifest.Volumes[index]
	dev, ino, err := directoryIdentity(record.Path)
	if err != nil {
		return err
	}
	if dev != record.Device || ino != record.Inode {
		return errors.New("pending volume directory was replaced; refusing registration")
	}
	path := filepath.Join(record.Path, markerName)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		entries, err := os.ReadDir(record.Path)
		if err != nil {
			return err
		}
		prefix := ".dubi-volume-" + record.ID + "-"
		for _, entry := range entries {
			// A crash may leave only this registration's staging files.
			if !strings.HasPrefix(entry.Name(), prefix) || !entry.Type().IsRegular() {
				return errors.New("pending volume acquired foreign data before activation")
			}
		}
		if err := writeJSON(path, prefix, marker{owner: r.manifest.owner, VolumeID: record.ID}, true); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if err := r.verify(record); err != nil {
		return err
	}
	if err := r.hold(record); err != nil {
		return err
	}
	// A previous attempt may have published the marker but failed to sync
	// its directory. Confirm durability before publishing Active in the registry.
	if err := syncDirectory(record.Path); err != nil {
		return err
	}
	r.manifest.Volumes[index].State = Active
	r.manifest.Volumes[index].Device, r.manifest.Volumes[index].Inode = 0, 0
	return r.save()
}

func (r *Registry) save() error {
	var err error
	if r.publish != nil {
		err = r.publish(r.manifest)
	} else {
		err = writeJSON(filepath.Join(r.root, manifestName), ".dubi-volumes-", r.manifest, false)
	}
	if err != nil {
		r.failed = err // Publication may have happened: require reopening before further mutations.
		return err
	}
	return nil
}

// Add appends a volume without changing any existing volume's state.
// Repeating the same path resumes its pending registration or returns its ID.
func (r *Registry) Add(path string) (Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkCatalogLocked(); err != nil {
		return Record{}, err
	}
	path, err := canonicalDirectory(path)
	if err != nil {
		return Record{}, err
	}
	for i, record := range r.manifest.Volumes {
		if record.Path != path {
			continue
		}
		if record.State == Removed || record.State == Draining {
			return Record{}, errors.New("volume is draining or removed")
		}
		if record.State == Registered {
			if err := r.activate(i); err != nil {
				return Record{}, err
			}
		} else {
			if err := r.verify(record); err != nil {
				return Record{}, err
			}
			if err := r.hold(record); err != nil {
				return Record{}, err
			}
		}
		return r.manifest.Volumes[i], nil
	}
	record, err := r.prepare(path)
	if err != nil {
		return Record{}, err
	}
	r.manifest.Volumes = append(r.manifest.Volumes, record)
	if err := r.save(); err != nil {
		return Record{}, err
	}
	if err := r.activate(len(r.manifest.Volumes) - 1); err != nil {
		return Record{}, err
	}
	return r.manifest.Volumes[len(r.manifest.Volumes)-1], nil
}

// BeginDrain durably stops new writes to one Volume while preserving reads.
// It does not move or delete data. A caller must first migrate/compact the
// Pack contents and then call RemoveDrained; retries observe the same state.
func (r *Registry) BeginDrain(id string) (Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkCatalogLocked(); err != nil {
		return Record{}, err
	}
	for i, record := range r.manifest.Volumes {
		if record.ID != id {
			continue
		}
		switch record.State {
		case Active:
			r.manifest.Volumes[i].State = Draining
			if err := r.save(); err != nil {
				return Record{}, err
			}
			return r.manifest.Volumes[i], nil
		case Draining:
			return record, nil
		default:
			return Record{}, fmt.Errorf("%w: %s", ErrVolumeNotActive, record.State)
		}
	}
	return Record{}, errors.New("unknown VolumeID")
}

// RemoveDrained finalizes removal when the volume has no Pack bytes, or after
// an explicit operator decision that its path is permanently gone. Collection
// metadata/locks may remain after successful migration; actual Pack data is
// the safety boundary checked here.
func (r *Registry) RemoveDrained(id string) (Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkCatalogLocked(); err != nil {
		return Record{}, err
	}
	for i, record := range r.manifest.Volumes {
		if record.ID != id {
			continue
		}
		if record.State != Draining {
			return Record{}, fmt.Errorf("%w: %s", ErrVolumeNotDraining, record.State)
		}
		if err := hasPackData(record.Path); err != nil {
			return Record{}, err
		}
		// A permanently failed disk is an explicit administrative decision,
		// not a successful verification. If the registered path is gone, the
		// old marker cannot be checked; Drain already records that no new data
		// may be placed there, and the operator is now authorizing its removal.
		if _, pathErr := os.Stat(record.Path); pathErr == nil {
			if err := r.verify(record); err != nil {
				return Record{}, err
			}
		} else if !errors.Is(pathErr, os.ErrNotExist) {
			return Record{}, pathErr
		}
		r.manifest.Volumes[i].State = Removed
		if err := r.save(); err != nil {
			return Record{}, err
		}
		if marker := r.markers[id]; marker != nil {
			if err := marker.Close(); err != nil {
				return Record{}, err
			}
			delete(r.markers, id)
		}
		return r.manifest.Volumes[i], nil
	}
	return Record{}, errors.New("unknown VolumeID")
}

// hasPackData distinguishes the persistent Pack collection directory from
// actual Pack bytes. A successful migration leaves the catalog and lock in
// place, so checking only whether "packs" exists would make safe removal
// impossible. Unknown regular files are treated as data and block removal.
func hasPackData(volumePath string) error {
	directory := filepath.Join(volumePath, "packs")
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "DUBI_PACKS" || name == ".DUBI_PACKS.lock" || strings.HasPrefix(name, ".dubi-") {
			continue
		}
		if entry.IsDir() || strings.HasSuffix(name, ".pack") || strings.HasSuffix(name, ".state") || name != "" {
			return ErrVolumeHasData
		}
	}
	return nil
}

func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var failures []error
	for _, file := range r.markers {
		failures = append(failures, file.Close())
	}
	if r.lock != nil {
		failures = append(failures, r.lock.Close())
	}
	return errors.Join(failures...)
}
