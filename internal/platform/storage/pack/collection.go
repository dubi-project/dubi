package pack

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/protocol"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"golang.org/x/sys/unix"
)

const (
	catalogName          = "DUBI_PACKS"
	collectionMarkerName = "DUBI_PACKS_ID"
	collectionLockName   = ".DUBI_PACKS.lock"
	maxCatalogSize       = 1 << 20
	maxCollectionPacks   = 16384
)

var ErrCollectionFull = errors.New("Pack collection limit reached; compaction/catalog scaling required")

// CompactReport describes the conservative Pack maintenance pass. A Pack is
// removed only when every physical record in it is unreachable from the
// caller's durable index. Packs containing a mixture of live and obsolete
// records are left intact until an index-aware rewrite is available.
type CompactReport struct {
	ScannedPacks   int
	RemovedPacks   int
	ReclaimedBytes int64
}

type collectionIdentity struct {
	StoreID      string `json:"store_id"`
	VolumeID     string `json:"volume_id"`
	CollectionID string `json:"collection_id"`
}

type catalog struct {
	collectionIdentity
	Version    int      `json:"version"`
	TargetSize int64    `json:"target_size"`
	Packs      []string `json:"packs"`             // Publication order; only the final Pack may be active.
	Pending    string   `json:"pending,omitempty"` // Durable creation intent; never contains user records.
}

// Collection owns all Packs for one Volume, shared by all Engines. It rotates transparently
// but does not choose volumes, deduplicate module keys, or publish logical indexes.
// Methods serialize; Scan callbacks must not re-enter Collection or Registry.
type Collection struct {
	mu        sync.Mutex
	registry  *volume.Registry
	catalog   catalog
	lock      *os.File
	directory os.FileInfo
	active    *Pack // At most one persistent Pack descriptor, independent of catalog size.
	failed    error
	closed    bool
}

// InitializeCollection creates an empty collection or reopens the persisted one.
// A zero target reuses its saved target (512 MiB on first use). A nonzero target
// must match on retry; initializing never adopts standalone/orphan Pack files.
func InitializeCollection(registry *volume.Registry, volumeID string, target int64) (*Collection, error) {
	return openCollection(registry, volumeID, target, true)
}

// OpenCollection never initializes missing directories or metadata. Both open
// paths recover a persisted creation intent and verify every cataloged Pack.
func OpenCollection(registry *volume.Registry, volumeID string) (*Collection, error) {
	return openCollection(registry, volumeID, 0, false)
}

func openCollection(registry *volume.Registry, volumeID string, target int64, initialize bool) (*Collection, error) {
	return openCollectionMode(registry, volumeID, target, initialize, true)
}

// Pool reads can recover already published Packs without spending space to
// finish a Pending creation. The next budgeted Append resumes that intent.
func openCollectionMode(registry *volume.Registry, volumeID string, target int64, initialize, resumePending bool) (*Collection, error) {
	if registry == nil || (target != 0 && (target <= headerSize || target > MaxTargetSize)) {
		return nil, errors.New("invalid collection registry or target")
	}
	m := &Collection{registry: registry}
	// Opening an existing collection for reads/maintenance must remain
	// possible after Drain. Initialization and pending creation need the write
	// gate; ordinary OpenCollection does not.
	err := registry.WithVolume(volumeID, initialize || resumePending, func(root *os.Root, storeID string) error {
		if err := rejectLegacyLayout(root); err != nil {
			return err
		}
		name := packDirectory
		_, markerErr := root.Lstat(collectionMarkerName)
		if initialize && errors.Is(markerErr, os.ErrNotExist) {
			if err := root.Mkdir(name, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			if err := syncDirectory(root); err != nil {
				return err
			}
		}
		dir, err := openDirectory(root, name)
		if err != nil {
			return err
		}
		defer dir.Close()
		m.directory, err = dir.Stat(".")
		if err != nil {
			return err
		}
		_, catalogErr := dir.Lstat(catalogName)
		flags := os.O_RDWR
		if initialize && errors.Is(catalogErr, os.ErrNotExist) && errors.Is(markerErr, os.ErrNotExist) {
			flags |= os.O_CREATE
		}
		m.lock, err = regularFile(dir, collectionLockName, flags)
		if err != nil {
			return err
		}
		if err := unix.Flock(int(m.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return fmt.Errorf("collection already open: %w", err)
		}
		err = readDocument(dir, catalogName, "DUBICS02", maxCatalogSize, &m.catalog)
		if errors.Is(err, os.ErrNotExist) && initialize {
			if !errors.Is(markerErr, os.ErrNotExist) {
				return errors.New("collection catalog is missing; refusing empty replacement")
			}
			if err := checkEmptyCollection(dir); err != nil {
				return err
			}
			id, err := randomID()
			if err != nil {
				return err
			}
			size := target
			if size == 0 {
				size = DefaultTargetSize
			}
			m.catalog = catalog{collectionIdentity: collectionIdentity{StoreID: storeID, VolumeID: volumeID, CollectionID: "set_" + id[5:]}, Version: protocol.PackCatalogFormatVersion, TargetSize: size, Packs: []string{}}
			if err := writeDocument(dir, catalogName, "DUBICS02", m.catalog, true); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if err := validateCatalog(m.catalog); err != nil {
			return err
		}
		if m.catalog.StoreID != storeID || m.catalog.VolumeID != volumeID || (target != 0 && target != m.catalog.TargetSize) {
			return errors.New("collection scope or target mismatch")
		}
		var marker collectionIdentity
		err = readDocument(root, collectionMarkerName, "DUBICI02", 1024, &marker)
		if errors.Is(err, os.ErrNotExist) && initialize && len(m.catalog.Packs) == 0 && m.catalog.Pending == "" {
			// Initialization publishes the empty catalog before its sentinel.
			// No Pack may be created until both are durable.
			if err := writeDocument(root, collectionMarkerName, "DUBICI02", m.catalog.collectionIdentity, true); err != nil {
				return err
			}
		} else if err != nil || marker != m.catalog.collectionIdentity {
			return errors.Join(err, errors.New("collection identity missing or mismatched"))
		}
		return errors.Join(syncDirectory(dir), syncDirectory(root))
	})
	if err == nil {
		err = m.recover(resumePending)
	}
	if err != nil {
		return nil, errors.Join(err, m.Close())
	}
	return m, nil
}

func validateCatalog(c catalog) error {
	if c.Version != protocol.PackCatalogFormatVersion || c.TargetSize <= headerSize || c.TargetSize > MaxTargetSize || len(c.Packs) > maxCollectionPacks || !strings.HasPrefix(c.CollectionID, "set_") || !idPattern.MatchString("pack_"+strings.TrimPrefix(c.CollectionID, "set_")) {
		return ErrCorrupt
	}
	seen := make(map[string]bool, len(c.Packs)+1)
	for _, id := range c.Packs {
		if !idPattern.MatchString(id) || seen[id] {
			return ErrCorrupt
		}
		seen[id] = true
	}
	if c.Pending != "" && (!idPattern.MatchString(c.Pending) || seen[c.Pending] || len(c.Packs) == maxCollectionPacks) {
		return ErrCorrupt
	}
	return nil
}

// access verifies disk authority before each operation. Never call Pack methods
// inside its callback: those independently acquire the Registry gate.
func (m *Collection) access(fn func(*os.Root) error) error {
	return m.accessMode(false, true, fn)
}

func (m *Collection) readAccess(fn func(*os.Root) error) error {
	return m.accessMode(false, false, fn)
}

func (m *Collection) maintenanceAccess(fn func(*os.Root) error) error {
	return m.accessMode(true, true, fn)
}

func (m *Collection) accessMode(maintenance, write bool, fn func(*os.Root) error) error {
	if m.closed || m.failed != nil {
		return errors.Join(m.failed, errors.New("collection is closed or requires recovery"))
	}
	access := m.registry.WithVolume
	if maintenance {
		return m.registry.WithVolumeMaintenance(m.catalog.VolumeID, func(root *os.Root, storeID string) error {
			return m.verifyAccess(root, storeID, fn)
		})
	}
	return access(m.catalog.VolumeID, write, func(root *os.Root, storeID string) error {
		return m.verifyAccess(root, storeID, fn)
	})
}

func (m *Collection) verifyAccess(root *os.Root, storeID string, fn func(*os.Root) error) error {
	if storeID != m.catalog.StoreID {
		return errors.New("collection Store changed")
	}
	dir, err := openDirectory(root, packDirectory)
	if err != nil {
		return err
	}
	defer dir.Close()
	info, err := dir.Stat(".")
	if err != nil {
		return err
	}
	if !os.SameFile(info, m.directory) {
		return errors.New("collection directory replaced")
	}
	info, err = dir.Lstat(collectionLockName)
	if err != nil {
		return err
	}
	held, err := m.lock.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || !os.SameFile(info, held) {
		return errors.New("collection lock replaced")
	}
	var marker collectionIdentity
	if err := readDocument(root, collectionMarkerName, "DUBICI02", 1024, &marker); err != nil {
		return err
	}
	if marker != m.catalog.collectionIdentity {
		return ErrCorrupt
	}
	var saved catalog
	if err := readDocument(dir, catalogName, "DUBICS02", maxCatalogSize, &saved); err != nil {
		return err
	}
	if !reflect.DeepEqual(saved, m.catalog) {
		return errors.New("collection catalog changed while open")
	}
	return fn(dir)
}

func (m *Collection) save(next catalog) error {
	return m.access(func(dir *os.Root) error {
		if err := validateCatalog(next); err != nil {
			return err
		}
		if err := writeDocument(dir, catalogName, "DUBICS02", next, false); err != nil {
			m.failed = err
			return err
		}
		m.catalog = next
		return nil
	})
}

func (m *Collection) recover(resumePending bool) error {
	for i, id := range m.catalog.Packs {
		p, err := open(m.registry, m.catalog.VolumeID, id, 0, openMode{managed: true, requireSealed: i != len(m.catalog.Packs)-1 || m.catalog.Pending != ""})
		if err != nil {
			return fmt.Errorf("open cataloged Pack %s: %w", id, err)
		}
		if p.header.TargetSize != m.catalog.TargetSize || (!p.state.Sealed && (i != len(m.catalog.Packs)-1 || m.catalog.Pending != "")) {
			return errors.Join(ErrCorrupt, p.Close())
		}
		if p.state.Sealed {
			if err := p.Close(); err != nil {
				return err
			}
		} else {
			m.active = p
		}
	}
	if m.catalog.Pending != "" && resumePending {
		return m.finishPending()
	}
	return nil
}

// A pending ID is durable proof that this Pack has never admitted records.
// Only its incomplete initial header (no checkpoint) may be rewritten. Any
// published checkpoint must describe a valid, empty, unsealed Pack.
func (m *Collection) finishPending() error {
	id := m.catalog.Pending
	if id == "" {
		return nil
	}
	err := m.access(func(dir *os.Root) error { return m.initializePending(dir, id) })
	if err != nil {
		return err
	}
	p, err := open(m.registry, m.catalog.VolumeID, id, 0, openMode{managed: true, requireEmpty: true})
	if err != nil {
		return err
	}
	if p.header.TargetSize != m.catalog.TargetSize || p.state.End != headerSize || p.state.Count != 0 || p.state.Sealed {
		return errors.Join(ErrCorrupt, p.Close())
	}
	next := m.catalog
	next.Packs = append(slices.Clone(next.Packs), id)
	next.Pending = ""
	if err := m.save(next); err != nil {
		return errors.Join(err, p.Close())
	}
	m.active = p
	return nil
}

func (m *Collection) ensureActive() error {
	if m.active != nil {
		return nil
	}
	if m.catalog.Pending != "" {
		return m.finishPending()
	}
	if len(m.catalog.Packs) == maxCollectionPacks {
		return ErrCollectionFull
	}
	id, err := randomID()
	if err != nil {
		return err
	}
	next := m.catalog
	next.Pending = id
	if err := m.save(next); err != nil {
		return err
	}
	return m.finishPending()
}

// Append automatically creates/rolls Packs. Catalog publication precedes record
// admission, so every returned Location is discoverable after restart.
func (m *Collection) Append(record Record) (Location, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := validateRecord(record); err != nil {
		return Location{}, err
	}
	if err := m.access(func(*os.Root) error { return nil }); err != nil {
		return Location{}, err
	}
	if err := m.ensureActive(); err != nil {
		if !errors.Is(err, ErrCollectionFull) {
			m.failed = err
		}
		return Location{}, err
	}
	loc, err := m.active.Append(record)
	if err != nil {
		m.failed = err
		return Location{}, err
	}
	if m.active.state.Sealed {
		p := m.active
		m.active = nil
		if err := p.Close(); err != nil {
			m.failed = err
			return Location{}, err
		}
	}
	return loc, nil
}

func (m *Collection) Read(loc Location) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.readAccess(func(*os.Root) error { return nil }); err != nil {
		return Record{}, err
	}
	if loc.StoreID != m.catalog.StoreID || loc.VolumeID != m.catalog.VolumeID || !slices.Contains(m.catalog.Packs, loc.PackID) {
		return Record{}, errors.New("location is not in this collection")
	}
	if m.active != nil && m.active.Scope().PackID == loc.PackID {
		return m.active.Read(loc)
	}
	p, err := m.openSealed(loc.PackID)
	if err != nil {
		return Record{}, err
	}
	record, err := p.Read(loc)
	return record, errors.Join(err, p.Close())
}

func (m *Collection) Scan(fn func(Location, Record) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fn == nil {
		return errors.New("nil Scan callback")
	}
	if err := m.readAccess(func(*os.Root) error { return nil }); err != nil {
		return err
	}
	for _, id := range m.catalog.Packs {
		if m.active != nil && m.active.Scope().PackID == id {
			if err := m.active.Scan(fn); err != nil {
				return err
			}
			continue
		}
		p, err := m.openSealed(id)
		if err != nil {
			return err
		}
		if err := errors.Join(p.Scan(fn), p.Close()); err != nil {
			return err
		}
	}
	return nil
}

func (m *Collection) openSealed(id string) (*Pack, error) {
	return open(m.registry, m.catalog.VolumeID, id, 0, openMode{managed: true, sealedRead: true})
}

// Seal seals the active Pack without creating an empty successor. The next
// successful Append creates it lazily. This is not a multi-resource checkpoint.
func (m *Collection) Seal() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.access(func(*os.Root) error { return nil }); err != nil {
		return err
	}
	if m.active == nil {
		return nil
	}
	if err := m.active.Seal(); err != nil {
		m.failed = err
		return err
	}
	p := m.active
	m.active = nil
	err := p.Close()
	if err != nil {
		m.failed = err
	}
	return err
}

// Compact removes unreachable sealed Packs. The live callback must come from
// a stable logical index snapshot held by the caller. It is evaluated for
// every physical record, so a malformed or unreadable Pack fails closed and
// is never removed. The catalog is durably published before old files are
// unlinked; an interruption therefore leaves either the old catalog or safe
// orphan files, never a catalog pointing at deleted data.
func (m *Collection) Compact(ctx context.Context, live func(Location) bool) (CompactReport, error) {
	var report CompactReport
	if live == nil {
		return report, errors.New("nil live Pack record predicate")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx == nil {
		return report, errors.New("nil compaction context")
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if err := m.maintenanceAccess(func(*os.Root) error { return nil }); err != nil {
		return report, err
	}
	if m.catalog.Pending != "" {
		return report, errors.New("Pack collection has an unfinished Pack creation")
	}
	// Freeze the append tail first. Compaction never copies or deletes from an
	// active Pack; sealing turns the current append boundary into the same
	// crash-safe input as older Packs, and the next write creates a successor.
	if m.active != nil {
		if err := m.active.Seal(); err != nil {
			return report, err
		}
		if err := m.active.Close(); err != nil {
			return report, err
		}
		m.active = nil
	}
	dead := make([]string, 0)
	deadBytes := make([]int64, 0)
	for _, id := range m.catalog.Packs {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		records, err := m.openSealed(id)
		if err != nil {
			return report, err
		}
		reachable := false
		var total int64
		err = records.Scan(func(loc Location, _ Record) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			total += loc.Length
			if live(loc) {
				reachable = true
			}
			return nil
		})
		err = errors.Join(err, records.Close())
		if err != nil {
			return report, err
		}
		report.ScannedPacks++
		if !reachable {
			dead = append(dead, id)
			deadBytes = append(deadBytes, total)
		}
	}
	if len(dead) == 0 {
		return report, nil
	}
	next := m.catalog
	next.Packs = make([]string, 0, len(m.catalog.Packs)-len(dead))
	for _, id := range m.catalog.Packs {
		if !slices.Contains(dead, id) {
			next.Packs = append(next.Packs, id)
		}
	}
	if err := m.maintenanceAccess(func(dir *os.Root) error {
		if err := validateCatalog(next); err != nil {
			return err
		}
		if err := writeDocument(dir, catalogName, "DUBICS02", next, false); err != nil {
			return err
		}
		if err := syncDirectory(dir); err != nil {
			return err
		}
		m.catalog = next
		for _, id := range dead {
			for _, suffix := range []string{".pack", ".state"} {
				if err := dir.Remove(id + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
		}
		return syncDirectory(dir)
	}); err != nil {
		return report, err
	}
	report.RemovedPacks = len(dead)
	for _, n := range deadBytes {
		report.ReclaimedBytes += n
	}
	return report, nil
}

func (m *Collection) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	var err error
	if m.active != nil {
		err = m.active.Close()
	}
	if m.lock != nil {
		err = errors.Join(err, m.lock.Close())
	}
	return err
}
