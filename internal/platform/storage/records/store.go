package records

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/protocol"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

var ErrNotHeld = errors.New("immutable record is not held by this Store")
var ErrCorrupt = errors.New("immutable record index or payload is corrupt")
var ErrUnavailable = errors.New("immutable record Store is unavailable")
var identityKey = []byte("meta/identity")

const recordIndexVersion = protocol.RecordIndexFormatVersion

// CompactReport describes the conservative physical garbage-collection pass.
// It removes only sealed Packs with no live index entry; Packs containing both
// live and obsolete records remain intact for a later rewrite protocol.
type CompactReport struct {
	ScannedPacks   int
	RemovedPacks   int
	ReclaimedBytes int64
}

type packBackend interface {
	StoreID() string
	Append(context.Context, pack.Record) (pack.Location, error)
	Read(pack.Location) (pack.Record, error)
}

// WriteErrors exposes node-local Pack quarantine observations without turning
// them into a claim about cluster durability.
func (s *Store) WriteErrors() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if backend, ok := s.packs.(interface{ WriteErrors() map[string]string }); ok {
		return backend.WriteErrors()
	}
	return nil
}

type entry struct {
	Version    int           `json:"version"`
	Descriptor Descriptor    `json:"descriptor"`
	Location   pack.Location `json:"location"`
}
type Store struct {
	mu                        sync.Mutex
	db                        *pebble.DB
	packs                     packBackend
	identity                  registration
	root                      string
	rootInfo, dirInfo, dbInfo os.FileInfo
	lease                     *os.File
	closed                    bool
	failed                    error
	commit                    func(*pebble.Batch) error // Test-only Sync uncertainty boundary.
	binding                   *volume.ResourceBinding
}

// OpenNodeStore creates only under a durable pending parent intent, otherwise
// reopens the exact registered resource. Missing files under an active parent
// NEVER mean an empty Store. Close this index before Pool and Registry.
func OpenNodeStore(pool *pack.Pool, registry *volume.Registry) (*Store, error) {
	return openNodeStore(pool, registry, nil)
}
func openNodeStore(pool *pack.Pool, registry *volume.Registry, hook func(string) error) (*Store, error) {
	if pool == nil || registry == nil {
		return nil, errors.New("immutable records require shared Pool and parent Store catalog")
	}
	binding, err := registry.Resource(volume.ImmutableRecordsResource)
	if err != nil {
		return nil, err
	}
	if binding.StoreID() != pool.StoreID() {
		return nil, errors.New("Pack Pool differs from parent Store")
	}
	intent, err := binding.Current()
	if err != nil {
		return nil, err
	}
	initialize := false
	if intent.State == volume.ResourcePending {
		_, err := readRegistration(binding.Root())
		if errors.Is(err, os.ErrNotExist) {
			initialize = true
		} else if err != nil {
			return nil, err
		}
	}
	s, err := openBound(binding.Root(), pool, initialize, hook, binding)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Store, error) { return nil, errors.Join(err, s.Close()) }
	if hook != nil {
		if err := hook("before-parent-active"); err != nil {
			return fail(err)
		}
	}
	if err := binding.Activate(); err != nil {
		return fail(err)
	}
	if hook != nil {
		if err := hook("after-parent-active"); err != nil {
			return fail(err)
		}
	}
	s.binding = binding
	if err := s.usable(); err != nil {
		return fail(err)
	}
	return s, nil
}

func (s *Store) usable() error {
	if s.closed {
		return fmt.Errorf("%w: closed", ErrUnavailable)
	}
	if s.failed != nil {
		return errors.Join(ErrUnavailable, s.failed)
	}
	if s.binding != nil {
		if err := s.binding.CheckActive(); err != nil {
			return errors.Join(ErrUnavailable, err)
		}
	}
	for path, want := range map[string]os.FileInfo{s.root: s.rootInfo, filepath.Join(s.root, directory): s.dirInfo, filepath.Join(s.root, directory, "index"): s.dbInfo} {
		got, err := os.Lstat(path)
		if err != nil || !got.IsDir() || !os.SameFile(want, got) {
			return errors.Join(ErrUnavailable, err, errors.New("immutable record resource directory changed"))
		}
	}
	r, err := readRegistration(s.root)
	if err != nil || r != s.identity {
		return errors.Join(ErrUnavailable, err, errors.New("immutable record registration changed"))
	}
	return nil
}

// Put acknowledges only after Pack synchronization and index Sync. It is
// content-idempotent, including after process restart. A duplicate is freshly
// read and checked, never accepted from the locator or a previous result alone.
// Different content at the same logical key remains separately addressable.
func (s *Store) Put(ctx context.Context, d Descriptor, payload []byte) (Holding, error) {
	if err := ctx.Err(); err != nil {
		return Holding{}, err
	}
	if err := d.Validate(); err != nil {
		return Holding{}, err
	}
	if uint64(len(payload)) != d.Size {
		return Holding{}, errors.New("record payload size differs from descriptor")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return Holding{}, err
	}
	if err := ctx.Err(); err != nil {
		return Holding{}, err
	}
	// Bound retained staging to one payload per Store, not one allocation per
	// queued caller. The caller must not mutate its input during this call.
	payload = bytes.Clone(payload)
	d = d.clone()
	if !d.matches(payload) {
		return Holding{}, errors.New("record payload differs from expected size or digest")
	}
	id, _ := d.ID()
	if existing, found, err := s.lookup(id); err != nil {
		return Holding{}, err
	} else if found {
		_, holding, err := s.read(id, d, existing)
		return holding, err
	}
	loc, err := s.packs.Append(ctx, pack.Record{EngineID: d.Keyspace.EngineID, TenantID: d.Keyspace.TenantID, Key: recordKey(id), Payload: payload})
	if err != nil {
		return Holding{}, err
	}
	if loc.StoreID != s.identity.StoreID {
		return Holding{}, errors.New("Pack append returned foreign Store")
	}
	if err := s.usable(); err != nil {
		return Holding{}, err
	}
	b := s.db.NewBatch()
	defer b.Close()
	e := entry{Version: recordIndexVersion, Descriptor: d, Location: loc}
	encoded, err := json.Marshal(e)
	if err != nil {
		return Holding{}, err
	}
	if err := b.Set(indexKey(id), encoded, nil); err != nil {
		return Holding{}, err
	}
	if s.commit != nil {
		err = s.commit(b)
	} else {
		err = b.Commit(pebble.Sync)
	}
	if err != nil {
		s.failed = fmt.Errorf("immutable record index Sync uncertain; reopen required: %w", err)
		return Holding{}, s.failed
	}
	// Confirm the physical record too. A backend's successful Append is not a
	// substitute for descriptor/identity/CRC/digest verification.
	_, holding, err := s.read(id, d, e)
	return holding, err
}

func (s *Store) Read(ctx context.Context, d Descriptor) ([]byte, Holding, error) {
	if err := ctx.Err(); err != nil {
		return nil, Holding{}, err
	}
	id, err := d.ID()
	if err != nil {
		return nil, Holding{}, err
	}
	d = d.clone()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, Holding{}, err
	}
	if err := ctx.Err(); err != nil {
		return nil, Holding{}, err
	}
	e, found, err := s.lookup(id)
	if err != nil {
		return nil, Holding{}, err
	}
	if !found {
		return nil, Holding{}, ErrNotHeld
	}
	payload, holding, err := s.read(id, d, e)
	if err != nil {
		return nil, Holding{}, err
	}
	if err := ctx.Err(); err != nil {
		return nil, Holding{}, err
	}
	return payload, holding, nil
}
func (s *Store) Verify(ctx context.Context, d Descriptor) (Holding, error) {
	_, h, err := s.Read(ctx, d)
	return h, err
}

// Compact snapshots the complete immutable-record index while holding the
// Store lock, then asks the shared Pack Pool to remove unreachable sealed
// Packs. The index is never changed by this operation. A missing/corrupt
// index entry fails closed so a Pack cannot be mistaken for garbage.
func (s *Store) Compact(ctx context.Context) (CompactReport, error) {
	var result CompactReport
	if ctx == nil {
		return result, errors.New("nil compaction context")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return result, err
	}
	compactor, ok := s.packs.(interface {
		Compact(context.Context, func(pack.Location) bool) (pack.CompactReport, error)
	})
	if !ok {
		return result, errors.New("Pack backend does not support compaction")
	}
	live := make(map[pack.Location]struct{})
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("record/"), UpperBound: []byte("record0")})
	if err != nil {
		return result, classifyIndexRead(err)
	}
	for ok := iter.SeekGE([]byte("record/")); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			_ = iter.Close()
			return result, err
		}
		if !bytes.HasPrefix(iter.Key(), []byte("record/")) {
			break
		}
		var e entry
		if len(iter.Value()) > 16<<10 || strictJSON(iter.Value(), &e) != nil {
			_ = iter.Close()
			return result, ErrCorrupt
		}
		id := strings.TrimPrefix(string(iter.Key()), "record/")
		if err := s.validateEntry(id, e); err != nil {
			_ = iter.Close()
			return result, err
		}
		live[e.Location] = struct{}{}
	}
	if err := errors.Join(iter.Error(), iter.Close()); err != nil {
		return result, classifyIndexRead(err)
	}
	report, err := compactor.Compact(ctx, func(loc pack.Location) bool {
		_, found := live[loc]
		return found
	})
	if err != nil {
		return result, err
	}
	return CompactReport{ScannedPacks: report.ScannedPacks, RemovedPacks: report.RemovedPacks, ReclaimedBytes: report.ReclaimedBytes}, nil
}

// RelocationNeeded distinguishes an unreadable Pack location from a missing or
// damaged index. It is only a preflight for authenticated repair: callers must
// recheck authority and RepairExisting repeats every condition under the lock.
func (s *Store) RelocationNeeded(ctx context.Context, d Descriptor) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	id, err := d.ID()
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return false, err
	}
	e, found, err := s.lookup(id)
	if err != nil {
		return false, err
	}
	if !found {
		return false, ErrNotHeld
	}
	if err := s.validateEntry(id, e); err != nil {
		return false, err
	}
	_, _, err = s.read(id, d, e)
	if err == nil {
		return false, nil
	}
	if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrCorrupt) {
		return true, nil
	}
	return false, err
}

// RepairExisting appends trusted, descriptor-matching bytes at a new local
// Pack location when an existing valid index points to unreadable bytes. The
// old locator stays published until the new bytes are physically verified and
// the replacement index entry Syncs. It never repairs a missing/corrupt index,
// changes a logical reference, or makes a quorum durability claim. The caller
// must obtain the payload from an authenticated, current cohort source.
func (s *Store) RepairExisting(ctx context.Context, d Descriptor, payload []byte) (Holding, error) {
	if err := ctx.Err(); err != nil {
		return Holding{}, err
	}
	id, err := d.ID()
	if err != nil {
		return Holding{}, err
	}
	if !d.matches(payload) {
		return Holding{}, errors.New("repair payload differs from immutable record descriptor")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return Holding{}, err
	}
	if err := ctx.Err(); err != nil {
		return Holding{}, err
	}
	payload = bytes.Clone(payload)
	d = d.clone()
	existing, found, err := s.lookup(id)
	if err != nil {
		return Holding{}, err
	}
	if !found {
		return Holding{}, ErrNotHeld
	}
	if err := s.validateEntry(id, existing); err != nil {
		return Holding{}, err
	}
	_, holding, err := s.read(id, d, existing)
	if err == nil {
		return holding, nil
	}
	if !errors.Is(err, ErrUnavailable) && !errors.Is(err, ErrCorrupt) {
		return Holding{}, err
	}
	loc, err := s.packs.Append(ctx, pack.Record{EngineID: d.Keyspace.EngineID, TenantID: d.Keyspace.TenantID, Key: recordKey(id), Payload: payload})
	if err != nil {
		return Holding{}, err
	}
	if loc.StoreID != s.identity.StoreID || loc == existing.Location {
		return Holding{}, errors.New("repair Pack append returned old or foreign location")
	}
	if err := s.usable(); err != nil {
		return Holding{}, err
	}
	if err := ctx.Err(); err != nil {
		return Holding{}, err
	}
	next := entry{Version: recordIndexVersion, Descriptor: d, Location: loc}
	if _, _, err := s.read(id, d, next); err != nil {
		return Holding{}, fmt.Errorf("verify replacement before index publication: %w", err)
	}
	b := s.db.NewBatch()
	defer b.Close()
	encoded, err := json.Marshal(next)
	if err != nil {
		return Holding{}, err
	}
	if err := b.Set(indexKey(id), encoded, nil); err != nil {
		return Holding{}, err
	}
	if s.commit != nil {
		err = s.commit(b)
	} else {
		err = b.Commit(pebble.Sync)
	}
	if err != nil {
		s.failed = fmt.Errorf("immutable record relocation Sync uncertain; reopen required: %w", err)
		return Holding{}, s.failed
	}
	_, holding, err = s.read(id, d, next)
	return holding, err
}

func (s *Store) validateEntry(id string, e entry) error {
	storedID, err := e.Descriptor.ID()
	if err != nil || e.Version != recordIndexVersion || storedID != id || e.Location.StoreID != s.identity.StoreID ||
		!strings.HasPrefix(e.Location.VolumeID, "volume_") || !validToken(strings.TrimPrefix(e.Location.VolumeID, "volume_")) ||
		!strings.HasPrefix(e.Location.PackID, "pack_") || !validToken(strings.TrimPrefix(e.Location.PackID, "pack_")) ||
		e.Location.Offset <= 0 || e.Location.Length <= 0 || e.Location.Length > pack.MaxPayloadSize+pack.MaxKeySize+pack.MaxEngineSize+pack.MaxTenantSize+1024 {
		return ErrCorrupt
	}
	return nil
}

func (s *Store) read(id string, d Descriptor, e entry) ([]byte, Holding, error) {
	if err := s.validateEntry(id, e); err != nil {
		return nil, Holding{}, err
	}
	r, err := s.packs.Read(e.Location)
	if err != nil {
		// Any Pack read failure means this node cannot currently prove its
		// physical holding. Preserve the cause, but let quorum callers count
		// only other nodes that actually verified the record. Known integrity
		// failures remain distinct for diagnostics and RPC DataLoss mapping.
		if errors.Is(err, pack.ErrCorrupt) {
			return nil, Holding{}, fmt.Errorf("%w: verify immutable Pack record: %w", ErrCorrupt, err)
		}
		return nil, Holding{}, fmt.Errorf("%w: verify immutable Pack record: %w", ErrUnavailable, err)
	}
	if r.EngineID != d.Keyspace.EngineID || r.TenantID != d.Keyspace.TenantID || !bytes.Equal(r.Key, recordKey(id)) || !d.matches(r.Payload) {
		return nil, Holding{}, ErrCorrupt
	}
	if err := s.usable(); err != nil {
		return nil, Holding{}, err
	}
	return r.Payload, Holding{RecordID: id, Descriptor: d.clone(), StoreID: s.identity.StoreID, VolumeID: e.Location.VolumeID}, nil
}
func (s *Store) lookup(id string) (entry, bool, error) {
	value, closer, err := s.db.Get(indexKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return entry{}, false, nil
	}
	if err != nil {
		return entry{}, false, classifyIndexRead(err)
	}
	defer closer.Close()
	var e entry
	if len(value) > 16<<10 || strictJSON(value, &e) != nil {
		return e, false, ErrCorrupt
	}
	return e, true, nil
}

func classifyIndexRead(err error) error {
	// An index IO failure cannot prove either absence or physical holding.
	// Keep known Pebble corruption distinct from transient unavailability,
	// while preserving the original cause for local diagnostics.
	if pebble.IsCorruptionError(err) {
		return fmt.Errorf("%w: read immutable record index: %w", ErrCorrupt, err)
	}
	return fmt.Errorf("%w: read immutable record index: %w", ErrUnavailable, err)
}

func indexKey(id string) []byte  { return []byte("record/" + id) }
func recordKey(id string) []byte { return []byte("DUBI immutable record v1/" + id) }
func strictJSON(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("trailing immutable record JSON")
	}
	return nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return errors.Join(s.db.Close(), s.lease.Close())
}
