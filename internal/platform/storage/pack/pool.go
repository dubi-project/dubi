package pack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

// Pool is the node-local entry point for budgeted Pack writes across Volumes.
// It does not own Registry. Close Pool before Registry. One Pool per Registry
// is intended; Collection locks reject competing ownership when first opened.
// This is not Placement, rebalancing, a logical index or a public data API.
type Pool struct {
	mu          sync.Mutex
	registry    *volume.Registry
	storeID     string
	collections map[string]*Collection
	blocked     map[string]error
	closed      bool
}

// StoreID is the immutable local ownership scope, not a replicated identity.
func (p *Pool) StoreID() string { return p.storeID }

// OpenPool does not create data files or require every Volume to be healthy.
// Collections are opened lazily under the registry's durable Volume identities.
func OpenPool(registry *volume.Registry) (*Pool, error) {
	if registry == nil {
		return nil, errors.New("nil Volume registry")
	}
	report, err := registry.Status()
	if err != nil {
		return nil, err
	}
	return &Pool{registry: registry, storeID: report.StoreID, collections: make(map[string]*Collection), blocked: make(map[string]error)}, nil
}

// Append reserves before initializing/recovering a Collection or allocating the
// encoded record. A failed attempt never silently retries on another Volume:
// an error after persistence may still mean the record was committed.
func (p *Pool) Append(ctx context.Context, record Record) (Location, error) {
	return p.appendCandidates(ctx, record, nil)
}

// AppendAwayFromVolume writes a new physical copy to a healthy Active Volume
// other than sourceVolume. It is used by node-local migration and never treats
// another Volume on the same filesystem as an additional durability vote.
func (p *Pool) AppendAwayFromVolume(ctx context.Context, sourceVolume string, record Record) (Location, error) {
	if sourceVolume == "" {
		return Location{}, errors.New("source Volume is required")
	}
	report, err := p.registry.Status()
	if err != nil {
		return Location{}, err
	}
	candidates := make([]string, 0, len(report.Volumes))
	for _, item := range report.Volumes {
		if item.ID != sourceVolume && item.State == volume.Active {
			candidates = append(candidates, item.ID)
		}
	}
	if len(candidates) == 0 {
		return Location{}, volume.ErrNoWritableVolume
	}
	return p.appendCandidates(ctx, record, candidates)
}

// AppendToVolume is a maintenance primitive for an explicitly selected
// healthy Volume. The caller must select only an Active target; Reservation
// rechecks that state before any bytes are written.
func (p *Pool) AppendToVolume(ctx context.Context, volumeID string, record Record) (Location, error) {
	if volumeID == "" {
		return Location{}, errors.New("target Volume is required")
	}
	return p.appendCandidates(ctx, record, []string{volumeID})
}

func (p *Pool) appendCandidates(ctx context.Context, record Record, candidates []string) (Location, error) {
	if err := validateRecord(record); err != nil {
		return Location{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return Location{}, errors.New("Pack pool is closed")
	}
	if err := ctx.Err(); err != nil {
		return Location{}, err
	}
	report, err := p.registry.Status()
	if err != nil {
		return Location{}, err
	}
	eligible := make([]string, 0, len(report.Volumes))
	allowed := make(map[string]struct{}, len(candidates))
	for _, id := range candidates {
		allowed[id] = struct{}{}
	}
	for _, v := range report.Volumes {
		if p.blocked[v.ID] != nil || candidates != nil && !containsCandidate(allowed, v.ID) {
			continue
		}
		eligible = append(eligible, v.ID)
	}
	lease, err := p.registry.Reserve(appendBudget(record), eligible)
	if err != nil {
		return Location{}, err
	}
	defer lease.Release()
	if err := ctx.Err(); err != nil {
		return Location{}, err
	}
	if err := lease.Check(); err != nil {
		return Location{}, err
	}
	id := lease.VolumeID()
	m := p.collections[id]
	if m == nil {
		m, err = InitializeCollection(p.registry, id, 0)
		if err != nil {
			p.blocked[id] = err
			return Location{}, fmt.Errorf("open write Volume %s: %w", id, err)
		}
		p.collections[id] = m
	}
	if err := ctx.Err(); err != nil {
		return Location{}, err
	}
	if err := lease.Check(); err != nil {
		return Location{}, err
	}
	loc, err := m.Append(record)
	if err != nil {
		p.blocked[id] = err
		return Location{}, fmt.Errorf("append on Volume %s (outcome may be uncertain; no automatic retry): %w", id, err)
	}
	// Cancellation racing a completed fsync does not turn a known success into
	// an ambiguous failure. Cancellation is checked before starting the append.
	return loc, nil
}

func containsCandidate(allowed map[string]struct{}, id string) bool {
	_, ok := allowed[id]
	return ok
}

// SealAll freezes every current append tail before a rewrite-style maintenance
// pass. It is deliberately explicit: normal writes remain append-only and do
// not pay a compaction pause.
func (p *Pool) SealAll(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sealAllLocked(ctx)
}

func (p *Pool) sealAllLocked(ctx context.Context) error {
	if p.closed {
		return errors.New("Pack pool is closed")
	}
	if ctx == nil {
		return errors.New("nil seal context")
	}
	report, err := p.registry.Status()
	if err != nil {
		return err
	}
	for _, v := range report.Volumes {
		if v.State == volume.Removed || v.State == volume.Registered || p.collections[v.ID] != nil {
			continue
		}
		if _, err := os.Stat(v.Path + string(os.PathSeparator) + collectionMarkerName); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		collection, err := OpenCollection(p.registry, v.ID)
		if err != nil {
			return err
		}
		p.collections[v.ID] = collection
	}
	for _, collection := range p.collections {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := collection.Seal(); err != nil {
			return err
		}
	}
	return nil
}

// SnapshotPacks creates a stable list of sealed Pack files for a node-local
// backup checkpoint. The Pool lock makes sealing and enumeration one local cut:
// callers may copy the returned files after this method returns without an
// active writer changing them. It does not publish a cluster Recovery Point
// or infer any placement/durability semantics.
func (p *Pool) SnapshotPacks(ctx context.Context) ([]SnapshotPack, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.sealAllLocked(ctx); err != nil {
		return nil, err
	}
	report, err := p.registry.Status()
	if err != nil {
		return nil, err
	}
	result := make([]SnapshotPack, 0)
	for _, item := range report.Volumes {
		if item.State == volume.Removed || item.State == volume.Registered {
			continue
		}
		collection := p.collections[item.ID]
		if collection == nil {
			continue
		}
		packs, err := collection.snapshotPacks(item.Path)
		if err != nil {
			for index := range result {
				_ = result[index].Close()
			}
			return nil, fmt.Errorf("snapshot Volume %s: %w", item.ID, err)
		}
		result = append(result, packs...)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].VolumeID != result[j].VolumeID {
			return result[i].VolumeID < result[j].VolumeID
		}
		return result[i].PackID < result[j].PackID
	})
	return result, nil
}

// ImportSealed installs a backup Pack into one explicitly selected Volume.
// The caller supplies the destination Volume mapping; the Pool never guesses a
// replacement Volume during restore.  A Collection is initialized lazily and
// the import itself validates the complete sealed image before publication.
func (p *Pool) ImportSealed(ctx context.Context, volumeID, packID string, source io.Reader, size int64) error {
	if p == nil || p.registry == nil {
		return errors.New("Pack pool is unavailable")
	}
	if ctx == nil {
		return errors.New("nil import context")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("Pack pool is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if volumeID == "" {
		return errors.New("destination Volume is required")
	}
	collection := p.collections[volumeID]
	var err error
	if collection == nil {
		collection, err = InitializeCollection(p.registry, volumeID, 0)
		if err != nil {
			return err
		}
		p.collections[volumeID] = collection
	}
	if err := collection.ImportSealed(ctx, packID, source, size); err != nil {
		return err
	}
	return nil
}

// Read follows the exact persisted VolumeID, never substitutes another copy.
// It needs no free-space budget and does not create a missing Collection or
// finish Pending creation. Low-space volumes can still serve committed data.
func (p *Pool) Read(loc Location) (Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return Record{}, errors.New("Pack pool is closed")
	}
	if loc.StoreID != p.storeID {
		return Record{}, errors.New("location belongs to another Store")
	}
	m := p.collections[loc.VolumeID]
	if m == nil {
		var err error
		m, err = openCollectionMode(p.registry, loc.VolumeID, 0, false, false)
		if err != nil {
			return Record{}, err
		}
		p.collections[loc.VolumeID] = m
	}
	return m.Read(loc)
}

// WriteErrors reports volumes blocked for this Pool lifetime after a storage
// error. There is no automatic unquarantine; close/reopen after investigation.
func (p *Pool) WriteErrors() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make(map[string]string, len(p.blocked))
	for id, err := range p.blocked {
		result[id] = err.Error()
	}
	return result
}

// DrainVolume seals all open collections on a Volume before durably stopping
// new writes there. Existing Pack bytes remain readable. It is the storage
// half of a higher-level rebalance operation; it never claims data moved.
func (p *Pool) DrainVolume(ctx context.Context, volumeID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("Pack pool is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	collection := p.collections[volumeID]
	if collection == nil {
		// Drain must also seal a persisted active Pack after a fresh process
		// start; otherwise the later Draining state would strand an append tail
		// that cannot be reopened for ordinary writes.
		var err error
		collection, err = OpenCollection(p.registry, volumeID)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				collection = nil
			} else {
				return err
			}
		}
		if collection != nil {
			p.collections[volumeID] = collection
		}
	}
	if collection != nil {
		if err := collection.Seal(); err != nil {
			return err
		}
	}
	_, err := p.registry.BeginDrain(volumeID)
	return err
}

// RemoveDrained closes local collection handles and delegates the final
// deletion guard to Registry. A readable Volume must have no Pack bytes; an
// explicitly failed/missing Volume may be removed after operator review.
func (p *Pool) RemoveDrained(ctx context.Context, volumeID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("Pack pool is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for id, collection := range p.collections {
		if collection.catalog.VolumeID != volumeID {
			continue
		}
		if err := collection.Close(); err != nil {
			return err
		}
		delete(p.collections, id)
	}
	_, err := p.registry.RemoveDrained(volumeID)
	return err
}

// Compact garbage-collects sealed Packs whose records are all unreachable
// from the caller's logical index. The predicate is evaluated while the Pool
// is exclusively held, so a Store can pass one stable index snapshot.
func (p *Pool) Compact(ctx context.Context, live func(Location) bool) (CompactReport, error) {
	var result CompactReport
	if live == nil {
		return result, errors.New("nil live Pack record predicate")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return result, errors.New("Pack pool is closed")
	}
	if ctx == nil {
		return result, errors.New("nil compaction context")
	}
	// A freshly opened Pool has no lazy Collection handles yet. Open existing
	// collection metadata for maintenance, while leaving untouched Volumes
	// that have never stored a Pack.
	report, err := p.registry.Status()
	if err != nil {
		return result, err
	}
	for _, v := range report.Volumes {
		if v.State == volume.Removed || v.State == volume.Registered {
			continue
		}
		if _, err := os.Stat(v.Path + string(os.PathSeparator) + collectionMarkerName); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return result, err
		}
		if p.collections[v.ID] == nil {
			collection, err := OpenCollection(p.registry, v.ID)
			if err != nil {
				return result, err
			}
			p.collections[v.ID] = collection
		}
	}
	for _, collection := range p.collections {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		report, err := collection.Compact(ctx, live)
		if err != nil {
			return result, err
		}
		result.ScannedPacks += report.ScannedPacks
		result.RemovedPacks += report.RemovedPacks
		result.ReclaimedBytes += report.ReclaimedBytes
	}
	return result, nil
}

func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var failures []error
	for _, m := range p.collections {
		failures = append(failures, m.Close())
	}
	return errors.Join(failures...)
}

func appendBudget(record Record) uint64 {
	encoded := uint64(recordHeaderSize + len(record.EngineID) + len(record.TenantID) + len(record.Key) + len(record.Payload))
	// Round data to 64 KiB and allow 4 MiB for catalog replacements, Pack header,
	// checkpoints and first-time metadata. This is a conservative byte estimate,
	// not an exact bound on CoW, journaling, quotas or physical block allocation.
	const granularity uint64 = 64 << 10
	return (encoded+granularity-1)/granularity*granularity + 4<<20
}
