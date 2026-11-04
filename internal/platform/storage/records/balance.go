package records

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

// ErrBalanceScanPending means a bounded metadata page advanced, with no task yet selected.
var ErrBalanceScanPending = errors.New("capacity balance discovery has another page")

// BeginVolumeBalance reuses the durable migration ledger, but keeps the source
// Active. One whole sealed Pack is selected below the hysteresis byte budget;
// this avoids oscillating partial copies that cannot reclaim source space.
func (s *Store) BeginVolumeBalance(ctx context.Context, plan volume.BalancePlan) (bool, error) {
	if ctx == nil || plan.Source == "" || plan.Target == "" || plan.Source == plan.Target || plan.MaximumBytes == 0 {
		return false, errors.New("invalid Volume balance plan")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return false, err
	}
	if _, found, err := s.loadMigrationLocked(); err != nil || found {
		return false, err
	}
	backend, ok := s.packs.(interface {
		SealAll(context.Context) error
		PhysicalSize(pack.Location) (uint64, error)
	})
	if !ok {
		return false, errors.New("Pack backend does not support capacity balancing")
	}
	if err := backend.SealAll(ctx); err != nil {
		return false, err
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("record/"), UpperBound: []byte("record0")})
	if err != nil {
		return false, err
	}
	defer iter.Close()
	seen := map[string]bool{}
	cursorKey := []byte("maintenance/balance-scan")
	var after []byte
	saved, closer, getErr := s.db.Get(cursorKey)
	if getErr == nil {
		after = bytes.Clone(saved)
		_ = closer.Close()
		if len(after) > 8192 || !bytes.HasPrefix(after, []byte("record/")) {
			return false, ErrCorrupt
		}
	} else if !errors.Is(getErr, pebble.ErrNotFound) {
		return false, getErr
	}
	valid := iter.First()
	if len(after) != 0 {
		valid = iter.SeekGE(after)
		if valid && bytes.Equal(iter.Key(), after) {
			valid = iter.Next()
		}
	}
	scanned := 0
	for ; valid; valid = iter.Next() {
		if scanned == 256 {
			if err := s.db.Set(cursorKey, after, pebble.Sync); err != nil {
				return false, err
			}
			return false, ErrBalanceScanPending
		}
		after = bytes.Clone(iter.Key())
		scanned++

		if err := ctx.Err(); err != nil {
			return false, err
		}
		var current entry
		if len(iter.Value()) > 16<<10 || strictJSON(iter.Value(), &current) != nil {
			return false, ErrCorrupt
		}
		if err := s.validateEntry(strings.TrimPrefix(string(iter.Key()), "record/"), current); err != nil {
			return false, err
		}
		if current.Location.VolumeID != plan.Source || seen[current.Location.PackID] {
			continue
		}
		seen[current.Location.PackID] = true
		size, err := backend.PhysicalSize(current.Location)
		if err != nil {
			return false, err
		}
		if size > plan.MaximumBytes {
			continue
		}
		if err := s.db.Delete(cursorKey, pebble.Sync); err != nil {
			return false, err
		}
		return true, s.saveMigrationLocked(volumeMigrationState{Version: volumeMigrationVer, SourceVolumeID: plan.Source,
			TargetVolumeID: plan.Target, SourcePackID: current.Location.PackID, StartedAt: time.Now().UTC()})
	}
	if err := iter.Error(); err != nil {
		return false, err
	}
	return false, s.db.Delete(cursorKey, pebble.Sync)
}

// migrationIOCost inspects one metadata page without retaining the Store lock
// while the shared scheduler waits. Actual migration revalidates the locator.
func (s *Store) migrationIOCost(source string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return 0, err
	}
	state, found, err := s.loadMigrationLocked()
	if err != nil {
		return 0, err
	}
	if found && state.SourceVolumeID != source {
		return 0, errors.New("another Volume migration is active")
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("record/"), UpperBound: []byte("record0")})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	valid := iter.First()
	if state.Cursor != "" {
		valid = iter.SeekGE([]byte(state.Cursor))
		if valid && string(iter.Key()) == state.Cursor {
			valid = iter.Next()
		}
	}
	for n := 0; valid && n < volumeMigrationLimit; valid, n = iter.Next(), n+1 {
		var current entry
		if len(iter.Value()) > 16<<10 || strictJSON(iter.Value(), &current) != nil {
			return 0, ErrCorrupt
		}
		if err := s.validateEntry(strings.TrimPrefix(string(iter.Key()), "record/"), current); err != nil {
			return 0, err
		}
		if current.Location.VolumeID == source && (state.SourcePackID == "" || current.Location.PackID == state.SourcePackID) {
			return current.Descriptor.Size * 3, nil
		}
	}
	return 4096, iter.Error()
}

// CancelVolumeBalance only abandons the optional balancing cursor. Verified
// record locators already committed stay authoritative and normal Repair
// handles a newly failed disk. Explicit drain tasks cannot be cancelled here.
func (s *Store) CancelVolumeBalance() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	state, found, err := s.loadMigrationLocked()
	if err != nil {
		return err
	}
	if !found || state.TargetVolumeID == "" {
		return nil
	}
	return s.clearMigrationLocked()
}

// CompleteVolumeBalance retires the durable task only after Pack collection
// succeeded. A restart after copying resumes collection instead of silently
// leaving old Packs occupying the pressured disk.
func (s *Store) CompleteVolumeBalance(source, target, packID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	state, found, err := s.loadMigrationLocked()
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if !state.CopyDone || state.SourceVolumeID != source || state.TargetVolumeID != target || state.SourcePackID != packID {
		return errors.New("capacity balance task changed before collection completed")
	}
	return s.clearMigrationLocked()
}
func (s *Store) NoteVolumeBalanceFailure(message string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	state, found, err := s.loadMigrationLocked()
	if err != nil {
		return err
	}
	if !found || state.TargetVolumeID == "" {
		return nil
	}
	text := []rune(message)
	state.LastError = string(text[:min(len(text), 1024)])
	return s.saveMigrationLocked(state)
}
