package records

// This file contains the node-local half of Volume migration. It deliberately
// moves immutable records through the normal Pack append path and publishes
// the new locator only after the destination bytes have been read back and
// verified. A crash can therefore leave orphan bytes, but never an index that
// points at an unverified destination.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
)

const (
	volumeMigrationFile  = "DUBI_VOLUME_MIGRATION"
	volumeMigrationVer   = 1
	volumeMigrationLimit = 64
)

// VolumeMigrationStatus is an operator-facing observation. It is not a
// cluster durability receipt; other Nodes and Regions are accounted for by
// the placement/repair layers.
type VolumeMigrationStatus struct {
	Active         bool      `json:"active"`
	SourceVolumeID string    `json:"source_volume_id,omitempty"`
	TargetVolumeID string    `json:"target_volume_id,omitempty"`
	SourcePackID   string    `json:"source_pack_id,omitempty"`
	Cursor         string    `json:"cursor,omitempty"`
	Migrated       uint64    `json:"migrated"`
	StartedAt      time.Time `json:"started_at,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
}

type volumeMigrationState struct {
	Version        int       `json:"version"`
	SourceVolumeID string    `json:"source_volume_id"`
	TargetVolumeID string    `json:"target_volume_id,omitempty"`
	SourcePackID   string    `json:"source_pack_id,omitempty"`
	Cursor         string    `json:"cursor,omitempty"`
	Migrated       uint64    `json:"migrated"`
	StartedAt      time.Time `json:"started_at"`
	LastError      string    `json:"last_error,omitempty"`
	CopyDone       bool      `json:"copy_done,omitempty"`
}

type volumeAppender interface {
	AppendAwayFromVolume(context.Context, string, pack.Record) (pack.Location, error)
}

// BeginVolumeMigration creates (or idempotently reopens) the durable task.
// Drain is a separate Volume lifecycle boundary; this task can be created
// before or after the operator adds the replacement Volume.
func (s *Store) BeginVolumeMigration(sourceVolume string) error {
	if sourceVolume == "" {
		return errors.New("source Volume is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	state, found, err := s.loadMigrationLocked()
	if err != nil {
		return err
	}
	if found && state.TargetVolumeID != "" {
		return s.saveMigrationLocked(volumeMigrationState{Version: volumeMigrationVer, SourceVolumeID: sourceVolume, StartedAt: time.Now().UTC()})
	}
	if found {
		if state.SourceVolumeID != sourceVolume {
			return fmt.Errorf("Volume migration for %s is already active", state.SourceVolumeID)
		}
		return nil
	}
	return s.saveMigrationLocked(volumeMigrationState{Version: volumeMigrationVer,
		SourceVolumeID: sourceVolume, StartedAt: time.Now().UTC()})
}

// VolumeMigration returns the last durable local migration observation.
func (s *Store) VolumeMigration() (VolumeMigrationStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return VolumeMigrationStatus{}, err
	}
	state, found, err := s.loadMigrationLocked()
	if err != nil {
		return VolumeMigrationStatus{}, err
	}
	if !found {
		return VolumeMigrationStatus{}, nil
	}
	return VolumeMigrationStatus{Active: true, SourceVolumeID: state.SourceVolumeID,
		TargetVolumeID: state.TargetVolumeID, SourcePackID: state.SourcePackID, Cursor: state.Cursor, Migrated: state.Migrated, StartedAt: state.StartedAt,
		LastError: state.LastError}, nil
}

// AbandonVolumeMigration records the explicit operator decision that the
// source media is permanently lost. It does not claim the records were
// recovered; the normal cross-Node/Region repair debt remains authoritative.
func (s *Store) AbandonVolumeMigration(sourceVolume string) error {
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
	if state.TargetVolumeID != "" && state.TargetVolumeID == sourceVolume {
		return s.clearMigrationLocked()
	}
	if sourceVolume == "" || state.SourceVolumeID != sourceVolume {
		return fmt.Errorf("Volume migration is for %s", state.SourceVolumeID)
	}
	return s.clearMigrationLocked()
}

// MigrateVolumeStep moves at most a bounded page and persists its cursor.
// Restarting the process resumes from the last published index update. The
// method is safe to call repeatedly and is the unit used by the maintenance
// worker and the synchronous administrative drain path.
func (s *Store) MigrateVolumeStep(ctx context.Context, sourceVolume string) (moved uint64, done bool, err error) {
	if ctx == nil {
		return 0, false, errors.New("nil migration context")
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if sourceVolume == "" {
		return 0, false, errors.New("source Volume is required")
	}
	cost, costErr := s.migrationIOCost(sourceVolume)
	if costErr != nil {
		return 0, false, costErr
	}
	if err := resource.WaitIO(ctx, cost); err != nil {
		return 0, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return 0, false, err
	}
	appender, ok := s.packs.(volumeAppender)
	if !ok {
		return 0, false, errors.New("Pack backend does not support Volume migration")
	}
	state, found, err := s.loadMigrationLocked()
	if err != nil {
		return 0, false, err
	}
	if found && state.SourceVolumeID != sourceVolume {
		return 0, false, fmt.Errorf("Volume migration for %s is already active", state.SourceVolumeID)
	}
	if !found {
		state = volumeMigrationState{Version: volumeMigrationVer, SourceVolumeID: sourceVolume, StartedAt: time.Now().UTC()}
		if err := s.saveMigrationLocked(state); err != nil {
			return 0, false, err
		}
	}
	if state.CopyDone {
		return 0, true, nil
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("record/"), UpperBound: []byte("record0")})
	if err != nil {
		return 0, false, classifyIndexRead(err)
	}
	defer iter.Close()
	if state.Cursor == "" {
		iter.SeekGE([]byte("record/"))
	} else {
		iter.SeekGE([]byte(state.Cursor))
		if iter.Valid() && string(iter.Key()) == state.Cursor {
			iter.Next()
		}
	}
	processed := 0
	for ; iter.Valid() && bytes.HasPrefix(iter.Key(), []byte("record/")); iter.Next() {
		if err := ctx.Err(); err != nil {
			state.LastError = err.Error()
			_ = s.saveMigrationLocked(state)
			return moved, false, err
		}
		key := string(iter.Key())
		var current entry
		if len(iter.Value()) > 16<<10 || strictJSON(iter.Value(), &current) != nil {
			return moved, false, ErrCorrupt
		}
		id := strings.TrimPrefix(key, "record/")
		if err := s.validateEntry(id, current); err != nil {
			return moved, false, err
		}
		if current.Location.VolumeID != sourceVolume || (state.SourcePackID != "" && current.Location.PackID != state.SourcePackID) {
			state.Cursor = key
			processed++
			if processed >= volumeMigrationLimit {
				if err := s.saveMigrationLocked(state); err != nil {
					return moved, false, err
				}
				return moved, false, nil
			}
			continue
		}
		physical, err := s.packs.Read(current.Location)
		if err != nil {
			state.LastError = err.Error()
			_ = s.saveMigrationLocked(state)
			return moved, false, fmt.Errorf("read source Volume %s record %s: %w", sourceVolume, id, err)
		}
		var location pack.Location
		if state.TargetVolumeID != "" {
			targetAppender, ok := s.packs.(interface {
				AppendToVolume(context.Context, string, pack.Record) (pack.Location, error)
			})
			if !ok {
				return moved, false, errors.New("Pack backend does not support target Volume")
			}
			location, err = targetAppender.AppendToVolume(ctx, state.TargetVolumeID, physical)
		} else {
			location, err = appender.AppendAwayFromVolume(ctx, sourceVolume, physical)
		}
		if err != nil {
			state.LastError = err.Error()
			_ = s.saveMigrationLocked(state)
			return moved, false, fmt.Errorf("append replacement for record %s: %w", id, err)
		}
		if location.VolumeID == sourceVolume || (state.TargetVolumeID != "" && location.VolumeID != state.TargetVolumeID) {
			return moved, false, errors.New("Volume migration appended to its source Volume")
		}
		replacement := entry{Version: current.Version, Descriptor: current.Descriptor.clone(), Location: location}
		if _, _, err := s.read(id, replacement.Descriptor, replacement); err != nil {
			state.LastError = err.Error()
			_ = s.saveMigrationLocked(state)
			return moved, false, fmt.Errorf("verify replacement for record %s: %w", id, err)
		}
		encoded, err := json.Marshal(replacement)
		if err != nil {
			return moved, false, err
		}
		batch := s.db.NewBatch()
		if err := batch.Set(indexKey(id), encoded, nil); err != nil {
			_ = batch.Close()
			return moved, false, err
		}
		err = batch.Commit(pebble.Sync)
		_ = batch.Close()
		if err != nil {
			s.failed = fmt.Errorf("Volume migration index Sync uncertain; reopen required: %w", err)
			return moved, false, s.failed
		}
		state.Cursor, state.Migrated, state.LastError = key, state.Migrated+1, ""
		moved++
		processed++
		if err := s.saveMigrationLocked(state); err != nil {
			return moved, false, err
		}
		if processed >= volumeMigrationLimit || (moved >= 1 && (state.TargetVolumeID != "" || resource.IsMaintenance(ctx))) {
			return moved, false, nil
		}
	}
	if err := iter.Error(); err != nil {
		return moved, false, classifyIndexRead(err)
	}
	if state.TargetVolumeID != "" {
		state.CopyDone = true
		return moved, true, s.saveMigrationLocked(state)
	}
	if err := s.clearMigrationLocked(); err != nil {
		return moved, false, err
	}
	return moved, true, nil
}

// MigrateVolume runs bounded steps until the source is empty or the context
// expires. It is used by an explicit administrative remove; the background
// worker calls MigrateVolumeStep so it cannot monopolize foreground IO.
func (s *Store) MigrateVolume(ctx context.Context, sourceVolume string) (uint64, error) {
	var moved uint64
	for {
		count, done, err := s.MigrateVolumeStep(ctx, sourceVolume)
		moved += count
		if err != nil || done {
			return moved, err
		}
	}
}

func (s *Store) loadMigrationLocked() (volumeMigrationState, bool, error) {
	var state volumeMigrationState
	data, err := os.ReadFile(filepath.Join(s.root, volumeMigrationFile))
	if errors.Is(err, os.ErrNotExist) {
		return state, false, nil
	}
	if err != nil {
		return state, false, err
	}
	if len(data) > 16<<10 || strictJSON(data, &state) != nil || state.Version != volumeMigrationVer || state.SourceVolumeID == "" || state.StartedAt.IsZero() || (state.TargetVolumeID == "") != (state.SourcePackID == "") || state.TargetVolumeID == state.SourceVolumeID || (state.CopyDone && state.TargetVolumeID == "") {
		return state, false, errors.New("invalid durable Volume migration state")
	}
	return state, true, nil
}

func (s *Store) saveMigrationLocked(state volumeMigrationState) error {
	if state.Version == 0 {
		state.Version = volumeMigrationVer
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.root, ".dubi-volume-migration-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer func() { _ = f.Close(); _ = os.Remove(name) }()
	if err := f.Chmod(0o640); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, filepath.Join(s.root, volumeMigrationFile)); err != nil {
		return err
	}
	return syncDirectory(s.root)
}

func (s *Store) clearMigrationLocked() error {
	err := os.Remove(filepath.Join(s.root, volumeMigrationFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(s.root)
}
