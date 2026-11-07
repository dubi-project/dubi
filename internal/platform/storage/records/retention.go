package records

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/resourceid"
)

var ErrRetained = errors.New("record is retained for an unfinished data handoff")
var ErrRetentionReleased = errors.New("record retention sequence was already released")

// Retention is a durable promise to a specific source identity. It has no
// time-based expiry: a lost heartbeat cannot authorize deletion. Each source
// serializes handoffs and advances its sequence only after every release ACK.
type Retention struct {
	SourceNodeID string `json:"source_node_id"`
	SourceRaftID uint64 `json:"source_raft_id,omitempty"`
	Sequence     uint64 `json:"sequence"`
	RecordID     string `json:"record_id"`
}

type retentionRow struct {
	Released uint64     `json:"released"`
	Active   *Retention `json:"active,omitempty"`
}

func retentionKey(source string) ([]byte, error) {
	if !resourceid.Valid(source, "node_") {
		return nil, errors.New("invalid retention source identity")
	}
	return []byte("retention/incoming/" + source), nil
}

func (s *Store) retentionRowLocked(source string) (retentionRow, error) {
	key, err := retentionKey(source)
	if err != nil {
		return retentionRow{}, err
	}
	encoded, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return retentionRow{}, nil
	}
	if err != nil {
		return retentionRow{}, classifyIndexRead(err)
	}
	defer closer.Close()
	var row retentionRow
	if len(encoded) > 4096 || strictJSON(encoded, &row) != nil {
		return row, ErrCorrupt
	}
	if row.Active != nil && (row.Active.SourceNodeID != source || row.Active.Sequence <= row.Released || !validRetainedRecordID(row.Active.RecordID)) {
		return row, ErrCorrupt
	}
	return row, nil
}

func validRetainedRecordID(id string) bool {
	if !strings.HasPrefix(id, "record_") || len(id) != 71 {
		return false
	}
	digest := strings.TrimPrefix(id, "record_")
	b, err := hex.DecodeString(digest)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == digest
}

// Retain verifies the actual bytes under the same Store lock as publishing
// the pin. An ACK is returned only after Sync. A concurrent local capacity
// eviction must inspect these pins under that same lock before deleting.
func (s *Store) Retain(ctx context.Context, source string, sequence uint64, descriptor Descriptor, sourceRaftID ...uint64) (Holding, error) {
	if ctx == nil || sequence == 0 || len(sourceRaftID) > 1 || (len(sourceRaftID) == 1 && sourceRaftID[0] == 0) {
		return Holding{}, errors.New("retention requires context and sequence")
	}
	id, err := descriptor.ID()
	if err != nil {
		return Holding{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return Holding{}, err
	}
	if err := ctx.Err(); err != nil {
		return Holding{}, err
	}
	row, err := s.retentionRowLocked(source)
	if err != nil {
		return Holding{}, err
	}
	if sequence <= row.Released {
		return Holding{}, ErrRetentionReleased
	}
	if row.Active != nil && (row.Active.Sequence != sequence || row.Active.RecordID != id) {
		return Holding{}, errors.New("source has another unfinished retention")
	}
	current, found, err := s.lookup(id)
	if err != nil {
		return Holding{}, err
	}
	if !found {
		return Holding{}, ErrNotHeld
	}
	_, holding, err := s.read(id, descriptor, current)
	if err != nil {
		return Holding{}, err
	}
	if row.Active != nil {
		return holding, nil
	}
	row.Active = &Retention{SourceNodeID: source, Sequence: sequence, RecordID: id}
	if len(sourceRaftID) == 1 {
		row.Active.SourceRaftID = sourceRaftID[0]
	}
	if err := s.saveRetentionLocked(source, row); err != nil {
		return Holding{}, err
	}
	return holding, nil
}

// RetentionPage bounds cleanup discovery, including released rows. Cursor
// fairness is process-local; the promises and release high-water marks persist.
func (s *Store) RetentionPage(ctx context.Context, after string) ([]Retention, string, bool, error) {
	if ctx == nil || (after != "" && !strings.HasPrefix(after, "retention/incoming/")) {
		return nil, "", false, errors.New("invalid retention scan context or cursor")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, "", false, err
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("retention/incoming/"), UpperBound: []byte("retention/incoming0")})
	if err != nil {
		return nil, "", false, err
	}
	defer iter.Close()
	valid := iter.First()
	if after != "" {
		valid = iter.SeekGE([]byte(after))
		if valid && string(iter.Key()) == after {
			valid = iter.Next()
		}
	}
	var active []Retention
	for scanned := 0; valid && scanned < 256; scanned++ {
		if err := ctx.Err(); err != nil {
			return nil, "", false, err
		}
		after = string(iter.Key())
		row, err := s.retentionRowLocked(strings.TrimPrefix(after, "retention/incoming/"))
		if err != nil {
			return nil, "", false, err
		}
		if row.Active != nil {
			active = append(active, *row.Active)
		}
		valid = iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, "", false, err
	}
	if !valid {
		return active, "", true, nil
	}
	return active, after, false, nil
}

// ReleaseRetention also persists a high-water mark when the pin never arrived.
// A delayed Retain RPC cannot recreate that pin after its release was ACKed.
func (s *Store) ReleaseRetention(ctx context.Context, source string, sequence uint64) error {
	if ctx == nil || sequence == 0 {
		return errors.New("retention release requires context and sequence")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	row, err := s.retentionRowLocked(source)
	if err != nil {
		return err
	}
	if sequence <= row.Released {
		return nil
	}
	if row.Active != nil {
		if row.Active.Sequence > sequence {
			// A late release may retire only its own older sequence.
			row.Released = sequence
			return s.saveRetentionLocked(source, row)
		}
		if row.Active.Sequence != sequence {
			return errors.New("release would bypass an unfinished earlier retention")
		}
		row.Active = nil
	}
	row.Released = sequence
	return s.saveRetentionLocked(source, row)
}

func (s *Store) saveRetentionLocked(source string, row retentionRow) error {
	key, err := retentionKey(source)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(key, encoded, nil); err != nil {
		return err
	}
	return s.commitMaintenanceLocked(batch)
}

func (s *Store) commitMaintenanceLocked(batch *pebble.Batch) error {
	var err error
	if s.commit != nil {
		err = s.commit(batch)
	} else {
		err = batch.Commit(pebble.Sync)
	}
	if err != nil {
		s.failed = fmt.Errorf("record handoff Sync uncertain; reopen required: %w", err)
		return s.failed
	}
	return nil
}

func (s *Store) retainedLocked(recordID string) (bool, error) {
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("retention/incoming/"), UpperBound: []byte("retention/incoming0")})
	if err != nil {
		return false, classifyIndexRead(err)
	}
	defer iterator.Close()
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var row retentionRow
		if len(iterator.Value()) > 4096 || strictJSON(iterator.Value(), &row) != nil {
			return false, ErrCorrupt
		}
		if row.Active != nil {
			if string(iterator.Key()) != "retention/incoming/"+row.Active.SourceNodeID || row.Active.Sequence <= row.Released || !validRetainedRecordID(row.Active.RecordID) {
				return false, ErrCorrupt
			}
			if row.Active.RecordID == recordID {
				return true, nil
			}
		}
	}
	return false, iterator.Error()
}
