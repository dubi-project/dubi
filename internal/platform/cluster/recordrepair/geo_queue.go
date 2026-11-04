package recordrepair

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
)

var geoAttemptCursor = []byte("scheduler/last-attempt")

// Count observes queue size without decoding all references on every status call.
// The count is rebuilt from durable keys when opening the ledger.
func (s *GeoRecordRepairStore) Count() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return 0, err
	}
	return s.count, nil
}

// NextPage reads a bounded round-robin page. The cursor records attempted work,
// not completion; retained failures cannot monopolize the head after restart.
func (s *GeoRecordRepairStore) NextPage(limit int) ([]enginestate.RecordReference, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, 0, err
	}
	if limit < 1 || limit > 64 {
		return nil, 0, errors.New("invalid Geo repair page limit")
	}
	var after []byte
	value, closer, err := s.db.Get(geoAttemptCursor)
	if err == nil {
		if len(value) > 8192 || !bytes.HasPrefix(value, []byte("geo-record/")) {
			_ = closer.Close()
			return nil, 0, errors.New("invalid Geo repair attempt cursor")
		}
		after = bytes.Clone(value)
		_ = closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return nil, 0, err
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("geo-record/"), UpperBound: []byte("geo-record0")})
	if err != nil {
		return nil, 0, err
	}
	defer iter.Close()
	var refs []enginestate.RecordReference
	read := func() error {
		if len(iter.Value()) > geoRepairMaxBytes {
			return errors.New("Geo repair entry exceeds size bound")
		}
		var entry geoRecordRepairEntry
		decoder := json.NewDecoder(bytes.NewReader(iter.Value()))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&entry) != nil || decoder.Decode(new(struct{})) != io.EOF || entry.Version != geoRepairTaskVersion {
			return errors.New("invalid Geo repair entry")
		}
		key, err := geoRecordRepairKey(entry.Reference)
		if err != nil || !bytes.Equal(key, iter.Key()) {
			return fmt.Errorf("Geo repair key differs from reference")
		}
		refs = append(refs, entry.Reference)
		return nil
	}
	valid := iter.First()
	if len(after) != 0 {
		valid = iter.SeekGE(after)
		if valid && bytes.Equal(iter.Key(), after) {
			valid = iter.Next()
		}
	}
	for ; valid && len(refs) < limit; valid = iter.Next() {
		if err := read(); err != nil {
			return nil, 0, err
		}
	}
	if len(after) != 0 && len(refs) < limit {
		for valid = iter.First(); valid && bytes.Compare(iter.Key(), after) <= 0 && len(refs) < limit; valid = iter.Next() {
			if err := read(); err != nil {
				return nil, 0, err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return nil, 0, err
	}
	return refs, s.count, nil
}

func (s *GeoRecordRepairStore) MarkAttempt(ref enginestate.RecordReference) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	key, err := geoRecordRepairKey(ref)
	if err != nil {
		return err
	}
	return s.db.Set(geoAttemptCursor, key, pebble.Sync)
}
