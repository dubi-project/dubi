package recordwrite

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
)

var geoMirrorAttemptCursor = []byte("scheduler/last-mirror-attempt")

// Count observes queue size without decoding all references on every status call.
// The count is rebuilt from durable keys when opening the ledger.
func (s *GeoMetadataRetryStore) Count() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return 0, err
	}
	return s.count, nil
}

// NextPage reads a bounded round-robin page. The cursor records attempted work,
// not completion; retained failures cannot monopolize the head after restart.
func (s *GeoMetadataRetryStore) NextPage(limit int) ([]routedstate.GeoMetadataMirror, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, 0, err
	}
	if limit < 1 || limit > 64 {
		return nil, 0, errors.New("invalid Geo repair page limit")
	}
	var after []byte
	value, closer, err := s.db.Get(geoMirrorAttemptCursor)
	if err == nil {
		if len(value) > 8192 || !bytes.HasPrefix(value, []byte("geo-mirror/")) {
			_ = closer.Close()
			return nil, 0, errors.New("invalid Geo repair attempt cursor")
		}
		after = bytes.Clone(value)
		_ = closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return nil, 0, err
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("geo-mirror/"), UpperBound: []byte("geo-mirror0")})
	if err != nil {
		return nil, 0, err
	}
	defer iter.Close()
	var refs []routedstate.GeoMetadataMirror
	read := func() error {
		if len(iter.Value()) > geoRetryMaxBytes {
			return errors.New("Geo repair entry exceeds size bound")
		}
		var entry geoRetryEntry
		decoder := json.NewDecoder(bytes.NewReader(iter.Value()))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&entry) != nil || decoder.Decode(new(struct{})) != io.EOF || entry.Version != geoRetryVersion || entry.Mirror.Validate() != nil {
			return errors.New("invalid Geo repair entry")
		}
		requestID, err := mirrorRequestID(entry.Mirror)
		key := geoRetryKey(requestID)
		if err != nil || !bytes.Equal(key, iter.Key()) {
			return fmt.Errorf("Geo repair key differs from reference")
		}
		refs = append(refs, entry.Mirror)
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

func (s *GeoMetadataRetryStore) MarkAttempt(ref routedstate.GeoMetadataMirror) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.recoverWriteFatal(&err)
	if err := s.usable(); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	requestID, err := mirrorRequestID(ref)
	key := geoRetryKey(requestID)
	if err != nil {
		return err
	}
	if err := s.db.Set(geoMirrorAttemptCursor, key, pebble.Sync); err != nil {
		s.failed = err
		return err
	}
	return nil
}
