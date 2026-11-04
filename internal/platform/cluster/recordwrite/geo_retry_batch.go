package recordwrite

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
)

const geoRetryBatchEntries = 32
const geoRetryBatchBytes = 4 << 20

type geoRetryEncoded struct{ key, value []byte }

// saveMirrors synchronously persists bounded batches before any delivery. A
// later batch failure may leave an earlier batch as retry debt, never success.
// No timer, background writer or additional on-disk format is needed.
func (s *GeoMetadataRetryStore) saveMirrors(mirrors []routedstate.GeoMetadataMirror) error {
	var rows []geoRetryEncoded
	var size int
	for _, mirror := range mirrors {
		if mirror.Validate() != nil {
			return errors.New("invalid geo metadata retry mirror")
		}
		id, err := mirrorRequestID(mirror)
		if err != nil {
			return err
		}
		value, err := json.Marshal(geoRetryEntry{Version: geoRetryVersion, Mirror: mirror})
		if err != nil {
			return err
		}
		if len(value) > geoRetryMaxBytes {
			return errors.New("geo metadata retry entry exceeds size bound")
		}
		row := geoRetryEncoded{key: geoRetryKey(id), value: value}
		n := len(row.key) + len(row.value)
		if len(rows) == geoRetryBatchEntries || size+n > geoRetryBatchBytes {
			if err := s.saveEncoded(rows); err != nil {
				return err
			}
			rows, size = nil, 0
		}
		rows, size = append(rows, row), size+n
	}
	return s.saveEncoded(rows)
}

func (s *GeoMetadataRetryStore) saveEncoded(rows []geoRetryEncoded) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.recoverWriteFatal(&err)
	if err := s.usable(); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	seen := make(map[string][]byte, len(rows))
	added := 0
	for _, row := range rows {
		key := string(row.key)
		if previous, ok := seen[key]; ok {
			if !bytes.Equal(previous, row.value) {
				return errors.New("conflicting Geo retry batch entry")
			}
			continue
		}
		seen[key] = row.value
		value, closer, err := s.db.Get(row.key)
		if err == nil {
			identical := bytes.Equal(value, row.value)
			_ = closer.Close()
			if !identical {
				return fmt.Errorf("Geo retry entry differs from deterministic key %q", key)
			}
			// Existing entries were synchronously committed. Re-saving identical
			// debt needs no new WAL entry or disk sync. Delete uses this same lock.
			continue
		}
		if !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
		if err := batch.Set(row.key, row.value, nil); err != nil {
			return err
		}
		added++
	}
	if added == 0 {
		return nil
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		// A failed sync has an uncertain durable outcome. Stop using this
		// handle; reopening reconstructs count from the recovered ledger.
		s.failed = err
		return err
	}
	s.count += added
	return nil
}

// Pebble may panic with its logger's typed fatal error on a WAL sync failure
// instead of returning an error. Keep the uncertain ledger unusable until it
// is reopened. Never swallow unrelated programming panics. Caller holds mu.
func (s *GeoMetadataRetryStore) recoverWriteFatal(err *error) {
	if recovered := recover(); recovered != nil {
		fatal, ok := recovered.(pebbleutil.FatalError)
		if !ok {
			panic(recovered)
		}
		s.failed = fatal
		*err = fatal
	}
}
