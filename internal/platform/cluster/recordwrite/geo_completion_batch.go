package recordwrite

import (
	"errors"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
)

// Completion storage is owned by one prepared page, with one slot per planned
// mirror. It has no timer, worker, or persistent format of its own.
type geoMirrorCompletions struct {
	mu      sync.Mutex
	limit   int
	mirrors []routedstate.GeoMetadataMirror
}

func (c *geoMirrorCompletions) add(mirror routedstate.GeoMetadataMirror) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.mirrors) >= c.limit {
		return errors.New("Geo completion exceeds prepared page bound")
	}
	c.mirrors = append(c.mirrors, mirror)
	return nil
}

const geoRetryDeleteBatchBytes = 4 << 10

// deleteMirrors is called only for mirrors with exact successful durable
// destination Results. Each batch commits at most 32 keys / 4 KiB of keys;
// multiple batches are not a transaction. A crash before cleanup merely leaves
// idempotent debt to retry. A failed sync makes the handle unusable until reopen.
func (s *GeoMetadataRetryStore) deleteMirrors(mirrors []routedstate.GeoMetadataMirror) error {
	var keys [][]byte
	size := 0
	for _, mirror := range mirrors {
		if mirror.Validate() != nil {
			return errors.New("invalid geo metadata retry mirror")
		}
		id, err := mirrorRequestID(mirror)
		if err != nil {
			return err
		}
		key := geoRetryKey(id)
		if len(keys) == geoRetryBatchEntries || size+len(key) > geoRetryDeleteBatchBytes {
			if err := s.deleteKeys(keys); err != nil {
				return err
			}
			keys, size = nil, 0
		}
		keys, size = append(keys, key), size+len(key)
	}
	return s.deleteKeys(keys)
}

func (s *GeoMetadataRetryStore) deleteKeys(keys [][]byte) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.recoverWriteFatal(&err)
	if err := s.usable(); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	seen := make(map[string]bool, len(keys))
	removed := 0
	for _, key := range keys {
		if seen[string(key)] {
			continue
		}
		seen[string(key)] = true
		_, closer, err := s.db.Get(key)
		if errors.Is(err, pebble.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		_ = closer.Close()
		if err := batch.Delete(key, nil); err != nil {
			return err
		}
		removed++
	}
	if removed == 0 {
		return nil
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		s.failed = err
		return err
	}
	s.count -= removed
	return nil
}
