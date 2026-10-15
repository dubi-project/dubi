package recordwrite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
)

const (
	geoRetryVersion    = 1
	geoRetryMaxEntries = 4096
	geoRetryMaxBytes   = 1 << 20
)

// GeoMetadataRetryStore is a local durable ledger of mirror commands that
// were not accepted by any destination voter. It is a retry hint only: the
// mirror still carries its Region fence and the destination StateMachine is
// the authority. The ledger never changes a quorum denominator.
type GeoMetadataRetryStore struct {
	mu     sync.Mutex
	db     *pebble.DB
	closed bool
}

// GeoMirrorPlanResolver must derive a fresh current Region plan from Root. It
// must reject stale generation/epoch/fence data rather than adapting the old
// mirror to whatever nodes happen to be online.
type GeoMirrorPlanResolver func(context.Context, routedstate.GeoMetadataMirror) (placement.LocalPlan, error)

type GeoMirrorRetryReport struct {
	Attempted uint64
	Completed uint64
	Pending   uint64
}

// RetryPendingGeoMetadata is one bounded worker round. Successful mirrors are
// removed only after a durable destination Result. Failed or stale entries
// remain available for a later Root-authorized round.
func RetryPendingGeoMetadata(ctx context.Context, transport GeoMetadataMirrorTransport,
	store *GeoMetadataRetryStore, resolve GeoMirrorPlanResolver) (GeoMirrorRetryReport, error) {
	if ctx == nil || transport == nil || store == nil || resolve == nil {
		return GeoMirrorRetryReport{}, errors.New("geo metadata retry requires context, transport, store and resolver")
	}
	mirrors, err := store.List()
	if err != nil {
		return GeoMirrorRetryReport{}, err
	}
	report := GeoMirrorRetryReport{Pending: uint64(len(mirrors))}
	for _, mirror := range mirrors {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		report.Attempted++
		plan, err := resolve(ctx, mirror)
		if err != nil {
			continue
		}
		if _, err := RetryGeoMetadata(ctx, transport, plan, mirror); err != nil {
			continue
		}
		if err := store.Delete(mirror); err != nil {
			return report, err
		}
		report.Completed++
	}
	report.Pending -= report.Completed
	return report, nil
}

type geoRetryEntry struct {
	Version int                           `json:"version"`
	Mirror  routedstate.GeoMetadataMirror `json:"mirror"`
}

func OpenGeoMetadataRetryStore(root string) (*GeoMetadataRetryStore, error) {
	if root == "" {
		return nil, errors.New("geo metadata retry directory is empty")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	db, err := pebbleutil.Open(filepath.Join(root, "dubi-geo-metadata-retry"), &pebble.Options{Logger: pebbleutil.QuietLogger{}})
	if err != nil {
		return nil, err
	}
	return &GeoMetadataRetryStore{db: db}, nil
}

func geoRetryKey(requestID string) []byte { return []byte("geo-mirror/" + requestID) }

func (s *GeoMetadataRetryStore) Save(mirror routedstate.GeoMetadataMirror) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if mirror.Validate() != nil {
		return errors.New("invalid geo metadata retry mirror")
	}
	requestID, err := mirrorRequestID(mirror)
	if err != nil {
		return err
	}
	entry := geoRetryEntry{Version: geoRetryVersion, Mirror: mirror}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if len(encoded) > geoRetryMaxBytes {
		return errors.New("geo metadata retry entry exceeds size bound")
	}
	count, err := s.countLocked()
	if err != nil {
		return err
	}
	if _, closer, getErr := s.db.Get(geoRetryKey(requestID)); getErr == nil {
		_ = closer.Close()
	} else if !errors.Is(getErr, pebble.ErrNotFound) {
		return getErr
	} else if count >= geoRetryMaxEntries {
		return errors.New("geo metadata retry ledger is full")
	}
	return s.db.Set(geoRetryKey(requestID), encoded, pebble.Sync)
}

func (s *GeoMetadataRetryStore) List() ([]routedstate.GeoMetadataMirror, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("geo-mirror/"), UpperBound: []byte("geo-mirror0")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	result := make([]routedstate.GeoMetadataMirror, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		if len(result) >= geoRetryMaxEntries {
			return nil, errors.New("geo metadata retry ledger exceeds entry bound")
		}
		var entry geoRetryEntry
		if len(iter.Value()) > geoRetryMaxBytes || json.Unmarshal(iter.Value(), &entry) != nil || entry.Version != geoRetryVersion || entry.Mirror.Validate() != nil {
			return nil, fmt.Errorf("invalid geo metadata retry entry %q", string(iter.Key()))
		}
		result = append(result, entry.Mirror)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	slices.SortFunc(result, func(a, b routedstate.GeoMetadataMirror) int {
		aID, _ := mirrorRequestID(a)
		bID, _ := mirrorRequestID(b)
		if aID < bID {
			return -1
		}
		if aID > bID {
			return 1
		}
		return 0
	})
	return result, nil
}

func (s *GeoMetadataRetryStore) Delete(mirror routedstate.GeoMetadataMirror) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if mirror.Validate() != nil {
		return errors.New("invalid geo metadata retry mirror")
	}
	requestID, err := mirrorRequestID(mirror)
	if err != nil {
		return err
	}
	return s.db.Delete(geoRetryKey(requestID), pebble.Sync)
}

func (s *GeoMetadataRetryStore) countLocked() (int, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("geo-mirror/"), UpperBound: []byte("geo-mirror0")})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
		if count > geoRetryMaxEntries {
			break
		}
	}
	return count, iter.Error()
}

func (s *GeoMetadataRetryStore) usable() error {
	if s == nil || s.db == nil || s.closed {
		return errors.New("geo metadata retry store is closed")
	}
	return nil
}

func (s *GeoMetadataRetryStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}
