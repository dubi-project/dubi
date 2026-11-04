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
	"time"

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
	failed error
	count  int
}

// GeoMirrorPlanResolver must derive a fresh current Region plan from Root. A
// retry may rebind a mirror to a later generation of the same Region/Group,
// but the resolver remains responsible for rejecting a stale Owner route or
// publication reference.
type GeoMirrorPlanResolver func(context.Context, routedstate.GeoMetadataMirror) (placement.LocalPlan, error)

type GeoMirrorRetryReport struct {
	Attempted uint64
	Completed uint64
	Pending   uint64
}

// RetryPendingGeoMetadata is one bounded worker round. Successful mirrors are
// removed only after a durable destination Result. Failed entries remain
// available for a later Root-authorized round. A mirror from an older
// generation is re-bound only to the current generation of the same Region
// and Group; it is never moved to a different Region or routing Owner.
func RetryPendingGeoMetadata(ctx context.Context, transport GeoMetadataMirrorTransport,
	store *GeoMetadataRetryStore, resolve GeoMirrorPlanResolver) (GeoMirrorRetryReport, error) {
	if ctx == nil || transport == nil || store == nil || resolve == nil {
		return GeoMirrorRetryReport{}, errors.New("geo metadata retry requires context, transport, store and resolver")
	}
	mirrors, pending, err := store.NextPage(8)
	if err != nil {
		return GeoMirrorRetryReport{}, err
	}
	report := GeoMirrorRetryReport{Pending: uint64(pending)}
	if _, ok := transport.(GeoMetadataMirrorBatchTransport); ok {
		return retryGeoMetadataPage(ctx, transport, store, resolve, mirrors, report)
	}
	for _, mirror := range mirrors {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if err := store.MarkAttempt(mirror); err != nil {
			return report, err
		}
		report.Attempted++
		taskCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		completed, err := retryOneGeoMetadata(taskCtx, transport, store, resolve, mirror)
		cancel()
		if err != nil {
			return report, err
		}
		if !completed {
			continue
		}
		report.Completed++
	}
	report.Pending -= report.Completed
	return report, nil
}

// Resolve each row against fresh authority and share transport only once its
// destination is known. The persisted cursor advances in admission order;
// completion order never puts a failed row back at the queue head.
func retryGeoMetadataPage(ctx context.Context, transport GeoMetadataMirrorTransport, store *GeoMetadataRetryStore,
	resolve GeoMirrorPlanResolver, mirrors []routedstate.GeoMetadataMirror, report GeoMirrorRetryReport) (GeoMirrorRetryReport, error) {
	if len(mirrors) == 0 {
		return report, nil
	}
	for _, mirror := range mirrors {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if err := store.MarkAttempt(mirror); err != nil {
			return report, err
		}
	}
	work, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	pooled, closePool := newGeoMirrorPagePool(work, transport)
	defer closePool()
	completed := make([]bool, len(mirrors))
	failures := make([]error, len(mirrors))
	var workers sync.WaitGroup
	for i, mirror := range mirrors {
		workers.Add(1)
		go func() {
			defer workers.Done()
			completed[i], failures[i] = retryOneGeoMetadata(work, pooled, store, resolve, mirror)
		}()
	}
	workers.Wait()
	report.Attempted += uint64(len(mirrors))
	for _, done := range completed {
		if done {
			report.Completed++
		}
	}
	report.Pending -= report.Completed
	return report, errors.Join(append(failures, ctx.Err())...)
}

func retryOneGeoMetadata(ctx context.Context, transport GeoMetadataMirrorTransport, store *GeoMetadataRetryStore, resolve GeoMirrorPlanResolver, mirror routedstate.GeoMetadataMirror) (bool, error) {
	plan, err := resolve(ctx, mirror)
	if err != nil {
		return false, nil
	}
	current, ok := rebindGeoMetadataMirror(mirror, plan)
	if !ok {
		return false, nil
	}
	if _, err := RetryGeoMetadata(ctx, transport, plan, current); err != nil {
		return false, nil
	}
	if err := store.Delete(mirror); err != nil {
		return false, err
	}
	return true, nil
}

func rebindGeoMetadataMirror(mirror routedstate.GeoMetadataMirror, plan placement.LocalPlan) (routedstate.GeoMetadataMirror, bool) {
	if mirror.Validate() != nil || mirror.RegionID != plan.RegionID || mirror.DestinationGroupID != plan.GroupID ||
		plan.Generation == 0 || plan.GroupEpoch == 0 || plan.FenceIndex == 0 {
		return routedstate.GeoMetadataMirror{}, false
	}
	// A task from the future cannot be made current by a later retry. This
	// protects against a stale Root cut or a corrupted local ledger.
	if mirror.Generation > plan.Generation {
		return routedstate.GeoMetadataMirror{}, false
	}
	if mirror.Generation == plan.Generation && mirror.GroupEpoch == plan.GroupEpoch && mirror.FenceIndex == plan.FenceIndex {
		return mirror, true
	}
	current := mirror
	current.Generation = plan.Generation
	current.GroupEpoch = plan.GroupEpoch
	current.FenceIndex = plan.FenceIndex
	if current.Validate() != nil {
		return routedstate.GeoMetadataMirror{}, false
	}
	return current, true
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
	store := &GeoMetadataRetryStore{db: db}
	count, err := store.countLocked()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	store.count = count
	return store, nil
}

func geoRetryKey(requestID string) []byte { return []byte("geo-mirror/" + requestID) }

func (s *GeoMetadataRetryStore) Save(mirror routedstate.GeoMetadataMirror) error {
	return s.saveMirrors([]routedstate.GeoMetadataMirror{mirror})
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
	return s.deleteMirrors([]routedstate.GeoMetadataMirror{mirror})
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
	}
	return count, iter.Error()
}

func (s *GeoMetadataRetryStore) usable() error {
	if s == nil || s.db == nil || s.closed {
		return errors.New("geo metadata retry store is closed")
	}
	if s.failed != nil {
		return s.failed
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
