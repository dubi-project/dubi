package recordrepair

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

const (
	geoRepairTaskVersion = 1
	geoRepairMaxEntries  = 4096
	geoRepairMaxBytes    = 1 << 20
)

// GeoRecordRepairStore is a durable retry hint for physical data that should
// be restored in a Region other than the current local target. It stores the
// authoritative reference cut, never a Volume, Pack location, replica count
// or operator-selected address. Every retry must derive a fresh GeoPlan.
type GeoRecordRepairStore struct {
	mu     sync.Mutex
	db     *pebble.DB
	closed bool
}

type geoRecordRepairEntry struct {
	Version   int                      `json:"version"`
	Reference capstate.RecordReference `json:"reference"`
}

// GeoRecordPlanResolver must derive a fresh confirmed GeoPlan and the Region
// to prefer from the current Root/Range placement. It must not reuse a task's
// historical cohort after a Region generation changes.
type GeoRecordPlanResolver func(context.Context, capstate.RecordReference) (placement.GeoPlan, string, error)

// GeoRecordObserver authenticates one physical observation on a specific
// current Node. A failed observation is not evidence of a missing byte; the
// retry round simply leaves the task pending.
type GeoRecordObserver func(context.Context, rootstate.Node, records.Descriptor) (raftrpc.HoldingObservation, error)

type GeoRecordRepairReport struct {
	Attempted uint64
	Completed uint64
	Pending   uint64
}

// RetryPendingGeoRecords performs one bounded retry round. It only removes a
// task after a fresh observation proves the complete GeoPlan has reached its
// configured Region majority. Physical writes and metadata/GeoReady claims
// remain separate operations.
func RetryPendingGeoRecords(ctx context.Context, store *GeoRecordRepairStore, resolver GeoRecordPlanResolver,
	observer GeoRecordObserver, transport RemoteRepairer) (GeoRecordRepairReport, error) {
	if ctx == nil || store == nil || resolver == nil || observer == nil || transport == nil {
		return GeoRecordRepairReport{}, errors.New("Geo record repair retry requires context, store, resolver, observer and transport")
	}
	references, err := store.List()
	if err != nil {
		return GeoRecordRepairReport{}, err
	}
	report := GeoRecordRepairReport{Pending: uint64(len(references))}
	for _, ref := range references {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		report.Attempted++
		plan, preferredRegion, err := resolver(ctx, ref)
		if err != nil {
			continue
		}
		recordID, err := ref.Descriptor.ID()
		if err != nil {
			continue
		}
		evidence, holdings, nodes, err := observeGeoRecord(ctx, plan, ref.Descriptor, recordID, observer)
		if err != nil {
			continue
		}
		coverage, err := plan.Assess(recordID, evidence)
		if err == nil && coverage.HasMajority() {
			if err := store.Delete(ref); err != nil {
				return report, err
			}
			report.Completed++
			continue
		}
		target, err := plan.SelectRepairCandidates(recordID, preferredRegion, holdings)
		if err != nil || len(holdings) == 0 {
			continue
		}
		source := nodes[holdings[0].Holding.RaftID]
		_, _ = RepairSelectedTargets(ctx, plan, target, source, ref.Descriptor.GroupID, ref.Descriptor, transport)
		freshEvidence, _, _, observeErr := observeGeoRecord(ctx, plan, ref.Descriptor, recordID, observer)
		if observeErr != nil {
			continue
		}
		freshCoverage, assessErr := plan.Assess(recordID, freshEvidence)
		if assessErr != nil || !freshCoverage.HasMajority() {
			continue
		}
		if err := store.Delete(ref); err != nil {
			return report, err
		}
		report.Completed++
	}
	report.Pending -= report.Completed
	return report, nil
}

func observeGeoRecord(ctx context.Context, plan placement.GeoPlan, descriptor records.Descriptor, recordID string,
	observer GeoRecordObserver) ([]placement.RegionHoldings, []placement.RegionalHolding, map[uint64]rootstate.Node, error) {
	regions := plan.Regions()
	evidenceByRegion := make(map[string][]placement.Holding, len(regions))
	regional := make([]placement.RegionalHolding, 0)
	nodes := make(map[uint64]rootstate.Node)
	for _, region := range regions {
		for _, node := range region.Nodes() {
			nodes[node.RaftID] = node
			observation, err := observer(ctx, node, descriptor)
			if err != nil {
				continue
			}
			actual, idErr := observation.Holding.Descriptor.ID()
			if idErr != nil || actual != recordID || observation.RaftID != node.RaftID || observation.NodeID != node.NodeID || observation.RootConfirmedIndex == 0 {
				continue
			}
			holding := placement.Holding{RaftID: node.RaftID, NodeID: node.NodeID, StoreID: observation.Holding.StoreID, RecordID: observation.Holding.RecordID}
			if holding.StoreID == "" || holding.RecordID != recordID {
				continue
			}
			evidenceByRegion[region.RegionID] = append(evidenceByRegion[region.RegionID], holding)
			regional = append(regional, placement.RegionalHolding{RegionID: region.RegionID, Holding: holding})
		}
	}
	evidence := make([]placement.RegionHoldings, 0, len(regions))
	for _, region := range regions {
		if holdings := evidenceByRegion[region.RegionID]; len(holdings) != 0 {
			evidence = append(evidence, placement.RegionHoldings{RegionID: region.RegionID, Generation: region.Generation,
				GroupEpoch: region.GroupEpoch, FenceIndex: region.FenceIndex, Nodes: regionIDs(region.Nodes()), Holdings: holdings})
		}
	}
	return evidence, regional, nodes, nil
}

func regionIDs(nodes []rootstate.Node) []uint64 {
	ids := make([]uint64, len(nodes))
	for i, node := range nodes {
		ids[i] = node.RaftID
	}
	return ids
}

func OpenGeoRecordRepairStore(root string) (*GeoRecordRepairStore, error) {
	if root == "" {
		return nil, errors.New("Geo record repair directory is empty")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	db, err := pebbleutil.Open(filepath.Join(root, "dubi-geo-record-repair"), &pebble.Options{Logger: pebbleutil.QuietLogger{}})
	if err != nil {
		return nil, err
	}
	return &GeoRecordRepairStore{db: db}, nil
}

func validateGeoRepairReference(ref capstate.RecordReference) error {
	if ref.Version != 2 || ref.Descriptor.Validate() != nil || raft.ValidateGroupID(ref.OwnerGroupID) != nil ||
		ref.OwnerGroupID == raft.RootGroupID || rootstate.ValidateRangeID(ref.RangeID) != nil || ref.RangeEpoch == 0 ||
		len(ref.RoutingKey) > 4096 || rootstate.ValidateRequestID(ref.RequestID) != nil || ref.Index == 0 || len(ref.CommandHash) != 64 {
		return errors.New("invalid Geo record repair reference")
	}
	digest, err := hex.DecodeString(ref.CommandHash)
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != ref.CommandHash {
		return errors.New("invalid Geo record repair command hash")
	}
	return nil
}

func geoRecordRepairKey(ref capstate.RecordReference) ([]byte, error) {
	if err := validateGeoRepairReference(ref); err != nil {
		return nil, err
	}
	recordID, err := ref.Descriptor.ID()
	if err != nil {
		return nil, err
	}
	return []byte("geo-record/" + recordID + "/" + ref.RequestID), nil
}

func (s *GeoRecordRepairStore) Save(ref capstate.RecordReference) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	key, err := geoRecordRepairKey(ref)
	if err != nil {
		return err
	}
	entry := geoRecordRepairEntry{Version: geoRepairTaskVersion, Reference: ref}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if len(encoded) > geoRepairMaxBytes {
		return errors.New("Geo record repair entry exceeds size bound")
	}
	count, err := s.countLocked()
	if err != nil {
		return err
	}
	if _, closer, getErr := s.db.Get(key); getErr == nil {
		_ = closer.Close()
	} else if !errors.Is(getErr, pebble.ErrNotFound) {
		return getErr
	} else if count >= geoRepairMaxEntries {
		return errors.New("Geo record repair ledger is full")
	}
	return s.db.Set(key, encoded, pebble.Sync)
}

func (s *GeoRecordRepairStore) List() ([]capstate.RecordReference, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("geo-record/"), UpperBound: []byte("geo-record0")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	result := make([]capstate.RecordReference, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		if len(result) >= geoRepairMaxEntries {
			return nil, errors.New("Geo record repair ledger exceeds entry bound")
		}
		if len(iter.Value()) > geoRepairMaxBytes {
			return nil, fmt.Errorf("Geo record repair entry %q exceeds size bound", string(iter.Key()))
		}
		var entry geoRecordRepairEntry
		decoder := json.NewDecoder(bytes.NewReader(iter.Value()))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&entry) != nil || decoder.Decode(new(struct{})) != io.EOF || entry.Version != geoRepairTaskVersion || validateGeoRepairReference(entry.Reference) != nil {
			return nil, fmt.Errorf("invalid Geo record repair entry %q", string(iter.Key()))
		}
		key, keyErr := geoRecordRepairKey(entry.Reference)
		if keyErr != nil || string(key) != string(iter.Key()) {
			return nil, fmt.Errorf("Geo record repair key differs from reference %q", string(iter.Key()))
		}
		result = append(result, entry.Reference)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	slices.SortFunc(result, func(a, b capstate.RecordReference) int {
		ak, _ := geoRecordRepairKey(a)
		bk, _ := geoRecordRepairKey(b)
		return bytes.Compare(ak, bk)
	})
	return result, nil
}

func (s *GeoRecordRepairStore) Delete(ref capstate.RecordReference) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	key, err := geoRecordRepairKey(ref)
	if err != nil {
		return err
	}
	return s.db.Delete(key, pebble.Sync)
}

func (s *GeoRecordRepairStore) countLocked() (int, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("geo-record/"), UpperBound: []byte("geo-record0")})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
		if count > geoRepairMaxEntries {
			break
		}
	}
	return count, iter.Error()
}

func (s *GeoRecordRepairStore) usable() error {
	if s == nil || s.db == nil || s.closed {
		return errors.New("Geo record repair store is closed")
	}
	return nil
}

func (s *GeoRecordRepairStore) Close() error {
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
