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
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

const (
	geoRepairTaskVersion  = 1
	geoRepairMaxEntries   = 4096
	geoRepairMaxBytes     = 1 << 20
	geoObserveConcurrency = 6
)

// GeoRecordRepairStore is a durable retry hint for physical data that should
// be restored in a Region other than the current local target. It stores the
// authoritative reference cut, never a Volume, Pack location, replica count
// or operator-selected address. Every retry must derive a fresh GeoPlan.
type GeoRecordRepairStore struct {
	mu     sync.Mutex
	db     *pebble.DB
	closed bool
	failed error
	count  int
}

type geoRecordRepairEntry struct {
	Version   int                         `json:"version"`
	Reference enginestate.RecordReference `json:"reference"`
}

// GeoRecordPlanResolver must derive a fresh confirmed GeoPlan, its Root cut,
// and the Region to prefer from the current Root/Range placement. It must not
// reuse a task's historical cohort after a Region generation changes.
type GeoRecordPlanResolver func(context.Context, enginestate.RecordReference) (placement.GeoPlan, string, uint64, error)

// ErrGeoReferenceSuperseded is returned only after a quorum-confirmed Owner
// read proves that the queued publication is no longer current. A missing
// Group, unreadable Owner, stale view or changed placement is not this proof.
var ErrGeoReferenceSuperseded = errors.New("queued Geo publication is no longer current")

// GeoRecordObserver authenticates one physical observation on a specific
// current Node. A failed observation is not evidence of a missing byte; the
// retry round simply leaves the task pending.
type GeoRecordObserver func(context.Context, rootstate.Node, records.Descriptor) (raftrpc.HoldingObservation, error)

type GeoRecordRepairReport struct {
	Attempted  uint64
	Completed  uint64
	Superseded uint64
	Pending    uint64
}

// RetryPendingGeoRecords performs one bounded retry round. It only removes a
// task after a fresh observation proves the complete GeoPlan has reached its
// configured Region majority, or the resolver proves that its publication
// was superseded at confirmed Root/Owner cuts. Superseded tasks do not count
// as physical completion. Physical writes and metadata/GeoReady claims remain
// separate operations.
func RetryPendingGeoRecords(ctx context.Context, store *GeoRecordRepairStore, resolver GeoRecordPlanResolver,
	observer GeoRecordObserver, transport RemoteRepairer) (GeoRecordRepairReport, error) {
	return RetryPendingGeoRecordsWithLocal(ctx, store, resolver, observer, transport, 0, nil)
}

// RetryPendingGeoRecordsWithLocal also supports a selected repair target on
// the current Node. That target is written through the local Store; remote
// targets continue through authenticated Node-to-Node RPC.
func RetryPendingGeoRecordsWithLocal(ctx context.Context, store *GeoRecordRepairStore, resolver GeoRecordPlanResolver,
	observer GeoRecordObserver, transport RemoteRepairer, localRaftID uint64, localRepair LocalGeoRepairer) (GeoRecordRepairReport, error) {
	if ctx == nil || store == nil || resolver == nil || observer == nil || transport == nil {
		return GeoRecordRepairReport{}, errors.New("Geo record repair retry requires context, store, resolver, observer and transport")
	}
	references, pending, err := store.NextPage(8)
	if err != nil {
		return GeoRecordRepairReport{}, err
	}
	report := GeoRecordRepairReport{Pending: uint64(pending)}
	var diagnostic error
	for _, ref := range references {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if err := store.MarkAttempt(ref); err != nil {
			return report, err
		}
		taskCtx, cancelTask := context.WithTimeout(ctx, 15*time.Second+time.Duration(ref.Descriptor.Size/(8<<20))*5*time.Second)
		taskReport, taskErr := retryGeoRecord(taskCtx, store, resolver, observer, transport, localRaftID, localRepair, ref)
		cancelTask()
		report.Attempted++
		report.Completed += taskReport.Completed
		report.Superseded += taskReport.Superseded
		report.Pending -= taskReport.Completed + taskReport.Superseded
		if taskErr != nil && diagnostic == nil {
			diagnostic = taskErr
		}
	}
	if report.Pending > 0 && diagnostic != nil {
		return report, fmt.Errorf("%d Geo record repair item(s) remain; first failure: %w", report.Pending, diagnostic)
	}
	return report, nil
}

func retryGeoRecord(ctx context.Context, store *GeoRecordRepairStore, resolver GeoRecordPlanResolver,
	observer GeoRecordObserver, transport RemoteRepairer, localRaftID uint64, localRepair LocalGeoRepairer,
	ref enginestate.RecordReference) (GeoRecordRepairReport, error) {
	report := GeoRecordRepairReport{Pending: 1}
	var diagnostic error
	for range 1 {
		plan, preferredRegion, rootCut, err := resolver(ctx, ref)
		if err != nil {
			if ctx.Err() != nil {
				return report, fmt.Errorf("resolve fresh Geo plan: %w", ctx.Err())
			}
			if rootCut != 0 && errors.Is(err, ErrGeoReferenceSuperseded) {
				if err := store.Delete(ref); err != nil {
					return report, err
				}
				report.Superseded++
				report.Pending--
				continue
			}
			if diagnostic == nil {
				diagnostic = fmt.Errorf("resolve fresh Geo plan: %w", err)
			}
			continue
		}
		if rootCut == 0 {
			if diagnostic == nil {
				diagnostic = errors.New("resolve fresh Geo plan: resolver returned no confirmed Root cut")
			}
			continue
		}
		recordID, err := ref.Descriptor.ID()
		if err != nil {
			continue
		}
		// The plan resolver just obtained this quorum-confirmed Root cut. Carry
		// it into the holding RPCs so destinations can qualify the caller against
		// their applied Root state without starting another Root ReadIndex round
		// before and after each local Pack verification.
		observeCtx := router.WithConfirmedRootIndex(ctx, rootCut)
		evidence, holdings, nodes, err := observeGeoRecord(observeCtx, plan, ref.Descriptor, recordID, observer)
		if err != nil {
			if ctx.Err() != nil {
				return report, fmt.Errorf("observe physical holdings: %w", ctx.Err())
			}
			if diagnostic == nil {
				diagnostic = fmt.Errorf("observe physical holdings: %w", err)
			}
			continue
		}
		coverage, err := plan.Assess(recordID, evidence)
		if err == nil && coverage.HasMajority() {
			if err := store.Delete(ref); err != nil {
				return report, err
			}
			report.Completed++
			report.Pending--
			continue
		}
		var target placement.RepairTarget
		if capacity, ok := transport.(interface {
			RepairHasCapacity(context.Context, rootstate.Node, uint64) (bool, error)
		}); ok {
			availability, capacityErr := repairAvailability(ctx, plan, holdings, ref.Descriptor.Size, capacity.RepairHasCapacity)
			if capacityErr != nil {
				return report, capacityErr
			}

			target, err = plan.SelectRepairTarget(recordID, preferredRegion, 0, holdings, availability)
		} else {
			target, err = plan.SelectRepairCandidates(recordID, preferredRegion, holdings)
		}
		if err != nil {
			if diagnostic == nil {
				diagnostic = fmt.Errorf("select Geo repair target: %w", err)
			}
			continue
		}
		if len(holdings) == 0 {
			if diagnostic == nil {
				diagnostic = errors.New("cannot repair Geo record: no verified physical source is available")
			}
			continue
		}
		source, found := geoRepairSource(target, holdings, nodes)
		if !found {
			if diagnostic == nil {
				diagnostic = errors.New("cannot repair Geo record: no verified source is outside the selected Region")
			}
			continue
		}
		_, repairErr := RepairSelectedTargetsWithLocal(ctx, plan, target, source, ref.Descriptor.GroupID,
			ref.Descriptor, transport, localRaftID, localRepair)
		if repairErr != nil && diagnostic == nil {
			diagnostic = fmt.Errorf("repair Geo target in Region %q: %w", target.RegionID, repairErr)
		}
		if ctx.Err() != nil {
			return report, fmt.Errorf("repair selected Geo targets: %w", ctx.Err())
		}
		freshEvidence, _, _, observeErr := observeGeoRecord(observeCtx, plan, ref.Descriptor, recordID, observer)
		if observeErr != nil {
			if ctx.Err() != nil {
				return report, fmt.Errorf("verify repaired Geo holdings: %w", ctx.Err())
			}
			if diagnostic == nil {
				diagnostic = fmt.Errorf("verify repaired Geo holdings: %w", observeErr)
			}
			continue
		}
		freshCoverage, assessErr := plan.Assess(recordID, freshEvidence)
		if assessErr != nil || !freshCoverage.HasMajority() {
			if diagnostic == nil {
				diagnostic = errors.New("Geo repair attempt finished without verified Region majority")
			}
			continue
		}
		if err := store.Delete(ref); err != nil {
			return report, err
		}
		report.Completed++
		report.Pending--
	}
	if report.Pending > 0 && diagnostic != nil {
		return report, fmt.Errorf("%d Geo record repair item(s) remain; first failure: %w", report.Pending, diagnostic)
	}
	return report, nil
}

func geoRepairSource(target placement.RepairTarget, holdings []placement.RegionalHolding,
	nodes map[uint64]rootstate.Node) (rootstate.Node, bool) {
	var source rootstate.Node
	for _, observed := range holdings {
		candidate, found := nodes[observed.Holding.RaftID]
		if !found || candidate.NodeID != observed.Holding.NodeID {
			continue
		}
		if target.Scope == placement.RepairInOtherRegion && candidate.RegionID == target.RegionID {
			continue
		}
		if source.RaftID == 0 || candidate.RaftID < source.RaftID {
			source = candidate
		}
	}
	return source, source.RaftID != 0
}

func observeGeoRecord(ctx context.Context, plan placement.GeoPlan, descriptor records.Descriptor, recordID string,
	observer GeoRecordObserver) ([]placement.RegionHoldings, []placement.RegionalHolding, map[uint64]rootstate.Node, error) {
	regions := plan.Regions()
	nodes := make(map[uint64]rootstate.Node)
	for _, region := range regions {
		for _, node := range region.Nodes() {
			nodes[node.RaftID] = node
		}
	}
	type result struct {
		node        rootstate.Node
		observation raftrpc.HoldingObservation
		err         error
	}
	jobs := make(chan rootstate.Node)
	results := make(chan result, len(nodes))
	workers := min(geoObserveConcurrency, len(nodes))
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for node := range jobs {
				observation, err := observer(ctx, node, descriptor)
				results <- result{node: node, observation: observation, err: err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, node := range nodes {
			select {
			case jobs <- node:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		wait.Wait()
		close(results)
	}()
	evidenceByRegion := make(map[string][]placement.Holding, len(regions))
	regional := make([]placement.RegionalHolding, 0)
	for observed := range results {
		if observed.err != nil {
			continue
		}
		observation, node := observed.observation, observed.node
		actual, idErr := observation.Holding.Descriptor.ID()
		if idErr != nil || actual != recordID || observation.RaftID != node.RaftID || observation.NodeID != node.NodeID || observation.RootConfirmedIndex == 0 {
			continue
		}
		holding := placement.Holding{RaftID: node.RaftID, NodeID: node.NodeID, StoreID: observation.Holding.StoreID, RecordID: observation.Holding.RecordID}
		if holding.StoreID == "" || holding.RecordID != recordID {
			continue
		}
		evidenceByRegion[node.RegionID] = append(evidenceByRegion[node.RegionID], holding)
		regional = append(regional, placement.RegionalHolding{RegionID: node.RegionID, Holding: holding})
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nodes, err
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
	store := &GeoRecordRepairStore{db: db}
	count, err := store.countLocked()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	store.count = count
	return store, nil
}

func validateGeoRepairReference(ref enginestate.RecordReference) error {
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

func geoRecordRepairKey(ref enginestate.RecordReference) ([]byte, error) {
	if err := validateGeoRepairReference(ref); err != nil {
		return nil, err
	}
	recordID, err := ref.Descriptor.ID()
	if err != nil {
		return nil, err
	}
	return []byte("geo-record/" + recordID + "/" + ref.RequestID), nil
}

func (s *GeoRecordRepairStore) Save(ref enginestate.RecordReference) error {
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
	added := false
	if _, closer, getErr := s.db.Get(key); getErr == nil {
		_ = closer.Close()
	} else if !errors.Is(getErr, pebble.ErrNotFound) {
		return getErr
	} else {
		added = true
	}
	if err := s.db.Set(key, encoded, pebble.Sync); err != nil {
		s.failed = err
		return err
	}
	if added {
		s.count++
	}
	return nil
}

func (s *GeoRecordRepairStore) List() ([]enginestate.RecordReference, error) {
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
	result := make([]enginestate.RecordReference, 0)
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
	slices.SortFunc(result, func(a, b enginestate.RecordReference) int {
		ak, _ := geoRecordRepairKey(a)
		bk, _ := geoRecordRepairKey(b)
		return bytes.Compare(ak, bk)
	})
	return result, nil
}

func (s *GeoRecordRepairStore) Delete(ref enginestate.RecordReference) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	key, err := geoRecordRepairKey(ref)
	if err != nil {
		return err
	}
	_, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	_ = closer.Close()
	if err := s.db.Delete(key, pebble.Sync); err != nil {
		s.failed = err
		return err
	}
	s.count--
	return nil
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
	}
	return count, iter.Error()
}

func (s *GeoRecordRepairStore) usable() error {
	if s == nil || s.db == nil || s.closed {
		return errors.New("Geo record repair store is closed")
	}
	if s.failed != nil {
		return s.failed
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

// Probe capacity concurrently with a bounded worker set. A dead Region must
// not consume a separate timeout for every Node ahead of healthy destinations.
func repairAvailability(ctx context.Context, plan placement.GeoPlan, holdings []placement.RegionalHolding, size uint64,
	probe func(context.Context, rootstate.Node, uint64) (bool, error)) ([]placement.NodeAvailability, error) {
	held := map[uint64]bool{}
	for _, h := range holdings {
		held[h.Holding.RaftID] = true
	}
	var candidates []rootstate.Node
	for _, region := range plan.Regions() {
		for _, node := range region.Nodes() {
			if !held[node.RaftID] {
				candidates = append(candidates, node)
			}
		}
	}
	jobs := make(chan rootstate.Node)
	results := make(chan placement.NodeAvailability, len(candidates))
	var wait sync.WaitGroup
	for range min(geoObserveConcurrency, len(candidates)) {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for node := range jobs {
				attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
				available, err := probe(attempt, node, size)
				cancel()
				results <- placement.NodeAvailability{Node: node, Healthy: err == nil, HasCapacity: available}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, node := range candidates {
			select {
			case jobs <- node:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wait.Wait(); close(results) }()
	var availability []placement.NodeAvailability
	for result := range results {
		availability = append(availability, result)
	}
	return availability, ctx.Err()
}
