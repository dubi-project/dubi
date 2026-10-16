package recordwrite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// EnsureCurrentGeoConfirmation refreshes a GeoCoverage receipt after a
// Region generation, Group epoch, or fence changes. It never republishes the
// engine mutation: the immutable reference is read first and the new
// physical observation is bound to that exact publication before the Owner
// Group records the replacement receipt.
func (w Writer) EnsureCurrentGeoConfirmation(ctx context.Context, ref enginestate.RecordReference,
	maxRegions, maxNodes int) (bool, error) {
	if ctx == nil || w.Barrier == nil || w.Root == nil || w.Owner == nil || w.Store == nil ||
		w.Observer == nil || w.MirrorGeo == nil || w.Submit == nil || w.LocalID == 0 ||
		w.LocalNodeID == "" || ref.Descriptor.Validate() != nil || ref.Index == 0 ||
		ref.OwnerGroupID != w.Owner.GroupID() {
		return false, errors.New("Geo reconfirmation requires a complete Owner and Node runtime")
	}
	root, ok := w.Root.(rangeGeoRoot)
	if !ok {
		return false, errors.New("Root does not expose durable Range Region placement")
	}
	target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID,
		OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: bytes.Clone(ref.RoutingKey)}
	if err := target.Validate(); err != nil {
		return false, err
	}
	_, plan, err := placement.ReadRangeGeoPlan(ctx, w.Barrier, root, ref.RangeID, maxRegions, maxNodes)
	if err != nil {
		return false, err
	}
	var current enginestate.RecordReference
	var receipt enginestate.GeoCoverageRecord
	var foundReceipt bool
	if err := readOwner(ctx, w.Barrier, w.Owner, target, func(view enginestate.ReadView) error {
		var found bool
		var readErr error
		current, found, readErr = view.RecordReference(ref.Descriptor.Key)
		if readErr != nil {
			return readErr
		}
		if !found || !sameReference(current, ref) {
			return errors.New("Geo reconfirmation reference is no longer current")
		}
		receipt, foundReceipt, readErr = view.GeoCoverage(ref.Descriptor.Key)
		return readErr
	}); err != nil {
		return false, err
	}
	if foundReceipt && geoReceiptMatchesPlan(receipt, ref, target, plan) {
		return false, nil
	}
	recordID, err := ref.Descriptor.ID()
	if err != nil {
		return false, err
	}
	evidence, err := observeGeoHoldings(ctx, plan, w.LocalID, w.LocalNodeID, w.Store, w.Observer, ref.Descriptor)
	if err != nil {
		return false, err
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: ref.RequestID, Index: ref.Index,
		CommandHash: ref.CommandHash, RangeID: ref.RangeID, RangeEpoch: ref.RangeEpoch,
		RoutingKey: ref.RoutingKey}
	coverage, err := plan.AssessPublication(publication, evidence)
	if err != nil || !coverage.HasMajority() {
		return false, errors.Join(err, errors.New("Geo reconfirmation lacks a Region majority"))
	}
	mirrors, err := GeoMetadataMirrors(target, ref.Descriptor, publication, plan, evidence)
	if err != nil {
		return false, err
	}
	if _, err := ReplicateGeoMetadataWithRetryStore(ctx, w.MirrorGeo, plan, mirrors, w.MirrorRetry); err != nil {
		return false, fmt.Errorf("replicate Geo reconfirmation mirrors: %w", err)
	}
	claim, err := GeoConfirmationClaim(target, ref.Descriptor.Key, coverage, evidence)
	if err != nil {
		return false, err
	}
	confirmID, err := geoReconfirmationID(claim)
	if err != nil {
		return false, err
	}
	command, err := routedstate.EncodeGeoConfirmation(confirmID, claim)
	if err != nil {
		return false, err
	}
	if err := w.Submit(ctx, target.OwnerGroupID, command); err != nil {
		return false, fmt.Errorf("submit Geo reconfirmation: %w", err)
	}
	result, err := w.awaitResult(ctx, target.OwnerGroupID, func() (enginestate.Result, bool, error) {
		return w.Owner.ResultForGeoConfirmation(confirmID, claim)
	})
	if err != nil {
		return false, err
	}
	if !result.Applied || result.Error != "" {
		return false, fmt.Errorf("Geo reconfirmation rejected: %s", result.Error)
	}
	if _, err := CheckCommittedGeo(ctx, w.Barrier, w.Owner, target, ref.Descriptor, publication,
		plan, evidence, confirmID, claim); err != nil {
		return false, err
	}
	return true, nil
}

func sameReference(a, b enginestate.RecordReference) bool {
	return a.OwnerGroupID == b.OwnerGroupID && a.RequestID == b.RequestID && a.Index == b.Index && a.CommandHash == b.CommandHash &&
		a.RangeID == b.RangeID && a.RangeEpoch == b.RangeEpoch &&
		bytes.Equal(a.RoutingKey, b.RoutingKey) && reflect.DeepEqual(a.Descriptor, b.Descriptor)
}

func geoReceiptMatchesPlan(receipt enginestate.GeoCoverageRecord, ref enginestate.RecordReference,
	target router.Target, plan placement.GeoPlan) bool {
	if receipt.Claim.Validate() != nil || !reflect.DeepEqual(receipt.Claim.Target, target) ||
		!bytes.Equal(receipt.Claim.Key, ref.Descriptor.Key) || receipt.Claim.ReferenceRequestID != ref.RequestID ||
		receipt.Claim.ReferenceIndex != ref.Index || receipt.Claim.RecordID != recordIDForReference(ref) ||
		receipt.ReferenceCommandHash != ref.CommandHash {
		return false
	}
	known := make(map[string]placement.LocalPlan, len(plan.Regions()))
	for _, region := range plan.Regions() {
		known[region.RegionID] = region
	}
	if len(receipt.Claim.Regions) < plan.RequiredRegions() {
		return false
	}
	for _, observed := range receipt.Claim.Regions {
		region, found := known[observed.RegionID]
		nodes := region.Nodes()
		ids := make([]uint64, len(nodes))
		for i, node := range nodes {
			ids[i] = node.RaftID
		}
		if !found || observed.Generation != region.Generation || observed.GroupEpoch != region.GroupEpoch ||
			observed.FenceIndex != region.FenceIndex || !slices.Equal(observed.Nodes, ids) {
			return false
		}
	}
	return true
}

func recordIDForReference(ref enginestate.RecordReference) string {
	id, _ := ref.Descriptor.ID()
	return id
}

// GeoReceiptMatchesPlan reports whether a durable GeoCoverage receipt still
// describes the supplied immutable publication and current Region cohort.
// Callers use the negative result to enqueue a bounded background
// reconfirmation; it does not by itself assert that the bytes are currently
// held.
func GeoReceiptMatchesPlan(receipt enginestate.GeoCoverageRecord, ref enginestate.RecordReference,
	target router.Target, plan placement.GeoPlan) bool {
	return geoReceiptMatchesPlan(receipt, ref, target, plan)
}

func geoReconfirmationID(claim routedstate.GeoConfirmation) (string, error) {
	encoded, err := json.Marshal(claim)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	requestID := "geo-reconfirm-" + hex.EncodeToString(digest[:])
	if err := rootstate.ValidateRequestID(requestID); err != nil {
		return "", err
	}
	return requestID, nil
}

func observeGeoHoldings(ctx context.Context, plan placement.GeoPlan, localID uint64, localNodeID string,
	store Verifier, observer Observer, descriptor records.Descriptor) ([]placement.RegionHoldings, error) {
	if ctx == nil || localID == 0 || localNodeID == "" || store == nil || observer == nil || descriptor.Validate() != nil {
		return nil, errors.New("Geo observation requires a complete local verifier and observer")
	}
	recordID, err := descriptor.ID()
	if err != nil {
		return nil, err
	}
	regions := plan.Regions()
	localFound := false
	for _, region := range regions {
		for _, node := range region.Nodes() {
			if node.RaftID != localID {
				continue
			}
			if node.NodeID != localNodeID || localFound {
				return nil, errors.New("local Node identity differs from Geo Region plan")
			}
			localFound = true
		}
	}
	if !localFound {
		return nil, errors.New("local Node is not in Geo Region plan")
	}
	evidence := make([]placement.RegionHoldings, 0, plan.RequiredRegions())
	for _, region := range regions {
		if len(evidence) >= plan.RequiredRegions() {
			break
		}
		var holdings []placement.Holding
		for _, node := range region.Nodes() {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			var holding placement.Holding
			if node.RaftID == localID {
				verified, verifyErr := store.Verify(ctx, descriptor)
				if verifyErr != nil {
					continue
				}
				holding = placement.Holding{RaftID: node.RaftID, NodeID: node.NodeID, StoreID: verified.StoreID, RecordID: verified.RecordID}
			} else {
				observed, observeErr := observer.ObserveRecordHolding(ctx, node.RaftID, descriptor)
				if observeErr != nil || observed.RaftID != node.RaftID || observed.NodeID != node.NodeID || observed.RootConfirmedIndex == 0 {
					continue
				}
				holding = placement.Holding{RaftID: observed.RaftID, NodeID: observed.NodeID,
					StoreID: observed.Holding.StoreID, RecordID: observed.Holding.RecordID}
			}
			if holding.RecordID != recordID || holding.StoreID == "" {
				continue
			}
			holdings = append(holdings, holding)
			count, countErr := region.Count(recordID, holdings)
			if countErr != nil {
				return nil, countErr
			}
			if count >= region.Required {
				break
			}
		}
		count, err := region.Count(recordID, holdings)
		if err != nil {
			return nil, err
		}
		if count < region.Required {
			continue
		}
		ids := make([]uint64, len(region.Nodes()))
		for i, node := range region.Nodes() {
			ids[i] = node.RaftID
		}
		evidence = append(evidence, placement.RegionHoldings{RegionID: region.RegionID,
			Generation: region.Generation, GroupEpoch: region.GroupEpoch, FenceIndex: region.FenceIndex,
			Nodes: ids, Holdings: holdings})
	}
	coverage, err := plan.Assess(recordID, evidence)
	if err != nil || !coverage.HasMajority() {
		return nil, errors.Join(err, errors.New("Geo observation lacks a Region majority"))
	}
	slices.SortFunc(evidence, func(a, b placement.RegionHoldings) int { return strings.Compare(a.RegionID, b.RegionID) })
	return evidence, nil
}
