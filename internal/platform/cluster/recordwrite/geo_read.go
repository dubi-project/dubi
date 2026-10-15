package recordwrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// ReadGeo serves a publication whose durable Owner receipt proves physical
// coverage in a majority of Regions. It re-observes every Region at a fresh
// Root cut before returning bytes; the persisted claim is a hint, never a
// lease that survives a topology or publication change.
func (r Reader) ReadGeo(ctx context.Context, target router.Target, key []byte, maxRegions, maxNodes int) (LocalRead, error) {
	if ctx == nil || r.Barrier == nil || r.Root == nil || r.Owner == nil || r.Store == nil ||
		r.Observer == nil || r.Fetcher == nil || r.LocalID == 0 || r.LocalNodeID == "" ||
		target.Validate() != nil || !bytes.Equal(target.RoutingKey, key) {
		return LocalRead{}, errors.New("geo read requires a valid Owner, route and Node runtime")
	}
	root, ok := r.Root.(interface {
		RangeRegionPlacementView(string, int, int) (rootstate.RangePlacement, rootstate.RegionPlacementView, error)
	})
	if !ok {
		return LocalRead{}, errors.New("Root does not expose durable Range Region placement")
	}
	_, plan, err := placement.ReadRangeGeoPlan(ctx, r.Barrier, root, target.RangeID, maxRegions, maxNodes)
	if err != nil {
		return LocalRead{}, err
	}
	var ref capstate.RecordReference
	var receipt capstate.GeoCoverageRecord
	var foundRef, foundReceipt bool
	if err := readOwner(ctx, r.Barrier, r.Owner, target, func(view capstate.ReadView) error {
		var readErr error
		ref, foundRef, readErr = view.RecordReference(key)
		if readErr != nil {
			return readErr
		}
		receipt, foundReceipt, readErr = view.GeoCoverage(key)
		return readErr
	}); err != nil {
		return LocalRead{}, err
	}
	if !foundRef || !foundReceipt || ref.Descriptor.Validate() != nil || receipt.Claim.Validate() != nil ||
		receipt.Claim.ReferenceRequestID != ref.RequestID || receipt.Claim.ReferenceIndex != ref.Index ||
		receipt.ReferenceCommandHash != ref.CommandHash || !reflect.DeepEqual(receipt.Claim.Target, target) ||
		!bytes.Equal(receipt.Claim.Key, key) {
		return LocalRead{}, errors.New("record has no current cross-Region durability confirmation")
	}
	recordID, err := ref.Descriptor.ID()
	if err != nil || recordID != receipt.Claim.RecordID {
		return LocalRead{}, errors.New("geo coverage differs from the current immutable reference")
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: ref.RequestID, Index: ref.Index,
		CommandHash: ref.CommandHash, RangeID: ref.RangeID, RangeEpoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
	evidence, nodes, err := r.observeGeo(ctx, plan, ref.Descriptor)
	if err != nil {
		return LocalRead{}, err
	}
	if _, err := CheckCommittedGeo(ctx, r.Barrier, r.Owner, target, ref.Descriptor, publication,
		plan, evidence, receipt.RequestID, receipt.Claim); err != nil {
		return LocalRead{}, err
	}
	body, _, err := r.Store.Read(ctx, ref.Descriptor)
	if errors.Is(err, records.ErrNotHeld) {
		var failures []error
		for _, node := range nodes {
			if node.RaftID == r.LocalID {
				continue
			}
			if _, fetchErr := r.Fetcher.FetchRecord(ctx, node, ref.Descriptor, r.Store); fetchErr != nil {
				failures = append(failures, fmt.Errorf("Node %d: %w", node.RaftID, fetchErr))
				continue
			}
			body, _, err = r.Store.Read(ctx, ref.Descriptor)
			if err == nil {
				break
			}
			if !errors.Is(err, records.ErrNotHeld) {
				return LocalRead{}, err
			}
		}
		if err != nil {
			return LocalRead{}, errors.Join(append([]error{errors.New("cross-Region majority could not serve immutable bytes")}, failures...)...)
		}
	} else if err != nil {
		return LocalRead{}, err
	}
	var final capstate.RecordReference
	if err := readOwner(ctx, r.Barrier, r.Owner, target, func(view capstate.ReadView) error {
		var ok bool
		var readErr error
		final, ok, readErr = view.RecordReference(key)
		if readErr != nil {
			return readErr
		}
		if !ok || final.RequestID != ref.RequestID || final.Index != ref.Index || final.CommandHash != ref.CommandHash {
			return errors.New("immutable reference changed during geo read")
		}
		return nil
	}); err != nil {
		return LocalRead{}, err
	}
	return LocalRead{Body: body, Reference: final, RegionGeneration: localRegionGeneration(plan, r.LocalID), State: "geo_ready"}, nil
}

func (r Reader) observeGeo(ctx context.Context, plan placement.GeoPlan, descriptor records.Descriptor) ([]placement.RegionHoldings, []rootstate.Node, error) {
	recordID, err := descriptor.ID()
	if err != nil {
		return nil, nil, err
	}
	var evidence []placement.RegionHoldings
	var allNodes []rootstate.Node
	localFound := false
	for _, region := range plan.Regions() {
		nodes := region.Nodes()
		allNodes = append(allNodes, nodes...)
		var holdings []placement.Holding
		for _, node := range nodes {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			var holding placement.Holding
			if node.RaftID == r.LocalID {
				if node.NodeID != r.LocalNodeID {
					return nil, nil, errors.New("local Node identity differs from geo Region plan")
				}
				localFound = true
				verified, verifyErr := r.Store.Verify(ctx, descriptor)
				if verifyErr != nil {
					continue
				}
				holding = placement.Holding{RaftID: node.RaftID, NodeID: node.NodeID, StoreID: verified.StoreID, RecordID: verified.RecordID}
			} else {
				observed, observeErr := r.Observer.ObserveRecordHolding(ctx, node.RaftID, descriptor)
				if observeErr != nil {
					continue
				}
				holding = placement.Holding{RaftID: observed.RaftID, NodeID: observed.NodeID,
					StoreID: observed.Holding.StoreID, RecordID: observed.Holding.RecordID}
			}
			if holding.RecordID != recordID {
				continue
			}
			holdings = append(holdings, holding)
			count, countErr := region.Count(recordID, holdings)
			if countErr != nil {
				return nil, nil, countErr
			}
			if count >= region.Required {
				break
			}
		}
		count, countErr := region.Count(recordID, holdings)
		if countErr != nil || count < region.Required {
			return nil, nil, errors.Join(countErr, fmt.Errorf("Region %s lacks a fresh physical majority", region.RegionID))
		}
		ids := make([]uint64, len(nodes))
		for i, node := range nodes {
			ids[i] = node.RaftID
		}
		evidence = append(evidence, placement.RegionHoldings{RegionID: region.RegionID, Generation: region.Generation,
			GroupEpoch: region.GroupEpoch, FenceIndex: region.FenceIndex, Nodes: ids, Holdings: holdings})
	}
	if !localFound {
		return nil, nil, errors.New("local Node is not in geo Region plan")
	}
	slices.SortFunc(evidence, func(a, b placement.RegionHoldings) int {
		if a.RegionID < b.RegionID {
			return -1
		}
		if a.RegionID > b.RegionID {
			return 1
		}
		return 0
	})
	return evidence, allNodes, nil
}
