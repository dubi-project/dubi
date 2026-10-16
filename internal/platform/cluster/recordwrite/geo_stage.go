package recordwrite

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// GeoStaged is a cross-Region physical staging attempt. It contains no
// authoritative publication and cannot by itself be returned as GeoReady.
type GeoStaged struct {
	Plan     placement.GeoPlan
	RecordID string
	Evidence []placement.RegionHoldings
}

// StageGeo writes one immutable record to a strict local majority in enough
// selected Regions to satisfy the plan's strict Region majority. The Region
// denominator is never reduced for unavailable Nodes. The local Region is
// always attempted first and is mandatory; unavailable non-local Regions are
// left for repair as long as the fixed Region majority is still reached.
// This is physical evidence only: the caller must publish the authoritative
// reference and bind the evidence to a durable metadata claim.
func StageGeo(ctx context.Context, plan placement.GeoPlan, localID uint64, localNodeID string,
	store Store, sender Sender, descriptor records.Descriptor, body []byte) (GeoStaged, error) {
	if ctx == nil || localID == 0 || localNodeID == "" || store == nil || sender == nil ||
		descriptor.Validate() != nil || descriptor.Size != uint64(len(body)) {
		return GeoStaged{}, errors.New("geo staging requires a valid Node, Store, sender and descriptor")
	}
	if err := validateLocalPayload(descriptor, body); err != nil {
		return GeoStaged{}, err
	}
	recordID, err := descriptor.ID()
	if err != nil {
		return GeoStaged{}, err
	}
	regions := plan.Regions()
	if len(regions) == 0 {
		return GeoStaged{}, errors.New("geo staging requires a non-empty Region plan")
	}
	staged := GeoStaged{Plan: plan, RecordID: recordID}
	// A small-Region write must retain a physical copy in the Owner Region.
	// Process it first so a remote majority can never mask a missing local copy.
	localRegion := -1
	for i, region := range regions {
		for _, node := range region.Nodes() {
			if node.RaftID == localID {
				localRegion = i
				break
			}
		}
		if localRegion >= 0 {
			break
		}
	}
	if localRegion < 0 {
		return GeoStaged{}, errors.New("local Node is not in geo Region plan")
	}
	if localRegion != 0 {
		regions[0], regions[localRegion] = regions[localRegion], regions[0]
	}
	requiredRegions := plan.RequiredRegions()
	localFound := false
	var regionFailures []error
	for _, region := range regions {
		if len(staged.Evidence) >= requiredRegions {
			break
		}
		nodes := region.Nodes()
		var holdings []placement.Holding
		var failures []error
		remaining := len(nodes)
		for _, node := range nodes {
			if err := ctx.Err(); err != nil {
				return GeoStaged{}, err
			}
			var holding placement.Holding
			if node.RaftID == localID {
				if node.NodeID != localNodeID {
					return GeoStaged{}, errors.New("local Node identity differs from geo Region plan")
				}
				if localFound {
					return GeoStaged{}, errors.New("local Node appears in multiple geo Regions")
				}
				localFound = true
				if _, err := store.Put(ctx, descriptor, body); err != nil {
					return GeoStaged{}, fmt.Errorf("store local geo record: %w", err)
				}
				verified, err := store.Verify(ctx, descriptor)
				if err != nil {
					return GeoStaged{}, fmt.Errorf("verify local geo record: %w", err)
				}
				id, idErr := verified.Descriptor.ID()
				if idErr != nil || id != recordID {
					return GeoStaged{}, errors.New("local Store verified a different geo descriptor")
				}
				holding = placement.Holding{RaftID: localID, NodeID: localNodeID, StoreID: verified.StoreID, RecordID: verified.RecordID}
			} else {
				attempt, done := remoteCandidateContext(ctx, remaining)
				remaining--
				observed, err := sender.PutRecord(attempt, node, descriptor, body)
				done()
				if err != nil {
					if ctx.Err() != nil {
						return GeoStaged{}, ctx.Err()
					}
					if retryable(err) || errors.Is(err, context.DeadlineExceeded) {
						failures = append(failures, fmt.Errorf("Region %s Node %d: %w", region.RegionID, node.RaftID, err))
						continue
					}
					return GeoStaged{}, fmt.Errorf("transfer geo record to Region %s Node %d: %w", region.RegionID, node.RaftID, err)
				}
				id, idErr := observed.Holding.Descriptor.ID()
				if idErr != nil || id != recordID || observed.RootConfirmedIndex == 0 {
					return GeoStaged{}, errors.New("remote Node returned a different geo descriptor or no Root confirmation")
				}
				holding = placement.Holding{RaftID: observed.RaftID, NodeID: observed.NodeID,
					StoreID: observed.Holding.StoreID, RecordID: observed.Holding.RecordID}
			}
			holdings = append(holdings, holding)
			count, err := region.Count(recordID, holdings)
			if err != nil {
				return GeoStaged{}, fmt.Errorf("validate Region %s physical holding: %w", region.RegionID, err)
			}
			if count >= region.Required {
				break
			}
		}
		count, err := region.Count(recordID, holdings)
		if err != nil || count < region.Required {
			if err != nil {
				regionFailures = append(regionFailures, fmt.Errorf("Region %s: %w", region.RegionID, err))
			} else {
				regionFailures = append(regionFailures, fmt.Errorf("Region %s lacks its stable local data majority", region.RegionID))
			}
			if region.RegionID == regions[0].RegionID {
				return GeoStaged{}, errors.Join(append(failures, regionFailures...)...)
			}
			continue
		}
		ids := make([]uint64, len(nodes))
		for i, node := range nodes {
			ids[i] = node.RaftID
		}
		staged.Evidence = append(staged.Evidence, placement.RegionHoldings{RegionID: region.RegionID,
			Generation: region.Generation, GroupEpoch: region.GroupEpoch, FenceIndex: region.FenceIndex,
			Nodes: ids, Holdings: holdings})
	}
	if !localFound {
		return GeoStaged{}, errors.New("local Node is not in geo Region plan")
	}
	coverage, err := plan.Assess(recordID, staged.Evidence)
	if err != nil || !coverage.HasMajority() {
		regionFailures = append(regionFailures, errors.New("geo Region majority was not physically verified"))
		return GeoStaged{}, errors.Join(append(regionFailures, err)...)
	}
	// Keep evidence deterministic for the subsequent publication claim.
	slices.SortFunc(staged.Evidence, func(a, b placement.RegionHoldings) int {
		if a.RegionID < b.RegionID {
			return -1
		}
		if a.RegionID > b.RegionID {
			return 1
		}
		return 0
	})
	return staged, nil
}
