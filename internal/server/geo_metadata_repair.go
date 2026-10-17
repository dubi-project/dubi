package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

const geoMetadataRepairInterval = 5 * time.Second

// geoMetadataRepair retries only durable mirror debt. It never infers a new
// placement from online Nodes: every task is resolved against one fresh Root
// placement cut, and stale generation/epoch/fence tasks remain pending.
type geoMetadataRepair struct {
	runtime *node.Runtime
	client  *raftrpc.GroupClient
	store   *recordwrite.GeoMetadataRetryStore
	records *recordrepair.GeoRecordRepairStore
	report  func(error)
	// geoCursors bound discovery work to one reference page per Group and
	// round. They are only an optimization; a restart or failed page restarts
	// that Group's scan from the beginning.
	geoCursors map[string][]byte
}

func (worker *geoMetadataRepair) run(ctx context.Context) {
	if worker == nil || worker.runtime == nil || worker.client == nil || worker.store == nil {
		return
	}
	ticker := time.NewTicker(geoMetadataRepairInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
			_, metadataErr := worker.step(attempt)
			_, recordErr := worker.stepRecords(attempt)
			cancel()
			if ctx.Err() == nil && worker.report != nil {
				if metadataErr != nil {
					worker.report(fmt.Errorf("retry Geo metadata mirrors: %w", metadataErr))
				}
				if recordErr != nil {
					worker.report(fmt.Errorf("retry Geo records: %w", recordErr))
				}
			}
		}
	}
}

func (worker *geoMetadataRepair) stepRecords(ctx context.Context) (recordrepair.GeoRecordRepairReport, error) {
	if worker == nil || worker.records == nil {
		return recordrepair.GeoRecordRepairReport{}, nil
	}
	if _, err := worker.runtime.ReadRootIndex(ctx); err != nil {
		return recordrepair.GeoRecordRepairReport{}, err
	}
	stale, err := worker.discoverStaleGeoRecords(ctx)
	if err != nil {
		return recordrepair.GeoRecordRepairReport{}, err
	}
	report, retryErr := recordrepair.RetryPendingGeoRecords(ctx, worker.records,
		func(resolveCtx context.Context, ref enginestate.RecordReference) (placement.GeoPlan, string, error) {
			_, plan, err := placement.ReadRangeGeoPlan(resolveCtx, worker.runtime.Host, worker.runtime.Root,
				ref.RangeID, rootstate.MaxRegionCohortNodes, rootstate.MaxRegionCohortNodes)
			if err != nil {
				return placement.GeoPlan{}, "", err
			}
			progress, found, err := worker.runtime.Root.RegionProgress(ref.OwnerGroupID)
			if err != nil || !found {
				return placement.GeoPlan{}, "", fmt.Errorf("Owner Group %q has no current Region", ref.OwnerGroupID)
			}
			owner, found := worker.runtime.Engines.Resolve(ref.OwnerGroupID)
			if !found || owner == nil {
				return placement.GeoPlan{}, "", fmt.Errorf("Owner Group %q is not open locally", ref.OwnerGroupID)
			}
			target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID,
				OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
			var current enginestate.RecordReference
			var currentFound bool
			if _, err := owner.ReadRouted(resolveCtx, worker.runtime.Host, target, func(view enginestate.ReadView) error {
				var readErr error
				current, currentFound, readErr = view.RecordReference(ref.Descriptor.Key)
				return readErr
			}); err != nil {
				return placement.GeoPlan{}, "", fmt.Errorf("current Owner reference is unavailable: %w", err)
			}
			if !currentFound {
				return placement.GeoPlan{}, "", errors.New("current Owner reference is no longer present")
			}
			currentID, idErr := current.Descriptor.ID()
			wantedID, wantedErr := ref.Descriptor.ID()
			if idErr != nil || wantedErr != nil || currentID != wantedID || current.RequestID != ref.RequestID || current.Index != ref.Index ||
				current.CommandHash != ref.CommandHash || !bytes.Equal(current.Descriptor.Key, ref.Descriptor.Key) {
				return placement.GeoPlan{}, "", errors.New("Geo repair task reference is no longer current")
			}
			return plan, progress.Current.RegionID, nil
		},
		func(observeCtx context.Context, node rootstate.Node, descriptor records.Descriptor) (raftrpc.HoldingObservation, error) {
			return worker.client.ObserveRecordHolding(observeCtx, node.RaftID, descriptor)
		}, worker.client)
	if retryErr != nil {
		return report, retryErr
	}
	if err := worker.reconfirmStaleGeoRecords(ctx, stale); err != nil {
		return report, err
	}
	return report, nil
}

// discoverStaleGeoRecords turns a current, but generation-bound, GeoCoverage
// mismatch into the same durable record-repair ledger used for missing bytes.
// The discovery walk is bounded per page and never infers placement from
// online Nodes.
func (worker *geoMetadataRepair) discoverStaleGeoRecords(ctx context.Context) ([]enginestate.RecordReference, error) {
	if worker == nil || worker.runtime == nil || worker.records == nil {
		return nil, nil
	}
	descriptors, err := worker.runtime.Root.Groups()
	if err != nil {
		return nil, err
	}
	var stale []enginestate.RecordReference
	if worker.geoCursors == nil {
		worker.geoCursors = make(map[string][]byte)
	}
	active := make(map[string]bool, len(descriptors))
	for _, descriptor := range descriptors {
		active[descriptor.GroupID] = true
		owner, found := worker.runtime.Engines.Resolve(descriptor.GroupID)
		if !found || owner == nil {
			continue
		}
		cursor := worker.geoCursors[descriptor.GroupID]
		page, pageErr := owner.WalkCurrentReferencesPage(ctx, worker.runtime.Host, cursor, liveRepairPageLimit,
			func(ref enginestate.RecordReference) error {
				_, plan, planErr := placement.ReadRangeGeoPlan(ctx, worker.runtime.Host, worker.runtime.Root,
					ref.RangeID, rootstate.MaxRegionCohortNodes, rootstate.MaxRegionCohortNodes)
				if planErr != nil || len(plan.Regions()) < 2 {
					return nil
				}
				target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID,
					OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
				var receipt enginestate.GeoCoverageRecord
				var hasReceipt bool
				if _, readErr := owner.ReadRouted(ctx, worker.runtime.Host, target, func(view enginestate.ReadView) error {
					var err error
					receipt, hasReceipt, err = view.GeoCoverage(ref.Descriptor.Key)
					return err
				}); readErr != nil {
					return readErr
				}
				if !hasReceipt || recordwrite.GeoReceiptMatchesPlan(receipt, ref, target, plan) {
					return nil
				}
				if saveErr := worker.records.Save(ref); saveErr != nil {
					return saveErr
				}
				stale = append(stale, ref)
				return nil
			})
		if pageErr != nil {
			delete(worker.geoCursors, descriptor.GroupID)
			return stale, pageErr
		}
		if page.Done {
			delete(worker.geoCursors, descriptor.GroupID)
		} else if len(page.Cursor) == 0 || bytes.Equal(page.Cursor, cursor) {
			delete(worker.geoCursors, descriptor.GroupID)
			return stale, fmt.Errorf("Group %s Geo discovery cursor did not advance", descriptor.GroupID)
		} else {
			worker.geoCursors[descriptor.GroupID] = page.Cursor
		}
	}
	for groupID := range worker.geoCursors {
		if !active[groupID] {
			delete(worker.geoCursors, groupID)
		}
	}
	return stale, nil
}

func (worker *geoMetadataRepair) reconfirmStaleGeoRecords(ctx context.Context, refs []enginestate.RecordReference) error {
	if len(refs) == 0 {
		return nil
	}
	for _, ref := range refs {
		owner, found := worker.runtime.Engines.Resolve(ref.OwnerGroupID)
		if !found || owner == nil {
			continue
		}
		writer := recordwrite.Writer{Barrier: worker.runtime.Host, Root: worker.runtime.Root, Owner: owner,
			LocalID: worker.runtime.Identity.RaftID, LocalNodeID: worker.runtime.Identity.NodeID,
			Store: worker.runtime.Records, Sender: worker.client, Observer: worker.client,
			Submit: func(call context.Context, groupID string, command []byte) error {
				if err := call.Err(); err != nil {
					return err
				}
				return worker.runtime.Host.Submit(groupID, command)
			}, MirrorGeo: worker.client, MirrorRetry: worker.store}
		confirmed, err := writer.EnsureCurrentGeoConfirmation(ctx, ref,
			rootstate.MaxRegionCohortNodes, rootstate.MaxRegionCohortNodes)
		if err != nil || !confirmed {
			continue
		}
		if err := worker.records.Delete(ref); err != nil {
			return err
		}
	}
	return nil
}

func (worker *geoMetadataRepair) step(ctx context.Context) (recordwrite.GeoMirrorRetryReport, error) {
	if ctx == nil || worker == nil || worker.runtime == nil || worker.client == nil || worker.store == nil {
		return recordwrite.GeoMirrorRetryReport{}, fmt.Errorf("Geo metadata retry worker is not configured")
	}
	if _, err := worker.runtime.ReadRootIndex(ctx); err != nil {
		return recordwrite.GeoMirrorRetryReport{}, err
	}
	return recordwrite.RetryPendingGeoMetadata(ctx, worker.client, worker.store,
		func(resolveCtx context.Context, mirror routedstate.GeoMetadataMirror) (placement.LocalPlan, error) {
			_, plan, err := placement.ReadRangeGeoPlan(resolveCtx, worker.runtime.Host, worker.runtime.Root,
				mirror.Target.RangeID, rootstate.MaxRegionCohortNodes, rootstate.MaxRegionCohortNodes)
			if err != nil {
				return placement.LocalPlan{}, err
			}
			for _, region := range plan.Regions() {
				if region.RegionID == mirror.RegionID {
					return region, nil
				}
			}
			return placement.LocalPlan{}, fmt.Errorf("current Geo plan has no Region %q", mirror.RegionID)
		})
}
