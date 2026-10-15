package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/raft"
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
	if _, err := worker.runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return recordrepair.GeoRecordRepairReport{}, err
	}
	return recordrepair.RetryPendingGeoRecords(ctx, worker.records,
		func(resolveCtx context.Context, ref capstate.RecordReference) (placement.GeoPlan, string, error) {
			_, plan, err := placement.ReadRangeGeoPlan(resolveCtx, worker.runtime.Host, worker.runtime.Root,
				ref.RangeID, rootstate.MaxRegionCohortNodes, rootstate.MaxRegionCohortNodes)
			if err != nil {
				return placement.GeoPlan{}, "", err
			}
			progress, found, err := worker.runtime.Root.RegionProgress(ref.OwnerGroupID)
			if err != nil || !found {
				return placement.GeoPlan{}, "", fmt.Errorf("Owner Group %q has no current Region", ref.OwnerGroupID)
			}
			owner, found := worker.runtime.Capabilities.Resolve(ref.OwnerGroupID)
			if !found || owner == nil {
				return placement.GeoPlan{}, "", fmt.Errorf("Owner Group %q is not open locally", ref.OwnerGroupID)
			}
			target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID,
				OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
			var current capstate.RecordReference
			var currentFound bool
			if _, err := owner.ReadRouted(resolveCtx, worker.runtime.Host, target, func(view capstate.ReadView) error {
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
}

func (worker *geoMetadataRepair) step(ctx context.Context) (recordwrite.GeoMirrorRetryReport, error) {
	if ctx == nil || worker == nil || worker.runtime == nil || worker.client == nil || worker.store == nil {
		return recordwrite.GeoMirrorRetryReport{}, fmt.Errorf("Geo metadata retry worker is not configured")
	}
	if _, err := worker.runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
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
