package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

const geoMetadataRepairInterval = 5 * time.Second
const geoMetadataRepairTimeout = 30 * time.Second
const geoRecordRepairTimeout = 2 * time.Minute
const geoRecordDiagnosticInterval = 2 * time.Minute
const geoDiscoveryContinuationDelay = 100 * time.Millisecond
const geoDiscoveryPageLimit = 256

// Discovery retains a bounded work page, independent of its healthy-reference
// scan limit. Confirmation uses at most eight workers within this page.
const geoDiscoveryPendingLimit = recordwrite.GeoConfirmationRepairPageLimit

// geoMetadataRepair retries only durable mirror debt. It never infers a new
// placement from online Nodes: every task is resolved against one fresh Root
// placement cut, and stale generation/epoch/fence tasks remain pending.
type geoMetadataRepair struct {
	runtime              *node.Runtime
	io                   *resource.Maintenance
	client               *raftrpc.GroupClient
	store                *recordwrite.GeoMetadataRetryStore
	records              *recordrepair.GeoRecordRepairStore
	report               func(error)
	lastRecordDiagnostic time.Time
	// geoCursors bound discovery work to one reference page per Group and
	// round. They are only an optimization; a restart or failed page restarts
	// that Group's scan from the beginning.
	geoCursors        map[string][]byte
	discoveryAdvanced bool
	discoveryNext     int
}

func (worker *geoMetadataRepair) run(ctx context.Context) {
	if worker == nil || worker.runtime == nil || worker.client == nil || worker.store == nil {
		return
	}
	timer := time.NewTimer(geoMetadataRepairInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			maintenanceCtx, finish, admissionErr := worker.io.TryStart(ctx, "geo-repair")
			if admissionErr != nil {
				timer.Reset(geoMetadataRepairInterval)
				continue
			}
			metadataAttempt, cancelMetadata := context.WithTimeout(maintenanceCtx, geoMetadataRepairTimeout)
			_, metadataErr := worker.step(metadataAttempt)
			cancelMetadata()
			recordAttempt, cancelRecords := context.WithTimeout(maintenanceCtx, geoRecordRepairTimeout)
			_, recordErr := worker.stepRecords(recordAttempt)
			cancelRecords()
			finish()
			if ctx.Err() == nil && worker.report != nil {
				if metadataErr != nil {
					worker.report(fmt.Errorf("retry Geo metadata mirrors: %w", metadataErr))
				}
				if recordErr != nil && (worker.lastRecordDiagnostic.IsZero() || time.Since(worker.lastRecordDiagnostic) >= geoRecordDiagnosticInterval) {
					worker.report(fmt.Errorf("retry Geo records: %w", recordErr))
					worker.lastRecordDiagnostic = time.Now()
				}
			}
			timer.Reset(worker.nextDelay())
		}
	}
}

func (worker *geoMetadataRepair) nextDelay() time.Duration {
	if worker.discoveryAdvanced {
		return geoDiscoveryContinuationDelay
	}
	return geoMetadataRepairInterval
}

func (worker *geoMetadataRepair) stepRecords(ctx context.Context) (recordrepair.GeoRecordRepairReport, error) {
	if worker == nil {
		return recordrepair.GeoRecordRepairReport{}, nil
	}
	worker.discoveryAdvanced = false
	if worker.records == nil {
		return recordrepair.GeoRecordRepairReport{}, nil
	}
	ctx, finishTiming := withRepairTiming(ctx, "geo")
	defer finishTiming()
	started := time.Now()
	rootCut, err := worker.runtime.ReadRootIndex(ctx)
	recordwrite.Trace(ctx, "repair.root", started)
	if err != nil {
		return recordrepair.GeoRecordRepairReport{}, fmt.Errorf("confirm Root before Geo record discovery: %w", err)
	}
	discoveryCtx, cancelDiscovery := context.WithTimeout(ctx, 20*time.Second)
	stale, err := worker.discoverStaleGeoRecords(discoveryCtx, rootCut)
	cancelDiscovery()
	discoveryErr := err
	// A generation-only refresh can prove physical majorities and persist its
	// receipt directly. Avoid observing all holders once for physical repair
	// and then again for the same confirmation. Unconfirmed rows stay queued
	// for the existing physical repair path, followed by another confirmation.
	pending, _ := worker.reconfirmStaleGeoRecordsAtCut(ctx, stale, rootCut)
	if err := ctx.Err(); err != nil {
		return recordrepair.GeoRecordRepairReport{}, err
	}
	report, retryErr := recordrepair.RetryPendingGeoRecordsWithLocal(ctx, worker.records,
		func(resolveCtx context.Context, ref enginestate.RecordReference) (placement.GeoPlan, string, uint64, error) {
			if err := resource.WaitIO(resolveCtx, min(ref.Descriptor.Size, 1<<20)); err != nil {
				return placement.GeoPlan{}, "", 0, err
			}
			planCut, err := worker.runtime.ReadRootIndex(resolveCtx)
			if err != nil {
				return placement.GeoPlan{}, "", 0, err
			}
			_, plan, err := placement.ReadRangeGeoPlanAtCut(planCut, worker.runtime.Root,
				ref.RangeID, rootstate.MaxRegionCohortNodes, rootstate.MaxRegionCohortNodes)
			if err != nil {
				return placement.GeoPlan{}, "", 0, err
			}
			progress, found, err := worker.runtime.Root.RegionProgress(ref.OwnerGroupID)
			if err != nil || !found {
				return placement.GeoPlan{}, "", 0, fmt.Errorf("Owner Group %q has no current Region", ref.OwnerGroupID)
			}
			owner, found := worker.runtime.Engines.Resolve(ref.OwnerGroupID)
			if !found || owner == nil {
				return placement.GeoPlan{}, "", 0, fmt.Errorf("Owner Group %q is not open locally", ref.OwnerGroupID)
			}
			target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID,
				OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
			var current enginestate.RecordReference
			var currentFound bool
			ownerCut, err := owner.ReadRouted(resolveCtx, worker.runtime, target, func(view enginestate.ReadView) error {
				var readErr error
				current, currentFound, readErr = view.RecordReference(ref.Descriptor.Key)
				return readErr
			})
			if err != nil {
				return placement.GeoPlan{}, "", 0, fmt.Errorf("current Owner reference is unavailable: %w", err)
			}
			if (!currentFound && ownerCut >= ref.Index) || (currentFound && current.Index > ref.Index) {
				return plan, progress.Current.RegionID, planCut, recordrepair.ErrGeoReferenceSuperseded
			}
			if !currentFound {
				return placement.GeoPlan{}, "", 0, errors.New("Owner read cut has not reached the queued publication")
			}
			currentID, idErr := current.Descriptor.ID()
			wantedID, wantedErr := ref.Descriptor.ID()
			if idErr != nil || wantedErr != nil || currentID != wantedID || current.RequestID != ref.RequestID || current.Index != ref.Index ||
				current.CommandHash != ref.CommandHash || !bytes.Equal(current.Descriptor.Key, ref.Descriptor.Key) {
				return placement.GeoPlan{}, "", 0, errors.New("Geo repair task reference is no longer current")
			}
			return plan, progress.Current.RegionID, planCut, nil
		},
		geoRepairObserver(worker.runtime.Identity, worker.runtime.Root, worker.runtime.Records,
			func(observeCtx context.Context, node rootstate.Node, descriptor records.Descriptor) (raftrpc.HoldingObservation, error) {
				return worker.client.ObserveRecordHolding(observeCtx, node.RaftID, descriptor)
			}), &capacityRepairClient{GroupClient: worker.client, runtime: worker.runtime, samples: make(map[uint64]repairCapacitySample)}, worker.runtime.Identity.RaftID, func(repairCtx context.Context, source rootstate.Node,
			_ string, descriptor records.Descriptor) (records.Holding, error) {
			result, repairErr := worker.client.RepairRecord(repairCtx, source, descriptor, worker.runtime.Records)
			if repairErr != nil {
				result, repairErr = worker.client.FetchRecord(repairCtx, source, descriptor, worker.runtime.Records)
			}
			if repairErr != nil {
				return records.Holding{}, repairErr
			}
			return result.Local, nil
		})
	if retryErr != nil {
		return report, fmt.Errorf("retry pending Geo records: %w", retryErr)
	}
	report.Attempted += uint64(len(stale))
	report.Completed += uint64(len(stale) - len(pending))
	if _, err := worker.reconfirmStaleGeoRecords(ctx, pending); err != nil {
		return report, fmt.Errorf("reconfirm stale Geo records: %w", err)
	}
	count, err := worker.records.Count()
	if err != nil {
		return report, err
	}
	report.Pending = uint64(count)
	return report, discoveryErr
}

// discoverStaleGeoRecords turns a current, but generation-bound, GeoCoverage
// mismatch into the same durable record-repair ledger used for missing bytes.
// The discovery walk is bounded per page and never infers placement from
// online Nodes.
func (worker *geoMetadataRepair) discoverStaleGeoRecords(ctx context.Context, rootCut uint64) ([]enginestate.RecordReference, error) {
	if worker == nil || worker.runtime == nil || worker.records == nil {
		return nil, nil
	}
	if rootCut == 0 {
		return nil, errors.New("Geo discovery requires a confirmed Root cut")
	}
	descriptors, err := worker.runtime.Root.Groups()
	if err != nil {
		return nil, err
	}
	// Rotate the first Group even when an Owner barrier fails. A slow Group
	// must not consume every discovery round ahead of healthy Groups.
	if len(descriptors) > 0 {
		start := worker.discoveryNext % len(descriptors)
		worker.discoveryNext = (start + 1) % len(descriptors)
		descriptors = append(slices.Clone(descriptors[start:]), descriptors[:start]...)
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
		status, statusErr := worker.runtime.Host.Status(descriptor.GroupID)
		if statusErr != nil || status.LeaderID != worker.runtime.Identity.RaftID {
			// Global discovery belongs to the current Owner leader. Every Node
			// still retries its durable local hints above, including after a
			// leadership change; this check grants no additional authority.
			delete(worker.geoCursors, descriptor.GroupID)
			continue
		}
		cursor := worker.geoCursors[descriptor.GroupID]
		plans := make(map[string]placement.GeoPlan)
		queued := 0
		page, pageErr := owner.WalkCurrentReferencesWithGeoCoveragePage(ctx, repairTimingBarrier{worker.runtime}, cursor, geoDiscoveryPageLimit,
			func(ref enginestate.RecordReference, receipt enginestate.GeoCoverageRecord, hasReceipt bool) (bool, error) {
				plan, planErr := cachedGeoDiscoveryPlan(plans, ref.RangeID, func() (placement.GeoPlan, error) {
					_, current, err := placement.ReadRangeGeoPlanAtCut(rootCut, worker.runtime.Root,
						ref.RangeID, rootstate.MaxRegionCohortNodes, rootstate.MaxRegionCohortNodes)
					return current, err
				})
				if planErr != nil {
					return true, nil
				}
				if len(plan.Regions()) < 2 {
					return true, nil
				}
				target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID,
					OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
				if !geoRecordNeedsRepair(hasReceipt, receipt, ref, target, plan) {
					return true, nil
				}
				started := time.Now()
				saveErr := worker.records.Save(ref)
				recordwrite.Trace(ctx, "repair.record_save", started)
				if saveErr != nil {
					return false, saveErr
				}
				stale = append(stale, ref)
				queued++
				return queued < geoDiscoveryPendingLimit, nil
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
			worker.discoveryAdvanced = true
		}
	}
	for groupID := range worker.geoCursors {
		if !active[groupID] {
			delete(worker.geoCursors, groupID)
		}
	}
	return stale, nil
}

// The first reference needs the resolved plan as well as the cache entry.
// Keep resolution in one scope so a cache miss cannot shadow the returned plan.
func cachedGeoDiscoveryPlan(plans map[string]placement.GeoPlan, rangeID string,
	resolve func() (placement.GeoPlan, error)) (placement.GeoPlan, error) {
	if plan, found := plans[rangeID]; found {
		return plan, nil
	}
	plan, err := resolve()
	if err != nil {
		return placement.GeoPlan{}, err
	}
	plans[rangeID] = plan
	return plan, nil
}

func geoRecordNeedsRepair(hasReceipt bool, receipt enginestate.GeoCoverageRecord,
	ref enginestate.RecordReference, target router.Target, plan placement.GeoPlan) bool {
	// A missing receipt is the normal state immediately after a local-majority
	// write in a Region with at least three Nodes. Queue it for asynchronous
	// cross-Region repair just like a receipt invalidated by a topology change.
	return !hasReceipt || !recordwrite.GeoReceiptMatchesPlan(receipt, ref, target, plan)
}

// Unconfirmed rows remain durable hints. In particular a failed final page
// cut must re-save every row: an intervening physical repair may have cleared
// its byte-repair hint, but has not completed the Geo metadata confirmation.
func (worker *geoMetadataRepair) reconfirmStaleGeoRecords(ctx context.Context, refs []enginestate.RecordReference) ([]enginestate.RecordReference, error) {
	return worker.reconfirmStaleGeoRecordsAtCut(ctx, refs, 0)
}

// Only discovery's first bounded page may continue its already-confirmed
// admission cut. Consume it before invoking the page, including on failure;
// later pages and the post-physical-repair retry obtain new cuts themselves.
func (worker *geoMetadataRepair) reconfirmStaleGeoRecordsAtCut(ctx context.Context, refs []enginestate.RecordReference, discoveryCut uint64) ([]enginestate.RecordReference, error) {
	byOwner := make(map[string][]enginestate.RecordReference)
	for _, ref := range refs {
		byOwner[ref.OwnerGroupID] = append(byOwner[ref.OwnerGroupID], ref)
	}
	var pending []enginestate.RecordReference
	var diagnostic error
	keep := func(ref enginestate.RecordReference, err error) error {
		if err != nil {
			if saveErr := worker.records.Save(ref); saveErr != nil {
				return saveErr
			}
			if diagnostic == nil {
				diagnostic = fmt.Errorf("Geo record %s confirmation: %w", ref.RequestID, err)
			}
		}
		pending = append(pending, ref)
		return nil
	}
	for groupID, groupRefs := range byOwner {
		owner, found := worker.runtime.Engines.Resolve(groupID)
		if !found || owner == nil {
			for _, ref := range groupRefs {
				if err := keep(ref, fmt.Errorf("Owner Group %q is not open locally", groupID)); err != nil {
					return refs, err
				}
			}
			continue
		}
		writer := recordwrite.Writer{Barrier: repairTimingBarrier{worker.runtime}, Root: worker.runtime.Root, Owner: owner,
			LocalID: worker.runtime.Identity.RaftID, LocalNodeID: worker.runtime.Identity.NodeID,
			Store: repairTimingVerifier{worker.runtime.Records}, Sender: worker.client, Observer: repairTimingObserver{worker.client},
			Submit: func(call context.Context, groupID string, command []byte) error {
				if err := call.Err(); err != nil {
					return err
				}
				started := time.Now()
				defer recordwrite.Trace(call, "repair.owner_submit", started)
				return worker.runtime.Host.Submit(groupID, command)
			}, SubmitBatch: func(call context.Context, groupID string, commands [][]byte) ([]error, error) {
				if err := call.Err(); err != nil {
					return nil, err
				}
				return worker.runtime.Host.SubmitBatch(groupID, commands)
			}, MirrorGeo: repairTimingMirror{worker.client, groupID}, MirrorRetry: worker.store}
		for len(groupRefs) > 0 {
			page := recordwrite.GeoConfirmationRepairPage(groupRefs)
			groupRefs = groupRefs[len(page):]
			started := time.Now()
			index := discoveryCut
			discoveryCut = 0
			var results []recordwrite.GeoConfirmationRefresh
			var pageErr error
			if index != 0 {
				results, pageErr = writer.RefreshGeoConfirmationPageAtCut(ctx, page,
					rootstate.MaxRegionCohortNodes, rootstate.MaxRegionCohortNodes, index)
			} else {
				results, pageErr = writer.RefreshGeoConfirmationPage(ctx, page,
					rootstate.MaxRegionCohortNodes, rootstate.MaxRegionCohortNodes)
			}
			recordwrite.Trace(ctx, "repair.geo_page", started)
			for i, ref := range page {
				if pageErr != nil {
					if err := keep(ref, pageErr); err != nil {
						return refs, err
					}
					continue
				}
				if results[i].Confirmed {
					if err := worker.records.Delete(ref); err != nil {
						return refs, err
					}
				} else if err := keep(ref, results[i].Err); err != nil {
					return refs, err
				}
			}
		}
	}
	return pending, diagnostic
}

func (worker *geoMetadataRepair) step(ctx context.Context) (recordwrite.GeoMirrorRetryReport, error) {
	if ctx == nil || worker == nil || worker.runtime == nil || worker.client == nil || worker.store == nil {
		return recordwrite.GeoMirrorRetryReport{}, fmt.Errorf("Geo metadata retry worker is not configured")
	}
	if err := ctx.Err(); err != nil {
		return recordwrite.GeoMirrorRetryReport{}, err
	}
	count, err := worker.store.Count()
	if err != nil {
		return recordwrite.GeoMirrorRetryReport{}, err
	}
	// With no durable mirror task there is no remote operation to authorize.
	// A concurrently enqueued mirror remains durable for the next worker round.
	if count == 0 {
		return recordwrite.GeoMirrorRetryReport{}, nil
	}
	if _, err := worker.runtime.ReadRootIndex(ctx); err != nil {
		return recordwrite.GeoMirrorRetryReport{}, err
	}
	return recordwrite.RetryPendingGeoMetadata(ctx, worker.client, worker.store,
		func(resolveCtx context.Context, mirror routedstate.GeoMetadataMirror) (placement.LocalPlan, error) {
			_, plan, err := placement.ReadRangeGeoPlan(resolveCtx, worker.runtime, worker.runtime.Root,
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
