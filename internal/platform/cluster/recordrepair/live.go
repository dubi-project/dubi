package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LiveReport describes one completed fixed Owner cut, not a promise that no
// later mutation or physical loss has occurred. A failed pass returns no
// completed report even if some immutable bytes were already fetched.
type LiveReport struct {
	Cut         enginestate.LiveReferenceCut
	AlreadyHeld uint64
	Fetched     uint64
}

// LivePageReport covers one bounded Owner state cut, not a whole-Group sweep.
// Page.Done means traversal ended, not that Unrepaired is zero.
type LivePageReport struct {
	Page        enginestate.LiveReferencePage
	AlreadyHeld uint64
	Fetched     uint64
	Unrepaired  uint64
	Failures    []error                       // Bounded by the page limit; debt is retried next sweep.
	References  []enginestate.RecordReference // Bounded by the requested page limit.
	Unresolved  []enginestate.RecordReference // Bounded references eligible for Geo repair retry.
}

// SourceClient is the authenticated Node transport needed by live repair.
type SourceClient interface {
	ObserveRecordHolding(context.Context, uint64, records.Descriptor) (raftrpc.HoldingObservation, error)
	FetchRecord(context.Context, rootstate.Node, records.Descriptor, *records.Store) (raftrpc.RecordFetchResult, error)
	RepairRecord(context.Context, rootstate.Node, records.Descriptor, *records.Store) (raftrpc.RecordFetchResult, error)
}

// RemoteRepairer is optional so the local repair algorithm remains usable in
// unit tests and for a healthy local Store. When the local Store has no usable
// Volume, a GroupClient can ask another current cohort Node to pull the bytes
// from the same authenticated source and write its own Pack.
type RemoteRepairer interface {
	RepairRecordOnNode(context.Context, rootstate.Node, rootstate.Node, string, records.Descriptor) (records.Holding, error)
	FetchRecordOnNode(context.Context, rootstate.Node, rootstate.Node, string, records.Descriptor) (records.Holding, error)
}

var errRemoteRepairCompleted = errors.New("remote repair completed; local holding remains unresolved")

// RepairCurrentLocal fills records missing on one current cohort Node. It
// discovers authoritative references through an Owner quorum read and fixed
// state cut; sources come only from the exact confirmed Root cohort. Missing
// bytes may be retried next pass. A valid index with unreadable Pack bytes may
// be explicitly relocated from an authenticated current source; a damaged
// index or unusable Store is never overwritten. This does not update LocalReady
// receipts or schedule itself.
func RepairCurrentLocal(ctx context.Context, barrier placement.ReadBarrier, root placement.RootReader,
	owner *enginestate.State, groupID string, localID uint64, localNodeID string,
	destination *records.Store, client SourceClient) (LiveReport, error) {
	page, err := repairCurrentLocal(ctx, barrier, root, owner, groupID, localID, localNodeID, destination, client, nil, 0)
	if err != nil {
		return LiveReport{}, err
	}
	return LiveReport{Cut: page.Page.Cut, AlreadyHeld: page.AlreadyHeld, Fetched: page.Fetched}, nil
}

// RepairCurrentLocalPage is the bounded unit used by an in-process repair
// worker. Its cursor is valid only for this Group and should be discarded on
// a returned error. Per-reference failures are instead reported as bounded
// Unrepaired debt so later references are not starved. Each call rechecks the
// Root cohort before returning a structurally valid page.
func RepairCurrentLocalPage(ctx context.Context, barrier placement.ReadBarrier, root placement.RootReader,
	owner *enginestate.State, groupID string, localID uint64, localNodeID string,
	destination *records.Store, client SourceClient, cursor []byte,
	limit uint32) (LivePageReport, error) {
	if limit == 0 || limit > enginestate.MaxLiveReferencePage {
		return LivePageReport{}, errors.New("invalid live repair page limit")
	}
	return repairCurrentLocal(ctx, barrier, root, owner, groupID, localID, localNodeID, destination, client, cursor, limit)
}

func repairCurrentLocal(ctx context.Context, barrier placement.ReadBarrier, root placement.RootReader,
	owner *enginestate.State, groupID string, localID uint64, localNodeID string,
	destination *records.Store, client SourceClient, cursor []byte,
	limit uint32) (LivePageReport, error) {
	if ctx == nil || barrier == nil || root == nil || owner == nil || localID == 0 ||
		localNodeID == "" || destination == nil || client == nil || owner.GroupID() != groupID {
		return LivePageReport{}, errors.New("live record repair requires Owner, Root, Store and authenticated client")
	}
	plan, err := placement.ReadLocalPlan(ctx, barrier, root, groupID)
	if err != nil {
		return LivePageReport{}, err
	}
	local := false
	for _, node := range plan.Nodes() {
		if node.RaftID == localID {
			if node.NodeID != localNodeID {
				return LivePageReport{}, errors.New("repair destination identity differs from confirmed cohort")
			}
			local = true
		}
	}
	if !local {
		return LivePageReport{}, errors.New("repair destination is not in the confirmed cohort")
	}
	var report LivePageReport
	visit := func(ref enginestate.RecordReference) error {
		return processRepairReference(ctx, plan, ref, localID, destination, client, limit, &report)
	}
	var page enginestate.LiveReferencePage
	for {
		if limit == 0 {
			var cut enginestate.LiveReferenceCut
			cut, err = owner.WalkCurrentReferences(ctx, barrier, visit)
			page = enginestate.LiveReferencePage{Cut: cut, Done: err == nil}
		} else {
			page, err = owner.WalkCurrentReferencesPage(ctx, barrier, cursor, limit, visit)
		}
		if !errors.Is(err, enginestate.ErrStateBehind) {
			break
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return LivePageReport{}, ctx.Err()
		case <-timer.C:
		}
	}
	if err != nil {
		return LivePageReport{}, err
	}
	latest, err := placement.ReadLocalPlan(ctx, barrier, root, groupID)
	if err != nil {
		return LivePageReport{}, err
	}
	if !plan.Matches(latest) {
		return LivePageReport{}, errors.New("Region cohort changed during live record repair")
	}
	report.Page = page
	return report, nil
}

func processRepairReference(ctx context.Context, plan placement.LocalPlan, ref enginestate.RecordReference,
	localID uint64, destination *records.Store, client SourceClient, limit uint32, report *LivePageReport) error {
	fetched, err := repairReference(ctx, plan, ref, localID, destination, client)
	if err != nil {
		if limit == 0 || ctx.Err() != nil {
			return err
		}
		report.Unrepaired++
		report.Failures = append(report.Failures, fmt.Errorf("reference %s: %w", ref.RequestID, err))
		report.Unresolved = append(report.Unresolved, ref)
		return nil
	}
	if fetched {
		report.Fetched++
	} else {
		report.AlreadyHeld++
	}
	if limit != 0 {
		report.References = append(report.References, ref)
	}
	return nil
}

func repairReference(ctx context.Context, plan placement.LocalPlan, ref enginestate.RecordReference,
	localID uint64, destination *records.Store, client SourceClient) (bool, error) {
	d := ref.Descriptor
	relocate := false
	localUnavailable := false
	if _, err := destination.Verify(ctx, d); err == nil {
		return false, nil
	} else if !errors.Is(err, records.ErrNotHeld) {
		// Only a valid existing index with unreadable physical bytes may be
		// relocated. Closed/foreign Stores and damaged indexes remain fenced.
		needed, checkErr := destination.RelocationNeeded(ctx, d)
		if checkErr != nil {
			if errors.Is(checkErr, records.ErrUnavailable) {
				localUnavailable = true
				// A closed or otherwise unusable local Store cannot be repaired
				// through the ordinary source-fetch path.  Do not even probe
				// cohort sources unless the caller supplied the explicit remote
				// target primitive; otherwise a failed local repair would look
				// like it made progress while no local holding can be published.
				if _, ok := client.(RemoteRepairer); !ok {
					return false, fmt.Errorf("local immutable-record Store is unavailable: %w", checkErr)
				}
			} else {
				return false, fmt.Errorf("verify destination immutable record: %w", checkErr)
			}
		} else if !needed {
			return false, nil // Another local repair completed between checks.
		} else {
			relocate = true
		}
	}
	id, err := d.ID()
	if err != nil {
		return false, err
	}
	var failures []error
	sources := plan.Nodes()
	remaining := 0
	for _, source := range sources {
		if source.RaftID != localID {
			remaining++
		}
	}
	for _, source := range sources {
		if source.RaftID == localID {
			continue
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		attempt, cancel := sourceAttemptContext(ctx, remaining)
		remaining--
		observed, err := client.ObserveRecordHolding(attempt, source.RaftID, d)
		if err != nil {
			cancel()
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if retryableRecordSource(err) {
				failures = append(failures, fmt.Errorf("observe Node %d: %w", source.RaftID, err))
				continue
			}
			return false, err
		}
		actual, err := observed.Holding.Descriptor.ID()
		if err != nil || actual != id || observed.RootConfirmedIndex == 0 {
			cancel()
			return false, errors.New("repair source returned a different immutable descriptor")
		}
		if _, err := plan.Count(id, []placement.Holding{{RaftID: observed.RaftID, NodeID: observed.NodeID,
			StoreID: observed.Holding.StoreID, RecordID: observed.Holding.RecordID}}); err != nil {
			cancel()
			return false, fmt.Errorf("repair source differs from confirmed cohort: %w", err)
		}
		var transferErr error
		remotePublished := false
		if localUnavailable {
			transferErr = remoteRepair(ctx, plan, localID, source, d.GroupID, d, client)
			remotePublished = transferErr == nil
		} else if relocate {
			_, transferErr = client.RepairRecord(attempt, source, d, destination)
			if transferErr != nil {
				if remoteErr := remoteRepair(ctx, plan, localID, source, d.GroupID, d, client); remoteErr == nil {
					transferErr, remotePublished = nil, true
				}
			}
		} else {
			_, transferErr = client.FetchRecord(attempt, source, d, destination)
			if transferErr != nil {
				if remoteErr := remoteRepair(ctx, plan, localID, source, d.GroupID, d, client); remoteErr == nil {
					transferErr, remotePublished = nil, true
				}
			}
		}
		if remotePublished {
			// This worker is repairing one specific local Node. A successful
			// remote target write must not make this local Node look healthy or
			// advance its local confirmation; retain the local debt explicitly.
			return false, errRemoteRepairCompleted
		}
		if err := transferErr; err != nil {
			cancel()
			if parentErr := ctx.Err(); parentErr != nil {
				return false, parentErr
			}
			if retryableRecordSource(err) {
				failures = append(failures, fmt.Errorf("fetch Node %d: %w", source.RaftID, err))
				continue
			}
			return false, err
		}
		cancel()
		if _, err := destination.Verify(ctx, d); err != nil {
			return false, fmt.Errorf("verify repaired immutable record: %w", err)
		}
		return true, nil
	}
	return false, errors.Join(append([]error{errors.New("no confirmed cohort Node supplied referenced bytes")}, failures...)...)
}

func remoteRepair(ctx context.Context, plan placement.LocalPlan, localID uint64, source rootstate.Node,
	group string, d records.Descriptor, client SourceClient) error {
	remote, ok := client.(RemoteRepairer)
	if !ok {
		return errors.New("no remote repair target is available")
	}
	var failures []error
	targets := plan.Nodes()
	remaining := 0
	for _, target := range targets {
		if target.RaftID != localID && target.RaftID != source.RaftID {
			remaining++
		}
	}
	for _, target := range targets {
		if target.RaftID == localID || target.RaftID == source.RaftID {
			continue
		}
		attempt, cancel := sourceAttemptContext(ctx, remaining)
		remaining--
		_, err := remote.RepairRecordOnNode(attempt, target, source, group, d)
		cancel()
		if err != nil {
			// The target may not have an index yet. RepairExisting is still
			// attempted first so a damaged existing location is replaced without
			// changing its logical index; only then publish a missing index.
			fetchAttempt, fetchCancel := sourceAttemptContext(ctx, remaining+1)
			_, fetchErr := remote.FetchRecordOnNode(fetchAttempt, target, source, group, d)
			fetchCancel()
			if fetchErr == nil {
				return nil
			} else {
				failures = append(failures, fmt.Errorf("target Node %d: %w; fetch: %v", target.RaftID, err, fetchErr))
			}
		} else {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return errors.Join(append([]error{errors.New("no current cohort Node accepted remote repair")}, failures...)...)
}

func retryableRecordSource(err error) bool {
	if errors.Is(err, records.ErrNotHeld) || errors.Is(err, records.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.NotFound, codes.DataLoss, codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	}
	return false
}
