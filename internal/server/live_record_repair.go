package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
)

// liveRecordRepair is a process-local worker, not an operator-configured job.
// Its cursor is only an optimization: restart, page-level error, or State
// replacement restarts a sweep. Per-reference debt advances and is retried
// on the next sweep from authoritative Owner references.
type liveRecordRepair struct {
	runtime          *node.Runtime
	client           *raftrpc.GroupClient
	progress         *recordrepair.ProgressStore
	geoRecordRepair  *recordrepair.GeoRecordRepairStore
	report           func(error)
	groups           map[string]liveRepairCursor
	next             int
	lastGroup        string
	lastDone         bool
	lastPageAdvanced bool
	statusMu         sync.RWMutex
	lastCheck        time.Time
	lastErr          map[string]string
}

type liveRepairCursor struct {
	owner        *capstate.State
	cursor       []byte
	unresolved   uint64
	firstFailure string
}

func (cursor *liveRepairCursor) addFailures(failures []error) {
	cursor.unresolved += uint64(len(failures))
	if cursor.firstFailure == "" && len(failures) != 0 {
		cursor.firstFailure = failures[0].Error()
	}
}

func (cursor liveRepairCursor) debtError(groupID string) error {
	if cursor.unresolved == 0 {
		return nil
	}
	return fmt.Errorf("Group %s has %d unresolved records in the completed sweep (first: %s)",
		groupID, cursor.unresolved, cursor.firstFailure)
}

const liveRepairPageLimit = 8
const liveRepairInterval = 5 * time.Second
const liveRepairContinuationDelay = 100 * time.Millisecond
const liveRepairTimeout = 30 * time.Second

func (worker *liveRecordRepair) run(ctx context.Context) {
	timer := time.NewTimer(liveRepairInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			stepCtx, cancel := context.WithTimeout(ctx, liveRepairTimeout)
			err := worker.step(stepCtx)
			cancel()
			worker.observeStep(ctx, err)
			timer.Reset(worker.nextDelay())
		}
	}
}

// An unfinished cursor means there is known work beyond the page just
// processed. Continue promptly, but keep a short fixed pause so background
// scanning does not become an unbounded busy loop. Full sweeps still use the
// low-frequency idle interval.
func (worker *liveRecordRepair) nextDelay() time.Duration {
	if worker.lastPageAdvanced || (worker.lastDone && len(worker.groups) != 0) {
		return liveRepairContinuationDelay
	}
	return liveRepairInterval
}

func (worker *liveRecordRepair) observeStep(ctx context.Context, err error) {
	worker.statusMu.Lock()
	worker.lastCheck = time.Now()
	if err == nil {
		delete(worker.lastErr, "") // Global discovery succeeded this round.
		// A clean page does not erase debt found earlier in this sweep.
		if worker.lastDone {
			delete(worker.lastErr, worker.lastGroup)
		}
		worker.statusMu.Unlock()
		return
	}
	if ctx.Err() != nil {
		worker.statusMu.Unlock()
		return
	}
	if worker.lastErr == nil {
		worker.lastErr = make(map[string]string)
	}
	if err.Error() == worker.lastErr[worker.lastGroup] {
		worker.statusMu.Unlock()
		return
	}
	worker.lastErr[worker.lastGroup] = err.Error()
	worker.statusMu.Unlock()
	if worker.report != nil {
		worker.report(fmt.Errorf("repair current immutable records: %w", err))
	}
}

type repairIssue struct {
	GroupID string
	Error   string
}

type repairObservation struct {
	LastCheck time.Time
	Issues    []repairIssue
}

func (worker *liveRecordRepair) snapshot() repairObservation {
	if worker == nil {
		return repairObservation{}
	}
	worker.statusMu.RLock()
	result := repairObservation{LastCheck: worker.lastCheck}
	for groupID, issue := range worker.lastErr {
		result.Issues = append(result.Issues, repairIssue{GroupID: groupID, Error: issue})
	}
	worker.statusMu.RUnlock()
	slices.SortFunc(result.Issues, func(a, b repairIssue) int { return strings.Compare(a.GroupID, b.GroupID) })
	return result
}

func (worker *liveRecordRepair) step(ctx context.Context) error {
	worker.lastGroup = ""
	worker.lastDone = false
	worker.lastPageAdvanced = false
	if err := ctx.Err(); err != nil {
		return err
	}
	descriptors, err := worker.runtime.Root.Groups()
	if err != nil {
		return err
	}
	groupIDs := make([]string, 0, len(descriptors))
	regionViews := make(map[string]rootstate.RegionProgress, len(descriptors))
	for _, descriptor := range descriptors {
		owner, found := worker.runtime.Capabilities.Resolve(descriptor.GroupID)
		if !found || owner == nil {
			continue
		}
		progress, confirmed, err := worker.runtime.Root.RegionProgress(descriptor.GroupID)
		if err != nil {
			return err
		}
		if !confirmed || !progress.Eligible || progress.Pending != nil ||
			progress.Head.PendingGeneration != 0 || len(progress.Current.Nodes) < 3 ||
			!slices.Contains(progress.Current.Nodes, worker.runtime.Identity.RaftID) {
			continue
		}
		groupIDs = append(groupIDs, descriptor.GroupID)
		regionViews[descriptor.GroupID] = progress
	}
	active := make(map[string]bool, len(groupIDs))
	for _, id := range groupIDs {
		active[id] = true
	}
	for id := range worker.groups {
		if !active[id] {
			delete(worker.groups, id)
		}
	}
	worker.statusMu.Lock()
	for id := range worker.lastErr {
		if id != "" && !active[id] {
			delete(worker.lastErr, id)
		}
	}
	worker.statusMu.Unlock()
	if len(groupIDs) == 0 {
		return nil
	}
	slices.Sort(groupIDs)
	groupID := groupIDs[worker.next%len(groupIDs)]
	worker.next = (worker.next + 1) % len(groupIDs)
	worker.lastGroup = groupID
	owner, found := worker.runtime.Capabilities.Resolve(groupID)
	if !found || owner == nil {
		delete(worker.groups, groupID)
		return nil
	}
	if worker.groups == nil {
		worker.groups = make(map[string]liveRepairCursor)
	}
	previous := worker.groups[groupID]
	if previous.owner != owner {
		previous = liveRepairCursor{}
	}
	if previous.owner == nil && worker.progress != nil {
		saved, found, err := worker.progress.Load(groupID)
		if err != nil {
			return fmt.Errorf("Group %s repair progress: %w", groupID, err)
		}
		if found && !saved.Done {
			if saved.Matches(regionViews[groupID]) {
				previous.cursor = saved.Cursor
				previous.unresolved = saved.Unresolved
				previous.firstFailure = saved.FirstError
			} else if err := worker.progress.Delete(groupID); err != nil {
				return fmt.Errorf("Group %s discard stale repair progress: %w", groupID, err)
			}
		}
	}
	page, err := recordrepair.RepairCurrentLocalPage(ctx, worker.runtime.Host, worker.runtime.Root,
		owner, groupID, worker.runtime.Identity.RaftID, worker.runtime.Identity.NodeID,
		worker.runtime.Records, worker.client, previous.cursor, liveRepairPageLimit)
	if err != nil {
		delete(worker.groups, groupID)
		return fmt.Errorf("Group %s: %w", groupID, err)
	}
	if worker.geoRecordRepair != nil {
		for _, ref := range page.Unresolved {
			if err := worker.geoRecordRepair.Save(ref); err != nil {
				delete(worker.groups, groupID)
				return fmt.Errorf("Group %s save Geo repair task: %w", groupID, err)
			}
		}
		for _, ref := range page.References {
			if err := worker.geoRecordRepair.Delete(ref); err != nil {
				delete(worker.groups, groupID)
				return fmt.Errorf("Group %s delete completed Geo repair task: %w", groupID, err)
			}
		}
	}
	status, err := worker.runtime.Host.Status(groupID)
	if err != nil {
		delete(worker.groups, groupID)
		return fmt.Errorf("Group %s leader status: %w", groupID, err)
	}
	failures := slices.Clone(page.Failures)
	if status.LeaderID == worker.runtime.Identity.RaftID {
		writer := recordwrite.Writer{Barrier: worker.runtime.Host, Root: worker.runtime.Root,
			Owner: owner, LocalID: worker.runtime.Identity.RaftID,
			LocalNodeID: worker.runtime.Identity.NodeID, Store: worker.runtime.Records,
			Observer: worker.client, Submit: func(call context.Context, groupID string, command []byte) error {
				if err := call.Err(); err != nil {
					return err
				}
				return worker.runtime.Host.Submit(groupID, command)
			}}
		for _, ref := range page.References {
			if _, err := writer.EnsureCurrentLocalConfirmation(ctx, ref); err != nil {
				if ctx.Err() != nil {
					delete(worker.groups, groupID)
					return ctx.Err()
				}
				failures = append(failures, fmt.Errorf("reconfirm %s: %w", ref.RequestID, err))
			}
		}
	}
	previous.addFailures(failures)
	if page.Page.Done {
		if worker.progress != nil {
			if err := worker.progress.Save(repairProgressForPage(regionViews[groupID], groupID, previous, page.Page.Done, nil)); err != nil {
				delete(worker.groups, groupID)
				return fmt.Errorf("Group %s save completed repair progress: %w", groupID, err)
			}
		}
		delete(worker.groups, groupID)
		worker.lastDone = true
	} else if len(page.Page.Cursor) == 0 || bytes.Equal(page.Page.Cursor, previous.cursor) {
		delete(worker.groups, groupID)
		return fmt.Errorf("Group %s repair cursor did not advance", groupID)
	} else {
		previous.owner, previous.cursor = owner, page.Page.Cursor
		if worker.progress != nil {
			if err := worker.progress.Save(repairProgressForPage(regionViews[groupID], groupID, previous, false, page.Page.Cursor)); err != nil {
				delete(worker.groups, groupID)
				return fmt.Errorf("Group %s save repair progress: %w", groupID, err)
			}
		}
		worker.groups[groupID] = previous
		worker.lastPageAdvanced = true
	}
	if worker.lastDone {
		return previous.debtError(groupID)
	}
	if len(failures) != 0 {
		return fmt.Errorf("Group %s has %d unresolved records: %w", groupID, len(failures), errors.Join(failures...))
	}
	return nil
}

func repairProgressForPage(view rootstate.RegionProgress, groupID string, cursor liveRepairCursor, done bool, next []byte) recordrepair.Progress {
	return recordrepair.Progress{GroupID: groupID, RegionID: view.Current.RegionID, Generation: view.Current.Generation,
		GroupEpoch: view.Current.GroupEpoch, FenceIndex: view.Current.GroupFenceIndex, Nodes: slices.Clone(view.Current.Nodes),
		Cursor: slices.Clone(next), Unresolved: cursor.unresolved, FirstError: cursor.firstFailure, Done: done}
}
