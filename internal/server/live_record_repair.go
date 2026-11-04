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

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
)

// liveRecordRepair is a process-local worker, not an operator-configured job.
// Its cursor is only an optimization: restart, page-level error, or State
// replacement restarts a sweep. Per-reference debt advances and is retried
// on the next sweep from authoritative Owner references.
type liveRecordRepair struct {
	runtime          *node.Runtime
	io               *resource.Maintenance
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
	lastReported     map[string]string // Health restoration is not diagnostic delivery.
	recordDebt       map[string]bool
	progressStatus   map[string]repairScanProgress // Protected by statusMu.
}

// A record debt survives clean pages until a complete clean sweep. A failed
// read/metadata operation is resolved by the next successful page for that
// Group, provided no actual unresolved record debt was observed earlier.
type liveRepairRecordDebt struct{ error }

func (debt *liveRepairRecordDebt) Unwrap() error { return debt.error }

type liveRepairCursor struct {
	owner                                        *enginestate.State
	cohort                                       rootstate.RegionCohort
	cursor                                       []byte
	unresolved                                   uint64
	firstFailure                                 string
	needsCleanSweep                              bool
	scanned                                      uint64
	startedUnix, lastProgressUnix, lastCleanUnix int64
}

// repairReadBarrier routes Root confirmation through the Node-level fallback
// path. Joined Nodes are Root learners during enrollment and cannot complete
// a local Root ReadIndex, while their regional Group is already able to serve
// local reads. A single Host barrier therefore deadlocks every live-repair
// pass at the Root cut.
type repairReadBarrier struct {
	runtime *node.Runtime
	client  repairGroupReadClient
	phase   func(string)
}

type repairGroupReadClient interface {
	GroupCandidates(groupID string) []uint64
	ReadIndex(ctx context.Context, target uint64, groupID string) (uint64, error)
}

type repairLocalGroupReader interface {
	ReadIndex(ctx context.Context, groupID string) (uint64, error)
	Status(groupID string) (raft.GroupStatus, error)
}

const repairGroupReadAttemptTimeout = 8 * time.Second
const repairLocalGroupReadAttemptTimeout = 2 * time.Second

func (barrier repairReadBarrier) ReadIndex(ctx context.Context, groupID string) (index uint64, resultErr error) {
	started := time.Now()
	defer traceRepairBarrier(ctx, groupID, started)
	if barrier.phase != nil {
		phase := "waiting_owner"
		if groupID == raft.RootGroupID {
			phase = "waiting_root"
		}
		barrier.phase(phase)
		defer func() {
			if resultErr == nil {
				barrier.phase("repairing")
			}
		}()
	}
	if groupID == raft.RootGroupID {
		index, err := barrier.runtime.ReadRootIndex(ctx)
		if err != nil {
			return 0, err
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for barrier.runtime.Root.AppliedIndex() < index {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-ticker.C:
			}
		}
		return index, nil
	}
	return readRepairOwnerIndex(ctx, barrier.runtime.Host, barrier.client,
		barrier.runtime.Identity.RaftID, groupID)
}

// A local Owner member may have a stale or temporarily empty leader hint while
// another confirmed member can still provide the Group ReadIndex. Keep local
// reads as the fast path, then try the current leader hint and remaining
// confirmed Group members. Callers still verify that their local state has
// applied the returned cut before using it.
func readRepairOwnerIndex(ctx context.Context, local repairLocalGroupReader, client repairGroupReadClient,
	localID uint64, groupID string,
) (uint64, error) {
	// A local member can have a stale leader view that keeps ReadIndex waiting
	// for its own quorum until the whole repair page expires. Bound that fast
	// path separately so a healthy confirmed peer can still provide the cut.
	localCtx, cancelLocal := context.WithTimeout(ctx, repairLocalGroupReadAttemptTimeout)
	index, localErr := local.ReadIndex(localCtx, groupID)
	cancelLocal()
	if localErr == nil && index != 0 {
		return index, nil
	}
	if localErr == nil {
		localErr = errors.New("local Owner Group returned an empty read cut")
	}
	if err := ctx.Err(); err != nil {
		return 0, errors.Join(localErr, err)
	}
	if client == nil {
		return 0, localErr
	}
	status, statusErr := local.Status(groupID)
	candidates := client.GroupCandidates(groupID)
	if len(candidates) == 0 {
		return 0, errors.Join(localErr, statusErr, errors.New("Owner Group has no confirmed read candidates"))
	}
	ordered := make([]uint64, 0, len(candidates))
	if statusErr == nil && status.LeaderID != 0 && status.LeaderID != localID {
		for _, candidate := range candidates {
			if candidate == status.LeaderID {
				ordered = append(ordered, candidate)
				break
			}
		}
	}
	for _, candidate := range candidates {
		if candidate == localID {
			continue
		}
		alreadyAdded := false
		for _, existing := range ordered {
			if existing == candidate {
				alreadyAdded = true
				break
			}
		}
		if !alreadyAdded {
			ordered = append(ordered, candidate)
		}
	}
	failures := []error{localErr}
	for _, candidate := range ordered {
		if err := ctx.Err(); err != nil {
			return 0, errors.Join(append(failures, err)...)
		}
		attempt, cancel := context.WithTimeout(ctx, repairGroupReadAttemptTimeout)
		remoteIndex, err := client.ReadIndex(attempt, candidate, groupID)
		cancel()
		if err == nil && remoteIndex != 0 {
			return remoteIndex, nil
		}
		if err == nil {
			err = errors.New("Owner Group member returned an empty read cut")
		}
		failures = append(failures, fmt.Errorf("Owner Group member %d: %w", candidate, err))
	}
	return 0, errors.Join(failures...)
}

func (cursor *liveRepairCursor) addFailures(failures []error) {
	cursor.unresolved += uint64(len(failures))
	if len(failures) != 0 {
		cursor.needsCleanSweep = true
	}
	if cursor.firstFailure == "" && len(failures) != 0 {
		message := []rune(failures[0].Error())
		cursor.firstFailure = string(message[:min(len(message), 1024)])
	}
}

func (cursor liveRepairCursor) debtError(groupID string) error {
	if cursor.unresolved == 0 {
		if cursor.needsCleanSweep {
			return &liveRepairRecordDebt{fmt.Errorf("Group %s has known record debt awaiting a full clean sweep (first: %s)", groupID, cursor.firstFailure)}
		}
		return nil
	}
	return &liveRepairRecordDebt{fmt.Errorf("Group %s has %d unresolved records in this sweep (first: %s)",
		groupID, cursor.unresolved, cursor.firstFailure)}
}

// Root and Owner quorum barriers are paid per page, even for already healthy
// records. Eight rows made a WAN sweep spend minutes on barriers before a
// returning Node reached newly written references. Retain only bounded
// descriptor metadata for 64 rows; payloads are still verified/copied one at a
// time under the shared IO budget and the existing page deadline.
const liveRepairPageLimit = 64
const liveRepairInterval = 5 * time.Second
const liveRepairContinuationDelay = 100 * time.Millisecond

// A repair page may cross Regions, read a current Root/Group cut, stream a
// Pack record and re-confirm its physical holding. Thirty seconds was enough
// for local tests but too short for a cold WAN path during Raft catch-up; the
// bounded two-minute budget still prevents a stuck source from monopolizing
// the worker while avoiding a permanent false repair debt.
const liveRepairTimeout = 2 * time.Minute

func (worker *liveRecordRepair) run(ctx context.Context) {
	timer := time.NewTimer(liveRepairInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			stepCtx, cancel := context.WithTimeout(ctx, liveRepairTimeout)
			// Local availability/confirmation repair must keep progressing while
			// a WAN Geo round owns the exclusive maintenance lane. It is already
			// one bounded worker, uses the shared transfer admission and charges
			// the SAME IO bucket. Remote authenticated repair uses this same
			// cooperative shaping path; no extra job, buffer or transfer slot is
			// created here. Geo/backup/migration remain mutually exclusive.
			maintenanceCtx := resource.WithMaintenance(stepCtx, worker.io)
			err := worker.step(maintenanceCtx)
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
	unfinished := false
	for _, cursor := range worker.groups {
		unfinished = unfinished || len(cursor.cursor) != 0 || cursor.lastProgressUnix == 0
	}
	if worker.lastPageAdvanced || (worker.lastDone && unfinished) {
		return liveRepairContinuationDelay
	}
	return liveRepairInterval
}

func (worker *liveRecordRepair) observeStep(ctx context.Context, err error) {
	worker.statusMu.Lock()
	worker.lastCheck = time.Now()
	if err == nil {
		delete(worker.lastErr, "") // Global discovery succeeded this round.
		delete(worker.lastReported, "")
		// Do not retain a recovered Root/Owner barrier error for an entire
		// large sweep, or erase known record debt after only one clean page.
		if worker.lastDone || !worker.recordDebt[worker.lastGroup] {
			delete(worker.lastErr, worker.lastGroup)
			delete(worker.lastReported, worker.lastGroup)
			delete(worker.recordDebt, worker.lastGroup)
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
	var recordDebt *liveRepairRecordDebt
	if errors.As(err, &recordDebt) {
		if worker.recordDebt == nil {
			worker.recordDebt = make(map[string]bool)
		}
		worker.recordDebt[worker.lastGroup] = true
	}
	worker.lastErr[worker.lastGroup] = err.Error()
	if err.Error() == worker.lastReported[worker.lastGroup] {
		worker.statusMu.Unlock()
		return
	}
	if worker.lastReported == nil {
		worker.lastReported = make(map[string]string)
	}
	worker.lastReported[worker.lastGroup] = err.Error()
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
	Progress  []repairScanProgress
}

type repairScanProgress struct {
	GroupID                                      string
	Phase                                        string
	Scanned, Unresolved                          uint64
	NeedsCleanSweep                              bool
	StartedUnix, LastProgressUnix, LastCleanUnix int64
}

func (worker *liveRecordRepair) publishProgress(groupID, phase string, cursor liveRepairCursor) {
	worker.statusMu.Lock()
	defer worker.statusMu.Unlock()
	if worker.progressStatus == nil {
		worker.progressStatus = make(map[string]repairScanProgress)
	}
	worker.progressStatus[groupID] = repairScanProgress{GroupID: groupID, Phase: phase,
		Scanned: cursor.scanned, Unresolved: cursor.unresolved, NeedsCleanSweep: cursor.needsCleanSweep,
		StartedUnix: cursor.startedUnix, LastProgressUnix: cursor.lastProgressUnix, LastCleanUnix: cursor.lastCleanUnix}
	// Restore durable debt before any potentially slow Root/Owner request.
	if debt := cursor.debtError(groupID); debt != nil {
		if worker.recordDebt == nil {
			worker.recordDebt = make(map[string]bool)
		}
		if worker.lastErr == nil {
			worker.lastErr = make(map[string]string)
		}
		worker.recordDebt[groupID] = true
		if worker.lastErr[groupID] == "" {
			worker.lastErr[groupID] = debt.Error()
		}
	}
}

func (worker *liveRecordRepair) setPhase(groupID, phase string) {
	worker.statusMu.Lock()
	defer worker.statusMu.Unlock()
	progress := worker.progressStatus[groupID]
	progress.Phase = phase
	if progress.GroupID != "" {
		worker.progressStatus[groupID] = progress
	}
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
	for _, progress := range worker.progressStatus {
		result.Progress = append(result.Progress, progress)
	}
	worker.statusMu.RUnlock()
	slices.SortFunc(result.Issues, func(a, b repairIssue) int { return strings.Compare(a.GroupID, b.GroupID) })
	slices.SortFunc(result.Progress, func(a, b repairScanProgress) int { return strings.Compare(a.GroupID, b.GroupID) })
	return result
}

func (worker *liveRecordRepair) step(ctx context.Context) error {
	worker.lastGroup = ""
	worker.lastDone = false
	worker.lastPageAdvanced = false
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, finishTiming := withRepairTiming(ctx, "local")
	defer finishTiming()
	descriptors, err := worker.runtime.Root.Groups()
	if err != nil {
		return err
	}
	groupIDs := make([]string, 0, len(descriptors))
	regionViews := make(map[string]rootstate.RegionProgress, len(descriptors))
	for _, descriptor := range descriptors {
		owner, found := worker.runtime.Engines.Resolve(descriptor.GroupID)
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
			delete(worker.lastReported, id)
			delete(worker.recordDebt, id)
		}
	}
	for id := range worker.progressStatus {
		if !active[id] {
			delete(worker.progressStatus, id)
		}
	}
	worker.statusMu.Unlock()
	if len(groupIDs) == 0 {
		return nil
	}
	slices.Sort(groupIDs)
	// Load every locally eligible Group before publishing the first successful
	// observation. Otherwise round-robin selection could hide another Group's
	// durable debt until its turn, especially while this Group waits on a cut.
	if worker.groups == nil {
		worker.groups = make(map[string]liveRepairCursor)
	}
	for _, id := range groupIDs {
		owner, _ := worker.runtime.Engines.Resolve(id)
		previous := worker.groups[id]
		current := regionViews[id].Current
		if previous.owner == owner && previous.cohort.Generation == current.Generation &&
			previous.cohort.GroupEpoch == current.GroupEpoch && previous.cohort.GroupFenceIndex == current.GroupFenceIndex &&
			slices.Equal(previous.cohort.Nodes, current.Nodes) {
			continue
		}
		cursor, err := worker.loadCursor(id, regionViews[id])
		if err != nil {
			return err
		}
		cursor.owner = owner
		worker.groups[id] = cursor
		worker.publishProgress(id, "pending", cursor)
	}
	groupID := groupIDs[worker.next%len(groupIDs)]
	worker.next = (worker.next + 1) % len(groupIDs)
	worker.lastGroup = groupID
	owner, found := worker.runtime.Engines.Resolve(groupID)
	if !found || owner == nil {
		delete(worker.groups, groupID)
		return nil
	}
	previous := worker.groups[groupID]
	worker.publishProgress(groupID, "repairing", previous)
	activePhase := "repairing"
	readBarrier := repairReadBarrier{runtime: worker.runtime, client: worker.client,
		phase: func(phase string) {
			if phase == "repairing" {
				phase = activePhase
			}
			worker.setPhase(groupID, phase)
		}}
	pageCut, err := beginRepairPage(ctx, readBarrier, worker.runtime.Root, groupID)
	if err != nil {
		delete(worker.groups, groupID)
		return fmt.Errorf("Group %s begin repair page: %w", groupID, err)
	}
	cohort := regionViews[groupID].Current
	if pageCut.plan.Generation != cohort.Generation || pageCut.plan.GroupEpoch != cohort.GroupEpoch || pageCut.plan.FenceIndex != cohort.GroupFenceIndex {
		delete(worker.groups, groupID)
		return fmt.Errorf("Group %s repair cursor cohort changed before admission", groupID)
	}
	page, err := recordrepair.RepairCurrentLocalPageWithRetry(ctx, pageCut, worker.runtime.Root,
		owner, groupID, worker.runtime.Identity.RaftID, worker.runtime.Identity.NodeID,
		worker.runtime.Records, worker.client, previous.cursor, liveRepairPageLimit)
	if err != nil {
		delete(worker.groups, groupID)
		return fmt.Errorf("Group %s: %w", groupID, err)
	}
	// Checkpoint newly discovered debt before metadata/confirmation operations
	// which can fail or be interrupted. Retry from the old cursor on failure.
	previous.addFailures(page.Failures)
	if len(page.Failures) != 0 {
		if err := worker.saveCursor(regionViews[groupID], groupID, previous, false, previous.cursor); err != nil {
			delete(worker.groups, groupID)
			return err
		}
		worker.publishProgress(groupID, "repairing", previous)
	}
	if err := worker.recordGeoRepairHints(page); err != nil {
		delete(worker.groups, groupID)
		return fmt.Errorf("Group %s save Geo repair task: %w", groupID, err)
	}
	status, err := worker.runtime.Host.Status(groupID)
	if err != nil {
		delete(worker.groups, groupID)
		return fmt.Errorf("Group %s leader status: %w", groupID, err)
	}
	// The local foreground repair path is only for Regions that can publish a
	// local physical-majority confirmation. One- and two-Node Regions use the
	// cross-Region/Geo repair path; treating them as local-repair failures would
	// create a false background error even when their configured policy is sound.
	if status.LeaderID == worker.runtime.Identity.RaftID && len(regionViews[groupID].Current.Nodes) >= 3 {
		activePhase = "confirming"
		worker.setPhase(groupID, "confirming")
		writer := recordwrite.Writer{Barrier: pageCut, Root: worker.runtime.Root,
			Owner: owner, LocalID: worker.runtime.Identity.RaftID,
			LocalNodeID: worker.runtime.Identity.NodeID, Store: repairTimingVerifier{worker.runtime.Records},
			Observer: repairTimingObserver{worker.client}, Submit: func(call context.Context, groupID string, command []byte) error {
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
			}}
		started := time.Now()
		failures, err := writer.ReconcileLocalConfirmationPage(ctx, page.References)
		recordwrite.Trace(ctx, "repair.local_page", started)
		if err != nil {
			delete(worker.groups, groupID)
			return fmt.Errorf("Group %s reconcile local confirmation page: %w", groupID, err)
		}
		if len(failures) != 0 {
			previous.addFailures(failures)
			if err := worker.saveCursor(regionViews[groupID], groupID, previous, false, previous.cursor); err != nil {
				delete(worker.groups, groupID)
				return err
			}
			worker.publishProgress(groupID, "confirming", previous)
		}
	}
	if err := pageCut.finish(ctx); err != nil {
		delete(worker.groups, groupID)
		return fmt.Errorf("Group %s finish repair page: %w", groupID, err)
	}
	return worker.advanceCursor(regionViews[groupID], groupID, owner, previous, page)
}

// advanceCursor commits a fully processed page before publishing completion.
func (worker *liveRecordRepair) advanceCursor(view rootstate.RegionProgress, groupID string, owner *enginestate.State, previous liveRepairCursor, page recordrepair.LivePageReport) error {
	previous.scanned += page.AlreadyHeld + page.Fetched + page.Unrepaired + page.Offloaded
	previous.lastProgressUnix = time.Now().Unix()
	if page.Page.Done {
		previous.needsCleanSweep = previous.unresolved != 0
		if !previous.needsCleanSweep {
			previous.firstFailure = ""
			previous.lastCleanUnix = previous.lastProgressUnix
		}
		if err := worker.saveCursor(view, groupID, previous, true, nil); err != nil {
			delete(worker.groups, groupID)
			return err
		}
		worker.publishProgress(groupID, "idle", previous)
		delete(worker.groups, groupID)
		worker.lastDone = true
	} else if len(page.Page.Cursor) == 0 || bytes.Equal(page.Page.Cursor, previous.cursor) {
		delete(worker.groups, groupID)
		return fmt.Errorf("Group %s repair cursor did not advance", groupID)
	} else {
		previous.owner, previous.cursor = owner, page.Page.Cursor
		if err := worker.saveCursor(view, groupID, previous, false, page.Page.Cursor); err != nil {
			delete(worker.groups, groupID)
			return err
		}
		worker.publishProgress(groupID, "pending", previous)
		worker.groups[groupID] = previous
		worker.lastPageAdvanced = true
	}
	return previous.debtError(groupID)
}

func (worker *liveRecordRepair) loadCursor(groupID string, view rootstate.RegionProgress) (liveRepairCursor, error) {
	cursor := liveRepairCursor{cohort: view.Current, startedUnix: time.Now().Unix()}
	if worker.progress == nil {
		return cursor, nil
	}
	saved, found, err := worker.progress.Load(groupID)
	if err != nil {
		return liveRepairCursor{}, fmt.Errorf("Group %s repair progress: %w", groupID, err)
	}
	if !found {
		return cursor, nil
	}
	// Old ledgers did not have a latch; their unresolved count is still debt.
	cursor.needsCleanSweep = saved.NeedsCleanSweep || saved.Unresolved != 0
	if cursor.needsCleanSweep {
		cursor.firstFailure = saved.FirstError
	}
	if saved.Matches(view) {
		cursor.lastCleanUnix, cursor.lastProgressUnix = saved.LastCleanUnix, saved.LastProgressUnix
		if !saved.Done {
			cursor.cursor, cursor.unresolved, cursor.scanned = saved.Cursor, saved.Unresolved, saved.Scanned
			if saved.StartedUnix != 0 {
				cursor.startedUnix = saved.StartedUnix
			}
		}
	}
	// A changed cohort rewinds the scan, never erases known debt.
	return cursor, nil
}

func (worker *liveRecordRepair) saveCursor(view rootstate.RegionProgress, groupID string, cursor liveRepairCursor, done bool, next []byte) error {
	if worker.progress == nil {
		return nil
	}
	if err := worker.progress.Save(repairProgressForPage(view, groupID, cursor, done, next)); err != nil {
		return fmt.Errorf("Group %s save repair progress: %w", groupID, err)
	}
	return nil
}

// recordGeoRepairHints hands local failures to the shared Geo retry ledger.
// A locally healthy reference does not prove cross-Region coverage. Only the
// Geo worker may retire a task after observing the full placement majority.
func (worker *liveRecordRepair) recordGeoRepairHints(page recordrepair.LivePageReport) error {
	if worker.geoRecordRepair == nil {
		return nil
	}
	for _, ref := range page.Unresolved {
		if err := worker.geoRecordRepair.Save(ref); err != nil {
			return err
		}
	}
	return nil
}

func repairProgressForPage(view rootstate.RegionProgress, groupID string, cursor liveRepairCursor, done bool, next []byte) recordrepair.Progress {
	return recordrepair.Progress{GroupID: groupID, RegionID: view.Current.RegionID, Generation: view.Current.Generation,
		GroupEpoch: view.Current.GroupEpoch, FenceIndex: view.Current.GroupFenceIndex, Nodes: slices.Clone(view.Current.Nodes),
		Cursor: slices.Clone(next), Unresolved: cursor.unresolved, FirstError: cursor.firstFailure, Done: done,
		NeedsCleanSweep: cursor.needsCleanSweep, Scanned: cursor.scanned, StartedUnix: cursor.startedUnix,
		LastProgressUnix: cursor.lastProgressUnix, LastCleanUnix: cursor.lastCleanUnix}
}
