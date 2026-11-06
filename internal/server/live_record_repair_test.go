package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLiveRepairKeepsGeoDebtAfterLocalRecovery(t *testing.T) {
	for _, done := range []bool{false, true} {
		t.Run(fmt.Sprintf("sweep_done_%t", done), func(t *testing.T) {
			root := t.TempDir()
			ledger, err := recordrepair.OpenGeoRecordRepairStore(root)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ledger.Close() })
			makeReference := func(key string) enginestate.RecordReference {
				descriptor, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "record", ModuleID: "items"},
					"region-west", []byte(key), []byte("body"))
				if err != nil {
					t.Fatal(err)
				}
				return enginestate.RecordReference{Version: 2, Descriptor: descriptor, OwnerGroupID: "region-west",
					RangeID: "range-items", RangeEpoch: 3, RoutingKey: []byte(key), RequestID: "write-" + key,
					Index: 7, CommandHash: hex.EncodeToString(make([]byte, 32))}
			}
			// Discovery already queued this locally held record because another
			// Region lacks bytes. The local scanner then recovers a second record.
			crossRegion, localFailure := makeReference("geo-pending"), makeReference("local-missing")
			if err := ledger.Save(crossRegion); err != nil {
				t.Fatal(err)
			}
			worker := &liveRecordRepair{geoRecordRepair: ledger}
			page := recordrepair.LivePageReport{Page: enginestate.LiveReferencePage{Done: done},
				References: []enginestate.RecordReference{crossRegion}, Unresolved: []enginestate.RecordReference{localFailure}}
			if err := worker.recordGeoRepairHints(page); err != nil {
				t.Fatal(err)
			}
			// A later clean page (or complete clean local sweep) cannot retire
			// either task without evidence of an independent Region majority.
			page.References = append(page.References, localFailure)
			page.Unresolved = nil
			if err := worker.recordGeoRepairHints(page); err != nil {
				t.Fatal(err)
			}
			if err := ledger.Close(); err != nil {
				t.Fatal(err)
			}
			ledger, err = recordrepair.OpenGeoRecordRepairStore(root)
			if err != nil {
				t.Fatal(err)
			}
			pending, err := ledger.List()
			if err != nil || len(pending) != 2 {
				t.Fatalf("local recovery erased durable Geo debt: pending=%v, err=%v", pending, err)
			}
		})
	}
}

func TestLiveRepairDebtSurvivesCleanPageUntilCleanSweep(t *testing.T) {
	var reports []error
	worker := &liveRecordRepair{lastGroup: "region-a", report: func(err error) { reports = append(reports, err) }}
	debt := liveRepairCursor{}
	debt.addFailures([]error{errors.New("first missing record"), errors.New("second missing record")})
	debt.addFailures(nil)
	if debt.unresolved != 2 || !strings.Contains(debt.debtError("region-a").Error(), "first missing record") {
		t.Fatal("page debt was not retained for the full sweep", debt)
	}
	debt.addFailures([]error{errors.New("last page still missing")})
	if debt.unresolved != 3 || !strings.Contains(debt.debtError("region-a").Error(), "3 unresolved") {
		t.Fatal("completed sweep did not aggregate all failed pages", debt)
	}
	ctx := context.Background()
	worker.observeStep(ctx, &liveRepairRecordDebt{errors.New("page has unresolved records")})
	worker.observeStep(ctx, nil) // Another page succeeded, but the sweep is incomplete.
	if worker.lastErr["region-a"] == "" || len(reports) != 1 {
		t.Fatal("clean page erased unresolved repair debt", worker.lastErr, reports)
	}
	worker.lastDone = true
	worker.observeStep(ctx, debt.debtError("region-a"))
	if worker.lastErr["region-a"] == "" || len(reports) != 2 {
		t.Fatal("completed sweep hid earlier page debt", worker.lastErr, reports)
	}
	worker.lastDone = false
	worker.observeStep(ctx, nil) // First page of the next sweep is not enough.
	if worker.lastErr["region-a"] == "" {
		t.Fatal("partial clean sweep erased repair debt")
	}
	worker.lastDone = true
	worker.observeStep(ctx, nil)
	if _, found := worker.lastErr["region-a"]; found {
		t.Fatal("fully clean sweep did not clear repair debt")
	}
	worker.lastGroup = ""
	worker.lastDone = false
	worker.observeStep(ctx, errors.New("Root discovery unavailable"))
	worker.observeStep(ctx, nil)
	if _, found := worker.lastErr[""]; found {
		t.Fatal("successful discovery did not clear prior global repair error")
	}
}

func TestLiveRepairRestoredHealthDoesNotSuppressFirstDiagnostic(t *testing.T) {
	var reports []error
	worker := &liveRecordRepair{lastGroup: "region-a", report: func(err error) { reports = append(reports, err) }}
	cursor := liveRepairCursor{}
	cursor.addFailures([]error{errors.New("new missing publication")})
	// Loading or checkpointing the debt must make status degraded immediately,
	// but is not evidence that this process already emitted a diagnostic.
	worker.publishProgress("region-a", "repairing", cursor)
	worker.observeStep(context.Background(), cursor.debtError("region-a"))
	worker.observeStep(context.Background(), cursor.debtError("region-a"))
	if len(reports) != 1 || len(worker.snapshot().Issues) != 1 {
		t.Fatalf("first error was hidden or repeated: reports=%v, status=%+v", reports, worker.snapshot())
	}
	worker.lastDone = true
	worker.observeStep(context.Background(), nil)
	worker.lastDone = false
	worker.publishProgress("region-a", "repairing", cursor)
	worker.observeStep(context.Background(), cursor.debtError("region-a"))
	if len(reports) != 2 {
		t.Fatal("a new incident after recovery reused the old diagnostic suppression", reports)
	}
}

func TestLiveRepairClearsRecoveredOperationButPreservesRecordDebt(t *testing.T) {
	ctx := context.Background()
	worker := &liveRecordRepair{lastGroup: "region-a"}
	worker.observeStep(ctx, errors.New("Root leader temporarily unavailable"))
	if len(worker.snapshot().Issues) != 1 {
		t.Fatal("failed barrier was not visible")
	}
	worker.observeStep(ctx, nil) // Same Group's Root/Owner barriers and page recovered.
	if len(worker.snapshot().Issues) != 0 {
		t.Fatal("recovered barrier required a full record sweep to clear")
	}
	// Previously persisted unresolved records remain debt even if this page
	// has no new record failures, and another operation then fails and recovers.
	restored := liveRepairCursor{unresolved: 2, firstFailure: "missing Pack bytes from prior pages"}
	worker.observeStep(ctx, restored.debtError("region-a"))
	worker.observeStep(ctx, errors.New("Owner barrier interrupted again"))
	worker.observeStep(ctx, nil)
	if len(worker.snapshot().Issues) != 1 || !worker.recordDebt["region-a"] {
		t.Fatal("operation failure/recovery erased known record debt", worker.snapshot())
	}
	worker.lastGroup = "region-b"
	worker.lastDone = true
	worker.observeStep(ctx, nil)
	if len(worker.snapshot().Issues) != 1 {
		t.Fatal("another Group's clean sweep erased record debt")
	}
	worker.lastGroup = "region-a"
	worker.observeStep(ctx, nil)
	if len(worker.snapshot().Issues) != 0 || len(worker.recordDebt) != 0 {
		t.Fatal("fully clean sweep did not retire record debt", worker.snapshot())
	}
}

func TestLocalRepairStatusIsBoundedAndRequiresLocalCertificate(t *testing.T) {
	worker := &liveRecordRepair{}
	worker.observeStep(context.Background(), errors.New("worker observation"))
	snapshot := worker.snapshot()
	if snapshot.LastCheck.IsZero() || len(snapshot.Issues) != 1 || snapshot.Issues[0].Error != "worker observation" {
		t.Fatal("repair observation not available to status", snapshot)
	}
	observation := repairObservation{LastCheck: time.Now()}
	for i := 0; i < 65; i++ {
		observation.Issues = append(observation.Issues, repairIssue{
			GroupID: fmt.Sprintf("group-%03d", i), Error: strings.Repeat("e", 2048)})
	}
	encoded := encodeRepairStatus(observation)
	if !encoded.GetWorkerEnabled() || encoded.GetTotalIssues() != 65 || len(encoded.GetIssues()) != 64 ||
		len([]rune(encoded.GetIssues()[0].GetError())) != 1024 {
		t.Fatal("repair status response is unbounded or hides omitted issues", encoded.GetTotalIssues(), len(encoded.GetIssues()))
	}
	service := &dataService{runtime: &node.Runtime{Identity: identity.Identity{
		ClusterID: "cluster_00000000000000000000000000000001", NodeID: "node_00000000000000000000000000000001", RaftID: 1}}, repair: worker}
	request := &datawire.LocalRepairStatusRequest{ClusterId: service.runtime.Identity.ClusterID, FromRaftId: 1}
	if _, err := service.LocalRepairStatus(context.Background(), request); status.Code(err) != codes.PermissionDenied {
		t.Fatal("unauthenticated repair status request was accepted", err)
	}
	request.FromRaftId = 2
	if _, err := service.LocalRepairStatus(context.Background(), request); status.Code(err) != codes.PermissionDenied {
		t.Fatal("foreign Node requested local repair status", err)
	}
}

func TestLiveRepairStatusConcurrentObservation(t *testing.T) {
	worker := &liveRecordRepair{lastGroup: "region-a"}
	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		for i := 0; i < 1000; i++ {
			worker.observeStep(context.Background(), fmt.Errorf("repair failure %d", i))
			worker.publishProgress("region-a", "pending", liveRepairCursor{scanned: uint64(i)})
			worker.setPhase("region-a", "waiting_owner")
		}
	}()
	for i := 0; i < 1000; i++ {
		observation := worker.snapshot()
		if len(observation.Issues) > 1 {
			t.Fatal("one Group produced multiple repair status issues", observation)
		}
		if len(observation.Progress) > 1 {
			t.Fatal("duplicated concurrent progress", observation.Progress)
		}
	}
	done.Wait()
	if len(worker.snapshot().Issues) != 1 {
		t.Fatal("last repair issue was lost")
	}
}

func TestLocalRepairProgressOutputIsBounded(t *testing.T) {
	observation := repairObservation{}
	for i := 0; i < 65; i++ {
		observation.Progress = append(observation.Progress, repairScanProgress{GroupID: fmt.Sprintf("region-%d", i),
			Phase: "waiting_root", Scanned: 80, NeedsCleanSweep: true, LastProgressUnix: 200})
	}
	response := encodeRepairStatus(observation)
	if !response.GetScanProgressSupported() || response.GetTotalScanGroups() != 65 || len(response.GetScanProgress()) != 64 ||
		response.GetScanProgress()[0].GetScanned() != 80 || !response.GetScanProgress()[0].GetNeedsCleanSweep() ||
		response.GetScanProgress()[0].GetLastProgressUnix() != 200 || response.GetScanProgress()[0].GetLastCleanUnix() != 0 {
		t.Fatal("bounded scan progress lost information", response)
	}
}

func TestLiveRepairContinuesUnfinishedPagesWithoutBusyIdleScan(t *testing.T) {
	worker := &liveRecordRepair{}
	if got := worker.nextDelay(); got != liveRepairInterval {
		t.Fatal("idle repair did not keep the low-frequency interval", got)
	}
	worker.groups = map[string]liveRepairCursor{"region-a": {cursor: []byte("next-page")}}
	if got := worker.nextDelay(); got != liveRepairInterval {
		t.Fatal("stalled repair retried at backlog speed without advancing", got)
	}
	worker.lastPageAdvanced = true
	if got := worker.nextDelay(); got != liveRepairContinuationDelay || got >= liveRepairInterval {
		t.Fatal("unfinished repair scan waited for the idle interval", got)
	}
	worker.lastPageAdvanced, worker.lastDone = false, true
	if got := worker.nextDelay(); got != liveRepairContinuationDelay {
		t.Fatal("other unfinished Group did not continue after a completed page", got)
	}
	delete(worker.groups, "region-a")
	worker.groups["region-completed"] = liveRepairCursor{lastProgressUnix: 100}
	if got := worker.nextDelay(); got != liveRepairInterval {
		t.Fatal("completed scan continued polling at backlog speed", got)
	}
}

func TestLocalRepairProgressesWhileGeoOwnsExclusiveMaintenance(t *testing.T) {
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	groups, err := root.Groups()
	if err != nil || len(groups) != 0 {
		t.Fatal("fixture must have no hosted data groups", groups, err)
	}
	io := resource.NewMaintenance(1024)
	_, finish, err := io.TryStart(context.Background(), "geo-repair")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	worker := &liveRecordRepair{runtime: &node.Runtime{Root: root}, io: io}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { worker.run(ctx); close(done) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("local worker did not join")
		}
	}()
	until := time.Now().Add(8 * time.Second)
	for time.Now().Before(until) {
		if !worker.snapshot().LastCheck.IsZero() {
			if io.Status().Active != "geo-repair" {
				t.Fatal("local worker released another job's admission", io.Status())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("local repair was starved by the active Geo lease")
}
