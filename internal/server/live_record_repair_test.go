package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	worker.observeStep(ctx, errors.New("page has unresolved records"))
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
		}
	}()
	for i := 0; i < 1000; i++ {
		observation := worker.snapshot()
		if len(observation.Issues) > 1 {
			t.Fatal("one Group produced multiple repair status issues", observation)
		}
	}
	done.Wait()
	if len(worker.snapshot().Issues) != 1 {
		t.Fatal("last repair issue was lost")
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
	if got := worker.nextDelay(); got != liveRepairInterval {
		t.Fatal("completed scan continued polling at backlog speed", got)
	}
}
