package enginedata

import (
	"context"
	"errors"
	"fmt"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/router"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRerouteDiscardsAdmissionCutAndPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = router.WithConfirmedRootIndex(ctx, 99)
	calls := 0
	const requestID = "same-request-through-reroute"
	result, err := retryStaleRoute(ctx, func(call context.Context) (string, error) {
		calls++
		_, cached := router.ConfirmedRootIndex(call)
		if calls == 1 {
			if !cached {
				t.Fatal("first attempt lost its valid admission cut")
			}
			return requestID, router.ErrStaleTarget
		}
		if cached {
			t.Fatal("reroute reused an obsolete Root cut")
		}
		if _, deadline := call.Deadline(); !deadline {
			t.Fatal("reroute discarded cancellation/deadline")
		}
		return requestID, nil
	}, func(_ string, err error) bool { return safeWriteReroute(err) })
	if err != nil || result != requestID || calls != 2 {
		t.Fatal(result, calls, err)
	}
}

func TestRerouteNeverMasksAmbiguousWritesOrAuthorizationFailures(t *testing.T) {
	for _, fault := range []error{context.DeadlineExceeded, status.Error(codes.PermissionDenied, "removed Node"),
		errors.Join(router.ErrStaleTarget, context.DeadlineExceeded), fmt.Errorf("attempts: %w", errors.Join(status.Error(codes.Aborted, "preflight"), errors.New("uncertain Submit acknowledgement")))} {
		calls := 0
		_, err := retryStaleRoute(context.Background(), func(context.Context) (WriteResult, error) { calls++; return WriteResult{State: WriteUnknown}, fault },
			func(result WriteResult, err error) bool { return result.State == WriteUnknown && safeWriteReroute(err) })
		if err == nil || calls != 1 {
			t.Fatal("uncertain or unauthorized write was rerouted", fault, calls, err)
		}
	}
	if safeReadReroute(errors.Join(router.ErrStaleTarget, status.Error(codes.PermissionDenied, "forbidden"))) {
		t.Fatal("read reroute masked authorization failure")
	}
	if !safeReadReroute(errors.Join(router.ErrRouteFenced, status.Error(codes.Unavailable, "old candidate"))) {
		t.Fatal("read did not refresh its obsolete route")
	}
}

func TestRerouteHasFiniteAttemptsAndStopsOnCancellation(t *testing.T) {
	calls := 0
	_, err := retryStaleRoute(context.Background(), func(context.Context) (int, error) { calls++; return 0, router.ErrStaleTarget }, func(_ int, err error) bool { return safeWriteReroute(err) })
	if err == nil || calls != maxRerouteAttempts {
		t.Fatal(calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls = 0
	_, err = retryStaleRoute(ctx, func(context.Context) (int, error) { calls++; cancel(); return 0, router.ErrStaleTarget }, func(_ int, err error) bool { return safeWriteReroute(err) })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal("cancellation did not stop rerouting", calls, err)
	}
}

func TestExecutionPreflightRejectsObsoleteTargetsFromActualRootCatalog(t *testing.T) {
	root, err := rootstate.Open(filepath.Join(t.TempDir(), "root"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	commands := []rootstate.Command{
		{Type: rootstate.RegisterGroup, Group: &rootstate.Group{GroupID: "owner", StateKind: rootstate.GroupStateEngine}},
		{Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: "tenant"}},
		{Type: rootstate.CreateDataset, Dataset: &rootstate.Dataset{TenantID: "tenant", EngineID: "record", DatasetID: "data", RangeID: "range-a", OwnerGroupID: "owner"}},
		{Type: rootstate.ActivateRange, Activation: &rootstate.RangeActivation{RangeID: "range-a", OwnerGroupID: "owner", Epoch: 1, OwnerRequestID: "activate-owner", OwnerAppliedIndex: 1}},
	}
	for i, command := range commands {
		command.Version = rootstate.FormatVersion
		command.RequestID = fmt.Sprintf("setup-%d", i)
		data, err := rootstate.EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := root.Apply(uint64(i+1), data); err != nil {
			t.Fatal(err)
		}
		result, found, err := root.ResultForCommand(data)
		if err != nil || !found || result.Error != "" {
			t.Fatal(result, found, err)
		}
	}
	// No Host/client is present: successful preflight must not add a WAN barrier.
	service := &Service{runtime: &node.Runtime{Root: root}}
	target := router.Target{Keyspace: rootstate.Keyspace{TenantID: "tenant", EngineID: "record", DatasetID: "data"}, RangeID: "range-a", OwnerGroupID: "owner", Epoch: 1, RoutingKey: []byte("key")}
	if err := service.checkExecutionRoute(target); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"epoch", "range", "owner"} {
		t.Run(mode, func(t *testing.T) {
			stale := target
			switch mode {
			case "epoch":
				stale.Epoch++
			case "range":
				stale.RangeID = "range-old"
			case "owner":
				stale.OwnerGroupID = "owner-old"
			}
			if err := service.checkExecutionRoute(stale); !errors.Is(err, router.ErrStaleTarget) || !safeWriteReroute(err) {
				t.Fatal("obsolete route reached mutation execution", err)
			}
		})
	}
}
