package rangebootstrap

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
)

type noOpHandler struct{}

func (noOpHandler) Apply(*capstate.Transaction, router.Target, []byte) (capstate.Outcome, error) {
	return capstate.Outcome{}, nil
}

func submitRoot(t *testing.T, host *raft.Host, state *rootstate.State, command rootstate.Command) {
	t.Helper()
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Submit(raft.RootGroupID, encoded); err != nil {
		t.Fatal(err)
	}
	if result, found, err := state.ResultForCommand(encoded); err != nil || !found || result.Error != "" {
		t.Fatalf("Root command = %+v, %v, %v", result, found, err)
	}
}

func TestBootstrapInstallsOwnerFenceBeforePublishingRootRoute(t *testing.T) {
	root := t.TempDir()
	id, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	rootState, err := rootstate.Open(filepath.Join(root, "root-state"))
	if err != nil {
		t.Fatal(err)
	}
	ownerState, err := capstate.Open(filepath.Join(root, "owner-state"), "data-1", noOpHandler{})
	if err != nil {
		t.Fatal(err)
	}
	ownerAdapter, err := routedstate.New(ownerState)
	if err != nil {
		t.Fatal(err)
	}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: root, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if err := host.AddGroup(raft.RootGroupID, []uint64{id.RaftID}, false, rootState); err != nil {
		t.Fatal(err)
	}
	if err := host.AddGroup("data-1", []uint64{id.RaftID}, false, ownerAdapter); err != nil {
		t.Fatal(err)
	}
	if err := host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	if err := host.Campaign("data-1"); err != nil {
		t.Fatal(err)
	}
	submitRoot(t, host, rootState, rootstate.Command{Version: 1, RequestID: "group-data", Type: rootstate.RegisterGroup,
		Group: &rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateCapability}})
	submitRoot(t, host, rootState, rootstate.Command{Version: 1, RequestID: "tenant-a", Type: rootstate.RegisterTenant,
		Tenant: &rootstate.Tenant{TenantID: "tenant-a"}})
	submitRoot(t, host, rootState, rootstate.Command{Version: 1, RequestID: "dataset-a", Type: rootstate.CreateDataset,
		Dataset: &rootstate.Dataset{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items",
			RangeID: "range-a", OwnerGroupID: "data-1"}})
	keyspace := rootstate.Keyspace{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items"}
	if _, found, err := rootState.RouteKeyspace(keyspace, nil); err != nil || found {
		t.Fatalf("pending Root route = found %v, err %v", found, err)
	}
	if _, found, err := ownerState.Range("range-a"); err != nil || found {
		t.Fatalf("Owner Fence before bootstrap = found %v, err %v", found, err)
	}
	localOwner, err := NewLocalOwner(host, ownerState)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapper, err := New(host, rootState, func(groupID string) (OwnerControl, bool) {
		return localOwner, groupID == "data-1"
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := bootstrapper.Reconcile(ctx, "range-a"); err != nil {
		t.Fatal(err)
	}
	if err := bootstrapper.Reconcile(ctx, "range-a"); err != nil {
		t.Fatalf("idempotent bootstrap = %v", err)
	}
	ownerRange, found, err := ownerState.Range("range-a")
	if err != nil || !found || ownerRange.State != rootstate.RangeStateActive {
		t.Fatalf("Owner Fence = %+v, %v, %v", ownerRange, found, err)
	}
	rootRange, found, err := rootState.RouteKeyspace(keyspace, []byte("key"))
	if err != nil || !found || !sameRange(rootRange, ownerRange) {
		t.Fatalf("published Root route = %+v, %v, %v", rootRange, found, err)
	}

	// Simulate a coordinator stopping after the Owner commit but before the
	// Root activation. Reconcile must discover the durable Fence and continue.
	submitRoot(t, host, rootState, rootstate.Command{Version: 1, RequestID: "dataset-b", Type: rootstate.CreateDataset,
		Dataset: &rootstate.Dataset{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "more",
			RangeID: "range-b", OwnerGroupID: "data-1"}})
	pending, found, err := rootState.Range("range-b")
	if err != nil || !found || pending.State != rootstate.RangeStatePending {
		t.Fatalf("second pending Range = %+v, %v, %v", pending, found, err)
	}
	active := pending
	active.State = rootstate.RangeStateActive
	ownerCommand, err := routedstate.EncodeActivateRange(OwnerRequestID(active), active)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Submit("data-1", ownerCommand); err != nil {
		t.Fatal(err)
	}
	if _, err := host.ReadIndex(ctx, "data-1"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := rootState.RouteKeyspace(pending.Keyspace(), nil); err != nil || found {
		t.Fatalf("Owner-ready but unpublished Root route = found %v, err %v", found, err)
	}
	if err := bootstrapper.Reconcile(ctx, "range-b"); err != nil {
		t.Fatalf("resumed bootstrap = %v", err)
	}
	if _, found, err := rootState.RouteKeyspace(pending.Keyspace(), nil); err != nil || !found {
		t.Fatalf("resumed Root route = found %v, err %v", found, err)
	}
	if pending, err := rootState.PendingRanges(); err != nil || len(pending) != 0 {
		t.Fatalf("pending Ranges after reconciliation = %+v, %v", pending, err)
	}
}

func TestBootstrapRejectsConflictingOwnerFence(t *testing.T) {
	// The exact mismatch behavior is covered without a second Raft fixture.
	left := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", CapabilityID: "record",
		DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	right := left
	right.Epoch = 2
	if sameRange(left, right) {
		t.Fatal("different Owner Fence generations compared equal")
	}
}
