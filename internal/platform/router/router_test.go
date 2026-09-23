package router

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

func TestRouterResolvesTenantScopedKeyspaceAfterRootReadIndex(t *testing.T) {
	root := t.TempDir()
	id, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	state, err := rootstate.Open(filepath.Join(root, "root-state"))
	if err != nil {
		t.Fatal(err)
	}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: root, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if err := host.AddGroup(raft.RootGroupID, []uint64{id.RaftID}, false, state); err != nil {
		t.Fatal(err)
	}
	if err := host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	commands := []rootstate.Command{
		{Version: rootstate.FormatVersion, RequestID: "group-data", Type: rootstate.RegisterGroup,
			Group: &rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateCapability}},
		{Version: rootstate.FormatVersion, RequestID: "tenant-a", Type: rootstate.RegisterTenant,
			Tenant: &rootstate.Tenant{TenantID: "tenant-a"}},
		{Version: rootstate.FormatVersion, RequestID: "dataset-a", Type: rootstate.CreateDataset,
			Dataset: &rootstate.Dataset{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items",
				RangeID: "range-a", OwnerGroupID: "data-1"}},
		{Version: rootstate.FormatVersion, RequestID: "activate-a", Type: rootstate.ActivateRange,
			Activation: &rootstate.RangeActivation{RangeID: "range-a", OwnerGroupID: "data-1", Epoch: 1,
				OwnerRequestID: "owner-range-a", OwnerAppliedIndex: 1}},
	}
	for _, command := range commands {
		encoded, err := rootstate.EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := host.Submit(raft.RootGroupID, encoded); err != nil {
			t.Fatal(err)
		}
		if result, found, err := state.ResultForCommand(encoded); err != nil || !found || result.Error != "" {
			t.Fatalf("Root command result = %+v, %v, %v", result, found, err)
		}
	}
	router, err := New(host, state)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	keyspace := rootstate.Keyspace{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items"}
	value, err := router.Resolve(ctx, keyspace, []byte("key-1"))
	if err != nil || value.RangeID != "range-a" || value.OwnerGroupID != "data-1" || value.Epoch != 1 {
		t.Fatalf("Root route = %+v, %v", value, err)
	}
	keyspace.TenantID = "tenant-b"
	if _, err := router.Resolve(ctx, keyspace, []byte("key-1")); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("unregistered Tenant route = %v", err)
	}
}

type fakeBarrier struct {
	index uint64
	err   error
}

func (f fakeBarrier) ReadIndex(_ context.Context, groupID string) (uint64, error) {
	if groupID != raft.RootGroupID {
		return 0, errors.New("wrong Group read barrier")
	}
	return f.index, f.err
}

type fakeCatalog struct {
	applied uint64
	value   rootstate.Range
	found   bool
	called  bool
}

func (f *fakeCatalog) AppliedIndex() uint64 { return f.applied }
func (f *fakeCatalog) RouteKeyspace(rootstate.Keyspace, []byte) (rootstate.Range, bool, error) {
	f.called = true
	return f.value, f.found, nil
}

func TestRouterFailsClosedWithoutConfirmedRootView(t *testing.T) {
	keyspace := rootstate.Keyspace{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items"}
	catalog := &fakeCatalog{applied: 10, found: true, value: rootstate.Range{
		RangeID: "range-a", TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items",
		OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}}
	router, err := New(fakeBarrier{err: context.DeadlineExceeded}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Resolve(context.Background(), keyspace, []byte("key")); !errors.Is(err, context.DeadlineExceeded) || catalog.called {
		t.Fatalf("unconfirmed Root route = %v, catalog called = %v", err, catalog.called)
	}
	router, _ = New(fakeBarrier{index: 11}, catalog)
	if _, err := router.Resolve(context.Background(), keyspace, []byte("key")); !errors.Is(err, ErrCatalogBehind) || catalog.called {
		t.Fatalf("behind Root route = %v, catalog called = %v", err, catalog.called)
	}
	router, _ = New(fakeBarrier{index: 10}, catalog)
	catalog.value.Epoch = 0
	if _, err := router.Resolve(context.Background(), keyspace, []byte("key")); !errors.Is(err, ErrDirectoryInvalid) {
		t.Fatalf("invalid Root descriptor route = %v", err)
	}
	catalog.value.Epoch = 1
	catalog.value.TenantID = "tenant-b"
	if _, err := router.Resolve(context.Background(), keyspace, []byte("key")); !errors.Is(err, ErrDirectoryInvalid) {
		t.Fatalf("cross-Tenant Root descriptor route = %v", err)
	}
	catalog.value.TenantID = "tenant-a"
	catalog.value.Start = []byte("m")
	if _, err := router.Resolve(context.Background(), keyspace, []byte("key")); !errors.Is(err, ErrDirectoryInvalid) {
		t.Fatalf("out-of-bounds Root descriptor route = %v", err)
	}
}

func TestContainsHalfOpenRange(t *testing.T) {
	value := rootstate.Range{Start: []byte("b"), End: []byte("m")}
	for _, check := range []struct {
		key  string
		want bool
	}{
		{"a", false}, {"b", true}, {"l", true}, {"m", false}, {"z", false},
	} {
		if got := Contains(value, []byte(check.key)); got != check.want {
			t.Fatalf("Contains(%q) = %v, want %v", check.key, got, check.want)
		}
	}
	if !Contains(rootstate.Range{}, []byte("any")) {
		t.Fatal("initial unbounded Range did not contain key")
	}
}

func TestResolveTargetDetachesRoutingKey(t *testing.T) {
	keyspace := rootstate.Keyspace{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items"}
	catalog := &fakeCatalog{applied: 4, found: true, value: rootstate.Range{
		RangeID: "range-a", TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items",
		OwnerGroupID: "data-1", Epoch: 3, State: rootstate.RangeStateActive}}
	router, err := New(fakeBarrier{index: 4}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	routingKey := []byte("key")
	target, err := router.ResolveTarget(context.Background(), keyspace, routingKey)
	if err != nil {
		t.Fatal(err)
	}
	routingKey[0] = 'x'
	if string(target.RoutingKey) != "key" || target.RangeID != "range-a" || target.Epoch != 3 {
		t.Fatalf("detached Target = %+v", target)
	}
}

func TestCheckOwnerRequiresExactAppliedGeneration(t *testing.T) {
	current := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", CapabilityID: "record",
		DatasetID: "items", Start: []byte("b"), End: []byte("m"), OwnerGroupID: "data-1", Epoch: 3,
		State: rootstate.RangeStateActive}
	target := Target{Keyspace: current.Keyspace(), RangeID: current.RangeID, OwnerGroupID: current.OwnerGroupID,
		Epoch: current.Epoch, RoutingKey: []byte("key")}
	if err := CheckOwner("data-1", current, target); err != nil {
		t.Fatalf("current Target rejected: %v", err)
	}

	stale := target
	stale.Epoch--
	if err := CheckOwner("data-1", current, stale); !errors.Is(err, ErrRouteFenced) {
		t.Fatalf("stale Target error = %v", err)
	}
	wrongOwner := target
	wrongOwner.OwnerGroupID = "data-2"
	if err := CheckOwner("data-1", current, wrongOwner); !errors.Is(err, ErrWrongOwner) {
		t.Fatalf("wrong Owner error = %v", err)
	}
	wrongKeyspace := target
	wrongKeyspace.Keyspace.TenantID = "tenant-b"
	if err := CheckOwner("data-1", current, wrongKeyspace); !errors.Is(err, ErrDirectoryInvalid) {
		t.Fatalf("cross-Tenant Target error = %v", err)
	}
	outOfBounds := target
	outOfBounds.RoutingKey = []byte("z")
	if err := CheckOwner("data-1", current, outOfBounds); !errors.Is(err, ErrDirectoryInvalid) {
		t.Fatalf("out-of-bounds Target error = %v", err)
	}
}
