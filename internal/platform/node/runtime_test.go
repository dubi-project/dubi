package node

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/engineruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"google.golang.org/grpc"
)

type nodeTestHandler struct{}

func (*nodeTestHandler) Apply(*enginestate.Transaction, router.Target, []byte) (enginestate.Outcome, error) {
	return enginestate.Outcome{}, nil
}

type nodePackHandler struct{}

func (*nodePackHandler) Apply(tx *enginestate.Transaction, _ router.Target, payload []byte) (enginestate.Outcome, error) {
	return enginestate.Outcome{Value: []byte("stored")}, tx.Set([]byte("item"), payload)
}

func TestRuntimeRaftApplyUsesPacksAndRecoversAfterSnapshot(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	registry := engineruntime.NewRegistry()
	if err := registry.Register("record", &nodePackHandler{}); err != nil {
		t.Fatal(err)
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := Open(Options{StoreRoot: store, TickInterval: time.Hour, RootBootstrapPeers: []uint64{id.RaftID}, EngineMux: mux})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	lease, err := runtime.Volumes.Reserve(8<<20, nil)
	if errors.Is(err, volume.ErrInsufficientSpace) {
		t.Skip("requires disk headroom")
	}
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if err := runtime.Host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	applyRoot(t, runtime, rootstate.Command{Version: 1, RequestID: "node", Type: rootstate.RegisterNode,
		Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "region-a", Address: "node-a:7443"}})
	applyRoot(t, runtime, rootstate.Command{Version: 1, RequestID: "group", Type: rootstate.RegisterGroup,
		Group: &rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateEngine}})
	state, err := runtime.Groups.NewState(context.Background(), "data-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Host.AddGroup("data-1", []uint64{id.RaftID}, false, state); err != nil {
		t.Fatal(err)
	}
	applyRoot(t, runtime, rootstate.Command{Version: 1, RequestID: "members", Type: rootstate.PublishGroupMembers,
		Membership: &rootstate.GroupMembership{GroupID: "data-1", ExpectedEpoch: 1,
			Members: []rootstate.GroupMember{{RaftID: id.RaftID, Role: rootstate.GroupMemberVoter}}}})
	if err := runtime.Host.Campaign("data-1"); err != nil {
		t.Fatal(err)
	}
	descriptor := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", EngineID: "record", DatasetID: "items",
		OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	activation, err := routedstate.EncodeActivateRange("activate", descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Host.Submit("data-1", activation); err != nil {
		t.Fatal(err)
	}
	target := router.Target{Keyspace: descriptor.Keyspace(), RangeID: descriptor.RangeID, OwnerGroupID: "data-1", Epoch: 1, RoutingKey: []byte("item")}
	large := bytes.Repeat([]byte("data"), enginestate.PackValueThreshold/4)
	mutation, err := routedstate.EncodeMutation("write", target, large)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Host.Submit("data-1", mutation); err != nil {
		t.Fatal(err)
	}
	local, found := runtime.Engines.Resolve("data-1")
	if !found {
		t.Fatal("missing state")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := local.ReadRouted(ctx, runtime.Host, target, func(view enginestate.ReadView) error {
		value, found, err := view.Get([]byte("item"))
		if err != nil || !found || !bytes.Equal(value, large) {
			return errors.New("routed Pack value mismatch")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Host.CreateSnapshot("data-1"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(Options{StoreRoot: store, TickInterval: time.Hour, EngineMux: mux})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.Host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	if err := restarted.RecoverLocalGroups(ctx); err != nil {
		t.Fatal(err)
	}
	local, found = restarted.Engines.Resolve("data-1")
	if !found {
		t.Fatal("Group not recovered")
	}
	value, found, err := local.Value(target.Keyspace, []byte("item"))
	if err != nil || !found || !bytes.Equal(value, large) {
		t.Fatal("lost Pack-backed state", err)
	}
	if err := restarted.Host.Campaign("data-1"); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Host.Submit("data-1", mutation); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	// Inspect actual physical records after releasing Runtime ownership: replay
	// and restart must not append a second body for the already-published result.
	volumes, err := volume.Open(store)
	if err != nil {
		t.Fatal(err)
	}
	defer volumes.Close()
	report, err := volumes.Status()
	if err != nil {
		t.Fatal(err)
	}
	collection, err := pack.OpenCollection(volumes, report.Volumes[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	count := 0
	if err := collection.Scan(func(_ pack.Location, record pack.Record) error {
		count++
		if !bytes.Equal(record.Payload, large) {
			return errors.New("wrong physical Pack body")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("published request appended twice", count)
	}
}

func nodeTestMux(t *testing.T) *engineruntime.Mux {
	t.Helper()
	registry := engineruntime.NewRegistry()
	if err := registry.Register("record", &nodeTestHandler{}); err != nil {
		t.Fatal(err)
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	return mux
}

func TestRuntimeAssemblesRPCAndRecoversPublishedGroups(t *testing.T) {
	storeRoot := t.TempDir()
	id, err := identity.Bootstrap(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	mux := nodeTestMux(t)
	runtime, err := Open(Options{StoreRoot: storeRoot, TickInterval: time.Hour,
		RootBootstrapPeers: []uint64{id.RaftID}, EngineMux: mux})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Identity != id || runtime.Host == nil || runtime.Root == nil || runtime.Groups == nil ||
		runtime.Engines == nil || runtime.JoinManager == nil || runtime.LocalMembership == nil || runtime.Packs == nil || runtime.Records == nil {
		t.Fatal("Node runtime did not assemble all core components")
	}
	if err := runtime.Host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	applyRoot(t, runtime, rootstate.Command{Version: 1, RequestID: "register-node", Type: rootstate.RegisterNode,
		Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "region-a", Address: "node-a:7443"}})
	applyRoot(t, runtime, rootstate.Command{Version: 1, RequestID: "register-data", Type: rootstate.RegisterGroup,
		Group: &rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateEngine}})
	state, err := runtime.Groups.NewState(context.Background(), "data-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Host.AddGroup("data-1", []uint64{id.RaftID}, false, state); err != nil {
		t.Fatal(err)
	}
	applyRoot(t, runtime, rootstate.Command{Version: 1, RequestID: "publish-data", Type: rootstate.PublishGroupMembers,
		Membership: &rootstate.GroupMembership{GroupID: "data-1", ExpectedEpoch: 1,
			Members: []rootstate.GroupMember{{RaftID: id.RaftID, Role: rootstate.GroupMemberVoter}}}})
	if _, found := runtime.Engines.Resolve("data-1"); !found {
		t.Fatal("open engine Group is absent from runtime directory")
	}
	grpcServer := grpc.NewServer()
	if err := runtime.RegisterInternalRPC(grpcServer, func(context.Context, uint64) error { return nil }, 0); err != nil {
		t.Fatal(err)
	}
	if services := grpcServer.GetServiceInfo(); len(services) != 9 || len(services["dubi.internal.raft.v1.RecordHoldingService"].Methods) != 1 || len(services["dubi.internal.raft.v1.RecordTransferService"].Methods) != 2 || len(services["dubi.internal.raft.v1.RecordCutService"].Methods) != 1 {
		t.Fatalf("registered internal RPC services = %v", services)
	}
	if err := runtime.RegisterInternalRPC(grpcServer, func(context.Context, uint64) error { return nil }, 0); err == nil {
		t.Fatal("duplicate internal RPC registration was accepted")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if _, found := runtime.Engines.Resolve("data-1"); found {
		t.Fatal("closed engine Group remained in runtime directory")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(Options{StoreRoot: storeRoot, TickInterval: time.Hour, EngineMux: mux})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.Host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	if err := restarted.RecoverLocalGroups(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !restarted.Host.HasGroup("data-1") {
		t.Fatal("published local Group was not recovered")
	}
	if _, found := restarted.Engines.Resolve("data-1"); !found {
		t.Fatal("recovered Group is absent from engine state directory")
	}
}

func TestRuntimeOwnsPackPoolAcrossRestart(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	mux := nodeTestMux(t)
	runtime, err := Open(Options{StoreRoot: store, TickInterval: time.Hour, RootBootstrapPeers: []uint64{id.RaftID}, EngineMux: mux})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	loc, err := runtime.Packs.Append(context.Background(), pack.Record{EngineID: "file", TenantID: "t", Key: []byte("k"), Payload: []byte("value")})
	if errors.Is(err, volume.ErrInsufficientSpace) {
		t.Skip("requires filesystem headroom")
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Packs.Read(loc); err == nil {
		t.Fatal("Pool outlived Node")
	}
	restarted, err := Open(Options{StoreRoot: store, TickInterval: time.Hour, EngineMux: mux})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	record, err := restarted.Packs.Read(loc)
	if err != nil || record.EngineID != "file" || string(record.Payload) != "value" {
		t.Fatal("Pack identity/data lost on runtime restart", record, err)
	}
}

func TestRuntimeOpenFailureReleasesStore(t *testing.T) {
	storeRoot := t.TempDir()
	id, err := identity.Bootstrap(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	other := uint64(1)
	if other == id.RaftID {
		other = 2
	}
	mux := nodeTestMux(t)
	if _, err := Open(Options{StoreRoot: storeRoot, TickInterval: time.Hour,
		RootBootstrapPeers: []uint64{other}, EngineMux: mux}); err == nil {
		t.Fatal("invalid Root bootstrap peers were accepted")
	}
	runtime, err := Open(Options{StoreRoot: storeRoot, TickInterval: time.Hour,
		RootBootstrapPeers: []uint64{id.RaftID}, EngineMux: mux})
	if err != nil {
		t.Fatalf("failed Open retained resources: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func applyRoot(t *testing.T, runtime *Runtime, command rootstate.Command) {
	t.Helper()
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Host.Submit(raft.RootGroupID, encoded); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Host.ReadIndex(context.Background(), raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	result, found, err := runtime.Root.ResultForCommand(encoded)
	if err != nil || !found || result.Error != "" {
		t.Fatalf("Root command result = %+v, found %v, err %v", result, found, err)
	}
}
