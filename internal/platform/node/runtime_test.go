package node

import (
	"context"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/capabilityruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"google.golang.org/grpc"
)

type nodeTestHandler struct{}

func (*nodeTestHandler) Apply(*capstate.Transaction, router.Target, []byte) (capstate.Outcome, error) {
	return capstate.Outcome{}, nil
}

func nodeTestMux(t *testing.T) *capabilityruntime.Mux {
	t.Helper()
	registry := capabilityruntime.NewRegistry()
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
		RootBootstrapPeers: []uint64{id.RaftID}, CapabilityMux: mux})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Identity != id || runtime.Host == nil || runtime.Root == nil || runtime.Groups == nil ||
		runtime.Capabilities == nil || runtime.JoinManager == nil || runtime.LocalMembership == nil {
		t.Fatal("Node runtime did not assemble all core components")
	}
	if err := runtime.Host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	applyRoot(t, runtime, rootstate.Command{Version: 1, RequestID: "register-node", Type: rootstate.RegisterNode,
		Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "region-a", Address: "node-a:7443"}})
	applyRoot(t, runtime, rootstate.Command{Version: 1, RequestID: "register-data", Type: rootstate.RegisterGroup,
		Group: &rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateCapability}})
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
	if _, found := runtime.Capabilities.Resolve("data-1"); !found {
		t.Fatal("open capability Group is absent from runtime directory")
	}
	grpcServer := grpc.NewServer()
	if err := runtime.RegisterInternalRPC(grpcServer, func(context.Context, uint64) error { return nil }, 0); err != nil {
		t.Fatal(err)
	}
	if services := grpcServer.GetServiceInfo(); len(services) != 5 {
		t.Fatalf("registered internal RPC services = %v", services)
	}
	if err := runtime.RegisterInternalRPC(grpcServer, func(context.Context, uint64) error { return nil }, 0); err == nil {
		t.Fatal("duplicate internal RPC registration was accepted")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if _, found := runtime.Capabilities.Resolve("data-1"); found {
		t.Fatal("closed capability Group remained in runtime directory")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, err := Open(Options{StoreRoot: storeRoot, TickInterval: time.Hour, CapabilityMux: mux})
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
	if _, found := restarted.Capabilities.Resolve("data-1"); !found {
		t.Fatal("recovered Group is absent from capability state directory")
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
		RootBootstrapPeers: []uint64{other}, CapabilityMux: mux}); err == nil {
		t.Fatal("invalid Root bootstrap peers were accepted")
	}
	runtime, err := Open(Options{StoreRoot: storeRoot, TickInterval: time.Hour,
		RootBootstrapPeers: []uint64{id.RaftID}, CapabilityMux: mux})
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
