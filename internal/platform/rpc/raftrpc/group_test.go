package raftrpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rangebootstrap"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type groupRPCHandler struct{}

func (groupRPCHandler) Apply(transaction *capstate.Transaction, _ router.Target, payload []byte) (capstate.Outcome, error) {
	separator := bytes.IndexByte(payload, '=')
	if separator <= 0 {
		return capstate.Outcome{Rejection: "invalid test mutation"}, nil
	}
	key, value := payload[:separator], payload[separator+1:]
	if err := transaction.Set(key, value); err != nil {
		return capstate.Outcome{}, err
	}
	return capstate.Outcome{Value: value}, nil
}

func (groupRPCHandler) Read(view capstate.ReadView, _ router.Target, payload []byte) (capstate.Outcome, error) {
	value, found, err := view.Get(payload)
	if err != nil {
		return capstate.Outcome{}, err
	}
	if !found {
		return capstate.Outcome{Rejection: "not found"}, nil
	}
	return capstate.Outcome{Value: value}, nil
}

func TestGroupRPCSubmitsAndConfirmsOverMutualTLS(t *testing.T) {
	serverRoot := t.TempDir()
	serverID, err := identity.Bootstrap(serverRoot)
	if err != nil {
		t.Fatal(err)
	}
	const clientID uint64 = 991
	pool, certificates := testCertificates(t, []uint64{serverID.RaftID, clientID})
	state := &memoryState{}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: serverRoot, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if err := host.AddGroup("data-1", []uint64{serverID.RaftID}, false, state); err != nil {
		t.Fatal(err)
	}
	if err := host.Campaign("data-1"); err != nil {
		t.Fatal(err)
	}
	authorize := func(ctx context.Context, claimed uint64) error {
		remote, ok := peer.FromContext(ctx)
		if !ok {
			return errors.New("peer is missing")
		}
		info := remote.AuthInfo.(credentials.TLSInfo)
		if info.State.PeerCertificates[0].Subject.CommonName != fmt.Sprintf("raft-%d", claimed) {
			return errors.New("certificate does not match RaftID")
		}
		return nil
	}
	groupServer, err := NewGroupServer(serverID.ClusterID, serverID.RaftID, host, authorize, 1024)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificates[serverID.RaftID]},
		ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})
	grpcServer := grpc.NewServer(grpc.Creds(serverTLS), grpc.MaxRecvMsgSize(2048))
	defer grpcServer.Stop()
	raftwire.RegisterGroupServiceServer(grpcServer, groupServer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() { _ = grpcServer.Serve(listener) }()
	directory := NewStaticDirectory()
	if err := directory.Update(serverID.RaftID, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	clientTLS := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificates[clientID]},
		RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})
	client, err := NewGroupClient(GroupClientOptions{ClusterID: serverID.ClusterID, RaftID: clientID,
		Directory: directory, Credentials: clientTLS, MaxSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	index, err := client.Submit(ctx, serverID.RaftID, "data-1", []byte("remote-command"))
	if err != nil || index == 0 || !state.has("remote-command") {
		t.Fatalf("remote Submit = index %d, applied %v, err %v", index, state.has("remote-command"), err)
	}
	readIndex, err := client.ReadIndex(ctx, serverID.RaftID, "data-1")
	if err != nil || readIndex < index || readIndex > state.AppliedIndex() {
		t.Fatalf("remote ReadIndex = %d, submitted %d, applied %d, err %v",
			readIndex, index, state.AppliedIndex(), err)
	}
	if _, err := client.Submit(ctx, serverID.RaftID, "data-1", make([]byte, 1025)); err == nil {
		t.Fatal("oversized Group command was accepted")
	}

	spoofed, err := NewGroupClient(GroupClientOptions{ClusterID: serverID.ClusterID, RaftID: clientID + 1,
		Directory: directory, Credentials: clientTLS})
	if err != nil {
		t.Fatal(err)
	}
	defer spoofed.Close()
	if _, err := spoofed.ReadIndex(ctx, serverID.RaftID, "data-1"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("spoofed Group identity error = %v", err)
	}
}

func TestRangeControlRPCConfirmsPersistentOwnerResult(t *testing.T) {
	serverRoot := t.TempDir()
	serverID, err := identity.Bootstrap(serverRoot)
	if err != nil {
		t.Fatal(err)
	}
	clientRoot := t.TempDir()
	if _, err := identity.PrepareJoin(clientRoot); err != nil {
		t.Fatal(err)
	}
	clientIdentity, err := identity.BindCluster(clientRoot, serverID.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	clientID := clientIdentity.RaftID
	pool, certificates := testCertificates(t, []uint64{serverID.RaftID, clientID})
	ownerState, err := capstate.Open(t.TempDir(), "data-1", groupRPCHandler{})
	if err != nil {
		t.Fatal(err)
	}
	ownerAdapter, err := routedstate.New(ownerState)
	if err != nil {
		t.Fatal(err)
	}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: serverRoot, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if err := host.AddGroup("data-1", []uint64{serverID.RaftID}, false, ownerAdapter); err != nil {
		t.Fatal(err)
	}
	if err := host.Campaign("data-1"); err != nil {
		t.Fatal(err)
	}
	localOwner, err := rangebootstrap.NewLocalOwner(host, ownerState)
	if err != nil {
		t.Fatal(err)
	}
	authorize := func(ctx context.Context, claimed uint64) error {
		remote, ok := peer.FromContext(ctx)
		if !ok {
			return errors.New("peer is missing")
		}
		info := remote.AuthInfo.(credentials.TLSInfo)
		if info.State.PeerCertificates[0].Subject.CommonName != fmt.Sprintf("raft-%d", claimed) {
			return errors.New("certificate does not match RaftID")
		}
		return nil
	}
	rangeServer, err := NewRangeControlServer(serverID.ClusterID, serverID.RaftID,
		func(groupID string) (rangebootstrap.OwnerControl, bool) {
			return localOwner, groupID == "data-1"
		}, authorize)
	if err != nil {
		t.Fatal(err)
	}
	capabilityServer, err := NewCapabilityServer(serverID.ClusterID, serverID.RaftID, host,
		func(groupID string) (*capstate.State, bool) {
			return ownerState, groupID == "data-1"
		}, authorize, 0)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificates[serverID.RaftID]},
		ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})
	grpcServer := grpc.NewServer(grpc.Creds(serverTLS), grpc.MaxRecvMsgSize(DefaultMaxMessageSize+1024))
	defer grpcServer.Stop()
	raftwire.RegisterRangeControlServiceServer(grpcServer, rangeServer)
	raftwire.RegisterCapabilityServiceServer(grpcServer, capabilityServer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() { _ = grpcServer.Serve(listener) }()
	directory := NewStaticDirectory()
	if err := directory.Update(serverID.RaftID, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	clientTLS := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificates[clientID]},
		RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})
	groupRoutes := NewStaticGroupRoutes()
	const unavailableCandidate uint64 = 999999
	if err := groupRoutes.Update("data-1", []uint64{unavailableCandidate, serverID.RaftID}); err != nil {
		t.Fatal(err)
	}
	client, err := NewGroupClient(GroupClientOptions{ClusterID: serverID.ClusterID, RaftID: clientID,
		Directory: directory, Credentials: clientTLS, GroupRoutes: groupRoutes})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	remote, err := client.Owner(serverID.RaftID, "data-1")
	if err != nil {
		t.Fatal(err)
	}
	descriptor := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", CapabilityID: "record",
		DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	proof, err := remote.EnsureInitialRange(ctx, descriptor)
	if err != nil || proof.RequestID != rangebootstrap.OwnerRequestID(descriptor) || proof.AppliedIndex == 0 {
		t.Fatalf("remote Owner proof = %+v, %v", proof, err)
	}
	repeated, err := remote.EnsureInitialRange(ctx, descriptor)
	if err != nil || repeated != proof {
		t.Fatalf("repeated remote Owner proof = %+v, %v", repeated, err)
	}
	result, found, err := ownerState.ResultForRange(proof.RequestID, descriptor)
	if err != nil || !found || !result.Applied || result.Index != proof.AppliedIndex {
		t.Fatalf("persistent Owner result = %+v, %v, %v", result, found, err)
	}
	conflict := descriptor
	conflict.DatasetID = "other"
	if _, err := remote.EnsureInitialRange(ctx, conflict); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("conflicting remote Fence error = %v", err)
	}

	rootState, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rootHost, err := raft.OpenHost(raft.HostOptions{StoreRoot: clientRoot, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer rootHost.Close()
	if err := rootHost.AddGroup(raft.RootGroupID, []uint64{clientID}, false, rootState); err != nil {
		t.Fatal(err)
	}
	if err := rootHost.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	rootCommands := []rootstate.Command{
		{Version: 1, RequestID: "group-data", Type: rootstate.RegisterGroup,
			Group: &rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateCapability}},
		{Version: 1, RequestID: "tenant-a", Type: rootstate.RegisterTenant,
			Tenant: &rootstate.Tenant{TenantID: "tenant-a"}},
		{Version: 1, RequestID: "dataset-a", Type: rootstate.CreateDataset,
			Dataset: &rootstate.Dataset{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items",
				RangeID: "range-a", OwnerGroupID: "data-1"}},
	}
	for _, command := range rootCommands {
		encoded, err := rootstate.EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := rootHost.Submit(raft.RootGroupID, encoded); err != nil {
			t.Fatal(err)
		}
	}
	bootstrapper, err := rangebootstrap.New(rootHost, rootState,
		func(groupID string) (rangebootstrap.OwnerControl, bool) {
			return remote, groupID == "data-1"
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrapper.Reconcile(ctx, "range-a"); err != nil {
		t.Fatalf("cross-Node Range bootstrap = %v", err)
	}
	keyspace := rootstate.Keyspace{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items"}
	if published, found, err := rootState.RouteKeyspace(keyspace, []byte("key")); err != nil || !found ||
		published.OwnerFenceRequestID != proof.RequestID || published.OwnerFenceIndex != proof.AppliedIndex {
		t.Fatalf("cross-Node published Range = %+v, %v, %v", published, found, err)
	}
	published, _, _ := rootState.RouteKeyspace(keyspace, []byte("key"))
	target := router.Target{Keyspace: keyspace, RangeID: published.RangeID,
		OwnerGroupID: published.OwnerGroupID, Epoch: published.Epoch, RoutingKey: []byte("key")}
	remoteCapability, err := client.RoutedCapability("data-1")
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := routedstate.EncodeMutation("remote-write", target, []byte("item=value"))
	if err != nil {
		t.Fatal(err)
	}
	mutationResult, err := remoteCapability.Mutate(ctx, mutation)
	if err != nil || !mutationResult.Applied || mutationResult.Error != "" || string(mutationResult.Value) != "value" {
		t.Fatalf("remote capability mutation = %+v, %v", mutationResult, err)
	}
	if candidates := groupRoutes.Candidates("data-1"); len(candidates) == 0 || candidates[0] != serverID.RaftID {
		t.Fatalf("observed Leader was not promoted: %v", candidates)
	}
	repeatedMutation, err := remoteCapability.Mutate(ctx, mutation)
	if err != nil || repeatedMutation.Index != mutationResult.Index {
		t.Fatalf("repeated remote mutation = %+v, %v", repeatedMutation, err)
	}
	readOutcome, readIndex, err := remoteCapability.Read(ctx, target, []byte("item"))
	if err != nil || readIndex < mutationResult.Index || readOutcome.Rejection != "" || string(readOutcome.Value) != "value" {
		t.Fatalf("remote capability read = %+v, index %d, %v", readOutcome, readIndex, err)
	}
	stale := target
	stale.Epoch++
	if _, _, err := remoteCapability.Read(ctx, stale, []byte("item")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale remote capability read = %v", err)
	}
}
