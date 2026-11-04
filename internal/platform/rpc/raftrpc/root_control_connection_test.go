package raftrpc

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
)

type occupiedEngineConnection struct {
	raftwire.UnimplementedEngineServiceServer
	entered chan struct{}
	release chan struct{}
}

func TestRootControlConnectionFollowsDirectoryAndCloses(t *testing.T) {
	pool, certificates := testCertificates(t, []uint64{1, 2})
	serve := func(index uint64) string {
		server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{certificates[2]}, ClientCAs: pool,
			ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13,
		})))
		raftwire.RegisterGroupServiceServer(server, &immediateRootControl{index: index})
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { server.Stop(); _ = listener.Close() })
		go func() { _ = server.Serve(listener) }()
		return listener.Addr().String()
	}
	directory := NewStaticDirectory()
	if err := directory.Update(2, serve(9)); err != nil {
		t.Fatal(err)
	}
	client, err := NewGroupClient(GroupClientOptions{ClusterID: "cluster-test", RaftID: 1,
		Directory: directory, Credentials: credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{certificates[1]}, RootCAs: pool,
			ServerName: "localhost", MinVersion: tls.VersionTLS13,
		})})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if index, err := client.ReadIndex(ctx, 2, raft.RootGroupID); err != nil || index != 9 {
		t.Fatalf("first Root endpoint: index=%d err=%v", index, err)
	}
	previous := client.peers[2].rootConn
	if err := directory.Update(2, serve(17)); err != nil {
		t.Fatal(err)
	}
	if index, err := client.ReadIndex(ctx, 2, raft.RootGroupID); err != nil || index != 17 {
		t.Fatalf("updated Root endpoint: index=%d err=%v", index, err)
	}
	if previous.GetState() != connectivity.Shutdown {
		t.Fatal("obsolete Root connection remained open after address change")
	}
	current := client.peers[2].rootConn
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if current.GetState() != connectivity.Shutdown {
		t.Fatal("Root connection remained open after GroupClient.Close")
	}
	if _, err := client.ReadIndex(ctx, 2, raft.RootGroupID); err == nil {
		t.Fatal("closed client still served Root reads")
	}
}

func (s *occupiedEngineConnection) Read(ctx context.Context, _ *raftwire.EngineReadRequest) (*raftwire.EngineReadResponse, error) {
	close(s.entered)
	select {
	case <-s.release:
		return &raftwire.EngineReadResponse{ConfirmedIndex: 1}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type immediateRootControl struct {
	raftwire.UnimplementedGroupServiceServer
	index uint64
}

func (s *immediateRootControl) ReadIndex(context.Context, *raftwire.GroupReadIndexRequest) (*raftwire.GroupReadIndexResponse, error) {
	return &raftwire.GroupReadIndexResponse{ConfirmedIndex: s.index, LeaderRaftId: 2}, nil
}

func (s *immediateRootControl) Submit(context.Context, *raftwire.GroupSubmitRequest) (*raftwire.GroupSubmitResponse, error) {
	return &raftwire.GroupSubmitResponse{ConfirmedIndex: s.index, LeaderRaftId: 2}, nil
}

func TestRootControlDoesNotWaitForOccupiedEngineConnection(t *testing.T) {
	pool, certificates := testCertificates(t, []uint64{1, 2})
	engine := &occupiedEngineConnection{entered: make(chan struct{}), release: make(chan struct{})}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificates[2]}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13,
	})), grpc.MaxConcurrentStreams(1))
	defer server.Stop()
	raftwire.RegisterEngineServiceServer(server, engine)
	raftwire.RegisterGroupServiceServer(server, &immediateRootControl{index: 9})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() { _ = server.Serve(listener) }()
	directory := NewStaticDirectory()
	if err := directory.Update(2, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	client, err := NewGroupClient(GroupClientOptions{ClusterID: "cluster-test", RaftID: 1,
		Directory: directory, Credentials: credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{certificates[1]}, RootCAs: pool,
			ServerName: "localhost", MinVersion: tls.VersionTLS13,
		})})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remote, err := client.enginePeer(2)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		_, err := remote.Read(ctx, &raftwire.EngineReadRequest{})
		finished <- err
	}()
	defer func() { close(engine.release); <-finished }()
	select {
	case <-engine.entered:
	case <-ctx.Done():
		t.Fatal("Engine request did not occupy its connection")
	}
	// One occupied stream exhausts this test server's advertised stream limit.
	// Root must still obtain its proof while the Engine handler remains blocked.
	rootCtx, rootCancel := context.WithTimeout(ctx, 2*time.Second)
	defer rootCancel()
	if index, err := client.ReadIndex(rootCtx, 2, raft.RootGroupID); err != nil || index != 9 {
		t.Fatalf("Root read waited behind Engine traffic: index=%d err=%v", index, err)
	}
	if index, err := client.Submit(rootCtx, 2, raft.RootGroupID, []byte("command")); err != nil || index != 9 {
		t.Fatalf("Root submit waited behind Engine traffic: index=%d err=%v", index, err)
	}
}
