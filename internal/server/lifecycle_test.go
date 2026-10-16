package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type failedListener struct {
	net.Listener
	err error
}

func (l failedListener) Accept() (net.Conn, error) { return nil, l.err }

func TestServerListenerFailureInterruptsStartupBeforeLeader(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, clientTLS := serverTestTLS(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("injected listener failure")
	server, err := Open(Options{StoreRoot: store, Listener: failedListener{Listener: listener, err: fault},
		ServerTLS: serverTLS, ClientTLS: clientTLS, AuthorizePeer: func(context.Context, uint64) error { return nil },
		EngineMux: serverTestMux(t), RootBootstrapPeers: []uint64{id.RaftID}, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Run(ctx); !errors.Is(err, fault) {
		t.Fatal("listener failure was lost or startup hung", err)
	}
	select {
	case <-server.Ready():
		t.Fatal("failed listener became ready")
	default:
	}
}

func TestServerCloseJoinsSchedulerBeforeReleasingStore(t *testing.T) {
	testServerCloseJoinsControlWorker(t, "scheduler")
}

func TestServerCloseJoinsRegionalFormationBeforeReleasingStore(t *testing.T) {
	testServerCloseJoinsControlWorker(t, "formation")
}

func TestServerCloseJoinsRegionalConfirmationBeforeReleasingStore(t *testing.T) {
	testServerCloseJoinsControlWorker(t, "confirmation")
}

func TestServerCloseJoinsRegionalGrowthBeforeReleasingStore(t *testing.T) {
	testServerCloseJoinsControlWorker(t, "growth")
}

func testServerCloseJoinsControlWorker(t *testing.T, stage string) {
	t.Helper()
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, clientTLS := serverTestTLS(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := Open(Options{StoreRoot: store, Listener: listener, ServerTLS: serverTLS, ClientTLS: clientTLS,
		AuthorizePeer: func(context.Context, uint64) error { return nil }, EngineMux: serverTestMux(t),
		RootBootstrapPeers: []uint64{id.RaftID}, TickInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	blocked := func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release // Simulate a worker finishing its cancellation cleanup.
		return ctx.Err()
	}
	if stage == "formation" {
		server.regional = &regionalFormation{coordinator: rootTaskFunc(blocked)}
	} else if stage == "confirmation" {
		server.regional = &regionalFormation{coordinator: rootTaskFunc(func(ctx context.Context) error { return ctx.Err() }), confirmer: rootTaskFunc(blocked)}
	} else if stage == "growth" {
		server.regional = &regionalFormation{coordinator: rootTaskFunc(func(ctx context.Context) error { return ctx.Err() }), grower: rootTaskFunc(blocked)}
	} else {
		server.scheduler, err = groupmembership.NewScheduler(server.Runtime.Host, dataTaskFunc(blocked), time.Hour, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("scheduler did not start")
	}
	closed := make(chan error, 2)
	go func() { closed <- server.Close() }()
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("Close did not cancel scheduler")
	}
	go func() { closed <- server.Close() }()
	select {
	case err := <-closed:
		t.Fatal("Close returned while worker still owns state", err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := server.Runtime.Host.Status(raft.RootGroupID); err != nil {
		t.Fatal("Host closed before worker exit", err)
	}
	if lock, err := storelock.Acquire(store); err == nil {
		_ = lock.Release()
		t.Fatal("Store lock released before scheduler exit")
	}
	unblock()
	for range 2 {
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("Close did not join")
		}
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Run survived Close")
	}
	lock, err := storelock.Acquire(store)
	if err != nil {
		t.Fatal("Store retained after all Close callers completed", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestServerCloseJoinsRPCHandlersBeforeReleasingStore(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, clientTLS := serverTestTLS(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	server, err := Open(Options{StoreRoot: store, Listener: listener, ServerTLS: serverTLS, ClientTLS: clientTLS,
		AuthorizePeer: func(ctx context.Context, _ uint64) error {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release // A handler still finishing cleanup after its transport stops.
			return ctx.Err()
		}, EngineMux: serverTestMux(t), RootBootstrapPeers: []uint64{id.RaftID}, TickInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unblock(); server.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run(ctx) }()
	select {
	case <-server.Ready():
	case <-ctx.Done():
		t.Fatal("server not ready")
	}
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	callDone := make(chan error, 1)
	go func() {
		_, err := raftwire.NewGroupServiceClient(conn).ReadIndex(ctx, &raftwire.GroupReadIndexRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID, ToRaftId: id.RaftID, GroupId: raft.RootGroupID})
		callDone <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("RPC handler not entered")
	}
	closed := make(chan error, 1)
	go func() { closed <- server.Close() }()
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("Close did not cancel RPC")
	}
	select {
	case err := <-closed:
		t.Fatal("Close returned before RPC cleanup", err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := server.Runtime.Host.Status(raft.RootGroupID); err != nil {
		t.Fatal("Host closed under active RPC", err)
	}
	if lock, err := storelock.Acquire(store); err == nil {
		lock.Release()
		t.Fatal("active RPC lost Store ownership")
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close failed to join RPC")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Run did not exit")
	}
	select {
	case err := <-callDone:
		if err == nil {
			t.Fatal("canceled RPC succeeded")
		}
	case <-ctx.Done():
		t.Fatal("RPC client did not exit")
	}
	lock, err := storelock.Acquire(store)
	if err != nil {
		t.Fatal("Store not released after RPC exit", err)
	}
	lock.Release()
}
