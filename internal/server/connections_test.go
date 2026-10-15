package server

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

func TestInternalConnectionLifetimePolicyAndGracefulRefresh(t *testing.T) {
	policy := connectionLifetimes()
	if policy.MaxConnectionAge != 6*time.Hour || policy.MaxConnectionAgeGrace < 25*time.Hour {
		t.Fatal("connection lifetime does not preserve renewal/snapshot margin", policy)
	}
	serverTLS, clientTLS := serverTestTLS(t)
	var handshakes atomic.Int64
	serverTLS.VerifyConnection = func(tls.ConnectionState) error { handshakes.Add(1); return nil }
	// Exercise the same constructor as formal Server with shortened internal
	// durations, not operator configuration. An established streaming RPC must
	// survive GOAWAY while new RPCs use a fresh authenticated connection.
	s := newInternalRPCServer(serverTLS, 0, keepalive.ServerParameters{MaxConnectionAge: 200 * time.Millisecond, MaxConnectionAgeGrace: 2 * time.Second})
	service := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s, service)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	t.Cleanup(func() {
		s.Stop()
		if err := <-done; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	})
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(tlscredentials.NewTLS(clientTLS)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client := grpc_health_v1.NewHealthClient(conn)
	watch, err := client.Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err != nil {
		t.Fatal(err)
	}
	initial := handshakes.Load()
	for {
		if _, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
			t.Fatal(err)
		}
		if handshakes.Load() > initial {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("old connection never refreshed", ctx.Err())
		}
		time.Sleep(10 * time.Millisecond)
	}
	service.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	response, err := watch.Recv()
	if err != nil || response.Status != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatal("active stream was killed at connection age", response, err)
	}
	// The grace is finite too: a never-ending stream cannot pin an obsolete
	// session forever. Normal production snapshots have their own 24h budget.
	if _, err := watch.Recv(); err == nil || status.Code(err) == codes.DeadlineExceeded || ctx.Err() != nil {
		t.Fatal("draining stream did not close at grace deadline", err, ctx.Err())
	}
}
