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
	"google.golang.org/grpc/credentials"
)

type delayedRootStep struct {
	raftwire.UnimplementedRaftServiceServer
	entered chan struct{}
	release chan struct{}
}

func (s *delayedRootStep) Step(ctx context.Context, _ *raftwire.StepRequest) (*raftwire.StepResponse, error) {
	s.entered <- struct{}{}
	select {
	case <-s.release:
		return &raftwire.StepResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestRootTransportPipelinesBoundedControlMessages(t *testing.T) {
	pool, certificates := testCertificates(t, []uint64{1, 2})
	handler := &delayedRootStep{entered: make(chan struct{}, 12), release: make(chan struct{})}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificates[2]}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13,
	})))
	defer server.Stop()
	raftwire.RegisterRaftServiceServer(server, handler)
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
	transport, err := NewTransport(TransportOptions{ClusterID: "cluster", RaftID: 1,
		Directory: directory, Credentials: credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{certificates[1]}, RootCAs: pool,
			ServerName: "localhost", MinVersion: tls.VersionTLS13,
		})})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	for range 12 {
		if err := transport.Send(raft.RootGroupID, logMessage(2)); err != nil {
			t.Fatal(err)
		}
	}
	// A delayed RPC must not serialize the entire cross-Region Root queue.
	// Keep the pipeline at four so a slow peer cannot grow active goroutines
	// or buffers with the number of queued messages.
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for i := 0; i < 4; i++ {
		select {
		case <-handler.entered:
		case <-deadline.C:
			t.Fatalf("Root control queue serialized after %d in-flight RPC(s)", i)
		}
	}
	select {
	case <-handler.entered:
		t.Fatal("Root control exceeded its bounded four-message pipeline")
	case <-time.After(30 * time.Millisecond):
	}
	if transport.ResourceStatus().Messages.UsedBytes == 0 {
		t.Fatal("queued and active Root log messages lost their byte reservations")
	}
	// Cancellation joins the blocked workers and releases queued leases.
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if status := transport.ResourceStatus(); status.Messages.UsedBytes != 0 || status.Control.UsedBytes != 0 {
		t.Fatal("Root pipeline retained byte reservations after Close", status)
	}
}
