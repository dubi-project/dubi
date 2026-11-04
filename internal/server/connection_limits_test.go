package server

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"google.golang.org/grpc/credentials"
)

func TestConnectionLimitRejectsExcessWithoutStoppingListener(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := limitListener(raw, 1)
	defer listener.Close()
	accepted := make(chan net.Conn, 2)
	done := make(chan error, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				done <- err
				return
			}
			accepted <- conn
		}
	}()
	defer func() { _ = listener.Close(); <-done }()
	first, err := net.Dial("tcp", raw.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	serverFirst := <-accepted
	second, err := net.Dial("tcp", raw.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := second.Read(make([]byte, 1)); err == nil {
		t.Fatal("excess connection admitted")
	}
	if listener.active.Load() != 1 || listener.rejected.Load() != 1 {
		t.Fatal(listener.active.Load(), listener.rejected.Load())
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = serverFirst.Close() }()
	}
	wg.Wait()
	if listener.active.Load() != 0 {
		t.Fatal("socket slot leaked or released twice")
	}
	third, err := net.Dial("tcp", raw.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	select {
	case conn := <-accepted:
		_ = conn.Close()
	case <-time.After(time.Second):
		t.Fatal("listener stopped after rejecting overload")
	}
}

func TestHandshakeLimitTimeoutAndRecoveryWithRealTLS(t *testing.T) {
	serverTLS, clientTLS := serverTestTLS(t)
	credentials := boundedHandshakes(tlscredentials.NewTLS(serverTLS))
	credentials.timeout = 100 * time.Millisecond
	clients := make([]net.Conn, 0, handshakeLimit)
	results := make(chan error, handshakeLimit)
	for range handshakeLimit {
		server, client := net.Pipe()
		clients = append(clients, client)
		go func() { _, _, err := credentials.ServerHandshake(server); results <- err }()
	}
	deadline := time.Now().Add(time.Second)
	for len(credentials.slots) != handshakeLimit && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(credentials.slots) != handshakeLimit {
		t.Fatal("handshakes never occupied the shared budget")
	}
	extraServer, extraClient := net.Pipe()
	defer extraClient.Close()
	if _, _, err := credentials.Clone().ServerHandshake(extraServer); err == nil {
		t.Fatal("credential clone bypassed Node handshake budget")
	}
	for range handshakeLimit {
		if err := <-results; err == nil {
			t.Fatal("stalled TLS handshake did not time out")
		}
	}
	for _, client := range clients {
		_ = client.Close()
	}
	if len(credentials.slots) != 0 {
		t.Fatal("handshake slots leaked")
	}
	server, client := net.Pipe()
	defer client.Close()
	complete := make(chan error, 1)
	go func() {
		conn, _, err := credentials.ServerHandshake(server)
		if err == nil {
			err = conn.Close()
		}
		complete <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, _, err := tlscredentials.NewTLS(clientTLS).ClientHandshake(ctx, "localhost", client)
	if err != nil {
		t.Fatal("valid TLS failed after overload cleared", err)
	}
	_ = conn.Close()
	if err := <-complete; err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
}

var _ credentials.TransportCredentials = (*handshakeCredentials)(nil)
