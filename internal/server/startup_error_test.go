package server

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
)

func TestStartupInternalTimeoutIsNotSuccessfulProcessShutdown(t *testing.T) {
	for _, fault := range []error{context.DeadlineExceeded, context.Canceled} {
		t.Run(fault.Error(), func(t *testing.T) {
			root := t.TempDir()
			id, err := identity.Bootstrap(root)
			if err != nil {
				t.Fatal(err)
			}
			serverTLS, clientTLS := serverTestTLS(t)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			s, err := Open(Options{StoreRoot: root, Listener: listener, ServerTLS: serverTLS, ClientTLS: clientTLS,
				AuthorizePeer: func(context.Context, uint64) error { return nil }, EngineMux: serverTestMux(t),
				RootBootstrapPeers: []uint64{id.RaftID}, TickInterval: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			s.Runtime.SetRootReadIndex(func(context.Context) (uint64, error) { return 0, fault })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = s.Run(ctx)
			if ctx.Err() != nil || !errors.Is(err, fault) {
				t.Fatal("internal startup error hidden by cleanup", err, ctx.Err())
			}
			select {
			case <-s.Ready():
				t.Fatal("failed startup reported ready")
			default:
			}
		})
	}
}
