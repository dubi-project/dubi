package server

import (
	"crypto/tls"
	"time"

	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func connectionLifetimes() keepalive.ServerParameters {
	return keepalive.ServerParameters{
		// gRPC adds +/-10% jitter and sends GOAWAY so new RPCs establish a
		// connection with the current leaf, before the old leaf expires.
		MaxConnectionAge: 6 * time.Hour,
		// File snapshots have a 24-hour total budget plus header/setup time.
		// Draining connections may finish that work; certificate expiry still
		// closes immediately and is never extended by this grace period.
		MaxConnectionAgeGrace: 25 * time.Hour,
	}
}

func newInternalRPCServer(configuration *tls.Config, maxSize int, lifetimes keepalive.ServerParameters) *grpc.Server {
	return grpc.NewServer(
		// Stop cancels transports and joins handlers before releasing Store.
		grpc.WaitForHandlers(true),
		grpc.Creds(tlscredentials.NewTLS(normalizeServerTLS(configuration))),
		grpc.KeepaliveParams(lifetimes),
		grpc.MaxRecvMsgSize(messageSize(maxSize)+1024),
	)
}
