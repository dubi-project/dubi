package raftrpc

import (
	"context"
	"time"

	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxSnapshotHeaders           = 8
	defaultSnapshotHeaderTimeout = 10 * time.Second
)

type receivedSnapshotFrame struct {
	frame *raftwire.InstallSnapshotRequest
	err   error
}

// gRPC Recv does not observe a derived handler context. Exactly one bounded
// worker performs a Recv; on timeout the handler returns, causing gRPC to close
// the stream and unblock that worker. Never wait for the worker before returning
// the handler (that would deadlock). Its own lease lasts until Recv really exits.
// Returning early cannot leave an unbounded population of blocked receivers.
func receiveSnapshotFrame(ctx context.Context, stream raftwire.RaftService_InstallSnapshotServer, workers chan struct{}) (*raftwire.InstallSnapshotRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	select {
	case workers <- struct{}{}:
	default:
		return nil, status.Error(codes.ResourceExhausted, "snapshot receive workers are busy")
	}
	result := make(chan receivedSnapshotFrame, 1)
	go func() {
		frame, err := stream.Recv()
		<-workers // Release before handing off to the next serial receive.
		result <- receivedSnapshotFrame{frame: frame, err: err}
	}()
	select {
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case received := <-result:
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		return received.frame, received.err
	}
}

func (s *Server) receiveSnapshotHeader(stream raftwire.RaftService_InstallSnapshotServer) (*raftwire.InstallSnapshotRequest, error) {
	select {
	case s.snapshotHeaders <- struct{}{}:
		defer func() { <-s.snapshotHeaders }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "snapshot header receivers are busy")
	}
	timeout := s.snapshotHeaderTimeout
	if timeout <= 0 {
		timeout = defaultSnapshotHeaderTimeout
	}
	ctx, cancel := context.WithTimeout(stream.Context(), timeout)
	defer cancel()
	return receiveSnapshotFrame(ctx, stream, s.snapshotReaders)
}

// Idle is measured while waiting for a frame/EOF, not while the installer is
// using the preceding frame. Slow local IO is governed by the separate total
// context and cannot be forcibly interrupted. A successful receive never
// extends the finite total lifetime.
type boundedSnapshotServer struct {
	raftwire.RaftService_InstallSnapshotServer
	ctx     context.Context
	idle    time.Duration
	workers chan struct{}
}

func (s *boundedSnapshotServer) Context() context.Context { return s.ctx }

func (s *boundedSnapshotServer) Recv() (*raftwire.InstallSnapshotRequest, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.idle)
	defer cancel()
	return receiveSnapshotFrame(ctx, s.RaftService_InstallSnapshotServer, s.workers)
}

func (s *boundedSnapshotServer) SendAndClose(response *raftwire.InstallSnapshotResponse) error {
	if err := s.ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return s.RaftService_InstallSnapshotServer.SendAndClose(response)
}
