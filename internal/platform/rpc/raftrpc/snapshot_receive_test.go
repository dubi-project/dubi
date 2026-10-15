package raftrpc

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func snapshotReceiveClient(t *testing.T, s *Server) raftwire.RaftServiceClient {
	t.Helper()
	pool, certs := testCertificates(t, []uint64{1, 2})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{certs[1]}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert})))
	raftwire.RegisterRaftServiceServer(g, s)
	done := make(chan error, 1)
	go func() { done <- g.Serve(listener) }()
	t.Cleanup(func() {
		g.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
		listener.Close()
	})
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certs[2]}, RootCAs: pool, ServerName: "localhost"})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return raftwire.NewRaftServiceClient(conn)
}

func waitSnapshotReceivers(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-deadline.C:
			t.Fatal("snapshot receive lifecycle did not settle")
		case <-tick.C:
		}
	}
}

func TestSnapshotReceiveNetworkIdleAndTotalDeadlines(t *testing.T) {
	for _, boundary := range []string{"header", "payload", "eof", "total", "slow-install"} {
		t.Run(boundary, func(t *testing.T) {
			var installed atomic.Int32
			s := budgetServer(t, &snapshotBackend{install: func(m *raftpb.Message) (uint64, error) {
				installed.Add(1)
				if boundary == "slow-install" {
					time.Sleep(80 * time.Millisecond)
				}
				return m.GetSnapshot().GetMetadata().GetIndex(), nil
			}})
			s.snapshotHeaderTimeout = 40 * time.Millisecond
			s.snapshotReceiveLimits = func(uint64) snapshotDeadlines {
				if boundary == "total" {
					return snapshotDeadlines{idle: time.Second, total: 80 * time.Millisecond}
				}
				return snapshotDeadlines{idle: 40 * time.Millisecond, total: 3 * time.Second}
			}
			client := snapshotReceiveClient(t, s)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := client.InstallSnapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			data, err := proto.Marshal(&raftpb.Message{Type: raftpb.MessageType_MsgSnap.Enum(), From: proto.Uint64(2), To: proto.Uint64(1),
				Snapshot: &raftpb.Snapshot{Data: make([]byte, 256), Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(5), Term: proto.Uint64(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2}}}}})
			if err != nil {
				t.Fatal(err)
			}
			if boundary != "header" {
				digest := sha256.Sum256(data)
				err = stream.Send(&raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Header{Header: &raftwire.SnapshotHeader{
					ClusterId: "cluster", GroupId: "data", FromRaftId: 2, ToRaftId: 1, EncodedSize: uint64(len(data)), Sha256Digest: digest[:]}}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "eof" || boundary == "slow-install" {
				if err := stream.Send(&raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{Data: data}}); err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "total" {
				// Keep making progress; it cannot extend the hard total deadline.
				done := make(chan struct{})
				go func() {
					defer close(done)
					for _, b := range data {
						if err := stream.Send(&raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{Data: []byte{b}}}); err != nil {
							return
						}
						select {
						case <-ctx.Done():
							return
						case <-time.After(10 * time.Millisecond):
						}
					}
				}()
				defer func() { cancel(); <-done }()
			}
			if boundary == "slow-install" {
				response, err := stream.CloseAndRecv()
				if err != nil || response.GetAppliedIndex() != 5 || installed.Load() != 1 {
					t.Fatal("idle timer covered local install", err)
				}
			} else {
				// RecvMsg intentionally does NOT half-close the request stream:
				// the server must terminate it without a cooperative client's EOF.
				err := stream.RecvMsg(new(raftwire.InstallSnapshotResponse))
				if status.Code(err) != codes.DeadlineExceeded || ctx.Err() != nil || installed.Load() != 0 {
					t.Fatal("server deadline not enforced", err, ctx.Err())
				}
			}
			waitSnapshotReceivers(t, func() bool {
				return len(s.snapshotHeaders) == 0 && len(s.snapshotReaders) == 0 && len(s.snapshotSlots) == 0 && s.SnapshotResources().UsedBytes == 0
			})
		})
	}
}

func TestSnapshotReceiveHeaderLimitDoesNotBlockConsensus(t *testing.T) {
	var heartbeats atomic.Int32
	backend := &fileSnapshotBackend{heartbeat: func() { heartbeats.Add(1) }}
	s, err := NewServer("cluster", 1, backend, func(context.Context, uint64) error { return nil }, 0)
	if err != nil {
		t.Fatal(err)
	}
	client := snapshotReceiveClient(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range maxSnapshotHeaders {
		if _, err := client.InstallSnapshot(ctx); err != nil {
			t.Fatal(err)
		}
		waitSnapshotReceivers(t, func() bool { return len(s.snapshotHeaders) == i+1 })
	}
	extra, err := client.InstallSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := extra.RecvMsg(new(raftwire.InstallSnapshotResponse)); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("unbounded header waiters", err)
	}
	message, _ := proto.Marshal(&raftpb.Message{Type: raftpb.MessageType_MsgHeartbeat.Enum(), From: proto.Uint64(2), To: proto.Uint64(1)})
	if _, err := client.Step(ctx, &raftwire.StepRequest{ClusterId: "cluster", GroupId: "data", FromRaftId: 2, ToRaftId: 1, Message: message}); err != nil || heartbeats.Load() != 1 {
		t.Fatal("idle headers blocked control traffic", err)
	}
	cancel()
	waitSnapshotReceivers(t, func() bool { return len(s.snapshotHeaders) == 0 && len(s.snapshotReaders) == 0 })
}

func TestSnapshotReceiveWorkerLeaseOutlivesHandlerTimeout(t *testing.T) {
	stream := snapshotStream(nil)
	stream.header = nil
	parent, stop := context.WithCancel(stream.ctx)
	defer stop()
	stream.ctx = parent
	workers := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(parent, 30*time.Millisecond)
	defer cancel()
	if _, err := receiveSnapshotFrame(ctx, stream, workers); status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(err)
	}
	if len(workers) != 1 {
		t.Fatal("blocked Recv lost its own resource lease")
	}
	if _, err := receiveSnapshotFrame(parent, stream, workers); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("timed-out reader bypassed bound", err)
	}
	stop() // Actual gRPC performs this when the timed-out handler returns.
	waitSnapshotReceivers(t, func() bool { return len(workers) == 0 })
}

func TestFileSnapshotReceiveIdleDuringImportPreservesDeadline(t *testing.T) {
	source, destination := snapshotArtifacts(t), snapshotArtifacts(t)
	ref := wireArtifact(t, source, 1, 16)
	backend := &fileSnapshotBackend{importSnapshot: func(ctx context.Context, _ string, _ *raftpb.Message, reader io.Reader) (uint64, error) {
		if err := destination.Import(ctx, ref, reader); err != nil {
			return 0, err
		}
		t.Error("incomplete file was published")
		return ref.Index, nil
	}}
	s, err := NewServer("cluster", 1, backend, func(context.Context, uint64) error { return nil }, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.snapshotReceiveLimits = func(uint64) snapshotDeadlines {
		return snapshotDeadlines{idle: 40 * time.Millisecond, total: time.Second}
	}
	client := snapshotReceiveClient(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.InstallSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	message, err := proto.Marshal(fileMessage(t, ref, 2, 1))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := snapshotEnvelope(message)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(envelope)
	if err := stream.Send(&raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Header{Header: &raftwire.SnapshotHeader{
		ClusterId: "cluster", GroupId: "data", FromRaftId: 2, ToRaftId: 1, EncodedSize: uint64(len(envelope)) + ref.Size, Sha256Digest: digest[:]}}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{Data: envelope}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(new(raftwire.InstallSnapshotResponse)); status.Code(err) != codes.DeadlineExceeded || ctx.Err() != nil {
		t.Fatal("import changed timeout to Unavailable", err)
	}
	waitSnapshotReceivers(t, func() bool {
		return len(s.snapshotReaders) == 0 && len(s.snapshotSlots) == 0 && s.SnapshotResources().UsedBytes == 0
	})
}
