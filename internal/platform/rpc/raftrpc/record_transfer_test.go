package raftrpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type recordWriteFunc func(context.Context, records.Descriptor, []byte) (records.Holding, error)

func (f recordWriteFunc) Put(ctx context.Context, d records.Descriptor, b []byte) (records.Holding, error) {
	return f(ctx, d, b)
}

type retainingRecordClient struct {
	grpc.ClientStream
	frame             *raftwire.PutRecordRequest
	sendErr, finalErr error
	received          bool
}

func (s *retainingRecordClient) Send(frame *raftwire.PutRecordRequest) error {
	s.frame = frame
	return s.sendErr
}
func (s *retainingRecordClient) CloseAndRecv() (*raftwire.ObserveRecordHoldingResponse, error) {
	s.received = true
	return nil, s.finalErr
}

func TestRecordTransferSendOwnsFrameAndPreservesEarlyRejection(t *testing.T) {
	payload := []byte("caller may reuse after transfer")
	frame := &raftwire.PutRecordRequest{Data: payload}
	stream := &retainingRecordClient{}
	if err := sendRecordFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	clear(payload)
	frame.Data = nil
	if string(stream.frame.Data) != "caller may reuse after transfer" {
		t.Fatal("retained gRPC message aliases caller memory")
	}
	rejected := status.Error(codes.ResourceExhausted, "full")
	stream.sendErr, stream.finalErr = io.EOF, rejected
	if err := sendRecordFrame(stream, frame); !errors.Is(err, rejected) || !stream.received {
		t.Fatal("early admission status lost", err)
	}
	stream.finalErr = nil
	if err := sendRecordFrame(stream, frame); !errors.Is(err, io.EOF) {
		t.Fatal("premature server success accepted", err)
	}
}

type putRecordStream struct {
	grpc.ServerStream
	ctx      context.Context
	frames   []*raftwire.PutRecordRequest
	response *raftwire.ObserveRecordHoldingResponse
	recv     func() (*raftwire.PutRecordRequest, error)
	ack      func(*raftwire.ObserveRecordHoldingResponse) error
}

func (s *putRecordStream) Context() context.Context { return s.ctx }
func (s *putRecordStream) Recv() (*raftwire.PutRecordRequest, error) {
	if s.recv != nil {
		return s.recv()
	}
	if len(s.frames) == 0 {
		return nil, io.EOF
	}
	f := s.frames[0]
	s.frames = s.frames[1:]
	return f, nil
}
func (s *putRecordStream) SendAndClose(r *raftwire.ObserveRecordHoldingResponse) error {
	if s.ack != nil {
		return s.ack(r)
	}
	s.response = r
	return nil
}

func transferFixture(t *testing.T) (*holdingFixture, *RecordTransferServer, records.Descriptor, []byte, *putRecordStream) {
	t.Helper()
	f := newHoldingFixture(t)
	s, err := NewRecordTransferServer(f.local, f.server.host, f.root, f.store, f.server.authorize)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("an opaque immutable transferred record")
	d, err := records.Describe(f.descriptor.Keyspace, f.descriptor.GroupID, []byte("transfer/new-key"), payload)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	h := proto.Clone(f.request).(*raftwire.ObserveRecordHoldingRequest)
	h.RecordDescriptor = encoded
	stream := &putRecordStream{ctx: f.ctx, frames: []*raftwire.PutRecordRequest{{Header: h}, {Data: payload}}}
	return f, s, d, payload, stream
}

func recordVolumeBytes(t *testing.T, path string) int64 {
	t.Helper()
	var n int64
	if err := filepath.WalkDir(path, func(_ string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.Type().IsRegular() {
			info, err := e.Info()
			if err != nil {
				return err
			}
			n += info.Size()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRecordTransferFramingAuthorizationAndPublication(t *testing.T) {
	for _, scenario := range []string{"normal", "empty-record", "no-header", "mixed-header", "second-header", "empty-frame", "short", "excess", "oversized-frame", "unknown-field", "bad-digest", "no-tls", "wrong-target", "wrong-cluster", "bad-descriptor", "budget", "busy", "canceled", "removed-before-io", "removed-after-io", "store-error", "store-corrupt", "wrong-holding"} {
		t.Run(scenario, func(t *testing.T) {
			f, s, d, payload, stream := transferFixture(t)
			want, writes := codes.OK, 0
			remove := func() {
				if err := f.root.ApplyMembership(f.root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{f.local.RaftID}, Learners: []uint64{f.other.RaftID}}); err != nil {
					t.Error(err)
				}
			}
			writer := recordWriteFunc(func(ctx context.Context, d records.Descriptor, b []byte) (records.Holding, error) {
				return f.store.Put(ctx, d, b)
			})
			expectWrite := false
			switch scenario {
			case "normal":
				expectWrite = true
			case "empty-record":
				payload = nil
				d, _ = records.Describe(d.Keyspace, d.GroupID, d.Key, nil)
				stream.frames[0].Header.RecordDescriptor, _ = json.Marshal(d)
				stream.frames = stream.frames[:1]
				expectWrite = true
			case "no-header":
				stream.frames = nil
				want = codes.InvalidArgument
			case "mixed-header":
				stream.frames[0].Data = payload
				want = codes.InvalidArgument
			case "second-header":
				stream.frames = append(stream.frames, stream.frames[0])
				want = codes.InvalidArgument
			case "empty-frame":
				stream.frames = append(stream.frames, &raftwire.PutRecordRequest{})
				want = codes.InvalidArgument
			case "short":
				stream.frames[1].Data = payload[:len(payload)-1]
				want = codes.InvalidArgument
			case "excess":
				stream.frames = append(stream.frames, &raftwire.PutRecordRequest{Data: []byte("x")})
				want = codes.InvalidArgument
			case "oversized-frame":
				stream.frames[1].Data = make([]byte, recordTransferFrameBytes+1)
				want = codes.InvalidArgument
			case "unknown-field":
				stream.frames[1].ProtoReflect().SetUnknown([]byte{0x18, 0x01})
				want = codes.InvalidArgument
			case "bad-digest":
				stream.frames[1].Data = bytes.Repeat([]byte("x"), len(payload))
				want = codes.DataLoss
			case "no-tls":
				stream.ctx = context.Background()
				want = codes.Unauthenticated
			case "wrong-target":
				stream.frames[0].Header.ToRaftId = f.other.RaftID
				want = codes.PermissionDenied
			case "wrong-cluster":
				stream.frames[0].Header.ClusterId = "other"
				want = codes.PermissionDenied
			case "bad-descriptor":
				stream.frames[0].Header.RecordDescriptor = []byte("{}")
				want = codes.InvalidArgument
			case "budget":
				s.buffers = resource.NewByteBudget(d.Size - 1)
				want = codes.ResourceExhausted
			case "busy":
				s.slots <- struct{}{}
				s.slots <- struct{}{}
				want = codes.ResourceExhausted
			case "canceled":
				ctx, cancel := context.WithCancel(f.ctx)
				cancel()
				stream.ctx = ctx
				want = codes.Canceled
			case "removed-before-io":
				frames := stream.frames
				stream.recv = func() (*raftwire.PutRecordRequest, error) {
					if len(frames) == 0 {
						remove()
						return nil, io.EOF
					}
					next := frames[0]
					frames = frames[1:]
					return next, nil
				}
				want = codes.PermissionDenied
			case "removed-after-io":
				writer = func(ctx context.Context, d records.Descriptor, b []byte) (records.Holding, error) {
					h, err := f.store.Put(ctx, d, b)
					remove()
					return h, err
				}
				want = codes.PermissionDenied
				expectWrite = true
			case "store-error":
				writer = func(context.Context, records.Descriptor, []byte) (records.Holding, error) {
					return records.Holding{}, errors.New("private-path/uncertain-sync")
				}
				want = codes.Unavailable
				expectWrite = true
			case "store-corrupt":
				writer = func(context.Context, records.Descriptor, []byte) (records.Holding, error) {
					return records.Holding{}, records.ErrCorrupt
				}
				want = codes.DataLoss
				expectWrite = true
			case "wrong-holding":
				writer = func(context.Context, records.Descriptor, []byte) (records.Holding, error) { return f.holding, nil }
				want = codes.FailedPrecondition
				expectWrite = true
			}
			s.store = recordWriteFunc(func(ctx context.Context, d records.Descriptor, b []byte) (records.Holding, error) {
				writes++
				return writer(ctx, d, b)
			})
			err := s.Put(stream)
			if status.Code(err) != want || (writes > 0) != expectWrite {
				t.Fatalf("code=%v want=%v writes=%d expected=%t: %v", status.Code(err), want, writes, expectWrite, err)
			}
			if s.buffers.Status().UsedBytes != 0 {
				t.Fatal("body reservation leaked")
			}
			if scenario != "busy" && len(s.slots) != 0 {
				t.Fatal("handler slot leaked")
			}
			if want == codes.OK {
				if _, err := decodeHoldingObservation(f.local.ClusterID, f.local.RaftID, d, stream.response); err != nil {
					t.Fatal(err)
				}
				got, _, err := f.store.Read(f.ctx, d)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("stored bytes differ", err)
				}
			} else if stream.response != nil {
				t.Fatal("failed transfer sent success")
			}
			if !expectWrite {
				if _, err := f.store.Verify(f.ctx, d); !errors.Is(err, records.ErrNotHeld) {
					t.Fatal("rejected transfer published", err)
				}
			}
		})
	}
}

func TestRecordTransferLostACKRetryDoesNotAppend(t *testing.T) {
	f, s, d, _, stream := transferFixture(t)
	frames := stream.frames
	lost := errors.New("connection lost after durable IO")
	stream.ack = func(r *raftwire.ObserveRecordHoldingResponse) error {
		if _, err := f.store.Verify(f.ctx, d); err != nil {
			t.Error("ACK before durable holding", err)
		}
		return lost
	}
	if err := s.Put(stream); !errors.Is(err, lost) {
		t.Fatal(err)
	}
	before := recordVolumeBytes(t, f.volumePath)
	stream.frames, stream.ack = frames, nil
	if err := s.Put(stream); err != nil {
		t.Fatal(err)
	}
	if recordVolumeBytes(t, f.volumePath) != before {
		t.Fatal("exact retry appended Pack bytes")
	}
	if _, err := decodeHoldingObservation(f.local.ClusterID, f.local.RaftID, d, stream.response); err != nil {
		t.Fatal(err)
	}
}

func TestRecordTransferTimeoutKeepsBlockedReceiversBounded(t *testing.T) {
	f, s, d, _, stream := transferFixture(t)
	s.headerTimeout, s.idle = 20*time.Millisecond, 20*time.Millisecond
	unblock := make(chan struct{})
	defer close(unblock)
	blocked := func() (*raftwire.PutRecordRequest, error) { <-unblock; return nil, io.EOF }
	for i := 0; i < cap(s.readers); i++ {
		st := &putRecordStream{ctx: f.ctx, recv: blocked}
		if err := s.Put(st); status.Code(err) != codes.DeadlineExceeded {
			t.Fatal(err)
		}
	}
	if len(s.readers) != cap(s.readers) {
		t.Fatal("blocked workers released early")
	}
	if err := s.Put(stream); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("unbounded receive admitted", err)
	}
	if _, err := f.store.Verify(f.ctx, d); !errors.Is(err, records.ErrNotHeld) {
		t.Fatal(err)
	}
}

func TestRecordTransferMissingEOFDoesNotPublish(t *testing.T) {
	f, s, d, _, stream := transferFixture(t)
	s.idle = 20 * time.Millisecond
	unblock := make(chan struct{})
	defer close(unblock)
	frames := stream.frames
	stream.recv = func() (*raftwire.PutRecordRequest, error) {
		if len(frames) == 0 {
			<-unblock
			return nil, io.EOF
		}
		next := frames[0]
		frames = frames[1:]
		return next, nil
	}
	if err := s.Put(stream); status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(err)
	}
	if _, err := f.store.Verify(f.ctx, d); !errors.Is(err, records.ErrNotHeld) {
		t.Fatal("missing EOF published", err)
	}
	if s.buffers.Status().UsedBytes != 0 {
		t.Fatal("staging reservation leaked")
	}
}

func TestRecordTransferProgressCannotExtendTotalLifetime(t *testing.T) {
	f, s, d, payload, stream := transferFixture(t)
	s.timeout, s.idle = 70*time.Millisecond, 50*time.Millisecond
	header := stream.frames[0]
	offset := -1
	unblock := make(chan struct{})
	defer close(unblock)
	stream.recv = func() (*raftwire.PutRecordRequest, error) {
		if offset == -1 {
			offset = 0
			return header, nil
		}
		select {
		case <-time.After(15 * time.Millisecond):
		case <-unblock:
			return nil, io.EOF
		}
		if offset == len(payload) {
			return nil, io.EOF
		}
		next := payload[offset : offset+1]
		offset++
		return &raftwire.PutRecordRequest{Data: next}, nil
	}
	if err := s.Put(stream); status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(err)
	}
	if _, err := f.store.Verify(f.ctx, d); !errors.Is(err, records.ErrNotHeld) {
		t.Fatal("expired transfer published", err)
	}
}

func TestRecordTransferBlockedIOOwnsSlotsAndBuffersUntilReturn(t *testing.T) {
	f, s, d, _, stream := transferFixture(t)
	frames := stream.frames
	entered := make(chan struct{}, recordTransferConcurrency)
	unblock := make(chan struct{})
	// Never detach a disk operation just because its context expires.
	s.store = recordWriteFunc(func(ctx context.Context, _ records.Descriptor, _ []byte) (records.Holding, error) {
		entered <- struct{}{}
		<-unblock
		return records.Holding{}, ctx.Err()
	})
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan error, recordTransferConcurrency)
	for i := 0; i < recordTransferConcurrency; i++ {
		go func() { done <- s.Put(&putRecordStream{ctx: ctx, frames: frames}) }()
	}
	for i := 0; i < recordTransferConcurrency; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(unblock)
			t.Fatal("write did not enter")
		}
	}
	cancel()
	if err := s.Put(&putRecordStream{ctx: f.ctx, frames: frames}); status.Code(err) != codes.ResourceExhausted {
		close(unblock)
		t.Fatal("blocked IO released capacity", err)
	}
	if s.buffers.Status().UsedBytes != uint64(recordTransferConcurrency)*d.Size {
		close(unblock)
		t.Fatal("blocked IO released staging bytes")
	}
	close(unblock)
	for i := 0; i < recordTransferConcurrency; i++ {
		select {
		case err := <-done:
			if status.Code(err) != codes.Canceled {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("write did not return")
		}
	}
	if len(s.slots) != 0 || s.buffers.Status().UsedBytes != 0 {
		t.Fatal("completed IO leaked capacity")
	}
}

type transferProxy struct {
	raftwire.UnimplementedRecordTransferServiceServer
	delegate *RecordTransferServer
	calls    atomic.Int32
}

func (s *transferProxy) Put(stream raftwire.RecordTransferService_PutServer) error {
	s.calls.Add(1)
	return s.delegate.Put(stream)
}

func TestRecordTransferMutualTLSPinsIdentityBeforeSending(t *testing.T) {
	for _, mode := range []string{"correct", "wrong-raft", "wrong-node"} {
		t.Run(mode, func(t *testing.T) {
			f, s, _, _, _ := transferFixture(t)
			certificate := f.certificates[f.local.RaftID]
			if mode == "wrong-raft" {
				certificate = f.certificates[f.other.RaftID]
			}
			if mode == "wrong-node" {
				wrong := f.local
				wrong.NodeID = fmt.Sprintf("node_%032x", 993)
				f.pool, f.certificates = holdingCertificates(t, wrong, f.caller)
				certificate = f.certificates[f.local.RaftID]
			}
			server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: f.pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
			t.Cleanup(server.Stop)
			proxy := &transferProxy{delegate: s}
			raftwire.RegisterRecordTransferServiceServer(server, proxy)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { listener.Close() })
			go func() { _ = server.Serve(listener) }()
			directory := NewStaticDirectory()
			if err := directory.Update(f.local.RaftID, listener.Addr().String()); err != nil {
				t.Fatal(err)
			}
			client, err := NewGroupClient(GroupClientOptions{ClusterID: f.local.ClusterID, RaftID: f.caller.RaftID, Directory: directory, Credentials: credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{f.certificates[f.caller.RaftID]}, RootCAs: f.pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { client.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			target := rootstate.Node{NodeID: f.local.NodeID, RaftID: f.local.RaftID}
			payload := bytes.Repeat([]byte("z"), recordTransferFrameBytes*2+1)
			d, _ := records.Describe(f.descriptor.Keyspace, f.descriptor.GroupID, []byte("network-record"), payload)
			got, err := client.PutRecord(ctx, target, d, payload)
			if mode != "correct" {
				if err == nil || proxy.calls.Load() != 0 {
					t.Fatal("wrong Node received application request", got, err, proxy.calls.Load())
				}
				return
			}
			if err != nil || got.NodeID != f.local.NodeID {
				t.Fatal(got, err)
			}
			body, holding, err := f.store.Read(ctx, d)
			if err != nil || !bytes.Equal(body, payload) || holding.RecordID != got.Holding.RecordID {
				t.Fatal("physical transfer mismatch", err)
			}
			before := recordVolumeBytes(t, f.volumePath)
			if _, err := client.PutRecord(ctx, target, d, payload); err != nil {
				t.Fatal(err)
			}
			if recordVolumeBytes(t, f.volumePath) != before {
				t.Fatal("network retry appended")
			}
			// A real gRPC peer sends the whole body but never EOF. The derived
			// idle timeout must return the handler, unblock transport Recv, and
			// reclaim both worker and body reservations without publication.
			s.idle = 20 * time.Millisecond
			remote, err := client.recordPeer(target)
			if err != nil {
				t.Fatal(err)
			}
			stalled, err := remote.Put(ctx)
			if err != nil {
				t.Fatal(err)
			}
			incomplete, _ := records.Describe(d.Keyspace, d.GroupID, []byte("no-eof"), []byte("body"))
			encoded, _ := json.Marshal(incomplete)
			if err := stalled.Send(&raftwire.PutRecordRequest{Header: &raftwire.ObserveRecordHoldingRequest{ClusterId: f.local.ClusterID, FromRaftId: f.caller.RaftID, ToRaftId: f.local.RaftID, RecordDescriptor: encoded}}); err != nil {
				t.Fatal(err)
			}
			if err := stalled.Send(&raftwire.PutRecordRequest{Data: []byte("body")}); err != nil {
				t.Fatal(err)
			}
			if err := stalled.RecvMsg(new(raftwire.ObserveRecordHoldingResponse)); status.Code(err) != codes.DeadlineExceeded {
				t.Fatal("real missing EOF was not bounded", err)
			}
			deadline := time.Now().Add(time.Second)
			for len(s.readers) != 0 || len(s.slots) != 0 || s.buffers.Status().UsedBytes != 0 {
				if time.Now().After(deadline) {
					t.Fatal("real canceled transfer leaked resources")
				}
				time.Sleep(time.Millisecond)
			}
			if _, err := f.store.Verify(ctx, incomplete); !errors.Is(err, records.ErrNotHeld) {
				t.Fatal("real missing EOF published", err)
			}
			if err := f.root.ApplyMembership(f.root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{f.local.RaftID}, Learners: []uint64{f.other.RaftID}}); err != nil {
				t.Fatal(err)
			}
			if _, err := client.PutRecord(ctx, target, d, payload); err == nil {
				t.Fatal("removed caller reused transfer connection")
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := client.PutRecord(ctx, target, d, payload); err == nil {
				t.Fatal("closed client transferred data")
			}
		})
	}
}
