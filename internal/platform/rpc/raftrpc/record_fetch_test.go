package raftrpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
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

type recordReadFunc func(context.Context, records.Descriptor) ([]byte, records.Holding, error)

func (f recordReadFunc) Read(ctx context.Context, d records.Descriptor) ([]byte, records.Holding, error) {
	return f(ctx, d)
}

type getRecordStream struct {
	grpc.ServerStream
	ctx    context.Context
	frames []*raftwire.GetRecordResponse
	send   func(*raftwire.GetRecordResponse) error
}

func (s *getRecordStream) Context() context.Context { return s.ctx }
func (s *getRecordStream) Send(frame *raftwire.GetRecordResponse) error {
	if s.send != nil {
		return s.send(frame)
	}
	s.frames = append(s.frames, frame)
	return nil
}

func TestRecordFetchServerChecksPhysicalBytesAndAuthorization(t *testing.T) {
	for _, mode := range []string{"normal", "nil-request", "wrong-target", "wrong-cluster", "unknown-field", "invalid-descriptor", "no-tls", "missing", "corrupt", "closed-store", "lost-volume", "storage-error", "bad-bytes", "wrong-holding", "removed-during-read", "removed-during-unavailable-read", "removed-during-send", "budget", "shared-slots", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			f, s, _, _, _ := transferFixture(t)
			req := proto.Clone(f.request).(*raftwire.ObserveRecordHoldingRequest)
			stream := &getRecordStream{ctx: f.ctx}
			want := codes.OK
			remove := func() {
				if err := f.root.ApplyMembership(f.root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{f.local.RaftID}, Learners: []uint64{f.other.RaftID}}); err != nil {
					t.Error(err)
				}
			}
			switch mode {
			case "nil-request":
				req = nil
				want = codes.InvalidArgument
			case "wrong-target":
				req.ToRaftId = f.other.RaftID
				want = codes.PermissionDenied
			case "wrong-cluster":
				req.ClusterId = "other"
				want = codes.PermissionDenied
			case "unknown-field":
				req.ProtoReflect().SetUnknown([]byte{0x28, 0x01})
				want = codes.InvalidArgument
			case "invalid-descriptor":
				req.RecordDescriptor = []byte("{}")
				want = codes.InvalidArgument
			case "no-tls":
				stream.ctx = context.Background()
				want = codes.Unauthenticated
			case "missing":
				d := f.descriptor
				d.Key = []byte("missing")
				req.RecordDescriptor, _ = json.Marshal(d)
				want = codes.NotFound
			case "corrupt":
				s.readStore = recordReadFunc(func(context.Context, records.Descriptor) ([]byte, records.Holding, error) {
					return nil, records.Holding{}, records.ErrCorrupt
				})
				want = codes.DataLoss
			case "closed-store":
				if err := f.store.Close(); err != nil {
					t.Fatal(err)
				}
				want = codes.Unavailable
			case "lost-volume":
				if err := os.Rename(f.volumePath, f.volumePath+".missing"); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.Rename(f.volumePath+".missing", f.volumePath); err != nil {
						t.Error(err)
					}
				}()
				want = codes.Unavailable
			case "storage-error":
				s.readStore = recordReadFunc(func(context.Context, records.Descriptor) ([]byte, records.Holding, error) {
					return nil, records.Holding{}, errors.New("private storage path")
				})
				want = codes.FailedPrecondition
			case "bad-bytes":
				s.readStore = recordReadFunc(func(context.Context, records.Descriptor) ([]byte, records.Holding, error) {
					return []byte("bad!"), f.holding, nil
				})
				want = codes.DataLoss
			case "wrong-holding":
				s.readStore = recordReadFunc(func(context.Context, records.Descriptor) ([]byte, records.Holding, error) {
					h := f.holding
					h.RecordID = "wrong"
					return []byte("data"), h, nil
				})
				want = codes.FailedPrecondition
			case "removed-during-read":
				s.readStore = recordReadFunc(func(ctx context.Context, d records.Descriptor) ([]byte, records.Holding, error) {
					b, h, err := f.store.Read(ctx, d)
					remove()
					return b, h, err
				})
				want = codes.PermissionDenied
			case "removed-during-unavailable-read":
				s.readStore = recordReadFunc(func(context.Context, records.Descriptor) ([]byte, records.Holding, error) {
					remove()
					return nil, records.Holding{}, records.ErrUnavailable
				})
				want = codes.PermissionDenied
			case "removed-during-send":
				stream.send = func(frame *raftwire.GetRecordResponse) error {
					if len(frame.Data) != 0 {
						remove()
					}
					return nil
				}
				want = codes.PermissionDenied
			case "budget":
				s.buffers = resource.NewByteBudget(1)
				want = codes.ResourceExhausted
			case "shared-slots":
				s.slots <- struct{}{}
				s.slots <- struct{}{}
				want = codes.ResourceExhausted
			case "canceled":
				ctx, cancel := context.WithCancel(f.ctx)
				cancel()
				stream.ctx = ctx
				want = codes.Canceled
			}
			if err := s.Get(req, stream); status.Code(err) != want {
				t.Fatal("unexpected Get status", err, want)
			}
			if s.buffers.Status().UsedBytes != 0 {
				t.Fatal("body reservation leaked")
			}
			if mode != "shared-slots" && len(s.slots) != 0 {
				t.Fatal("handler slot leaked")
			}
			if want == codes.OK {
				if len(stream.frames) != 2 || stream.frames[0].Header == nil || string(stream.frames[1].Data) != "data" {
					t.Fatal("bad record stream", stream.frames)
				}
				if _, err := decodeHoldingObservation(f.local.ClusterID, f.local.RaftID, f.descriptor, stream.frames[0].Header); err != nil {
					t.Fatal(err)
				}
			} else if len(stream.frames) != 0 {
				t.Fatal("unverified bytes were sent")
			}
		})
	}
}

func TestRecordFetchSendOwnsFrameAndBlockedWorkersStayBounded(t *testing.T) {
	f, s, _, _, _ := transferFixture(t)
	stream := &getRecordStream{ctx: f.ctx}
	data := []byte("owned")
	if err := s.send(f.ctx, stream, &raftwire.GetRecordResponse{Data: data}); err != nil {
		t.Fatal(err)
	}
	clear(data)
	if string(stream.frames[0].Data) != "owned" {
		t.Fatal("sent frame aliases body")
	}
	s.idle = 20 * time.Millisecond
	unblock := make(chan struct{})
	defer close(unblock)
	stream.send = func(*raftwire.GetRecordResponse) error { <-unblock; return nil }
	for i := 0; i < cap(s.writers); i++ {
		if err := s.send(f.ctx, stream, &raftwire.GetRecordResponse{Data: []byte("x")}); status.Code(err) != codes.DeadlineExceeded {
			t.Fatal(err)
		}
	}
	if len(s.writers) != cap(s.writers) {
		t.Fatal("blocked Send released its lease")
	}
	if err := s.send(f.ctx, stream, &raftwire.GetRecordResponse{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("unbounded send worker", err)
	}
}

type getProxy struct {
	raftwire.UnimplementedRecordTransferServiceServer
	get   func(*raftwire.ObserveRecordHoldingRequest, raftwire.RecordTransferService_GetServer) error
	calls atomic.Int32
}

func (s *getProxy) Get(r *raftwire.ObserveRecordHoldingRequest, st raftwire.RecordTransferService_GetServer) error {
	s.calls.Add(1)
	return s.get(r, st)
}

func recordFetchClient(t *testing.T, f *holdingFixture, certificate tls.Certificate, service raftwire.RecordTransferServiceServer) *GroupClient {
	t.Helper()
	server := grpc.NewServer(grpc.WaitForHandlers(true), grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: f.pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
	t.Cleanup(server.Stop)
	raftwire.RegisterRecordTransferServiceServer(server, service)
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
	return client
}

func TestRecordFetchMutualTLSAndMalformedStreamsNeverPublish(t *testing.T) {
	for _, mode := range []string{"normal", "empty", "wrong-raft", "wrong-node", "no-header", "mixed-header", "wrong-record", "short", "extra", "empty-frame", "second-header", "unknown-field", "oversized-frame", "wrong-digest", "final-error", "no-final-status", "no-first-frame", "progress-exceeds-total", "destination-error", "bad-local-holding", "client-budget"} {
		t.Run(mode, func(t *testing.T) {
			f, s, _, _, _ := transferFixture(t)
			destination := newHoldingFixture(t)
			payload := bytes.Repeat([]byte("r"), recordTransferFrameBytes*2+1)
			if mode == "empty" {
				payload = nil
			}
			d, err := records.Describe(f.descriptor.Keyspace, f.descriptor.GroupID, []byte("fetch-new"), payload)
			if err != nil {
				t.Fatal(err)
			}
			h, err := f.store.Put(f.ctx, d, payload)
			if err != nil {
				t.Fatal(err)
			}
			certificate := f.certificates[f.local.RaftID]
			if mode == "wrong-raft" {
				certificate = f.certificates[f.other.RaftID]
			}
			if mode == "wrong-node" {
				wrong := f.local
				wrong.NodeID = fmt.Sprintf("node_%032x", 994)
				f.pool, f.certificates = holdingCertificates(t, wrong, f.caller)
				certificate = f.certificates[f.local.RaftID]
			}
			proxy := &getProxy{}
			proxy.get = func(req *raftwire.ObserveRecordHoldingRequest, stream raftwire.RecordTransferService_GetServer) error {
				if mode == "normal" || mode == "empty" || mode == "wrong-raft" || mode == "wrong-node" || mode == "destination-error" || mode == "bad-local-holding" {
					return s.Get(req, stream)
				}
				if mode == "no-header" {
					return nil
				}
				if mode == "no-first-frame" {
					<-stream.Context().Done()
					return stream.Context().Err()
				}
				header, err := s.auth.holdingResponse(d, h, f.root.AppliedIndex())
				if err != nil {
					return err
				}
				first := &raftwire.GetRecordResponse{Header: header}
				if mode == "mixed-header" {
					first.Data = []byte("x")
				}
				if mode == "wrong-record" {
					first.Header.RecordId = f.holding.RecordID
				}
				if err := stream.Send(first); err != nil {
					return err
				}
				if mode == "progress-exceeds-total" {
					for _, b := range payload {
						select {
						case <-time.After(20 * time.Millisecond):
						case <-stream.Context().Done():
							return stream.Context().Err()
						}
						if err := stream.Send(&raftwire.GetRecordResponse{Data: []byte{b}}); err != nil {
							return err
						}
					}
					return nil
				}
				if mode == "oversized-frame" {
					return stream.Send(&raftwire.GetRecordResponse{Data: make([]byte, recordTransferFrameBytes+1)})
				}
				body := payload
				if mode == "short" {
					body = body[:len(body)-1]
				}
				if mode == "wrong-digest" {
					body = bytes.Repeat([]byte("z"), len(body))
				}
				for offset := 0; offset < len(body); offset += recordTransferFrameBytes {
					frame := &raftwire.GetRecordResponse{Data: body[offset:min(offset+recordTransferFrameBytes, len(body))]}
					if mode == "unknown-field" {
						frame.ProtoReflect().SetUnknown([]byte{0x18, 0x01})
					}
					if err := stream.Send(frame); err != nil {
						return err
					}
				}
				switch mode {
				case "extra":
					return stream.Send(&raftwire.GetRecordResponse{Data: []byte("x")})
				case "empty-frame":
					return stream.Send(&raftwire.GetRecordResponse{})
				case "second-header":
					return stream.Send(first)
				case "final-error":
					return status.Error(codes.PermissionDenied, "revoked before EOF")
				case "no-final-status":
					<-stream.Context().Done()
					return stream.Context().Err()
				}
				return nil
			}
			client := recordFetchClient(t, f, certificate, proxy)
			if mode == "client-budget" {
				client.recordBuffers = resource.NewByteBudget(1)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			target := rootstate.Node{NodeID: f.local.NodeID, RaftID: f.local.RaftID}
			writer := recordWriter(destination.store)
			if mode == "destination-error" {
				writer = recordWriteFunc(func(context.Context, records.Descriptor, []byte) (records.Holding, error) {
					return records.Holding{}, errors.New("uncertain destination Sync")
				})
			}
			if mode == "bad-local-holding" {
				writer = recordWriteFunc(func(context.Context, records.Descriptor, []byte) (records.Holding, error) {
					return destination.holding, nil
				})
			}
			limits := recordFetchLimits{2 * time.Second, 2 * time.Second, 4 * time.Second}
			if mode == "no-first-frame" {
				limits.header = 100 * time.Millisecond
			}
			if mode == "no-final-status" {
				limits.idle = 100 * time.Millisecond
			}
			if mode == "progress-exceeds-total" {
				limits.total = 250 * time.Millisecond
			}
			got, err := client.fetchRecord(ctx, target, d, writer, limits)
			if mode == "normal" || mode == "empty" {
				if err != nil || got.Source.NodeID != f.local.NodeID || got.Local.StoreID == got.Source.Holding.StoreID {
					t.Fatal("fetch failed", got, err)
				}
				body, local, err := destination.store.Read(ctx, d)
				if err != nil || !bytes.Equal(body, payload) || local.RecordID != got.Local.RecordID {
					t.Fatal("bad local bytes", err)
				}
				before := recordVolumeBytes(t, destination.volumePath)
				if _, err := client.FetchRecord(ctx, target, d, destination.store); err != nil {
					t.Fatal(err)
				}
				if recordVolumeBytes(t, destination.volumePath) != before {
					t.Fatal("fetch retry appended")
				}
			} else {
				if err == nil {
					t.Fatal("bad stream accepted", mode)
				}
				if _, err := destination.store.Verify(ctx, d); !errors.Is(err, records.ErrNotHeld) {
					t.Fatal("invalid fetch published", err)
				}
				if (mode == "wrong-raft" || mode == "wrong-node") && proxy.calls.Load() != 0 {
					t.Fatal("wrong TLS endpoint received application request")
				}
				if mode != "wrong-raft" && mode != "wrong-node" && mode != "client-budget" && proxy.calls.Load() != 1 {
					t.Fatal("malformed-stream test never reached endpoint")
				}
				if (mode == "no-first-frame" || mode == "no-final-status" || mode == "progress-exceeds-total") && status.Code(err) != codes.DeadlineExceeded {
					t.Fatal("idle timeout did not cause rejection", err)
				}
				if mode == "final-error" && status.Code(err) != codes.PermissionDenied {
					t.Fatal("final stream status lost", err)
				}
				if mode == "client-budget" && (status.Code(err) != codes.ResourceExhausted || proxy.calls.Load() != 0) {
					t.Fatal("fetch budget did not reject before IO", err)
				}
			}
			deadline := time.Now().Add(time.Second)
			for len(client.recordReaders) != 0 || len(client.recordSlots) != 0 || client.recordBuffers.Status().UsedBytes != 0 {
				if time.Now().After(deadline) {
					t.Fatal("fetch client leaked resources")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestRecordFetchInterruptedStreamRetriesWithoutPartialPublication(t *testing.T) {
	f, source, _, _, _ := transferFixture(t)
	destination := newHoldingFixture(t)
	body := bytes.Repeat([]byte("interrupted-record-frame-"), recordTransferFrameBytes/25+1)
	d, err := records.Describe(f.descriptor.Keyspace, f.descriptor.GroupID, []byte("interrupted-fetch"), body)
	if err != nil {
		t.Fatal(err)
	}
	h, err := f.store.Put(f.ctx, d, body)
	if err != nil {
		t.Fatal(err)
	}
	proxy := &getProxy{}
	proxy.get = func(req *raftwire.ObserveRecordHoldingRequest, stream raftwire.RecordTransferService_GetServer) error {
		if proxy.calls.Load() != 1 {
			return source.Get(req, stream)
		}
		header, err := source.auth.holdingResponse(d, h, f.root.AppliedIndex())
		if err != nil {
			return err
		}
		if err := stream.Send(&raftwire.GetRecordResponse{Header: header}); err != nil {
			return err
		}
		if err := stream.Send(&raftwire.GetRecordResponse{Data: body[:recordTransferFrameBytes]}); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "source stream interrupted")
	}
	client := recordFetchClient(t, f, f.certificates[f.local.RaftID], proxy)
	target := rootstate.Node{NodeID: f.local.NodeID, RaftID: f.local.RaftID}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := client.FetchRecord(ctx, target, d, destination.store); status.Code(err) != codes.Unavailable {
		t.Fatal("interrupted stream did not report a retryable source failure", err)
	}
	if _, err := destination.store.Verify(ctx, d); !errors.Is(err, records.ErrNotHeld) {
		t.Fatal("partial stream published a destination holding", err)
	}
	if _, err := client.FetchRecord(ctx, target, d, destination.store); err != nil {
		t.Fatal("same descriptor did not recover after interrupted source stream", err)
	}
	read, _, err := destination.store.Read(ctx, d)
	if err != nil || !bytes.Equal(read, body) || proxy.calls.Load() != 2 {
		t.Fatal("retry did not persist the exact complete record", err, proxy.calls.Load())
	}
	before := recordVolumeBytes(t, destination.volumePath)
	if _, err := client.FetchRecord(ctx, target, d, destination.store); err != nil {
		t.Fatal("repeat fetch after recovery", err)
	}
	if recordVolumeBytes(t, destination.volumePath) != before {
		t.Fatal("repeat fetch appended another physical Pack record")
	}
}

func TestRecordRepairFetchRelocatesOnlyAfterAuthenticatedCompleteStream(t *testing.T) {
	f, source, _, _, _ := transferFixture(t)
	destination := newHoldingFixture(t)
	d := f.descriptor
	if _, err := destination.store.Verify(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(destination.volumePath, destination.volumePath+".offline"); err != nil {
		t.Fatal(err)
	}
	if _, err := destination.store.Verify(context.Background(), d); !errors.Is(err, records.ErrUnavailable) {
		t.Fatal("lost destination Pack still counted as held", err)
	}
	replacement, err := destination.volumes.Add(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	proxy := &getProxy{}
	proxy.get = func(req *raftwire.ObserveRecordHoldingRequest, stream raftwire.RecordTransferService_GetServer) error {
		if proxy.calls.Load() != 2 {
			return source.Get(req, stream)
		}
		header, err := source.auth.holdingResponse(d, f.holding, f.root.AppliedIndex())
		if err != nil {
			return err
		}
		if err := stream.Send(&raftwire.GetRecordResponse{Header: header}); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "source stream stopped before body")
	}
	client := recordFetchClient(t, f, f.certificates[f.local.RaftID], proxy)
	target := rootstate.Node{NodeID: f.local.NodeID, RaftID: f.local.RaftID}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.FetchRecord(ctx, target, d, destination.store); !errors.Is(err, records.ErrUnavailable) {
		t.Fatal("ordinary fetch silently replaced an existing bad location", err)
	}
	if _, err := client.RepairRecord(ctx, target, d, destination.store); status.Code(err) != codes.Unavailable {
		t.Fatal("partial authenticated stream was accepted for relocation", err)
	}
	if _, err := destination.store.Verify(ctx, d); !errors.Is(err, records.ErrUnavailable) {
		t.Fatal("partial stream replaced the old locator", err)
	}
	result, err := client.RepairRecord(ctx, target, d, destination.store)
	if err != nil || result.Source.NodeID != f.local.NodeID || result.Local.VolumeID != replacement.ID {
		t.Fatal("authenticated repair did not publish healthy replacement", result, err)
	}
	if _, err := destination.store.Verify(ctx, d); err != nil {
		t.Fatal("replacement failed physical verification", err)
	}
	if _, err := client.RepairRecord(ctx, target, d, destination.store); err != nil || proxy.calls.Load() != 4 {
		t.Fatal("idempotent authenticated repair retry failed", err, proxy.calls.Load())
	}
}

type blockedGetClient struct {
	grpc.ClientStream
	unblock <-chan struct{}
}

func (s *blockedGetClient) Recv() (*raftwire.GetRecordResponse, error) {
	<-s.unblock
	return nil, io.EOF
}
func TestRecordFetchBlockedReceiversRemainBounded(t *testing.T) {
	c := &GroupClient{recordReaders: make(chan struct{}, recordTransferConcurrency)}
	unblock := make(chan struct{})
	defer close(unblock)
	stream := &blockedGetClient{unblock: unblock}
	for i := 0; i < cap(c.recordReaders); i++ {
		if _, err := c.receiveRecord(context.Background(), stream, 20*time.Millisecond); status.Code(err) != codes.DeadlineExceeded {
			t.Fatal(err)
		}
	}
	if _, err := c.receiveRecord(context.Background(), stream, time.Second); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("unbounded receive workers", err)
	}
}

func TestRecordFetchSlowNetworkReaderReclaimsServerResources(t *testing.T) {
	f, s, _, _, _ := transferFixture(t)
	s.idle = 50 * time.Millisecond
	s.timeout = 2 * time.Second
	body := bytes.Repeat([]byte("q"), 8<<20)
	d, err := records.Describe(f.descriptor.Keyspace, f.descriptor.GroupID, []byte("slow-reader"), body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Put(f.ctx, d, body); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	proxy := &getProxy{get: func(req *raftwire.ObserveRecordHoldingRequest, stream raftwire.RecordTransferService_GetServer) error {
		err := s.Get(req, stream)
		done <- err
		return err
	}}
	client := recordFetchClient(t, f, f.certificates[f.local.RaftID], proxy)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	remote, err := client.recordPeer(rootstate.Node{NodeID: f.local.NodeID, RaftID: f.local.RaftID})
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(d)
	// Do not consume even the header. HTTP/2 must eventually backpressure the
	// server, without requiring this client to cancel or close the connection.
	_, err = remote.Get(ctx, &raftwire.ObserveRecordHoldingRequest{ClusterId: f.local.ClusterID, FromRaftId: f.caller.RaftID, ToRaftId: f.local.RaftID, RecordDescriptor: encoded})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatal("slow reader not bounded", err)
		}
	case <-ctx.Done():
		t.Fatal("server waited for client deadline")
	}
	deadline := time.Now().Add(time.Second)
	for len(s.writers) != 0 || len(s.slots) != 0 || s.buffers.Status().UsedBytes != 0 {
		if time.Now().After(deadline) {
			t.Fatal("slow reader retained server resources")
		}
		time.Sleep(time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("client cancellation was needed for cleanup")
	}
}

func TestRecordFetchDestinationIOKeepsAdmissionAndAmbiguousDurableBytes(t *testing.T) {
	f, s, _, _, _ := transferFixture(t)
	destination := newHoldingFixture(t)
	client := recordFetchClient(t, f, f.certificates[f.local.RaftID], s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{}, recordTransferConcurrency)
	unblock := make(chan struct{})
	writer := recordWriteFunc(func(ctx context.Context, d records.Descriptor, b []byte) (records.Holding, error) {
		h, err := destination.store.Put(ctx, d, b)
		entered <- struct{}{}
		<-unblock
		return h, err
	})
	done := make(chan error, recordTransferConcurrency)
	target := rootstate.Node{NodeID: f.local.NodeID, RaftID: f.local.RaftID}
	for i := 0; i < recordTransferConcurrency; i++ {
		go func() {
			_, err := client.fetchRecord(ctx, target, f.descriptor, writer, recordFetchLimits{time.Second, time.Second, 5 * time.Second})
			done <- err
		}()
	}
	for i := 0; i < recordTransferConcurrency; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(unblock)
			t.Fatal("destination IO not entered")
		}
	}
	cancel()
	if len(client.recordSlots) != recordTransferConcurrency || client.recordBuffers.Status().UsedBytes != uint64(recordTransferConcurrency)*f.descriptor.Size {
		close(unblock)
		t.Fatal("canceled IO released capacity early")
	}
	queuedCtx, cancelQueued := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, putErr := client.PutRecord(queuedCtx, target, f.descriptor, []byte("data"))
	cancelQueued()
	if status.Code(putErr) != codes.DeadlineExceeded || len(client.recordWaiters) != 0 {
		close(unblock)
		t.Fatal("Put bypassed shared fetch slots or leaked a timed-out waiter", putErr)
	}
	close(unblock)
	for i := 0; i < recordTransferConcurrency; i++ {
		select {
		case err := <-done:
			if status.Code(err) != codes.Canceled {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("destination IO did not exit")
		}
	}
	if _, err := destination.store.Verify(context.Background(), f.descriptor); err != nil {
		t.Fatal("cancellation erased already durable bytes", err)
	}
	if len(client.recordSlots) != 0 || client.recordBuffers.Status().UsedBytes != 0 {
		t.Fatal("destination IO leaked capacity")
	}
}

func TestRecordFetchSourceIOKeepsSharedAdmission(t *testing.T) {
	f, s, _, _, _ := transferFixture(t)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	entered := make(chan struct{}, recordTransferConcurrency)
	unblock := make(chan struct{})
	s.readStore = recordReadFunc(func(ctx context.Context, _ records.Descriptor) ([]byte, records.Holding, error) {
		entered <- struct{}{}
		<-unblock
		return nil, records.Holding{}, ctx.Err()
	})
	done := make(chan error, recordTransferConcurrency)
	for i := 0; i < recordTransferConcurrency; i++ {
		go func() { done <- s.Get(f.request, &getRecordStream{ctx: ctx}) }()
	}
	for i := 0; i < recordTransferConcurrency; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(unblock)
			t.Fatal("source IO not entered")
		}
	}
	cancel()
	if len(s.slots) != recordTransferConcurrency || s.buffers.Status().UsedBytes != uint64(recordTransferConcurrency)*f.descriptor.Size {
		close(unblock)
		t.Fatal("source IO released capacity early")
	}
	if err := s.Put(&putRecordStream{ctx: f.ctx}); status.Code(err) != codes.ResourceExhausted {
		close(unblock)
		t.Fatal("Put bypassed shared Get slots", err)
	}
	close(unblock)
	for i := 0; i < recordTransferConcurrency; i++ {
		select {
		case err := <-done:
			if status.Code(err) != codes.Canceled {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("source IO did not exit")
		}
	}
	if len(s.slots) != 0 || s.buffers.Status().UsedBytes != 0 {
		t.Fatal("source IO leaked capacity")
	}
}
