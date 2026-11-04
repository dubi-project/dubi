package raftrpc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func holdingBatchFixture(t *testing.T, count int) (*holdingFixture, []records.Descriptor, *raftwire.ObserveRecordHoldingsRequest) {
	t.Helper()
	f := newHoldingFixture(t)
	req := &raftwire.ObserveRecordHoldingsRequest{ClusterId: f.local.ClusterID, FromRaftId: f.caller.RaftID, ToRaftId: f.local.RaftID}
	var ds []records.Descriptor
	for i := 0; i < count; i++ {
		d := f.descriptor
		d.Key = []byte(fmt.Sprintf("batch-%03d", i))
		if _, err := f.store.Put(context.Background(), d, []byte("data")); err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		ds = append(ds, d)
		req.RecordDescriptors = append(req.RecordDescriptors, b)
	}
	return f, ds, req
}

func TestHoldingBatchChecksEveryByteAndSharesOnlyRootAuthority(t *testing.T) {
	f, ds, req := holdingBatchFixture(t, 32)
	var reads atomic.Int32
	f.server.host = regionBarrierFunc(func(ctx context.Context, _ string) (uint64, error) {
		reads.Add(1)
		return f.root.AppliedIndex(), ctx.Err()
	})
	r, err := f.server.ObserveBatch(f.ctx, req)
	if err != nil || len(r.GetResults()) != 32 || reads.Load() != 2 {
		t.Fatal(r, err, reads.Load())
	}
	transport, _ := peer.FromContext(holdingPeerContext(t, f.pool, f.certificates[f.local.RaftID]))
	rows, err := decodeHoldingBatch(context.Background(), transport, f.local.ClusterID, f.local.RaftID, ds, r)
	if err != nil || len(rows) != 32 {
		t.Fatal(err, len(rows))
	}
	for i, row := range rows {
		id, _ := ds[i].ID()
		if row.Err != nil || row.Observation.Holding.RecordID != id {
			t.Fatal(i, row)
		}
	}
	cut := f.root.AppliedIndex()
	ctx := metadata.NewIncomingContext(f.ctx, metadata.Pairs(confirmedRootCutMetadataKey, fmt.Sprint(cut)))
	reads.Store(0)
	r, err = f.server.ObserveBatch(ctx, req)
	if err != nil || r.GetRootConfirmedIndex() != cut || reads.Load() != 0 {
		t.Fatal("sender cut was not reused", err, reads.Load())
	}
}

func TestHoldingBatchDoesNotLeakRowsAcrossAuthorizationFailures(t *testing.T) {
	for _, mode := range []string{"no-tls", "wrong-cluster", "wrong-target", "unknown-field", "empty", "duplicate", "count", "bytes", "no-quorum", "revoked-during-io", "bad-holding", "bad-cut"} {
		t.Run(mode, func(t *testing.T) {
			f, _, req := holdingBatchFixture(t, 2)
			want := codes.InvalidArgument
			switch mode {
			case "no-tls":
				f.ctx = context.Background()
				want = codes.Unauthenticated
			case "wrong-cluster":
				req.ClusterId = "other"
				want = codes.PermissionDenied
			case "wrong-target":
				req.ToRaftId++
				want = codes.PermissionDenied
			case "unknown-field":
				req.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
			case "empty":
				req.RecordDescriptors = nil
			case "duplicate":
				req.RecordDescriptors[1] = req.RecordDescriptors[0]
			case "count":
				for len(req.RecordDescriptors) <= 32 {
					req.RecordDescriptors = append(req.RecordDescriptors, req.RecordDescriptors[0])
				}
			case "bytes":
				var d records.Descriptor
				json.Unmarshal(req.RecordDescriptors[0], &d)
				d.Size = FetchRecordBatchBytes + 1
				req.RecordDescriptors[0], _ = json.Marshal(d)
			case "no-quorum":
				f.server.host = regionBarrierFunc(func(context.Context, string) (uint64, error) { return 0, errors.New("no quorum") })
				want = codes.Unavailable
			case "revoked-during-io":
				var once atomic.Bool
				f.server.store = holdingVerifyFunc(func(ctx context.Context, d records.Descriptor) (records.Holding, error) {
					if once.CompareAndSwap(false, true) {
						if err := f.root.ApplyMembership(f.root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{f.local.RaftID}, Learners: []uint64{f.other.RaftID}}); err != nil {
							return records.Holding{}, err
						}
					}
					return f.store.Verify(ctx, d)
				})
				want = codes.PermissionDenied
			case "bad-holding":
				f.server.store = holdingVerifyFunc(func(context.Context, records.Descriptor) (records.Holding, error) { return f.holding, nil })
				want = codes.FailedPrecondition
			case "bad-cut":
				f.ctx = metadata.NewIncomingContext(f.ctx, metadata.Pairs(confirmedRootCutMetadataKey, "invalid"))
			}
			r, err := f.server.ObserveBatch(f.ctx, req)
			if r != nil || status.Code(err) != want {
				t.Fatal("unsafe batch exposed rows", r, err, want)
			}
		})
	}
}

func TestHoldingBatchStorageFailureIsBoundToOneRow(t *testing.T) {
	f, ds, req := holdingBatchFixture(t, 3)
	f.server.store = holdingVerifyFunc(func(ctx context.Context, d records.Descriptor) (records.Holding, error) {
		if string(d.Key) == string(ds[1].Key) {
			return records.Holding{}, records.ErrCorrupt
		}
		return f.store.Verify(ctx, d)
	})
	r, err := f.server.ObserveBatch(f.ctx, req)
	if err != nil || len(r.Results) != 3 {
		t.Fatal(r, err)
	}
	if r.Results[0].Holding == nil || r.Results[2].Holding == nil || r.Results[1].Holding != nil || codes.Code(r.Results[1].ErrorCode) != codes.DataLoss {
		t.Fatal("row failure was lost or mixed with proof", r)
	}
}

func TestHoldingBatchRejectsWholeMalformedReply(t *testing.T) {
	f, ds, req := holdingBatchFixture(t, 3)
	r, err := f.server.ObserveBatch(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	transport, _ := peer.FromContext(holdingPeerContext(t, f.pool, f.certificates[f.local.RaftID]))
	for _, mode := range []string{"count", "node", "root", "row-cut", "order", "mixed-error", "bad-error", "empty-proof", "unknown-row", "unknown-response", "wrong-cert", "stale-cut"} {
		t.Run(mode, func(t *testing.T) {
			reply := proto.Clone(r).(*raftwire.ObserveRecordHoldingsResponse)
			ctx := context.Background()
			actual := transport
			switch mode {
			case "count":
				reply.Results = reply.Results[:2]
			case "node":
				reply.NodeId = f.other.NodeID
			case "root":
				reply.RootConfirmedIndex = 0
			case "row-cut":
				reply.Results[1].Holding.RootConfirmedIndex++
			case "order":
				reply.Results[0], reply.Results[1] = reply.Results[1], reply.Results[0]
			case "mixed-error":
				reply.Results[1].ErrorCode = uint32(codes.NotFound)
				reply.Results[1].Error = "not held"
			case "bad-error":
				reply.Results[1] = &raftwire.ObserveRecordHoldingResult{ErrorCode: uint32(codes.PermissionDenied), Error: "fenced"}
			case "empty-proof":
				reply.Results[1] = &raftwire.ObserveRecordHoldingResult{}
			case "unknown-row":
				reply.Results[1].ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
			case "unknown-response":
				reply.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
			case "wrong-cert":
				actual, _ = peer.FromContext(holdingPeerContext(t, f.pool, f.certificates[f.other.RaftID]))
			case "stale-cut":
				ctx = router.WithConfirmedRootIndex(ctx, reply.RootConfirmedIndex+1)
			}
			rows, err := decodeHoldingBatch(ctx, actual, f.local.ClusterID, f.local.RaftID, ds, reply)
			if err == nil || rows != nil {
				t.Fatal("partial or forged batch was exposed", rows, err)
			}
		})
	}
}

type holdingBatchProxy struct {
	raftwire.UnimplementedRecordHoldingServiceServer
	delegate *RecordHoldingServer
	legacy   bool
	deny     bool
	single   atomic.Int32
}

func (p *holdingBatchProxy) Observe(ctx context.Context, r *raftwire.ObserveRecordHoldingRequest) (*raftwire.ObserveRecordHoldingResponse, error) {
	p.single.Add(1)
	return p.delegate.Observe(ctx, r)
}
func (p *holdingBatchProxy) ObserveBatch(ctx context.Context, r *raftwire.ObserveRecordHoldingsRequest) (*raftwire.ObserveRecordHoldingsResponse, error) {
	if p.legacy {
		return nil, status.Error(codes.Unimplemented, "old peer")
	}
	if p.deny {
		return nil, status.Error(codes.PermissionDenied, "fenced")
	}
	return p.delegate.ObserveBatch(ctx, r)
}
func batchHoldingClient(t *testing.T, f *holdingFixture, p *holdingBatchProxy) *GroupClient {
	t.Helper()
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{f.certificates[f.local.RaftID]}, ClientCAs: f.pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
	t.Cleanup(server.Stop)
	raftwire.RegisterRecordHoldingServiceServer(server, p)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go server.Serve(listener)
	directory := NewStaticDirectory()
	if err := directory.Update(f.local.RaftID, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	c, err := NewGroupClient(GroupClientOptions{ClusterID: f.local.ClusterID, RaftID: f.caller.RaftID, Directory: directory, Credentials: credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{f.certificates[f.caller.RaftID]}, RootCAs: f.pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func TestHoldingBatchMutualTLSAndExplicitLegacyCapability(t *testing.T) {
	for _, mode := range []string{"current", "legacy", "denied", "wrong-certificate"} {
		t.Run(mode, func(t *testing.T) {
			f, ds, _ := holdingBatchFixture(t, 32)
			if mode == "wrong-certificate" {
				wrong := f.local
				wrong.NodeID = f.other.NodeID
				f.pool, f.certificates = holdingCertificates(t, f.caller, wrong)
			}
			proxy := &holdingBatchProxy{delegate: f.server, legacy: mode == "legacy", deny: mode == "denied"}
			c := batchHoldingClient(t, f, proxy)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ctx = router.WithConfirmedRootIndex(ctx, f.root.AppliedIndex())
			rows, err := c.ObserveRecordHoldings(ctx, f.local.RaftID, ds)
			switch mode {
			case "current":
				if err != nil || len(rows) != 32 {
					t.Fatal(rows, err)
				}
			case "legacy":
				if status.Code(err) != codes.Unimplemented || rows != nil {
					t.Fatal(rows, err)
				}
			case "denied":
				if status.Code(err) != codes.PermissionDenied || rows != nil {
					t.Fatal(rows, err)
				}
			default:
				if err == nil || rows != nil {
					t.Fatal("wrong TLS Node accepted", rows, err)
				}
			}
			if proxy.single.Load() != 0 {
				t.Fatal("batch client silently retried individual requests")
			}
		})
	}
}

func TestHoldingBatchSharesSlotsAndRetainsBlockedIO(t *testing.T) {
	f, _, req := holdingBatchFixture(t, 1)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	f.server.store = holdingVerifyFunc(func(ctx context.Context, d records.Descriptor) (records.Holding, error) {
		entered <- struct{}{}
		<-release
		return f.store.Verify(ctx, d)
	})
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan error, 2)
	go func() { _, err := f.server.ObserveBatch(ctx, req); done <- err }()
	go func() {
		_, err := f.server.Observe(ctx, &raftwire.ObserveRecordHoldingRequest{ClusterId: req.ClusterId, FromRaftId: req.FromRaftId, ToRaftId: req.ToRaftId, RecordDescriptor: req.RecordDescriptors[0]})
		done <- err
	}()
	defer func() {
		close(release)
		for i := 0; i < 2; i++ {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("blocked IO did not exit")
			}
		}
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("shared IO did not start")
		}
	}
	cancel()
	if len(f.server.slots) != 2 {
		t.Fatal("cancellation released owned storage work")
	}
	f.server.timeout = 20 * time.Millisecond
	if r, err := f.server.ObserveBatch(f.ctx, req); r != nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatal("batch exceeded shared slots", r, err)
	}
	select {
	case err := <-done:
		t.Fatal("handler returned with IO owned", err)
	default:
	}
}
