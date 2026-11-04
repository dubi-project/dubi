package raftrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecordRepairBatchRetainsIndexesUntilCompleteAuthenticatedStream(t *testing.T) {
	for _, mode := range []string{"normal", "missing-index", "final-denied", "partial-unimplemented", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			f, server, _, _, _ := transferFixture(t)
			ds := batchDescriptors(t, f, 3)
			destination := newHoldingFixture(t)
			if mode != "missing-index" {
				for _, d := range ds {
					body, _, err := f.store.Read(f.ctx, d)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := destination.store.Put(f.ctx, d, body); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.Rename(destination.volumePath, destination.volumePath+".offline"); err != nil {
					t.Fatal(err)
				}
				if _, err := destination.volumes.Add(t.TempDir()); err != nil {
					t.Fatal(err)
				}
			}
			proxy := &batchGetProxy{get: server.GetBatch}
			if mode == "final-denied" || mode == "partial-unimplemented" {
				proxy.get = func(req *raftwire.GetRecordBatchRequest, stream raftwire.RecordTransferService_GetBatchServer) error {
					captured := &getRecordStream{ctx: stream.Context()}
					if err := server.GetBatch(req, captured); err != nil {
						return err
					}
					for i, frame := range captured.frames {
						if err := stream.Send(frame); err != nil {
							return err
						}
						if mode == "partial-unimplemented" && i == 0 {
							return status.Error(codes.Unimplemented, "after first frame")
						}
					}
					return status.Error(codes.PermissionDenied, "revoked before final status")
				}
			}
			client := recordFetchClient(t, f, f.certificates[f.local.RaftID], proxy)
			if mode == "legacy" {
				client = recordFetchClient(t, f, f.certificates[f.local.RaftID], &getProxy{get: server.Get})
			}
			err := client.RepairRecords(f.ctx, rootstate.Node{NodeID: f.local.NodeID, RaftID: f.local.RaftID}, ds, destination.store)
			pass := mode == "normal" || mode == "legacy"
			if (err == nil) != pass {
				t.Fatal(mode, err)
			}
			for _, d := range ds {
				_, verifyErr := destination.store.Verify(f.ctx, d)
				if pass && verifyErr != nil {
					t.Fatal(verifyErr)
				}
				if !pass && verifyErr == nil {
					t.Fatal("repaired before successful authenticated stream end")
				}
				if mode == "missing-index" && !errors.Is(verifyErr, records.ErrNotHeld) {
					t.Fatal("repair created a new index", verifyErr)
				}
			}
			if client.recordBuffers.Status().UsedBytes != 0 || len(client.recordSlots) != 0 {
				t.Fatal("repair leaked transfer admission")
			}
		})
	}
}

type batchGetProxy struct {
	raftwire.UnimplementedRecordTransferServiceServer
	get   func(*raftwire.GetRecordBatchRequest, raftwire.RecordTransferService_GetBatchServer) error
	calls atomic.Int32
}

func (p *batchGetProxy) GetBatch(r *raftwire.GetRecordBatchRequest, s raftwire.RecordTransferService_GetBatchServer) error {
	p.calls.Add(1)
	return p.get(r, s)
}

func batchDescriptors(t *testing.T, f *holdingFixture, n int) []records.Descriptor {
	t.Helper()
	ds := make([]records.Descriptor, n)
	for i := range ds {
		b := []byte(fmt.Sprintf("body-%d", i))
		if i == 0 {
			b = nil
		}
		d, err := records.Describe(f.descriptor.Keyspace, f.descriptor.GroupID, []byte(fmt.Sprintf("batch-%d", i)), b)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.Put(f.ctx, d, b); err != nil {
			t.Fatal(err)
		}
		ds[i] = d
	}
	return ds
}

func TestRecordBatchServerSharesAdmissionAndFreshAuthorization(t *testing.T) {
	for _, mode := range []string{"normal", "nil", "empty", "count", "bytes", "descriptor", "unknown", "identity", "unauthenticated", "canceled", "slots", "budget", "missing", "corrupt", "fenced-read", "fenced-send", "final-barrier"} {
		t.Run(mode, func(t *testing.T) {
			f, s, _, _, _ := transferFixture(t)
			ds := batchDescriptors(t, f, FetchRecordBatchLimit)
			req := &raftwire.GetRecordBatchRequest{ClusterId: f.local.ClusterID, FromRaftId: f.caller.RaftID, ToRaftId: f.local.RaftID}
			for _, d := range ds {
				b, _ := json.Marshal(d)
				req.RecordDescriptors = append(req.RecordDescriptors, b)
			}
			st := &getRecordStream{ctx: f.ctx}
			want := codes.OK
			var barriers atomic.Int32
			s.auth.host = regionBarrierFunc(func(context.Context, string) (uint64, error) {
				n := barriers.Add(1)
				if mode == "final-barrier" && n == 3 {
					return 0, errors.New("lost Root quorum")
				}
				return f.root.AppliedIndex(), nil
			})
			remove := func() {
				if err := f.root.ApplyMembership(f.root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{f.local.RaftID}, Learners: []uint64{f.other.RaftID}}); err != nil {
					t.Error(err)
				}
			}
			switch mode {
			case "nil":
				req = nil
				want = codes.InvalidArgument
			case "empty":
				req.RecordDescriptors = nil
				want = codes.InvalidArgument
			case "count":
				req.RecordDescriptors = append(req.RecordDescriptors, req.RecordDescriptors[0])
				want = codes.InvalidArgument
			case "bytes":
				d := ds[1]
				d.Size = FetchRecordBatchBytes + 1
				req.RecordDescriptors[1], _ = json.Marshal(d)
				want = codes.InvalidArgument
			case "descriptor":
				req.RecordDescriptors[1] = []byte("{}")
				want = codes.InvalidArgument
			case "unknown":
				req.ProtoReflect().SetUnknown([]byte{0x28, 1})
				want = codes.InvalidArgument
			case "identity":
				req.ToRaftId = f.other.RaftID
				want = codes.PermissionDenied
			case "unauthenticated":
				st.ctx = context.Background()
				want = codes.Unauthenticated
			case "canceled":
				ctx, cancel := context.WithCancel(f.ctx)
				cancel()
				st.ctx = ctx
				want = codes.Canceled
			case "slots":
				s.slots <- struct{}{}
				s.slots <- struct{}{}
				want = codes.ResourceExhausted
			case "budget":
				s.buffers = resource.NewByteBudget(1)
				want = codes.ResourceExhausted
			case "missing":
				d := ds[1]
				d.Key = []byte("not-present")
				req.RecordDescriptors[1], _ = json.Marshal(d)
				want = codes.NotFound
			case "corrupt":
				s.readStore = recordReadFunc(func(context.Context, records.Descriptor) ([]byte, records.Holding, error) {
					return nil, records.Holding{}, records.ErrCorrupt
				})
				want = codes.DataLoss
			case "fenced-read":
				s.readStore = recordReadFunc(func(ctx context.Context, d records.Descriptor) ([]byte, records.Holding, error) {
					b, h, err := f.store.Read(ctx, d)
					remove()
					return b, h, err
				})
				want = codes.PermissionDenied
			case "fenced-send":
				st.send = func(*raftwire.GetRecordResponse) error { remove(); return nil }
				want = codes.PermissionDenied
			case "final-barrier":
				want = codes.Unavailable
			}
			err := s.GetBatch(req, st)
			if status.Code(err) != want {
				t.Fatal(mode, err, want)
			}
			if mode == "normal" {
				if barriers.Load() != 3 || len(st.frames) != 2*len(ds)-1 {
					t.Fatal("Root barriers were not amortized", barriers.Load(), len(st.frames))
				}
			}
			if mode != "slots" && len(s.slots) != 0 {
				t.Fatal("slot leak")
			}
			if s.buffers.Status().UsedBytes != 0 {
				t.Fatal("budget leak")
			}
		})
	}
}

func TestRecordBatchTLSRejectsIncompleteOrUntrustedBatchBeforeAnyPut(t *testing.T) {
	for _, mode := range []string{"normal", "short", "missing-record", "wrong-order", "extra", "bad-digest", "mixed-header", "unknown-header", "unknown-frame", "empty-frame", "oversized-frame", "final-error", "partial-unimplemented", "wrong-node", "no-final-status", "client-budget"} {
		t.Run(mode, func(t *testing.T) {
			f, s, _, _, _ := transferFixture(t)
			ds := batchDescriptors(t, f, 3)
			destination := newHoldingFixture(t)
			proxy := &batchGetProxy{}
			proxy.get = func(req *raftwire.GetRecordBatchRequest, st raftwire.RecordTransferService_GetBatchServer) error {
				if mode == "normal" {
					return s.GetBatch(req, st)
				}
				for i, d := range ds {
					if mode == "missing-record" && i == 2 {
						return nil
					}
					if mode == "wrong-order" {
						d = ds[len(ds)-1-i]
					}
					b, h, err := f.store.Read(f.ctx, d)
					if err != nil {
						return err
					}
					header, err := s.auth.holdingResponse(d, h, f.root.AppliedIndex())
					if err != nil {
						return err
					}
					if mode == "wrong-node" {
						header.NodeId = "node_00000000000000000000000000000999"
					}
					first := &raftwire.GetRecordResponse{Header: header}
					if mode == "mixed-header" {
						first.Data = []byte("x")
					}
					if mode == "unknown-header" {
						header.ProtoReflect().SetUnknown([]byte{0x58, 1})
					}
					if err := st.Send(first); err != nil {
						return err
					}
					if mode == "partial-unimplemented" {
						return status.Error(codes.Unimplemented, "after a header")
					}
					if mode == "short" && i == 1 {
						return nil
					}
					if mode == "bad-digest" {
						b = bytes.Repeat([]byte("x"), len(b))
					}
					if mode == "empty-frame" {
						return st.Send(&raftwire.GetRecordResponse{})
					}
					if mode == "oversized-frame" {
						return st.Send(&raftwire.GetRecordResponse{Data: make([]byte, recordTransferFrameBytes+1)})
					}
					if len(b) > 0 {
						frame := &raftwire.GetRecordResponse{Data: b}
						if mode == "unknown-frame" {
							frame.ProtoReflect().SetUnknown([]byte{0x18, 1})
						}
						if err := st.Send(frame); err != nil {
							return err
						}
					}
				}
				switch mode {
				case "extra":
					return st.Send(&raftwire.GetRecordResponse{Data: []byte("x")})
				case "final-error":
					return status.Error(codes.PermissionDenied, "revoked before final status")
				case "no-final-status":
					<-st.Context().Done()
					return st.Context().Err()
				}
				return nil
			}
			client := recordFetchClient(t, f, f.certificates[f.local.RaftID], proxy)
			if mode == "client-budget" {
				client.recordBuffers = resource.NewByteBudget(1)
			}
			limits := recordFetchLimits{time.Second, 150 * time.Millisecond, 3 * time.Second}
			unsupported, err := client.fetchRecords(f.ctx, rootstate.Node{NodeID: f.local.NodeID, RaftID: f.local.RaftID}, ds, destination.store, limits)
			if unsupported {
				t.Fatal("partial response permitted compatibility retry")
			}
			if mode == "normal" && err != nil {
				t.Fatal(err)
			}
			if mode != "normal" && err == nil {
				t.Fatal("accepted malformed batch")
			}
			for _, d := range ds {
				_, held := destination.store.Verify(f.ctx, d)
				if mode == "normal" && held != nil {
					t.Fatal(held)
				}
				if mode != "normal" && !errors.Is(held, records.ErrNotHeld) {
					t.Fatal("published before complete authenticated batch", held)
				}
			}
			if client.recordBuffers.Status().UsedBytes != 0 || len(client.recordSlots) != 0 {
				t.Fatal("client admission leaked")
			}
		})
	}
}

func TestRecordBatchOlderPeerFallsBackOnlyBeforeFirstFrame(t *testing.T) {
	f, s, _, _, _ := transferFixture(t)
	ds := batchDescriptors(t, f, 3)
	destination := newHoldingFixture(t)
	legacy := &getProxy{get: s.Get}
	client := recordFetchClient(t, f, f.certificates[f.local.RaftID], legacy)
	if err := client.FetchRecords(f.ctx, rootstate.Node{NodeID: f.local.NodeID, RaftID: f.local.RaftID}, ds, destination.store); err != nil {
		t.Fatal(err)
	}
	if legacy.calls.Load() != int32(len(ds)) {
		t.Fatal("legacy fallback missing", legacy.calls.Load())
	}
	for _, d := range ds {
		if _, err := destination.store.Verify(f.ctx, d); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecordBatchMultiFrameBodiesRespectAggregateBudget(t *testing.T) {
	f, s, _, _, _ := transferFixture(t)
	destination := newHoldingFixture(t)
	var ds []records.Descriptor
	for i := 0; i < 2; i++ {
		body := bytes.Repeat([]byte{byte(i + 1)}, FetchRecordBatchBytes/2)
		d, err := records.Describe(f.descriptor.Keyspace, f.descriptor.GroupID, []byte(fmt.Sprintf("large-batch-%d", i)), body)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.Put(f.ctx, d, body); err != nil {
			t.Fatal(err)
		}
		ds = append(ds, d)
	}
	// Exact aggregate reservation succeeds, one byte less rejects before any IO.
	client := recordFetchClient(t, f, f.certificates[f.local.RaftID], s)
	source := rootstate.Node{NodeID: f.local.NodeID, RaftID: f.local.RaftID}
	client.recordBuffers = resource.NewByteBudget(FetchRecordBatchBytes - 1)
	if err := client.FetchRecords(f.ctx, source, ds, destination.store); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	for _, d := range ds {
		if _, err := destination.store.Verify(f.ctx, d); !errors.Is(err, records.ErrNotHeld) {
			t.Fatal("budget rejection published bytes", err)
		}
	}
	client.recordBuffers = resource.NewByteBudget(FetchRecordBatchBytes)
	s.buffers = resource.NewByteBudget(FetchRecordBatchBytes)
	if err := client.FetchRecords(f.ctx, source, ds, destination.store); err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		if _, err := destination.store.Verify(f.ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	if client.recordBuffers.Status().UsedBytes != 0 || s.buffers.Status().UsedBytes != 0 {
		t.Fatal("aggregate reservation leaked")
	}
}
