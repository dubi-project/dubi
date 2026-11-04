package raftrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type dataExecutor struct {
	writes       int
	reads        int
	ref          enginestate.RecordReference
	body         []byte
	onWrite      func()
	onRead       func()
	writeFailure *EngineDataWriteResult
	writeErr     error
	readErr      error
}

func (e *dataExecutor) ExecuteWrite(_ context.Context, req EngineDataWrite) (EngineDataWriteResult, error) {
	e.writes++
	if !bytes.Equal(req.Body, e.body) || req.RequestID != e.ref.RequestID {
		return EngineDataWriteResult{State: "unknown"}, errors.New("unexpected forwarded write")
	}
	if e.onWrite != nil {
		e.onWrite()
	}
	if e.writeFailure != nil {
		return *e.writeFailure, e.writeErr
	}
	return EngineDataWriteResult{State: "local_ready", Reference: e.ref,
		ConfirmationIndex: 9, RegionGeneration: 2}, nil
}

func (e *dataExecutor) ExecuteRead(_ context.Context, target router.Target, key []byte) (EngineDataReadResult, error) {
	e.reads++
	if e.onRead != nil {
		e.onRead()
	}
	if e.readErr != nil {
		return EngineDataReadResult{}, e.readErr
	}
	if !bytes.Equal(key, target.RoutingKey) {
		return EngineDataReadResult{}, errors.New("wrong forwarded read key")
	}
	return EngineDataReadResult{Body: e.body, Reference: e.ref, RegionGeneration: 2, State: "local_ready"}, nil
}

type dataWriteStream struct {
	grpc.ServerStream
	ctx      context.Context
	frames   []*raftwire.EngineDataWriteRequest
	response *raftwire.EngineDataWriteResponse
}

func (s *dataWriteStream) Context() context.Context { return s.ctx }
func (s *dataWriteStream) Recv() (*raftwire.EngineDataWriteRequest, error) {
	if len(s.frames) == 0 {
		return nil, io.EOF
	}
	frame := s.frames[0]
	s.frames = s.frames[1:]
	return frame, nil
}
func (s *dataWriteStream) SendAndClose(response *raftwire.EngineDataWriteResponse) error {
	s.response = response
	return nil
}

type dataReadStream struct {
	grpc.ServerStream
	ctx    context.Context
	frames []*raftwire.EngineDataReadResponse
}

func (s *dataReadStream) Context() context.Context { return s.ctx }
func (s *dataReadStream) Send(frame *raftwire.EngineDataReadResponse) error {
	s.frames = append(s.frames, frame)
	return nil
}

func dataRouteFixture(t *testing.T) (*holdingFixture, *EngineDataServer, *dataExecutor,
	router.Target, *dataWriteStream) {
	t.Helper()
	f := newHoldingFixture(t)
	target := router.Target{Keyspace: f.descriptor.Keyspace, RangeID: "range-test",
		OwnerGroupID: f.descriptor.GroupID, Epoch: 1, RoutingKey: bytes.Clone(f.descriptor.Key)}
	executor := &dataExecutor{body: []byte("data"), ref: enginestate.RecordReference{Version: 2,
		Descriptor: f.descriptor, OwnerGroupID: target.OwnerGroupID, RangeID: target.RangeID,
		RangeEpoch: target.Epoch, RoutingKey: bytes.Clone(target.RoutingKey), RequestID: "test-write",
		CommandHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Index: 7}}
	server, err := NewEngineDataServer(f.local, f.server.host, f.root, f.store,
		f.server.authorize, executor, DefaultMaxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	route, _ := json.Marshal(target)
	descriptor, _ := json.Marshal(f.descriptor)
	header := &raftwire.EngineDataWriteHeader{ClusterId: f.local.ClusterID,
		GroupId: target.OwnerGroupID, FromRaftId: f.caller.RaftID, ToRaftId: f.local.RaftID,
		RouteTarget: route, RequestId: "test-write", RecordDescriptor: descriptor, MutationPayload: []byte("publish")}
	stream := &dataWriteStream{ctx: f.ctx, frames: []*raftwire.EngineDataWriteRequest{
		{Payload: &raftwire.EngineDataWriteRequest_Header{Header: header}},
		{Payload: &raftwire.EngineDataWriteRequest_Data{Data: []byte("data")}},
	}}
	return f, server, executor, target, stream
}

func TestEngineDataWriteRejectsUnauthenticatedOrMalformedFrames(t *testing.T) {
	for _, mode := range []string{"valid", "confirmed-root-cut", "no-tls", "wrong-target", "wrong-cluster", "wrong-descriptor", "missing-body", "wrong-digest", "second-header", "unknown-field", "caller-removed", "caller-removed-during-owner", "budget"} {
		t.Run(mode, func(t *testing.T) {
			f, server, executor, _, stream := dataRouteFixture(t)
			header := stream.frames[0].GetHeader()
			want := codes.OK
			switch mode {
			case "confirmed-root-cut":
				cut := f.root.AppliedIndex()
				stream.ctx = metadata.NewIncomingContext(stream.ctx, metadata.Pairs(confirmedRootCutMetadataKey, strconv.FormatUint(cut, 10)))
				server.auth.host = regionBarrierFunc(func(context.Context, string) (uint64, error) {
					return 0, errors.New("unexpected global Root ReadIndex")
				})
			case "no-tls":
				stream.ctx = context.Background()
				want = codes.Unauthenticated
			case "wrong-target":
				header.ToRaftId = f.other.RaftID
				want = codes.InvalidArgument
			case "wrong-cluster":
				header.ClusterId = "other"
				want = codes.InvalidArgument
			case "wrong-descriptor":
				header.RecordDescriptor = []byte("{}")
				want = codes.InvalidArgument
			case "missing-body":
				stream.frames = stream.frames[:1]
				want = codes.InvalidArgument
			case "wrong-digest":
				stream.frames[1].Payload = &raftwire.EngineDataWriteRequest_Data{Data: []byte("bad!")}
				want = codes.DataLoss
			case "second-header":
				stream.frames[1] = proto.Clone(stream.frames[0]).(*raftwire.EngineDataWriteRequest)
				want = codes.InvalidArgument
			case "unknown-field":
				header.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
				want = codes.InvalidArgument
			case "caller-removed":
				if err := f.root.ApplyMembership(f.root.AppliedIndex()+1, &raftpb.ConfState{
					Voters: []uint64{f.local.RaftID}, Learners: []uint64{f.other.RaftID}}); err != nil {
					t.Fatal(err)
				}
				want = codes.PermissionDenied
			case "caller-removed-during-owner":
				executor.onWrite = func() {
					if err := f.root.ApplyMembership(f.root.AppliedIndex()+1, &raftpb.ConfState{
						Voters: []uint64{f.local.RaftID}, Learners: []uint64{f.other.RaftID}}); err != nil {
						t.Error(err)
					}
				}
			case "budget":
				server.buffers = resource.NewByteBudget(2)
				want = codes.ResourceExhausted
			}
			err := server.Write(stream)
			if status.Code(err) != want {
				t.Fatal("unexpected routed write outcome", mode, err)
			}
			if want == codes.OK {
				if stream.response == nil || stream.response.State != "local_ready" || stream.response.ConfirmationIndex != 9 || executor.writes != 1 {
					t.Fatal("valid Owner execution lacked confirmation", stream.response, executor.writes)
				}
			} else if executor.writes != 0 {
				t.Fatal("invalid route reached Owner executor", mode, executor.writes)
			}
		})
	}
}

func TestEngineDataReadStreamsOnlyAuthenticatedVerifiedResult(t *testing.T) {
	f, server, executor, target, _ := dataRouteFixture(t)
	route, _ := json.Marshal(target)
	req := &raftwire.EngineDataReadRequest{ClusterId: f.local.ClusterID,
		GroupId: target.OwnerGroupID, FromRaftId: f.caller.RaftID, ToRaftId: f.local.RaftID,
		RouteTarget: route, Key: bytes.Clone(target.RoutingKey)}
	stream := &dataReadStream{ctx: f.ctx}
	if err := server.Read(req, stream); err != nil || executor.reads != 1 || len(stream.frames) != 2 ||
		stream.frames[0].GetHeader() == nil || !bytes.Equal(stream.frames[1].GetData(), []byte("data")) {
		t.Fatal("verified Owner read was not framed", stream.frames, err)
	}
	stream = &dataReadStream{ctx: context.Background()}
	if err := server.Read(req, stream); status.Code(err) != codes.Unauthenticated || executor.reads != 1 || len(stream.frames) != 0 {
		t.Fatal("unauthenticated gateway received data", err)
	}
	executor.body = []byte("bad!")
	stream = &dataReadStream{ctx: f.ctx}
	if err := server.Read(req, stream); status.Code(err) != codes.DataLoss || len(stream.frames) != 0 {
		t.Fatal("Owner streamed a mismatched body", err)
	}
}

func TestEngineDataReadReusesSenderRootCutAndStillRechecksMembership(t *testing.T) {
	t.Run("fallback when receiver has not applied sender cut", func(t *testing.T) {
		f, server, executor, target, _ := dataRouteFixture(t)
		rootReads := 0
		server.auth.host = regionBarrierFunc(func(context.Context, string) (uint64, error) {
			rootReads++
			return f.root.AppliedIndex(), nil
		})
		route, _ := json.Marshal(target)
		req := &raftwire.EngineDataReadRequest{ClusterId: f.local.ClusterID,
			GroupId: target.OwnerGroupID, FromRaftId: f.caller.RaftID, ToRaftId: f.local.RaftID,
			RouteTarget: route, Key: bytes.Clone(target.RoutingKey)}
		unappliedCut := f.root.AppliedIndex() + 1
		ctx := metadata.NewIncomingContext(f.ctx, metadata.Pairs(confirmedRootCutMetadataKey, strconv.FormatUint(unappliedCut, 10)))
		stream := &dataReadStream{ctx: ctx}
		if err := server.Read(req, stream); err != nil || executor.reads != 1 || rootReads != 2 || len(stream.frames) != 2 {
			t.Fatal("lagging receiver did not safely fall back to fresh Root reads", rootReads, executor.reads, stream.frames, err)
		}
	})

	t.Run("reuse cut and retain fresh post-read check", func(t *testing.T) {
		f, server, executor, target, _ := dataRouteFixture(t)
		cut := f.root.AppliedIndex()
		rootReads := 0
		server.auth.host = regionBarrierFunc(func(context.Context, string) (uint64, error) {
			rootReads++
			return f.root.AppliedIndex(), nil
		})
		route, _ := json.Marshal(target)
		req := &raftwire.EngineDataReadRequest{ClusterId: f.local.ClusterID,
			GroupId: target.OwnerGroupID, FromRaftId: f.caller.RaftID, ToRaftId: f.local.RaftID,
			RouteTarget: route, Key: bytes.Clone(target.RoutingKey)}
		ctx := metadata.NewIncomingContext(f.ctx, metadata.Pairs(confirmedRootCutMetadataKey, strconv.FormatUint(cut, 10)))
		stream := &dataReadStream{ctx: ctx}
		if err := server.Read(req, stream); err != nil || executor.reads != 1 || rootReads != 1 || len(stream.frames) != 2 {
			t.Fatal("read did not reuse the sender cut and retain one fresh post-read check", rootReads, executor.reads, stream.frames, err)
		}
	})

	t.Run("reject caller removed during Owner read", func(t *testing.T) {
		f, server, executor, target, _ := dataRouteFixture(t)
		cut := f.root.AppliedIndex()
		rootReads := 0
		server.auth.host = regionBarrierFunc(func(context.Context, string) (uint64, error) {
			rootReads++
			return f.root.AppliedIndex(), nil
		})
		executor.onRead = func() {
			if err := f.root.ApplyMembership(f.root.AppliedIndex()+1, &raftpb.ConfState{
				Voters: []uint64{f.local.RaftID}, Learners: []uint64{f.other.RaftID},
			}); err != nil {
				t.Error(err)
			}
		}
		route, _ := json.Marshal(target)
		req := &raftwire.EngineDataReadRequest{ClusterId: f.local.ClusterID,
			GroupId: target.OwnerGroupID, FromRaftId: f.caller.RaftID, ToRaftId: f.local.RaftID,
			RouteTarget: route, Key: bytes.Clone(target.RoutingKey)}
		ctx := metadata.NewIncomingContext(f.ctx, metadata.Pairs(confirmedRootCutMetadataKey, strconv.FormatUint(cut, 10)))
		stream := &dataReadStream{ctx: ctx}
		if err := server.Read(req, stream); status.Code(err) != codes.PermissionDenied || executor.reads != 1 || rootReads != 1 || len(stream.frames) != 0 {
			t.Fatal("read returned data after fresh membership check should reject removed caller", rootReads, executor.reads, stream.frames, err)
		}
	})
}

var _ EngineDataExecutor = (*dataExecutor)(nil)

func TestEngineDataRerouteOnlySignalsUnsubmittedWrites(t *testing.T) {
	for _, tc := range []struct {
		name    string
		result  EngineDataWriteResult
		failure error
		aborted bool
	}{
		{"preflight", EngineDataWriteResult{State: "unknown"}, router.ErrStaleTarget, true},
		{"joined-uncertainty", EngineDataWriteResult{State: "unknown"}, errors.Join(router.ErrStaleTarget, context.DeadlineExceeded), false},
		{"ambiguous", EngineDataWriteResult{State: "unknown"}, context.DeadlineExceeded, false},
		{"committed-reference", EngineDataWriteResult{State: "unknown", Reference: enginestate.RecordReference{Index: 7}}, router.ErrStaleTarget, false},
		{"confirmed", EngineDataWriteResult{State: "unknown", ConfirmationIndex: 9}, router.ErrStaleTarget, false},
		{"published", EngineDataWriteResult{State: "published_unconfirmed"}, router.ErrStaleTarget, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, server, executor, _, stream := dataRouteFixture(t)
			executor.writeFailure, executor.writeErr = &tc.result, tc.failure
			err := server.Write(stream)
			if tc.aborted {
				if status.Code(err) != codes.Aborted || stream.response != nil {
					t.Fatal("preflight did not request route refresh", err, stream.response)
				}
			} else if err != nil || stream.response == nil || stream.response.State != tc.result.State || stream.response.Error == "" {
				t.Fatal("uncertain publication was misrepresented as a safe reroute", err, stream.response)
			}
		})
	}
}

func TestEngineDataReadSignalsStaleRouteBeforeStreaming(t *testing.T) {
	f, server, executor, target, _ := dataRouteFixture(t)
	executor.readErr = router.ErrRouteFenced
	route, _ := json.Marshal(target)
	stream := &dataReadStream{ctx: f.ctx}
	err := server.Read(&raftwire.EngineDataReadRequest{ClusterId: f.local.ClusterID, GroupId: target.OwnerGroupID, FromRaftId: f.caller.RaftID, ToRaftId: f.local.RaftID, RouteTarget: route, Key: bytes.Clone(target.RoutingKey)}, stream)
	if status.Code(err) != codes.Aborted || len(stream.frames) != 0 {
		t.Fatal("stale read streamed bytes or failed to refresh", err, stream.frames)
	}
}

func TestEngineDataReadDoesNotRelabelAuthorizationFailureAsStaleRoute(t *testing.T) {
	f, server, executor, target, _ := dataRouteFixture(t)
	executor.readErr = errors.Join(router.ErrRouteFenced, status.Error(codes.PermissionDenied, "removed source"))
	route, _ := json.Marshal(target)
	stream := &dataReadStream{ctx: f.ctx}
	err := server.Read(&raftwire.EngineDataReadRequest{ClusterId: f.local.ClusterID, GroupId: target.OwnerGroupID, FromRaftId: f.caller.RaftID, ToRaftId: f.local.RaftID, RouteTarget: route, Key: bytes.Clone(target.RoutingKey)}, stream)
	if err == nil || status.Code(err) == codes.Aborted || len(stream.frames) != 0 {
		t.Fatal("authorization failure was hidden by route refresh", err, stream.frames)
	}
}
