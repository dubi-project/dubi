package raftrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type dataExecutor struct {
	writes int
	reads  int
	ref    capstate.RecordReference
	body   []byte
}

func (e *dataExecutor) ExecuteWrite(_ context.Context, req CapabilityDataWrite) (CapabilityDataWriteResult, error) {
	e.writes++
	if !bytes.Equal(req.Body, e.body) || req.RequestID != e.ref.RequestID {
		return CapabilityDataWriteResult{State: "unknown"}, errors.New("unexpected forwarded write")
	}
	return CapabilityDataWriteResult{State: "local_ready", Reference: e.ref,
		ConfirmationIndex: 9, RegionGeneration: 2}, nil
}

func (e *dataExecutor) ExecuteRead(_ context.Context, target router.Target, key []byte) (CapabilityDataReadResult, error) {
	e.reads++
	if !bytes.Equal(key, target.RoutingKey) {
		return CapabilityDataReadResult{}, errors.New("wrong forwarded read key")
	}
	return CapabilityDataReadResult{Body: e.body, Reference: e.ref, RegionGeneration: 2, State: "local_ready"}, nil
}

type dataWriteStream struct {
	grpc.ServerStream
	ctx      context.Context
	frames   []*raftwire.CapabilityDataWriteRequest
	response *raftwire.CapabilityDataWriteResponse
}

func (s *dataWriteStream) Context() context.Context { return s.ctx }
func (s *dataWriteStream) Recv() (*raftwire.CapabilityDataWriteRequest, error) {
	if len(s.frames) == 0 {
		return nil, io.EOF
	}
	frame := s.frames[0]
	s.frames = s.frames[1:]
	return frame, nil
}
func (s *dataWriteStream) SendAndClose(response *raftwire.CapabilityDataWriteResponse) error {
	s.response = response
	return nil
}

type dataReadStream struct {
	grpc.ServerStream
	ctx    context.Context
	frames []*raftwire.CapabilityDataReadResponse
}

func (s *dataReadStream) Context() context.Context { return s.ctx }
func (s *dataReadStream) Send(frame *raftwire.CapabilityDataReadResponse) error {
	s.frames = append(s.frames, frame)
	return nil
}

func dataRouteFixture(t *testing.T) (*holdingFixture, *CapabilityDataServer, *dataExecutor,
	router.Target, *dataWriteStream) {
	t.Helper()
	f := newHoldingFixture(t)
	target := router.Target{Keyspace: f.descriptor.Keyspace, RangeID: "range-test",
		OwnerGroupID: f.descriptor.GroupID, Epoch: 1, RoutingKey: bytes.Clone(f.descriptor.Key)}
	executor := &dataExecutor{body: []byte("data"), ref: capstate.RecordReference{Version: 2,
		Descriptor: f.descriptor, OwnerGroupID: target.OwnerGroupID, RangeID: target.RangeID,
		RangeEpoch: target.Epoch, RoutingKey: bytes.Clone(target.RoutingKey), RequestID: "test-write",
		CommandHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Index: 7}}
	server, err := NewCapabilityDataServer(f.local, f.server.host, f.root, f.store,
		f.server.authorize, executor, DefaultMaxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	route, _ := json.Marshal(target)
	descriptor, _ := json.Marshal(f.descriptor)
	header := &raftwire.CapabilityDataWriteHeader{ClusterId: f.local.ClusterID,
		GroupId: target.OwnerGroupID, FromRaftId: f.caller.RaftID, ToRaftId: f.local.RaftID,
		RouteTarget: route, RequestId: "test-write", RecordDescriptor: descriptor, MutationPayload: []byte("publish")}
	stream := &dataWriteStream{ctx: f.ctx, frames: []*raftwire.CapabilityDataWriteRequest{
		{Payload: &raftwire.CapabilityDataWriteRequest_Header{Header: header}},
		{Payload: &raftwire.CapabilityDataWriteRequest_Data{Data: []byte("data")}},
	}}
	return f, server, executor, target, stream
}

func TestCapabilityDataWriteRejectsUnauthenticatedOrMalformedFrames(t *testing.T) {
	for _, mode := range []string{"valid", "no-tls", "wrong-target", "wrong-cluster", "wrong-descriptor", "missing-body", "wrong-digest", "second-header", "unknown-field", "caller-removed", "budget"} {
		t.Run(mode, func(t *testing.T) {
			f, server, executor, _, stream := dataRouteFixture(t)
			header := stream.frames[0].GetHeader()
			want := codes.OK
			switch mode {
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
				stream.frames[1].Payload = &raftwire.CapabilityDataWriteRequest_Data{Data: []byte("bad!")}
				want = codes.DataLoss
			case "second-header":
				stream.frames[1] = proto.Clone(stream.frames[0]).(*raftwire.CapabilityDataWriteRequest)
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

func TestCapabilityDataReadStreamsOnlyAuthenticatedVerifiedResult(t *testing.T) {
	f, server, executor, target, _ := dataRouteFixture(t)
	route, _ := json.Marshal(target)
	req := &raftwire.CapabilityDataReadRequest{ClusterId: f.local.ClusterID,
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

var _ CapabilityDataExecutor = (*dataExecutor)(nil)
