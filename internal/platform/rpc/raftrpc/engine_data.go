package raftrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const engineDataFrameBytes = 1 << 20
const engineDataConcurrency = 2
const engineDataBufferBytes = 128 << 20
const engineDataTimeout = 5 * time.Minute
const engineDataIdle = 15 * time.Second

type EngineDataWrite struct {
	RequestID  string
	Target     router.Target
	Descriptor records.Descriptor
	Body       []byte
	Payload    []byte
}

type EngineDataWriteResult struct {
	State             string
	Reference         enginestate.RecordReference
	ConfirmationIndex uint64
	RegionGeneration  uint64
	RequiredRegions   int
}

type EngineDataReadResult struct {
	Body             []byte
	Reference        enginestate.RecordReference
	RegionGeneration uint64
	State            string
}

// EngineDataExecutor executes only on a locally hosted Owner. The
// gateway never treats the transport ACK as a data durability confirmation.
type EngineDataExecutor interface {
	ExecuteWrite(context.Context, EngineDataWrite) (EngineDataWriteResult, error)
	ExecuteRead(context.Context, router.Target, []byte) (EngineDataReadResult, error)
}

type EngineDataServer struct {
	raftwire.UnimplementedEngineDataServiceServer
	auth     *RecordHoldingServer
	executor EngineDataExecutor
	maxSize  int
	slots    chan struct{}
	readers  chan struct{}
	writers  chan struct{}
	buffers  *resource.ByteBudget
}

func NewEngineDataServer(local identity.Identity, host regionReadBarrier, root *rootstate.State,
	store *records.Store, authorize AuthorizePeer, executor EngineDataExecutor, maxSize int) (*EngineDataServer, error) {
	if executor == nil {
		return nil, errors.New("engine data route requires a local Owner executor")
	}
	auth, err := NewRecordHoldingServer(local, host, root, store, authorize)
	if err != nil {
		return nil, err
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxMessageSize
	}
	return &EngineDataServer{auth: auth, executor: executor, maxSize: maxSize,
		slots: make(chan struct{}, engineDataConcurrency), readers: make(chan struct{}, engineDataConcurrency),
		writers: make(chan struct{}, engineDataConcurrency), buffers: resource.NewByteBudget(engineDataBufferBytes)}, nil
}

func decodeDataTarget(encoded []byte, groupID string) (router.Target, error) {
	var target router.Target
	if len(encoded) == 0 || len(encoded) > recordDescriptorBytes {
		return target, errors.New("invalid engine data Route Target size")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&target) != nil || decoder.Decode(new(struct{})) != io.EOF || target.Validate() != nil ||
		target.OwnerGroupID != groupID || len(target.RoutingKey) == 0 || len(target.RoutingKey) > records.MaxKeyBytes {
		return router.Target{}, errors.New("invalid engine data Route Target")
	}
	return target, nil
}

func decodeDataReference(encoded []byte, target router.Target) (enginestate.RecordReference, error) {
	var ref enginestate.RecordReference
	if len(encoded) == 0 || len(encoded) > recordDescriptorBytes {
		return ref, errors.New("invalid engine data reference size")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&ref) != nil || decoder.Decode(new(struct{})) != io.EOF || ref.Version != 2 ||
		ref.Descriptor.Validate() != nil || ref.Descriptor.Keyspace != target.Keyspace ||
		ref.OwnerGroupID != target.OwnerGroupID || ref.RangeID != target.RangeID || ref.RangeEpoch != target.Epoch ||
		!bytes.Equal(ref.RoutingKey, target.RoutingKey) || ref.Index == 0 || ref.RequestID == "" {
		return enginestate.RecordReference{}, errors.New("invalid engine data reference")
	}
	return ref, nil
}

func (server *EngineDataServer) receive(ctx context.Context, stream raftwire.EngineDataService_WriteServer,
	timeout time.Duration) (*raftwire.EngineDataWriteRequest, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case server.readers <- struct{}{}:
	default:
		return nil, status.Error(codes.ResourceExhausted, "engine data receive workers are busy")
	}
	type result struct {
		frame *raftwire.EngineDataWriteRequest
		err   error
	}
	done := make(chan result, 1)
	go func() {
		frame, err := stream.Recv()
		<-server.readers
		done <- result{frame, err}
	}()
	select {
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case got := <-done:
		return got.frame, got.err
	}
}

func (server *EngineDataServer) Write(stream raftwire.EngineDataService_WriteServer) error {
	select {
	case server.slots <- struct{}{}:
		defer func() { <-server.slots }()
	default:
		return status.Error(codes.ResourceExhausted, "engine data routes are busy")
	}
	ctx, cancel := context.WithTimeout(stream.Context(), engineDataTimeout)
	defer cancel()
	first, err := server.receive(ctx, stream, engineDataIdle)
	if err != nil {
		return err
	}
	h := first.GetHeader()
	if h == nil || first.GetData() != nil || proto.Size(first) > server.maxSize ||
		len(first.ProtoReflect().GetUnknown()) != 0 || len(h.ProtoReflect().GetUnknown()) != 0 ||
		h.ClusterId != server.auth.local.ClusterID || h.FromRaftId == 0 || h.ToRaftId != server.auth.local.RaftID ||
		rootstate.ValidateRequestID(h.RequestId) != nil || len(h.MutationPayload) == 0 {
		return status.Error(codes.InvalidArgument, "invalid engine data write header")
	}
	target, err := decodeDataTarget(h.RouteTarget, h.GroupId)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	descriptor, err := decodeHoldingDescriptor(h.RecordDescriptor)
	if err != nil || descriptor.Keyspace != target.Keyspace || descriptor.GroupID != target.OwnerGroupID ||
		!bytes.Equal(descriptor.Key, target.RoutingKey) {
		return status.Error(codes.InvalidArgument, "engine data descriptor differs from route")
	}
	before, _, err := server.auth.qualify(ctx, h.FromRaftId)
	if err != nil {
		return err
	}
	release, err := server.buffers.TryAcquire(descriptor.Size)
	if err != nil {
		return status.Error(codes.ResourceExhausted, "engine data buffers are busy")
	}
	defer release()
	body := make([]byte, int(descriptor.Size))
	offset := 0
	for {
		frame, err := server.receive(ctx, stream, engineDataIdle)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if frame == nil || frame.GetHeader() != nil || len(frame.GetData()) == 0 ||
			len(frame.GetData()) > engineDataFrameBytes || len(frame.GetData()) > len(body)-offset ||
			len(frame.ProtoReflect().GetUnknown()) != 0 {
			return status.Error(codes.InvalidArgument, "invalid engine data write frame")
		}
		offset += copy(body[offset:], frame.GetData())
	}
	if offset != len(body) {
		return status.Error(codes.InvalidArgument, "incomplete engine data body")
	}
	digest := sha256.Sum256(body)
	if hex.EncodeToString(digest[:]) != descriptor.SHA256 {
		return status.Error(codes.DataLoss, "engine data body digest mismatch")
	}
	after, _, err := server.auth.qualify(ctx, h.FromRaftId)
	if err != nil {
		return err
	}
	if after != before {
		return status.Error(codes.FailedPrecondition, "engine data Node registration changed")
	}
	result, runErr := server.executor.ExecuteWrite(ctx, EngineDataWrite{RequestID: h.RequestId,
		Target: target, Descriptor: descriptor, Body: body, Payload: h.MutationPayload})
	final, _, err := server.auth.qualify(ctx, h.FromRaftId)
	if err != nil {
		return err
	}
	if final != before {
		return status.Error(codes.FailedPrecondition, "engine data Node registration changed")
	}
	response := &raftwire.EngineDataWriteResponse{State: result.State}
	if runErr != nil {
		if result.State != "unknown" && result.State != "published_unconfirmed" {
			return status.Error(codes.Internal, "Owner returned an invalid failed write state")
		}
		response.Error = runErr.Error()
		if len(response.Error) > 4096 {
			response.Error = response.Error[:4096]
		}
		return stream.SendAndClose(response)
	}
	if (result.State != "local_ready" && result.State != "geo_ready") || result.ConfirmationIndex == 0 || result.RegionGeneration == 0 {
		return status.Error(codes.Internal, "Owner returned no committed data confirmation")
	}
	response.Reference, err = json.Marshal(result.Reference)
	if err != nil || len(response.Reference) > recordDescriptorBytes {
		return status.Error(codes.Internal, "Owner returned an invalid data reference")
	}
	response.ConfirmationIndex, response.RegionGeneration, response.RequiredRegions = result.ConfirmationIndex, result.RegionGeneration, uint32(result.RequiredRegions)
	return stream.SendAndClose(response)
}

func (server *EngineDataServer) send(ctx context.Context, stream raftwire.EngineDataService_ReadServer,
	frame *raftwire.EngineDataReadResponse) error {
	select {
	case server.writers <- struct{}{}:
	default:
		return status.Error(codes.ResourceExhausted, "engine data send workers are busy")
	}
	owned := proto.Clone(frame).(*raftwire.EngineDataReadResponse)
	done := make(chan error, 1)
	go func() { err := stream.Send(owned); <-server.writers; done <- err }()
	timer := time.NewTimer(engineDataIdle)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-timer.C:
		return status.Error(codes.DeadlineExceeded, "engine data send stalled")
	case err := <-done:
		return err
	}
}

func (server *EngineDataServer) Read(req *raftwire.EngineDataReadRequest,
	stream raftwire.EngineDataService_ReadServer) error {
	if req == nil || proto.Size(req) > recordDescriptorBytes+4096 || len(req.ProtoReflect().GetUnknown()) != 0 ||
		req.ClusterId != server.auth.local.ClusterID || req.FromRaftId == 0 || req.ToRaftId != server.auth.local.RaftID {
		return status.Error(codes.InvalidArgument, "invalid engine data read request")
	}
	target, err := decodeDataTarget(req.RouteTarget, req.GroupId)
	if err != nil || !bytes.Equal(req.Key, target.RoutingKey) {
		return status.Error(codes.InvalidArgument, "invalid engine data read route")
	}
	select {
	case server.slots <- struct{}{}:
		defer func() { <-server.slots }()
	default:
		return status.Error(codes.ResourceExhausted, "engine data routes are busy")
	}
	ctx, cancel := context.WithTimeout(stream.Context(), engineDataTimeout)
	defer cancel()
	before, _, err := server.auth.qualify(ctx, req.FromRaftId)
	if err != nil {
		return err
	}
	result, err := server.executor.ExecuteRead(ctx, target, req.Key)
	if err != nil {
		return mutationError("read current engine data", err, codes.FailedPrecondition)
	}
	after, _, err := server.auth.qualify(ctx, req.FromRaftId)
	if err != nil {
		return err
	}
	if after != before {
		return status.Error(codes.FailedPrecondition, "engine data Node registration changed")
	}
	if result.State != "local_ready" && result.State != "geo_ready" {
		return status.Error(codes.Internal, "Owner returned no recognized data read state")
	}
	ref, err := json.Marshal(result.Reference)
	if err != nil || len(ref) > recordDescriptorBytes || result.RegionGeneration == 0 ||
		result.Reference.Descriptor.Size != uint64(len(result.Body)) {
		return status.Error(codes.Internal, "Owner returned invalid engine data")
	}
	digest := sha256.Sum256(result.Body)
	if hex.EncodeToString(digest[:]) != result.Reference.Descriptor.SHA256 {
		return status.Error(codes.DataLoss, "Owner returned a mismatched engine data body")
	}
	if err := server.send(ctx, stream, &raftwire.EngineDataReadResponse{Payload: &raftwire.EngineDataReadResponse_Header{
		Header: &raftwire.EngineDataReadHeader{Reference: ref, RegionGeneration: result.RegionGeneration, State: result.State}}}); err != nil {
		return err
	}
	for offset := 0; offset < len(result.Body); offset += engineDataFrameBytes {
		if err := server.send(ctx, stream, &raftwire.EngineDataReadResponse{Payload: &raftwire.EngineDataReadResponse_Data{
			Data: result.Body[offset:min(offset+engineDataFrameBytes, len(result.Body))]}}); err != nil {
			return err
		}
	}
	return nil
}

func (client *GroupClient) GroupCandidates(groupID string) []uint64 {
	if client == nil || client.options.GroupRoutes == nil {
		return nil
	}
	return client.options.GroupRoutes.Candidates(groupID)
}

func (client *GroupClient) engineDataPeer(target rootstate.Node) (raftwire.EngineDataServiceClient, error) {
	conn, err := client.recordConnection(target)
	if err != nil {
		return nil, err
	}
	return raftwire.NewEngineDataServiceClient(conn), nil
}

func (client *GroupClient) WriteEngineData(ctx context.Context, targetNode rootstate.Node,
	request EngineDataWrite) (EngineDataWriteResult, error) {
	result := EngineDataWriteResult{State: "unknown"}
	if client == nil || ctx == nil || request.Target.Validate() != nil || request.Descriptor.Validate() != nil ||
		request.Descriptor.Size != uint64(len(request.Body)) || len(request.Payload) == 0 ||
		rootstate.ValidateRequestID(request.RequestID) != nil {
		return result, errors.New("invalid routed engine data write")
	}
	digest := sha256.Sum256(request.Body)
	if hex.EncodeToString(digest[:]) != request.Descriptor.SHA256 {
		return result, errors.New("engine data body differs from descriptor")
	}
	select {
	case client.recordSlots <- struct{}{}:
		defer func() { <-client.recordSlots }()
	default:
		return result, status.Error(codes.ResourceExhausted, "engine data sends are busy")
	}
	remote, err := client.engineDataPeer(targetNode)
	if err != nil {
		return result, err
	}
	route, err := json.Marshal(request.Target)
	if err != nil {
		return result, err
	}
	descriptor, err := json.Marshal(request.Descriptor)
	if err != nil {
		return result, err
	}
	stream, err := remote.Write(ctx, grpc.MaxCallSendMsgSize(client.options.MaxSize+4096), grpc.MaxCallRecvMsgSize(recordDescriptorBytes+4096))
	if err != nil {
		return result, err
	}
	h := &raftwire.EngineDataWriteHeader{ClusterId: client.options.ClusterID,
		GroupId: request.Target.OwnerGroupID, FromRaftId: client.options.RaftID, ToRaftId: targetNode.RaftID,
		RouteTarget: route, RequestId: request.RequestID, RecordDescriptor: descriptor, MutationPayload: request.Payload}
	if proto.Size(h) > client.options.MaxSize {
		return result, errors.New("engine data publication exceeds Group message limit")
	}
	if err := sendEngineDataFrame(stream, &raftwire.EngineDataWriteRequest{Payload: &raftwire.EngineDataWriteRequest_Header{Header: h}}); err != nil {
		return result, err
	}
	for offset := 0; offset < len(request.Body); offset += engineDataFrameBytes {
		if err := sendEngineDataFrame(stream, &raftwire.EngineDataWriteRequest{Payload: &raftwire.EngineDataWriteRequest_Data{
			Data: request.Body[offset:min(offset+engineDataFrameBytes, len(request.Body))]}}); err != nil {
			return result, err
		}
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		return result, err
	}
	if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 {
		return result, errors.New("invalid engine data write response")
	}
	result.State = response.State
	if response.Error != "" {
		if result.State != "unknown" && result.State != "published_unconfirmed" {
			return EngineDataWriteResult{State: "unknown"}, errors.New("invalid failed engine data response")
		}
		return result, errors.New(response.Error)
	}
	if (result.State != "local_ready" && result.State != "geo_ready") || response.ConfirmationIndex == 0 || response.RegionGeneration == 0 {
		return EngineDataWriteResult{State: "unknown"}, errors.New("remote Owner did not prove LocalReady")
	}
	ref, err := decodeDataReference(response.Reference, request.Target)
	if err != nil {
		return EngineDataWriteResult{State: "unknown"}, err
	}
	wantID, _ := request.Descriptor.ID()
	gotID, _ := ref.Descriptor.ID()
	if ref.RequestID != request.RequestID || gotID != wantID {
		return EngineDataWriteResult{State: "unknown"}, errors.New("remote Owner confirmed a different immutable publication")
	}
	result.Reference, result.ConfirmationIndex, result.RegionGeneration, result.RequiredRegions = ref, response.ConfirmationIndex, response.RegionGeneration, int(response.RequiredRegions)
	return result, nil
}

func sendEngineDataFrame(stream raftwire.EngineDataService_WriteClient, frame *raftwire.EngineDataWriteRequest) error {
	owned := proto.Clone(frame).(*raftwire.EngineDataWriteRequest)
	if err := stream.Send(owned); err != nil {
		if errors.Is(err, io.EOF) {
			_, final := stream.CloseAndRecv()
			if final != nil {
				return final
			}
		}
		return err
	}
	return nil
}

func (client *GroupClient) ReadEngineData(ctx context.Context, targetNode rootstate.Node,
	target router.Target, key []byte) (EngineDataReadResult, error) {
	if client == nil || ctx == nil || target.Validate() != nil || !bytes.Equal(key, target.RoutingKey) {
		return EngineDataReadResult{}, errors.New("invalid routed engine data read")
	}
	select {
	case client.recordReaders <- struct{}{}:
		defer func() { <-client.recordReaders }()
	default:
		return EngineDataReadResult{}, status.Error(codes.ResourceExhausted, "engine data reads are busy")
	}
	remote, err := client.engineDataPeer(targetNode)
	if err != nil {
		return EngineDataReadResult{}, err
	}
	route, err := json.Marshal(target)
	if err != nil {
		return EngineDataReadResult{}, err
	}
	stream, err := remote.Read(ctx, &raftwire.EngineDataReadRequest{ClusterId: client.options.ClusterID,
		GroupId: target.OwnerGroupID, FromRaftId: client.options.RaftID, ToRaftId: targetNode.RaftID,
		RouteTarget: route, Key: key}, grpc.MaxCallRecvMsgSize(engineDataFrameBytes+recordDescriptorBytes+4096))
	if err != nil {
		return EngineDataReadResult{}, err
	}
	first, err := stream.Recv()
	if err != nil || first == nil || first.GetHeader() == nil || first.GetData() != nil ||
		len(first.ProtoReflect().GetUnknown()) != 0 || len(first.GetHeader().ProtoReflect().GetUnknown()) != 0 {
		return EngineDataReadResult{}, errors.Join(err, errors.New("engine data read header missing"))
	}
	ref, err := decodeDataReference(first.GetHeader().Reference, target)
	if err != nil || !bytes.Equal(ref.Descriptor.Key, key) || ref.Descriptor.Size > pack.MaxPayloadSize ||
		first.GetHeader().RegionGeneration == 0 {
		return EngineDataReadResult{}, errors.Join(err, errors.New("invalid engine data read reference"))
	}
	release, err := client.recordBuffers.TryAcquire(ref.Descriptor.Size)
	if err != nil {
		return EngineDataReadResult{}, err
	}
	defer release()
	body := make([]byte, int(ref.Descriptor.Size))
	offset := 0
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return EngineDataReadResult{}, err
		}
		if frame == nil || frame.GetHeader() != nil || len(frame.GetData()) == 0 ||
			len(frame.GetData()) > engineDataFrameBytes || len(frame.GetData()) > len(body)-offset ||
			len(frame.ProtoReflect().GetUnknown()) != 0 {
			return EngineDataReadResult{}, errors.New("invalid engine data read frame")
		}
		offset += copy(body[offset:], frame.GetData())
	}
	if offset != len(body) {
		return EngineDataReadResult{}, fmt.Errorf("incomplete engine data read: %d of %d bytes", offset, len(body))
	}
	digest := sha256.Sum256(body)
	if hex.EncodeToString(digest[:]) != ref.Descriptor.SHA256 {
		return EngineDataReadResult{}, errors.New("engine data read digest mismatch")
	}
	state := first.GetHeader().State
	if state != "local_ready" && state != "geo_ready" {
		return EngineDataReadResult{}, errors.New("invalid engine data read state")
	}
	return EngineDataReadResult{Body: body, Reference: ref,
		RegionGeneration: first.GetHeader().RegionGeneration, State: state}, nil
}
