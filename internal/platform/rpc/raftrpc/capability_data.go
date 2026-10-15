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

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
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

const capabilityDataFrameBytes = 1 << 20
const capabilityDataConcurrency = 2
const capabilityDataBufferBytes = 128 << 20
const capabilityDataTimeout = 5 * time.Minute
const capabilityDataIdle = 15 * time.Second

type CapabilityDataWrite struct {
	RequestID  string
	Target     router.Target
	Descriptor records.Descriptor
	Body       []byte
	Payload    []byte
}

type CapabilityDataWriteResult struct {
	State             string
	Reference         capstate.RecordReference
	ConfirmationIndex uint64
	RegionGeneration  uint64
}

type CapabilityDataReadResult struct {
	Body             []byte
	Reference        capstate.RecordReference
	RegionGeneration uint64
}

// CapabilityDataExecutor executes only on a locally hosted Owner. The
// gateway never treats the transport ACK as a data durability confirmation.
type CapabilityDataExecutor interface {
	ExecuteWrite(context.Context, CapabilityDataWrite) (CapabilityDataWriteResult, error)
	ExecuteRead(context.Context, router.Target, []byte) (CapabilityDataReadResult, error)
}

type CapabilityDataServer struct {
	raftwire.UnimplementedCapabilityDataServiceServer
	auth     *RecordHoldingServer
	executor CapabilityDataExecutor
	maxSize  int
	slots    chan struct{}
	readers  chan struct{}
	writers  chan struct{}
	buffers  *resource.ByteBudget
}

func NewCapabilityDataServer(local identity.Identity, host regionReadBarrier, root *rootstate.State,
	store *records.Store, authorize AuthorizePeer, executor CapabilityDataExecutor, maxSize int) (*CapabilityDataServer, error) {
	if executor == nil {
		return nil, errors.New("capability data route requires a local Owner executor")
	}
	auth, err := NewRecordHoldingServer(local, host, root, store, authorize)
	if err != nil {
		return nil, err
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxMessageSize
	}
	return &CapabilityDataServer{auth: auth, executor: executor, maxSize: maxSize,
		slots: make(chan struct{}, capabilityDataConcurrency), readers: make(chan struct{}, capabilityDataConcurrency),
		writers: make(chan struct{}, capabilityDataConcurrency), buffers: resource.NewByteBudget(capabilityDataBufferBytes)}, nil
}

func decodeDataTarget(encoded []byte, groupID string) (router.Target, error) {
	var target router.Target
	if len(encoded) == 0 || len(encoded) > recordDescriptorBytes {
		return target, errors.New("invalid capability data Route Target size")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&target) != nil || decoder.Decode(new(struct{})) != io.EOF || target.Validate() != nil ||
		target.OwnerGroupID != groupID || len(target.RoutingKey) == 0 || len(target.RoutingKey) > records.MaxKeyBytes {
		return router.Target{}, errors.New("invalid capability data Route Target")
	}
	return target, nil
}

func decodeDataReference(encoded []byte, target router.Target) (capstate.RecordReference, error) {
	var ref capstate.RecordReference
	if len(encoded) == 0 || len(encoded) > recordDescriptorBytes {
		return ref, errors.New("invalid capability data reference size")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&ref) != nil || decoder.Decode(new(struct{})) != io.EOF || ref.Version != 2 ||
		ref.Descriptor.Validate() != nil || ref.Descriptor.Keyspace != target.Keyspace ||
		ref.OwnerGroupID != target.OwnerGroupID || ref.RangeID != target.RangeID || ref.RangeEpoch != target.Epoch ||
		!bytes.Equal(ref.RoutingKey, target.RoutingKey) || ref.Index == 0 || ref.RequestID == "" {
		return capstate.RecordReference{}, errors.New("invalid capability data reference")
	}
	return ref, nil
}

func (server *CapabilityDataServer) receive(ctx context.Context, stream raftwire.CapabilityDataService_WriteServer,
	timeout time.Duration) (*raftwire.CapabilityDataWriteRequest, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case server.readers <- struct{}{}:
	default:
		return nil, status.Error(codes.ResourceExhausted, "capability data receive workers are busy")
	}
	type result struct {
		frame *raftwire.CapabilityDataWriteRequest
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

func (server *CapabilityDataServer) Write(stream raftwire.CapabilityDataService_WriteServer) error {
	select {
	case server.slots <- struct{}{}:
		defer func() { <-server.slots }()
	default:
		return status.Error(codes.ResourceExhausted, "capability data routes are busy")
	}
	ctx, cancel := context.WithTimeout(stream.Context(), capabilityDataTimeout)
	defer cancel()
	first, err := server.receive(ctx, stream, capabilityDataIdle)
	if err != nil {
		return err
	}
	h := first.GetHeader()
	if h == nil || first.GetData() != nil || proto.Size(first) > server.maxSize ||
		len(first.ProtoReflect().GetUnknown()) != 0 || len(h.ProtoReflect().GetUnknown()) != 0 ||
		h.ClusterId != server.auth.local.ClusterID || h.FromRaftId == 0 || h.ToRaftId != server.auth.local.RaftID ||
		rootstate.ValidateRequestID(h.RequestId) != nil || len(h.MutationPayload) == 0 {
		return status.Error(codes.InvalidArgument, "invalid capability data write header")
	}
	target, err := decodeDataTarget(h.RouteTarget, h.GroupId)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	descriptor, err := decodeHoldingDescriptor(h.RecordDescriptor)
	if err != nil || descriptor.Keyspace != target.Keyspace || descriptor.GroupID != target.OwnerGroupID ||
		!bytes.Equal(descriptor.Key, target.RoutingKey) {
		return status.Error(codes.InvalidArgument, "capability data descriptor differs from route")
	}
	before, _, err := server.auth.qualify(ctx, h.FromRaftId)
	if err != nil {
		return err
	}
	release, err := server.buffers.TryAcquire(descriptor.Size)
	if err != nil {
		return status.Error(codes.ResourceExhausted, "capability data buffers are busy")
	}
	defer release()
	body := make([]byte, int(descriptor.Size))
	offset := 0
	for {
		frame, err := server.receive(ctx, stream, capabilityDataIdle)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if frame == nil || frame.GetHeader() != nil || len(frame.GetData()) == 0 ||
			len(frame.GetData()) > capabilityDataFrameBytes || len(frame.GetData()) > len(body)-offset ||
			len(frame.ProtoReflect().GetUnknown()) != 0 {
			return status.Error(codes.InvalidArgument, "invalid capability data write frame")
		}
		offset += copy(body[offset:], frame.GetData())
	}
	if offset != len(body) {
		return status.Error(codes.InvalidArgument, "incomplete capability data body")
	}
	digest := sha256.Sum256(body)
	if hex.EncodeToString(digest[:]) != descriptor.SHA256 {
		return status.Error(codes.DataLoss, "capability data body digest mismatch")
	}
	after, _, err := server.auth.qualify(ctx, h.FromRaftId)
	if err != nil {
		return err
	}
	if after != before {
		return status.Error(codes.FailedPrecondition, "capability data Node registration changed")
	}
	result, runErr := server.executor.ExecuteWrite(ctx, CapabilityDataWrite{RequestID: h.RequestId,
		Target: target, Descriptor: descriptor, Body: body, Payload: h.MutationPayload})
	final, _, err := server.auth.qualify(ctx, h.FromRaftId)
	if err != nil {
		return err
	}
	if final != before {
		return status.Error(codes.FailedPrecondition, "capability data Node registration changed")
	}
	response := &raftwire.CapabilityDataWriteResponse{State: result.State}
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
	if result.State != "local_ready" || result.ConfirmationIndex == 0 || result.RegionGeneration == 0 {
		return status.Error(codes.Internal, "Owner returned no committed data confirmation")
	}
	response.Reference, err = json.Marshal(result.Reference)
	if err != nil || len(response.Reference) > recordDescriptorBytes {
		return status.Error(codes.Internal, "Owner returned an invalid data reference")
	}
	response.ConfirmationIndex, response.RegionGeneration = result.ConfirmationIndex, result.RegionGeneration
	return stream.SendAndClose(response)
}

func (server *CapabilityDataServer) send(ctx context.Context, stream raftwire.CapabilityDataService_ReadServer,
	frame *raftwire.CapabilityDataReadResponse) error {
	select {
	case server.writers <- struct{}{}:
	default:
		return status.Error(codes.ResourceExhausted, "capability data send workers are busy")
	}
	owned := proto.Clone(frame).(*raftwire.CapabilityDataReadResponse)
	done := make(chan error, 1)
	go func() { err := stream.Send(owned); <-server.writers; done <- err }()
	timer := time.NewTimer(capabilityDataIdle)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-timer.C:
		return status.Error(codes.DeadlineExceeded, "capability data send stalled")
	case err := <-done:
		return err
	}
}

func (server *CapabilityDataServer) Read(req *raftwire.CapabilityDataReadRequest,
	stream raftwire.CapabilityDataService_ReadServer) error {
	if req == nil || proto.Size(req) > recordDescriptorBytes+4096 || len(req.ProtoReflect().GetUnknown()) != 0 ||
		req.ClusterId != server.auth.local.ClusterID || req.FromRaftId == 0 || req.ToRaftId != server.auth.local.RaftID {
		return status.Error(codes.InvalidArgument, "invalid capability data read request")
	}
	target, err := decodeDataTarget(req.RouteTarget, req.GroupId)
	if err != nil || !bytes.Equal(req.Key, target.RoutingKey) {
		return status.Error(codes.InvalidArgument, "invalid capability data read route")
	}
	select {
	case server.slots <- struct{}{}:
		defer func() { <-server.slots }()
	default:
		return status.Error(codes.ResourceExhausted, "capability data routes are busy")
	}
	ctx, cancel := context.WithTimeout(stream.Context(), capabilityDataTimeout)
	defer cancel()
	before, _, err := server.auth.qualify(ctx, req.FromRaftId)
	if err != nil {
		return err
	}
	result, err := server.executor.ExecuteRead(ctx, target, req.Key)
	if err != nil {
		return mutationError("read current capability data", err, codes.FailedPrecondition)
	}
	after, _, err := server.auth.qualify(ctx, req.FromRaftId)
	if err != nil {
		return err
	}
	if after != before {
		return status.Error(codes.FailedPrecondition, "capability data Node registration changed")
	}
	ref, err := json.Marshal(result.Reference)
	if err != nil || len(ref) > recordDescriptorBytes || result.RegionGeneration == 0 ||
		result.Reference.Descriptor.Size != uint64(len(result.Body)) {
		return status.Error(codes.Internal, "Owner returned invalid capability data")
	}
	digest := sha256.Sum256(result.Body)
	if hex.EncodeToString(digest[:]) != result.Reference.Descriptor.SHA256 {
		return status.Error(codes.DataLoss, "Owner returned a mismatched capability data body")
	}
	if err := server.send(ctx, stream, &raftwire.CapabilityDataReadResponse{Payload: &raftwire.CapabilityDataReadResponse_Header{
		Header: &raftwire.CapabilityDataReadHeader{Reference: ref, RegionGeneration: result.RegionGeneration}}}); err != nil {
		return err
	}
	for offset := 0; offset < len(result.Body); offset += capabilityDataFrameBytes {
		if err := server.send(ctx, stream, &raftwire.CapabilityDataReadResponse{Payload: &raftwire.CapabilityDataReadResponse_Data{
			Data: result.Body[offset:min(offset+capabilityDataFrameBytes, len(result.Body))]}}); err != nil {
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

func (client *GroupClient) capabilityDataPeer(target rootstate.Node) (raftwire.CapabilityDataServiceClient, error) {
	conn, err := client.recordConnection(target)
	if err != nil {
		return nil, err
	}
	return raftwire.NewCapabilityDataServiceClient(conn), nil
}

func (client *GroupClient) WriteCapabilityData(ctx context.Context, targetNode rootstate.Node,
	request CapabilityDataWrite) (CapabilityDataWriteResult, error) {
	result := CapabilityDataWriteResult{State: "unknown"}
	if client == nil || ctx == nil || request.Target.Validate() != nil || request.Descriptor.Validate() != nil ||
		request.Descriptor.Size != uint64(len(request.Body)) || len(request.Payload) == 0 ||
		rootstate.ValidateRequestID(request.RequestID) != nil {
		return result, errors.New("invalid routed capability data write")
	}
	digest := sha256.Sum256(request.Body)
	if hex.EncodeToString(digest[:]) != request.Descriptor.SHA256 {
		return result, errors.New("capability data body differs from descriptor")
	}
	select {
	case client.recordSlots <- struct{}{}:
		defer func() { <-client.recordSlots }()
	default:
		return result, status.Error(codes.ResourceExhausted, "capability data sends are busy")
	}
	remote, err := client.capabilityDataPeer(targetNode)
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
	h := &raftwire.CapabilityDataWriteHeader{ClusterId: client.options.ClusterID,
		GroupId: request.Target.OwnerGroupID, FromRaftId: client.options.RaftID, ToRaftId: targetNode.RaftID,
		RouteTarget: route, RequestId: request.RequestID, RecordDescriptor: descriptor, MutationPayload: request.Payload}
	if proto.Size(h) > client.options.MaxSize {
		return result, errors.New("capability data publication exceeds Group message limit")
	}
	if err := sendCapabilityDataFrame(stream, &raftwire.CapabilityDataWriteRequest{Payload: &raftwire.CapabilityDataWriteRequest_Header{Header: h}}); err != nil {
		return result, err
	}
	for offset := 0; offset < len(request.Body); offset += capabilityDataFrameBytes {
		if err := sendCapabilityDataFrame(stream, &raftwire.CapabilityDataWriteRequest{Payload: &raftwire.CapabilityDataWriteRequest_Data{
			Data: request.Body[offset:min(offset+capabilityDataFrameBytes, len(request.Body))]}}); err != nil {
			return result, err
		}
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		return result, err
	}
	if response == nil || len(response.ProtoReflect().GetUnknown()) != 0 {
		return result, errors.New("invalid capability data write response")
	}
	result.State = response.State
	if response.Error != "" {
		if result.State != "unknown" && result.State != "published_unconfirmed" {
			return CapabilityDataWriteResult{State: "unknown"}, errors.New("invalid failed capability data response")
		}
		return result, errors.New(response.Error)
	}
	if result.State != "local_ready" || response.ConfirmationIndex == 0 || response.RegionGeneration == 0 {
		return CapabilityDataWriteResult{State: "unknown"}, errors.New("remote Owner did not prove LocalReady")
	}
	ref, err := decodeDataReference(response.Reference, request.Target)
	if err != nil {
		return CapabilityDataWriteResult{State: "unknown"}, err
	}
	wantID, _ := request.Descriptor.ID()
	gotID, _ := ref.Descriptor.ID()
	if ref.RequestID != request.RequestID || gotID != wantID {
		return CapabilityDataWriteResult{State: "unknown"}, errors.New("remote Owner confirmed a different immutable publication")
	}
	result.Reference, result.ConfirmationIndex, result.RegionGeneration = ref, response.ConfirmationIndex, response.RegionGeneration
	return result, nil
}

func sendCapabilityDataFrame(stream raftwire.CapabilityDataService_WriteClient, frame *raftwire.CapabilityDataWriteRequest) error {
	owned := proto.Clone(frame).(*raftwire.CapabilityDataWriteRequest)
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

func (client *GroupClient) ReadCapabilityData(ctx context.Context, targetNode rootstate.Node,
	target router.Target, key []byte) (CapabilityDataReadResult, error) {
	if client == nil || ctx == nil || target.Validate() != nil || !bytes.Equal(key, target.RoutingKey) {
		return CapabilityDataReadResult{}, errors.New("invalid routed capability data read")
	}
	select {
	case client.recordReaders <- struct{}{}:
		defer func() { <-client.recordReaders }()
	default:
		return CapabilityDataReadResult{}, status.Error(codes.ResourceExhausted, "capability data reads are busy")
	}
	remote, err := client.capabilityDataPeer(targetNode)
	if err != nil {
		return CapabilityDataReadResult{}, err
	}
	route, err := json.Marshal(target)
	if err != nil {
		return CapabilityDataReadResult{}, err
	}
	stream, err := remote.Read(ctx, &raftwire.CapabilityDataReadRequest{ClusterId: client.options.ClusterID,
		GroupId: target.OwnerGroupID, FromRaftId: client.options.RaftID, ToRaftId: targetNode.RaftID,
		RouteTarget: route, Key: key}, grpc.MaxCallRecvMsgSize(capabilityDataFrameBytes+recordDescriptorBytes+4096))
	if err != nil {
		return CapabilityDataReadResult{}, err
	}
	first, err := stream.Recv()
	if err != nil || first == nil || first.GetHeader() == nil || first.GetData() != nil ||
		len(first.ProtoReflect().GetUnknown()) != 0 || len(first.GetHeader().ProtoReflect().GetUnknown()) != 0 {
		return CapabilityDataReadResult{}, errors.Join(err, errors.New("capability data read header missing"))
	}
	ref, err := decodeDataReference(first.GetHeader().Reference, target)
	if err != nil || !bytes.Equal(ref.Descriptor.Key, key) || ref.Descriptor.Size > pack.MaxPayloadSize ||
		first.GetHeader().RegionGeneration == 0 {
		return CapabilityDataReadResult{}, errors.Join(err, errors.New("invalid capability data read reference"))
	}
	release, err := client.recordBuffers.TryAcquire(ref.Descriptor.Size)
	if err != nil {
		return CapabilityDataReadResult{}, err
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
			return CapabilityDataReadResult{}, err
		}
		if frame == nil || frame.GetHeader() != nil || len(frame.GetData()) == 0 ||
			len(frame.GetData()) > capabilityDataFrameBytes || len(frame.GetData()) > len(body)-offset ||
			len(frame.ProtoReflect().GetUnknown()) != 0 {
			return CapabilityDataReadResult{}, errors.New("invalid capability data read frame")
		}
		offset += copy(body[offset:], frame.GetData())
	}
	if offset != len(body) {
		return CapabilityDataReadResult{}, fmt.Errorf("incomplete capability data read: %d of %d bytes", offset, len(body))
	}
	digest := sha256.Sum256(body)
	if hex.EncodeToString(digest[:]) != ref.Descriptor.SHA256 {
		return CapabilityDataReadResult{}, errors.New("capability data read digest mismatch")
	}
	return CapabilityDataReadResult{Body: body, Reference: ref,
		RegionGeneration: first.GetHeader().RegionGeneration}, nil
}
