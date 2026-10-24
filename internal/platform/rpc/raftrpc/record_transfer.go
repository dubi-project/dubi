package raftrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	recordTransferFrameBytes    = 1 << 20
	recordTransferConcurrency   = 2
	recordTransferBufferBytes   = 128 << 20
	recordTransferTimeout       = 5 * time.Minute
	recordTransferIdle          = 15 * time.Second
	recordTransferHeaderTimeout = 10 * time.Second
)

type recordWriter interface {
	Put(context.Context, records.Descriptor, []byte) (records.Holding, error)
}

type RecordTransferServer struct {
	raftwire.UnimplementedRecordTransferServiceServer
	auth                         *RecordHoldingServer
	store                        recordWriter
	readStore                    recordReader
	slots, readers, writers      chan struct{}
	buffers                      *resource.ByteBudget
	headerTimeout, idle, timeout time.Duration
}

func NewRecordTransferServer(local identity.Identity, host regionReadBarrier, root *rootstate.State, store *records.Store, authorize AuthorizePeer) (*RecordTransferServer, error) {
	auth, err := NewRecordHoldingServer(local, host, root, store, authorize)
	if err != nil {
		return nil, err
	}
	return &RecordTransferServer{auth: auth, store: store, readStore: store,
		slots: make(chan struct{}, recordTransferConcurrency), readers: make(chan struct{}, recordTransferConcurrency), writers: make(chan struct{}, recordTransferConcurrency),
		buffers: resource.NewByteBudget(recordTransferBufferBytes), headerTimeout: recordTransferHeaderTimeout,
		idle: recordTransferIdle, timeout: recordTransferTimeout}, nil
}

// As with snapshot receive, the worker owns its slot until Recv ACTUALLY exits.
// A timed-out handler returns so gRPC can cancel that Recv. Do not wait here or
// release a still-blocked worker's lease; both permit resource/lifecycle bugs.
func (s *RecordTransferServer) receive(ctx context.Context, stream raftwire.RecordTransferService_PutServer, timeout time.Duration) (*raftwire.PutRecordRequest, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	select {
	case s.readers <- struct{}{}:
	default:
		return nil, status.Error(codes.ResourceExhausted, "record receive workers are busy")
	}
	type result struct {
		frame *raftwire.PutRecordRequest
		err   error
	}
	done := make(chan result, 1)
	go func() {
		frame, err := stream.Recv()
		<-s.readers
		done <- result{frame, err}
	}()
	select {
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case r := <-done:
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		return r.frame, r.err
	}
}

func (s *RecordTransferServer) Put(stream raftwire.RecordTransferService_PutServer) error {
	started := time.Now()
	recordID := "invalid"
	defer func() { traceRecordRPC("transfer_put", recordID, "total", started) }()
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return status.Error(codes.ResourceExhausted, "record transfers are busy")
	}
	ctx, cancel := context.WithTimeout(stream.Context(), s.timeout)
	defer cancel()
	first, err := s.receive(ctx, stream, s.headerTimeout)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "record header missing")
		}
		return err
	}
	if first == nil || first.Header == nil || len(first.Data) != 0 || proto.Size(first) > recordHoldingMessageBytes || len(first.ProtoReflect().GetUnknown()) != 0 || len(first.Header.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "invalid record transfer header")
	}
	header := first.Header
	if header.ClusterId != s.auth.local.ClusterID || header.FromRaftId == 0 || header.ToRaftId != s.auth.local.RaftID {
		return status.Error(codes.PermissionDenied, "record transfer identity mismatch")
	}
	d, err := decodeHoldingDescriptor(header.RecordDescriptor)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid record transfer descriptor")
	}
	recordID = traceRecordID(d.ID)
	phaseStarted := time.Now()
	before, _, err := s.auth.qualify(ctx, header.FromRaftId)
	traceRecordRPC("transfer_put", recordID, "root_qualify_before_receive", phaseStarted)
	if err != nil {
		return err
	}
	// This reservation bounds ONLY receiver staging bodies. Store/Pack IO,
	// protobuf buffers and caller-owned payloads have separate lifetimes.
	release, err := s.buffers.TryAcquire(d.Size)
	if err != nil {
		return status.Error(codes.ResourceExhausted, "record transfer buffers are busy")
	}
	defer release()
	payload := make([]byte, int(d.Size))
	offset := 0
	phaseStarted = time.Now()
	for {
		frame, err := s.receive(ctx, stream, s.idle)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if frame == nil || frame.Header != nil || len(frame.Data) == 0 || len(frame.Data) > recordTransferFrameBytes || len(frame.ProtoReflect().GetUnknown()) != 0 || len(frame.Data) > len(payload)-offset {
			return status.Error(codes.InvalidArgument, "invalid record data frame")
		}
		offset += copy(payload[offset:], frame.Data)
	}
	traceRecordRPC("transfer_put", recordID, "receive_payload", phaseStarted)
	if offset != len(payload) {
		return status.Error(codes.InvalidArgument, "incomplete record payload")
	}
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != d.SHA256 {
		return status.Error(codes.DataLoss, "record payload digest mismatch")
	}
	// Receiving all bytes is not authorization to publish indefinitely. Recheck
	// the two Node registrations immediately before and after synchronous IO.
	phaseStarted = time.Now()
	ready, _, err := s.auth.qualify(ctx, header.FromRaftId)
	traceRecordRPC("transfer_put", recordID, "root_qualify_before_write", phaseStarted)
	if err != nil {
		return err
	}
	if ready != before {
		return status.Error(codes.FailedPrecondition, "record transfer Node registration changed")
	}
	phaseStarted = time.Now()
	holding, putErr := s.store.Put(ctx, d, payload)
	traceRecordRPC("transfer_put", recordID, "pack_write", phaseStarted)
	phaseStarted = time.Now()
	after, index, err := s.auth.qualify(ctx, header.FromRaftId)
	traceRecordRPC("transfer_put", recordID, "root_qualify_after_write", phaseStarted)
	if err != nil {
		return err
	}
	if after != before {
		return status.Error(codes.FailedPrecondition, "record transfer Node registration changed")
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if putErr != nil {
		if errors.Is(putErr, context.Canceled) || errors.Is(putErr, context.DeadlineExceeded) {
			return status.FromContextError(putErr).Err()
		}
		if errors.Is(putErr, records.ErrCorrupt) {
			return status.Error(codes.DataLoss, "record Store integrity check failed")
		}
		// This destination has not confirmed a physical copy. The coordinator
		// may still obtain the same strict majority from other cohort members.
		return status.Error(codes.Unavailable, "record Store cannot confirm durable holding")
	}
	response, err := s.auth.holdingResponse(d, holding, index)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	// Pack Sync, index Sync and physical verification have completed. An ACK
	// failure can leave durable bytes. Exact descriptor retries deduplicate;
	// no rollback/removal or majority claim is made here.
	return stream.SendAndClose(response)
}

// recordPeer uses a separate cached, exact-identity-pinned TLS connection.
// Merely checking a response is too late when sending confidential bytes.
func (c *GroupClient) recordPeer(target rootstate.Node) (raftwire.RecordTransferServiceClient, error) {
	conn, err := c.recordConnection(target)
	if err != nil {
		return nil, err
	}
	return raftwire.NewRecordTransferServiceClient(conn), nil
}

func (c *GroupClient) recordConnection(target rootstate.Node) (*grpc.ClientConn, error) {
	if !validHoldingID(target.NodeID, "node_") {
		return nil, errors.New("record target requires exact Node identity")
	}
	if _, err := c.peer(target.RaftID); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.peers[target.RaftID] == nil {
		return nil, errors.New("Group client is closed")
	}
	p := c.peers[target.RaftID]
	if p.recordConn != nil && p.recordNodeID != target.NodeID {
		_ = p.recordConn.Close()
		p.recordConn = nil
	}
	if p.recordConn == nil {
		creds, err := tlscredentials.ForNode(c.options.Credentials, identity.CertificateIdentity{ClusterID: c.options.ClusterID, RaftID: target.RaftID, NodeID: target.NodeID})
		if err != nil {
			return nil, err
		}
		conn, err := grpc.NewClient(p.address, grpc.WithTransportCredentials(creds))
		if err != nil {
			return nil, err
		}
		p.recordConn, p.recordNodeID = conn, target.NodeID
	}
	return p.recordConn, nil
}

// NodeAdminConnection returns the exact-identity-pinned mTLS connection used
// for local Node administration RPCs. The connection is owned by GroupClient
// and must not be closed by the caller.
func (c *GroupClient) NodeAdminConnection(target rootstate.Node) (*grpc.ClientConn, error) {
	if c == nil {
		return nil, errors.New("Group client is nil")
	}
	return c.recordConnection(target)
}

// PutRecord transfers one immutable record to an explicitly selected Node.
// The caller must supply an authorized target and keep payload immutable until
// return. This API neither selects Placement nor publishes a logical reference.
func (c *GroupClient) PutRecord(ctx context.Context, target rootstate.Node, d records.Descriptor, payload []byte) (HoldingObservation, error) {
	if c == nil || ctx == nil {
		return HoldingObservation{}, errors.New("record transfer requires client and context")
	}
	if err := d.Validate(); err != nil {
		return HoldingObservation{}, err
	}
	if d.Size != uint64(len(payload)) {
		return HoldingObservation{}, errors.New("record transfer payload size mismatch")
	}
	ctx, cancel := context.WithTimeout(ctx, recordTransferTimeout)
	defer cancel()
	releaseSlot, err := acquireQueuedSlot(ctx, c.recordSlots, c.recordWaiters, transferQueueTimeout,
		"record sends are busy")
	if err != nil {
		return HoldingObservation{}, err
	}
	defer releaseSlot()
	d.Key = bytes.Clone(d.Key)
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != d.SHA256 {
		return HoldingObservation{}, errors.New("record transfer payload digest mismatch")
	}
	encoded, err := json.Marshal(d)
	if err != nil || len(encoded) > recordDescriptorBytes {
		return HoldingObservation{}, errors.New("invalid record transfer descriptor")
	}
	remote, err := c.recordPeer(target)
	if err != nil {
		return HoldingObservation{}, err
	}
	var transportPeer peer.Peer
	stream, err := remote.Put(ctx, grpc.Peer(&transportPeer), grpc.MaxCallSendMsgSize(recordTransferFrameBytes+recordHoldingMessageBytes), grpc.MaxCallRecvMsgSize(recordHoldingMessageBytes))
	if err != nil {
		return HoldingObservation{}, err
	}
	if err := sendRecordFrame(stream, &raftwire.PutRecordRequest{Header: &raftwire.ObserveRecordHoldingRequest{ClusterId: c.options.ClusterID, FromRaftId: c.options.RaftID, ToRaftId: target.RaftID, RecordDescriptor: encoded}}); err != nil {
		return HoldingObservation{}, err
	}
	for offset := 0; offset < len(payload); offset += recordTransferFrameBytes {
		if err := sendRecordFrame(stream, &raftwire.PutRecordRequest{Data: payload[offset:min(offset+recordTransferFrameBytes, len(payload))]}); err != nil {
			return HoldingObservation{}, err
		}
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		return HoldingObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return HoldingObservation{}, err
	}
	got, err := authenticatedHoldingObservation(ctx, &transportPeer, c.options.ClusterID, target.RaftID, d, response, time.Now())
	if err == nil && got.NodeID != target.NodeID {
		return HoldingObservation{}, errors.New("record transfer responder Node identity mismatch")
	}
	return got, err
}

func sendRecordFrame(stream raftwire.RecordTransferService_PutClient, frame *raftwire.PutRecordRequest) error {
	// SendMsg tracing/stats may retain the message even after SendMsg returns.
	// Never hand gRPC a slice of the caller's reusable payload (nor a pooled
	// mutable frame). Each bounded message owns its immutable bytes.
	err := stream.Send(proto.Clone(frame).(*raftwire.PutRecordRequest))
	if errors.Is(err, io.EOF) {
		// Send reports EOF when the server rejects early; the actual admission
		// status is available through Recv. Preserve it for bounded retries.
		if _, finalErr := stream.CloseAndRecv(); finalErr != nil {
			return finalErr
		}
	}
	return err
}
