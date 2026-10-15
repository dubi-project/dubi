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
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type recordReader interface {
	Read(context.Context, records.Descriptor) ([]byte, records.Holding, error)
}

// Send cannot use a derived context. On idle expiry the handler returns so
// gRPC cancels the transport; the bounded worker remains leased until Send
// actually exits. Its message owns only a cloned bounded frame, NOT a slice
// retaining the entire body after the handler releases its body reservation.
func (s *RecordTransferServer) send(ctx context.Context, stream raftwire.RecordTransferService_GetServer, frame *raftwire.GetRecordResponse) error {
	ctx, cancel := context.WithTimeout(ctx, s.idle)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	select {
	case s.writers <- struct{}{}:
	default:
		return status.Error(codes.ResourceExhausted, "record send workers are busy")
	}
	owned := proto.Clone(frame).(*raftwire.GetRecordResponse)
	done := make(chan error, 1)
	go func() { err := stream.Send(owned); <-s.writers; done <- err }()
	select {
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case err := <-done:
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		return err
	}
}

func (s *RecordTransferServer) Get(req *raftwire.ObserveRecordHoldingRequest, stream raftwire.RecordTransferService_GetServer) error {
	if req == nil || proto.Size(req) > recordHoldingMessageBytes || len(req.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "invalid record read request")
	}
	if req.ClusterId != s.auth.local.ClusterID || req.FromRaftId == 0 || req.ToRaftId != s.auth.local.RaftID {
		return status.Error(codes.PermissionDenied, "record read identity mismatch")
	}
	d, err := decodeHoldingDescriptor(req.RecordDescriptor)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid record read descriptor")
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return status.Error(codes.ResourceExhausted, "record transfers are busy")
	}
	ctx, cancel := context.WithTimeout(stream.Context(), s.timeout)
	defer cancel()
	before, _, err := s.auth.qualify(ctx, req.FromRaftId)
	if err != nil {
		return err
	}
	release, err := s.buffers.TryAcquire(d.Size)
	if err != nil {
		return status.Error(codes.ResourceExhausted, "record transfer buffers are busy")
	}
	defer release()
	payload, holding, readErr := s.readStore.Read(ctx, d)
	after, index, err := s.auth.qualify(ctx, req.FromRaftId)
	if err != nil {
		return err
	}
	if before != after {
		return status.Error(codes.FailedPrecondition, "record read Node registration changed")
	}
	if readErr != nil {
		switch {
		case errors.Is(readErr, context.Canceled):
			return status.FromContextError(context.Canceled).Err()
		case errors.Is(readErr, context.DeadlineExceeded):
			return status.FromContextError(context.DeadlineExceeded).Err()
		case errors.Is(readErr, records.ErrNotHeld):
			return status.Error(codes.NotFound, "record is not held on this Node")
		case errors.Is(readErr, records.ErrCorrupt):
			return status.Error(codes.DataLoss, "record Store integrity check failed")
		case errors.Is(readErr, records.ErrUnavailable):
			return status.Error(codes.Unavailable, "record Store is unavailable")
		default:
			return status.Error(codes.FailedPrecondition, "record Store cannot provide verified bytes")
		}
	}
	if uint64(len(payload)) != d.Size {
		return status.Error(codes.DataLoss, "record Store payload size mismatch")
	}
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != d.SHA256 {
		return status.Error(codes.DataLoss, "record Store payload digest mismatch")
	}
	header, err := s.auth.holdingResponse(d, holding, index)
	if err != nil {
		return err
	}
	if err := s.send(ctx, stream, &raftwire.GetRecordResponse{Header: header}); err != nil {
		return err
	}
	for offset := 0; offset < len(payload); offset += recordTransferFrameBytes {
		if err := authorizeSender(ctx, req.FromRaftId, s.auth.authorize); err != nil {
			return err
		}
		if err := s.send(ctx, stream, &raftwire.GetRecordResponse{Data: payload[offset:min(offset+recordTransferFrameBytes, len(payload))]}); err != nil {
			return err
		}
	}
	// All bytes can have reached the receiver, but only successful final status
	// authorizes it to publish them. These are fresh point-in-time membership
	// checks, not atomic revocation or a generation/retention guarantee.
	final, _, err := s.auth.qualify(ctx, req.FromRaftId)
	if err != nil {
		return err
	}
	if final != before {
		return status.Error(codes.FailedPrecondition, "record read Node registration changed")
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

type RecordFetchResult struct {
	Source HoldingObservation
	Local  records.Holding
}

type recordFetchLimits struct{ header, idle, total time.Duration }

// FetchRecord copies verified remote bytes into the supplied local Store. It
// keeps staging admission through synchronous local publication, never exposes
// an unbudgeted caller-owned body, and never publishes a logical reference.
// Caller-selected source membership/Placement and destination lifecycle remain
// the caller's responsibility. Local is NOT another authenticated Node vote.
func (c *GroupClient) FetchRecord(ctx context.Context, source rootstate.Node, d records.Descriptor, destination *records.Store) (RecordFetchResult, error) {
	if destination == nil {
		return RecordFetchResult{}, errors.New("record fetch requires a destination Store")
	}
	return c.fetchRecord(ctx, source, d, destination, recordFetchLimits{recordTransferHeaderTimeout, recordTransferIdle, recordTransferTimeout})
}

// RepairRecord fetches from the same authenticated source, but can replace an
// existing unreadable local Pack location. Store.RepairExisting independently
// rechecks the index and bytes before making the new location durable.
func (c *GroupClient) RepairRecord(ctx context.Context, source rootstate.Node, d records.Descriptor, destination *records.Store) (RecordFetchResult, error) {
	if destination == nil {
		return RecordFetchResult{}, errors.New("record repair requires a destination Store")
	}
	return c.fetchRecord(ctx, source, d, relocationWriter{destination}, recordFetchLimits{recordTransferHeaderTimeout, recordTransferIdle, recordTransferTimeout})
}

type relocationWriter struct{ store *records.Store }

func (w relocationWriter) Put(ctx context.Context, d records.Descriptor, payload []byte) (records.Holding, error) {
	return w.store.RepairExisting(ctx, d, payload)
}

// The worker owns its lease until the actual Recv returns. fetchRecord cancels
// the RPC context on ALL exit paths, including a derived idle timeout.
func (c *GroupClient) receiveRecord(ctx context.Context, stream raftwire.RecordTransferService_GetClient, timeout time.Duration) (*raftwire.GetRecordResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	select {
	case c.recordReaders <- struct{}{}:
	default:
		return nil, status.Error(codes.ResourceExhausted, "record fetch workers are busy")
	}
	type result struct {
		frame *raftwire.GetRecordResponse
		err   error
	}
	done := make(chan result, 1)
	go func() { frame, err := stream.Recv(); <-c.recordReaders; done <- result{frame, err} }()
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

func (c *GroupClient) fetchRecord(ctx context.Context, source rootstate.Node, d records.Descriptor, destination recordWriter, limits recordFetchLimits) (RecordFetchResult, error) {
	var result RecordFetchResult
	if c == nil || ctx == nil || destination == nil {
		return result, errors.New("record fetch requires client, context and destination")
	}
	if err := d.Validate(); err != nil {
		return result, err
	}
	d.Key = bytes.Clone(d.Key)
	select {
	case c.recordSlots <- struct{}{}:
		defer func() { <-c.recordSlots }()
	default:
		return result, status.Error(codes.ResourceExhausted, "record transfers are busy")
	}
	release, err := c.recordBuffers.TryAcquire(d.Size)
	if err != nil {
		return result, status.Error(codes.ResourceExhausted, "record fetch buffers are busy")
	}
	defer release()
	ctx, cancel := context.WithTimeout(ctx, limits.total)
	defer cancel()
	remote, err := c.recordPeer(source)
	if err != nil {
		return result, err
	}
	encoded, err := json.Marshal(d)
	if err != nil || len(encoded) > recordDescriptorBytes {
		return result, errors.New("invalid record fetch descriptor")
	}
	stream, err := remote.Get(ctx, &raftwire.ObserveRecordHoldingRequest{ClusterId: c.options.ClusterID, FromRaftId: c.options.RaftID, ToRaftId: source.RaftID, RecordDescriptor: encoded}, grpc.MaxCallSendMsgSize(recordHoldingMessageBytes), grpc.MaxCallRecvMsgSize(recordTransferFrameBytes+recordHoldingMessageBytes))
	if err != nil {
		return result, err
	}
	first, err := c.receiveRecord(ctx, stream, limits.header)
	if err != nil {
		return result, err
	}
	if first == nil || first.Header == nil || len(first.Data) != 0 || proto.Size(first) > recordHoldingMessageBytes || len(first.ProtoReflect().GetUnknown()) != 0 || len(first.Header.ProtoReflect().GetUnknown()) != 0 {
		return result, errors.New("invalid record fetch header")
	}
	actualPeer, _ := peer.FromContext(stream.Context())
	observation, err := authenticatedHoldingObservation(ctx, actualPeer, c.options.ClusterID, source.RaftID, d, first.Header, time.Now())
	if err != nil {
		return result, err
	}
	if observation.NodeID != source.NodeID {
		return result, errors.New("record fetch source identity mismatch")
	}
	payload := make([]byte, int(d.Size))
	offset := 0
	for {
		frame, err := c.receiveRecord(ctx, stream, limits.idle)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return result, err
		}
		if frame == nil || frame.Header != nil || len(frame.Data) == 0 || len(frame.Data) > recordTransferFrameBytes || len(frame.ProtoReflect().GetUnknown()) != 0 || len(frame.Data) > len(payload)-offset {
			return result, errors.New("invalid record fetch data frame")
		}
		offset += copy(payload[offset:], frame.Data)
	}
	if offset != len(payload) {
		return result, errors.New("incomplete record fetch payload")
	}
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != d.SHA256 {
		return result, errors.New("record fetch payload digest mismatch")
	}
	if _, err := authenticatedHoldingObservation(ctx, actualPeer, c.options.ClusterID, source.RaftID, d, first.Header, time.Now()); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, status.FromContextError(err).Err()
	}
	local, err := destination.Put(ctx, d, payload)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, status.FromContextError(err).Err()
	}
	if _, err := authenticatedHoldingObservation(ctx, actualPeer, c.options.ClusterID, source.RaftID, d, first.Header, time.Now()); err != nil {
		return result, err
	}
	want, _ := d.ID()
	got, err := local.Descriptor.ID()
	if err != nil || got != want || local.RecordID != want || !validHoldingID(local.StoreID, "store_") || !validHoldingID(local.VolumeID, "volume_") {
		return result, errors.New("record fetch destination returned inconsistent holding")
	}
	return RecordFetchResult{Source: observation, Local: local}, nil
}
