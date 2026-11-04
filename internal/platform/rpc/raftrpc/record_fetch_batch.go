package raftrpc

import (
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

// Batch reads amortize fresh Root authorization for small immutable records.
// They share the ordinary transfer slots and byte budget; concurrency does not
// grow with the number of records. Larger individual records use FetchRecord.
const (
	FetchRecordBatchLimit   = 32
	FetchRecordBatchBytes   = 4 << 20
	recordBatchRequestBytes = FetchRecordBatchLimit*recordDescriptorBytes + 4096
)

func validateRecordBatch(ds []records.Descriptor) (uint64, error) {
	if len(ds) == 0 || len(ds) > FetchRecordBatchLimit {
		return 0, errors.New("invalid record batch count")
	}
	var total uint64
	for _, d := range ds {
		if err := d.Validate(); err != nil {
			return 0, err
		}
		if d.Size > FetchRecordBatchBytes-total {
			return 0, errors.New("record batch exceeds byte limit")
		}
		total += d.Size
	}
	return total, nil
}

func (s *RecordTransferServer) GetBatch(req *raftwire.GetRecordBatchRequest, stream raftwire.RecordTransferService_GetBatchServer) error {
	if req == nil || proto.Size(req) > recordBatchRequestBytes || len(req.ProtoReflect().GetUnknown()) != 0 || len(req.RecordDescriptors) == 0 || len(req.RecordDescriptors) > FetchRecordBatchLimit {
		return status.Error(codes.InvalidArgument, "invalid record batch request")
	}
	if req.ClusterId != s.auth.local.ClusterID || req.FromRaftId == 0 || req.ToRaftId != s.auth.local.RaftID {
		return status.Error(codes.PermissionDenied, "record batch identity mismatch")
	}
	ds := make([]records.Descriptor, len(req.RecordDescriptors))
	for i, b := range req.RecordDescriptors {
		d, err := decodeHoldingDescriptor(b)
		if err != nil {
			return status.Error(codes.InvalidArgument, "invalid record batch descriptor")
		}
		ds[i] = d
	}
	total, err := validateRecordBatch(ds)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
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
	release, err := s.buffers.TryAcquire(total)
	if err != nil {
		return status.Error(codes.ResourceExhausted, "record transfer buffers are busy")
	}
	defer release()
	payloads := make([][]byte, len(ds))
	holdings := make([]records.Holding, len(ds))
	var readErr error
	for i, d := range ds {
		if err := ctx.Err(); err != nil {
			readErr = status.FromContextError(err).Err()
			break
		}
		b, h, e := s.readStore.Read(ctx, d)
		if e != nil {
			readErr = recordBatchReadError(e)
			break
		}
		if uint64(len(b)) != d.Size {
			readErr = status.Error(codes.DataLoss, "record Store payload size mismatch")
			break
		}
		digest := sha256.Sum256(b)
		if hex.EncodeToString(digest[:]) != d.SHA256 {
			readErr = status.Error(codes.DataLoss, "record Store payload digest mismatch")
			break
		}
		payloads[i], holdings[i] = b, h
	}
	// Recheck authorization before revealing even a physical read failure.
	after, index, err := s.auth.qualify(ctx, req.FromRaftId)
	if err != nil {
		return err
	}
	if before != after {
		return status.Error(codes.FailedPrecondition, "record batch Node registration changed")
	}
	if readErr != nil {
		return readErr
	}
	headers := make([]*raftwire.ObserveRecordHoldingResponse, len(ds))
	for i, d := range ds {
		headers[i], err = s.auth.holdingResponse(d, holdings[i], index)
		if err != nil {
			return err
		}
	}
	for i, b := range payloads {
		if err := authorizeSender(ctx, req.FromRaftId, s.auth.authorize); err != nil {
			return err
		}
		if err := s.send(ctx, stream, &raftwire.GetRecordResponse{Header: headers[i]}); err != nil {
			return err
		}
		for offset := 0; offset < len(b); offset += recordTransferFrameBytes {
			if err := authorizeSender(ctx, req.FromRaftId, s.auth.authorize); err != nil {
				return err
			}
			if err := s.send(ctx, stream, &raftwire.GetRecordResponse{Data: b[offset:min(offset+recordTransferFrameBytes, len(b))]}); err != nil {
				return err
			}
		}
	}
	// The receiver publishes nothing until this fresh final barrier succeeds.
	final, _, err := s.auth.qualify(ctx, req.FromRaftId)
	if err != nil {
		return err
	}
	if final != before {
		return status.Error(codes.FailedPrecondition, "record batch Node registration changed")
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

func recordBatchReadError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.FromContextError(context.Canceled).Err()
	case errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(context.DeadlineExceeded).Err()
	case errors.Is(err, records.ErrNotHeld):
		return status.Error(codes.NotFound, "record is not held on this Node")
	case errors.Is(err, records.ErrCorrupt):
		return status.Error(codes.DataLoss, "record Store integrity check failed")
	case errors.Is(err, records.ErrUnavailable):
		return status.Error(codes.Unavailable, "record Store is unavailable")
	default:
		return status.Error(codes.FailedPrecondition, "record Store cannot provide verified bytes")
	}
}

// FetchRecords copies a small bounded batch. Authentication, digests and final
// successful stream status are checked for ALL records before the first Put.
// Existing older peers may fall back to individual reads only if the batch RPC
// is unimplemented before any response frame. A partial or denied stream never
// permits that fallback. Cached bytes are not logical references or votes.
func (c *GroupClient) FetchRecords(ctx context.Context, source rootstate.Node, ds []records.Descriptor, destination *records.Store) error {
	if c == nil || ctx == nil || destination == nil {
		return errors.New("record batch requires client, context and destination")
	}
	if _, err := validateRecordBatch(ds); err != nil {
		return err
	}
	unsupported, err := c.fetchRecords(ctx, source, ds, destination, recordFetchLimits{recordTransferHeaderTimeout, recordTransferIdle, recordTransferTimeout})
	if !unsupported {
		return err
	}
	for _, d := range ds {
		if _, err := c.FetchRecord(ctx, source, d, destination); err != nil {
			return err
		}
	}
	return nil
}

// RepairRecords uses the same bounded, authenticated batch stream, but only
// relocates existing unreadable indexes. It never turns a missing index into
// a new publication or relaxes RepairExisting's checks under the Store lock.
func (c *GroupClient) RepairRecords(ctx context.Context, source rootstate.Node, ds []records.Descriptor, destination *records.Store) error {
	if c == nil || ctx == nil || destination == nil {
		return errors.New("record batch repair requires client, context and destination")
	}
	if _, err := validateRecordBatch(ds); err != nil {
		return err
	}
	unsupported, err := c.fetchRecords(ctx, source, ds, relocationWriter{destination}, recordFetchLimits{recordTransferHeaderTimeout, recordTransferIdle, recordTransferTimeout})
	if !unsupported {
		return err
	}
	for _, d := range ds {
		if _, err := c.RepairRecord(ctx, source, d, destination); err != nil {
			return err
		}
	}
	return nil
}

func (c *GroupClient) fetchRecords(ctx context.Context, source rootstate.Node, ds []records.Descriptor, destination recordWriter, limits recordFetchLimits) (bool, error) {
	total, err := validateRecordBatch(ds)
	if err != nil {
		return false, err
	}
	// Own the descriptors for the duration of asynchronous transport operations.
	encoded := make([][]byte, len(ds))
	owned := make([]records.Descriptor, len(ds))
	for i, d := range ds {
		b, err := json.Marshal(d)
		if err != nil || len(b) > recordDescriptorBytes {
			return false, errors.New("invalid record batch descriptor")
		}
		encoded[i] = b
		if err := json.Unmarshal(b, &owned[i]); err != nil {
			return false, err
		}
	}
	ds = owned
	ctx, cancel := context.WithTimeout(ctx, limits.total)
	defer cancel()
	releaseSlot, err := acquireQueuedSlot(ctx, c.recordSlots, c.recordWaiters, transferQueueTimeout, "record transfers are busy")
	if err != nil {
		return false, err
	}
	defer releaseSlot()
	release, err := c.recordBuffers.TryAcquire(total)
	if err != nil {
		return false, status.Error(codes.ResourceExhausted, "record fetch buffers are busy")
	}
	defer release()
	remote, err := c.recordPeer(source)
	if err != nil {
		return false, err
	}
	stream, err := remote.GetBatch(ctx, &raftwire.GetRecordBatchRequest{ClusterId: c.options.ClusterID, FromRaftId: c.options.RaftID, ToRaftId: source.RaftID, RecordDescriptors: encoded}, grpc.MaxCallSendMsgSize(recordBatchRequestBytes), grpc.MaxCallRecvMsgSize(recordTransferFrameBytes+recordHoldingMessageBytes))
	if err != nil {
		return status.Code(err) == codes.Unimplemented, err
	}
	payloads := make([][]byte, len(ds))
	headers := make([]*raftwire.ObserveRecordHoldingResponse, len(ds))
	actualPeer, _ := peer.FromContext(stream.Context())
	for i, d := range ds {
		timeout := limits.idle
		if i == 0 {
			timeout = limits.header
		}
		first, err := c.receiveRecord(ctx, stream, timeout)
		if err != nil {
			return i == 0 && status.Code(err) == codes.Unimplemented, err
		}
		if first == nil || first.Header == nil || len(first.Data) != 0 || proto.Size(first) > recordHoldingMessageBytes || len(first.ProtoReflect().GetUnknown()) != 0 || len(first.Header.ProtoReflect().GetUnknown()) != 0 {
			return false, errors.New("invalid record batch header")
		}
		observation, err := authenticatedHoldingObservation(ctx, actualPeer, c.options.ClusterID, source.RaftID, d, first.Header, time.Now())
		if err != nil {
			return false, err
		}
		if observation.NodeID != source.NodeID {
			return false, errors.New("record batch source identity mismatch")
		}
		headers[i] = first.Header
		payload := make([]byte, int(d.Size))
		for offset := 0; offset < len(payload); {
			frame, err := c.receiveRecord(ctx, stream, limits.idle)
			if err != nil {
				return false, err
			}
			if frame == nil || frame.Header != nil || len(frame.Data) == 0 || len(frame.Data) > recordTransferFrameBytes || len(frame.ProtoReflect().GetUnknown()) != 0 || len(frame.Data) > len(payload)-offset {
				return false, errors.New("invalid record batch data frame")
			}
			offset += copy(payload[offset:], frame.Data)
		}
		digest := sha256.Sum256(payload)
		if hex.EncodeToString(digest[:]) != d.SHA256 {
			return false, errors.New("record batch payload digest mismatch")
		}
		payloads[i] = payload
	}
	if _, err := c.receiveRecord(ctx, stream, limits.idle); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("unexpected record batch trailing frame")
		}
		return false, err
	}
	// Reauthenticate every header before publishing any of the bounded payloads.
	for i, d := range ds {
		if _, err := authenticatedHoldingObservation(ctx, actualPeer, c.options.ClusterID, source.RaftID, d, headers[i], time.Now()); err != nil {
			return false, err
		}
	}
	for i, d := range ds {
		if err := ctx.Err(); err != nil {
			return false, status.FromContextError(err).Err()
		}
		h, err := destination.Put(ctx, d, payloads[i])
		if err != nil {
			return false, err
		}
		if err := ctx.Err(); err != nil {
			return false, status.FromContextError(err).Err()
		}
		if _, err := authenticatedHoldingObservation(ctx, actualPeer, c.options.ClusterID, source.RaftID, d, headers[i], time.Now()); err != nil {
			return false, err
		}
		want, _ := d.ID()
		got, err := h.Descriptor.ID()
		if err != nil || got != want || h.RecordID != want || !validHoldingID(h.StoreID, "store_") || !validHoldingID(h.VolumeID, "volume_") {
			return false, errors.New("record batch destination returned inconsistent holding")
		}
	}
	return false, nil
}
