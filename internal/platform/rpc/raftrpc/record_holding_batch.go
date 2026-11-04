package raftrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const recordHoldingBatchMessageBytes = FetchRecordBatchLimit*recordHoldingMessageBytes + 4096

// RecordHoldingResult is a fresh physical observation or one bounded storage
// error. A row is never a logical reference or a reusable durability receipt.
type RecordHoldingResult struct {
	Observation HoldingObservation
	Err         error
}

func validateHoldingBatch(ds []records.Descriptor) error {
	if _, err := validateRecordBatch(ds); err != nil {
		return err
	}
	seen := make(map[string]bool, len(ds))
	for _, d := range ds {
		id, _ := d.ID()
		if seen[id] {
			return errors.New("holding batch repeats a record")
		}
		seen[id] = true
	}
	return nil
}

// ObserveBatch uses the same admission slots, authority checks and physical
// verifier as Observe. Pack reads are sequential, with at most 4 MiB in the
// entire requested page. Only metadata survives until the final authority
// recheck; no result is exposed if authentication or Root confirmation fails.
func (s *RecordHoldingServer) ObserveBatch(ctx context.Context, req *raftwire.ObserveRecordHoldingsRequest) (*raftwire.ObserveRecordHoldingsResponse, error) {
	if req == nil || proto.Size(req) > recordHoldingBatchMessageBytes || len(req.ProtoReflect().GetUnknown()) != 0 || len(req.RecordDescriptors) == 0 || len(req.RecordDescriptors) > FetchRecordBatchLimit {
		return nil, status.Error(codes.InvalidArgument, "invalid record holding batch")
	}
	if req.ClusterId != s.local.ClusterID || req.FromRaftId == 0 || req.ToRaftId != s.local.RaftID {
		return nil, status.Error(codes.PermissionDenied, "record observation identity mismatch")
	}
	ds := make([]records.Descriptor, len(req.RecordDescriptors))
	for i, b := range req.RecordDescriptors {
		d, err := decodeHoldingDescriptor(b)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid holding batch descriptor")
		}
		ds[i] = d
	}
	if err := validateHoldingBatch(ds); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	release, err := acquireQueuedSlot(ctx, s.slots, s.waiters, transferQueueTimeout, "record observation is busy")
	if err != nil {
		return nil, err
	}
	defer release()
	index, hasCut, err := confirmedRootCutFromMetadata(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Root authorization cut")
	}
	var before [2]rootstate.Node
	if hasCut {
		before, index, err = s.qualifyAtRootCut(ctx, req.FromRaftId, index)
		if status.Code(err) == codes.Unavailable {
			hasCut = false
			before, index, err = s.qualify(ctx, req.FromRaftId)
		}
	} else {
		before, index, err = s.qualify(ctx, req.FromRaftId)
	}
	if err != nil {
		return nil, err
	}
	holdings := make([]records.Holding, len(ds))
	failures := make([]error, len(ds))
	for i, d := range ds {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		holdings[i], failures[i] = s.store.Verify(ctx, d)
	}
	var after [2]rootstate.Node
	if hasCut {
		after, _, err = s.qualifyAtRootCut(ctx, req.FromRaftId, index)
	} else {
		after, index, err = s.qualify(ctx, req.FromRaftId)
	}
	if err != nil {
		return nil, err
	}
	if before != after {
		return nil, status.Error(codes.FailedPrecondition, "record observation Node registration changed")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	response := &raftwire.ObserveRecordHoldingsResponse{Version: 1, ClusterId: s.local.ClusterID, RaftId: s.local.RaftID, NodeId: s.local.NodeID, RootConfirmedIndex: index}
	for i, d := range ds {
		row := &raftwire.ObserveRecordHoldingResult{}
		if failures[i] != nil {
			e := status.Convert(recordBatchReadError(failures[i]))
			row.ErrorCode = uint32(e.Code())
			row.Error = e.Message()
		} else {
			h, err := s.holdingResponse(d, holdings[i], index)
			if err != nil {
				return nil, err
			}
			row.Holding = h
		}
		response.Results = append(response.Results, row)
	}
	return response, nil
}

// ObserveRecordHoldings amortizes transport/authentication for a small page.
// Unimplemented is returned to the page caller for bounded individual fallback.
// Malformed, denied or partially valid replies never expose successful rows.
func (c *GroupClient) ObserveRecordHoldings(ctx context.Context, target uint64, ds []records.Descriptor) ([]RecordHoldingResult, error) {
	if c == nil || ctx == nil || target == 0 || target == c.options.RaftID {
		return nil, errors.New("invalid record observation target")
	}
	if err := validateHoldingBatch(ds); err != nil {
		return nil, err
	}
	owned := make([]records.Descriptor, len(ds))
	encoded := make([][]byte, len(ds))
	for i, d := range ds {
		d.Key = bytes.Clone(d.Key)
		b, err := json.Marshal(d)
		if err != nil || len(b) > recordDescriptorBytes {
			return nil, errors.New("invalid holding batch descriptor")
		}
		owned[i], encoded[i] = d, b
	}
	ds = owned
	ctx, cancel := context.WithTimeout(ctx, recordHoldingTimeout)
	defer cancel()
	ctx = appendConfirmedRootCut(ctx)
	if _, err := c.peer(target); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed || c.peers[target] == nil {
		c.mu.Unlock()
		return nil, errors.New("Group client is closed")
	}
	remote := raftwire.NewRecordHoldingServiceClient(c.peers[target].conn)
	c.mu.Unlock()
	var actual peer.Peer
	response, err := remote.ObserveBatch(ctx, &raftwire.ObserveRecordHoldingsRequest{ClusterId: c.options.ClusterID, FromRaftId: c.options.RaftID, ToRaftId: target, RecordDescriptors: encoded}, grpc.Peer(&actual), grpc.MaxCallSendMsgSize(recordHoldingBatchMessageBytes), grpc.MaxCallRecvMsgSize(recordHoldingBatchMessageBytes))

	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return decodeHoldingBatch(ctx, &actual, c.options.ClusterID, target, ds, response)
}

func decodeHoldingBatch(ctx context.Context, transport *peer.Peer, cluster string, target uint64, ds []records.Descriptor, r *raftwire.ObserveRecordHoldingsResponse) ([]RecordHoldingResult, error) {
	bad := errors.New("invalid record holding batch response")
	if r == nil || r.Version != 1 || proto.Size(r) > recordHoldingBatchMessageBytes || len(r.ProtoReflect().GetUnknown()) != 0 || r.ClusterId != cluster || r.RaftId != target || r.RootConfirmedIndex == 0 || !validHoldingID(r.NodeId, "node_") || len(r.Results) != len(ds) || transport == nil {
		return nil, bad
	}
	if minimum, ok := router.ConfirmedRootIndex(ctx); ok && r.RootConfirmedIndex < minimum {
		return nil, bad
	}
	cert, err := identity.VerifiedPeerCertificate(peer.NewContext(ctx, transport), time.Now())
	if err != nil {
		return nil, bad
	}
	actual, err := identity.ParseNodeCertificate(cert)
	if err != nil || actual.ClusterID != cluster || actual.RaftID != target || actual.NodeID != r.NodeId {
		return nil, bad
	}
	rows := make([]RecordHoldingResult, len(ds))
	for i, result := range r.Results {
		if result == nil || len(result.ProtoReflect().GetUnknown()) != 0 {
			return nil, bad
		}
		if result.ErrorCode != 0 {
			code := codes.Code(result.ErrorCode)
			if result.Holding != nil || len(result.Error) == 0 || len(result.Error) > 256 {
				return nil, bad
			}
			switch code {
			case codes.NotFound, codes.DataLoss, codes.Unavailable, codes.FailedPrecondition, codes.ResourceExhausted:
			default:
				return nil, bad
			}
			rows[i].Err = status.Error(code, result.Error)
			continue
		}
		h := result.Holding
		if result.Error != "" || h == nil || len(h.ProtoReflect().GetUnknown()) != 0 || h.NodeId != r.NodeId || h.RootConfirmedIndex != r.RootConfirmedIndex {
			return nil, bad
		}
		rows[i].Observation, err = decodeHoldingObservation(cluster, target, ds[i], h)
		if err != nil {
			return nil, bad
		}
	}
	return rows, nil
}
