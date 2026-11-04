package raftrpc

import (
	"context"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const GeoMirrorBatchLimit = 32
const GeoMirrorBatchBytes = 256 << 10
const geoMirrorBatchWireBytes = GeoMirrorBatchBytes + 4096

// A row is either an exact durable result (including durable rejection), or a
// transport/application wait error. Missing rows never imply completion.
type GeoMirrorBatchResult struct {
	Result enginestate.Result
	Err    error
}

func decodeMirrorBatch(group string, encoded [][]byte) ([]routedstate.Command, error) {
	if len(encoded) == 0 || len(encoded) > GeoMirrorBatchLimit {
		return nil, status.Error(codes.InvalidArgument, "invalid Geo mirror batch count")
	}
	commands := make([]routedstate.Command, len(encoded))
	seen := make(map[string]bool, len(encoded))
	total := 0
	for i, b := range encoded {
		if len(b) == 0 || len(b) > GeoMirrorBatchBytes-total {
			return nil, status.Error(codes.ResourceExhausted, "Geo mirror batch exceeds byte bound")
		}
		total += len(b)
		command, err := routedstate.DecodeCommand(b)
		if err != nil || command.Type != routedstate.MirrorGeo || command.GeoMetadata == nil ||
			command.GeoMetadata.DestinationGroupID != group || seen[command.RequestID] {
			return nil, status.Error(codes.InvalidArgument, "invalid or repeated Geo mirror command")
		}
		commands[i], seen[command.RequestID] = command, true
	}
	return commands, nil
}

// MirrorGeoBatch submits independent, idempotent commands, then obtains ONE
// fresh quorum cut. Ready rows are returned immediately; a missing result is
// retryable and cannot hold successful rows behind its application wait.
func (s *EngineServer) MirrorGeoBatch(ctx context.Context, req *raftwire.EngineMirrorBatchRequest) (*raftwire.EngineMirrorBatchResponse, error) {
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 || proto.Size(req) > geoMirrorBatchWireBytes {
		return nil, status.Error(codes.InvalidArgument, "invalid Geo mirror batch envelope")
	}
	if err := s.validate(ctx, req.ClusterId, req.GroupId, req.FromRaftId, req.ToRaftId); err != nil {
		return nil, err
	}
	commands, err := decodeMirrorBatch(req.GroupId, req.RoutedCommands)
	if err != nil {
		return nil, err
	}
	for _, b := range req.RoutedCommands {
		if len(b) > s.maxSize {
			return nil, status.Error(codes.ResourceExhausted, "Geo mirror exceeds Group command limit")
		}
	}
	state, found := s.states(req.GroupId)
	if !found || state == nil {
		return nil, status.Error(codes.NotFound, "engine Group is not hosted on this Node")
	}
	select {
	case s.mirrorSlots <- struct{}{}:
		defer func() { <-s.mirrorSlots }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "Geo mirror batch workers are busy")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	response := &raftwire.EngineMirrorBatchResponse{Items: make([]*raftwire.EngineMirrorBatchItem, len(commands))}
	for i, command := range commands {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		row := &raftwire.EngineMirrorBatchItem{RequestId: command.RequestID}
		response.Items[i] = row
		if err := s.backend.Submit(req.GroupId, req.RoutedCommands[i]); err != nil {
			row.ErrorCode, row.Error = uint32(status.Code(mutationError("submit Geo mirror", err, codes.Unavailable))), "Geo mirror proposal was not accepted"
		}
	}
	cut, err := s.backend.ReadIndex(ctx, req.GroupId)
	if err != nil {
		return nil, mutationError("confirm Geo mirror batch", err, codes.Unavailable)
	}
	if cut == 0 {
		return nil, status.Error(codes.Unavailable, "Geo mirror batch has no confirmed index")
	}
	for state.AppliedIndex() < cut {
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
	}
	for i, command := range commands {
		row := response.Items[i]
		if row.ErrorCode != 0 {
			continue
		}
		result, found, err := engineResult(state, command)
		switch {
		case err != nil:
			row.ErrorCode, row.Error = uint32(codes.FailedPrecondition), "Geo mirror result does not match the command"
		case !found:
			row.ErrorCode, row.Error = uint32(codes.Unavailable), "Geo mirror result is pending"
		default:
			row.Result = &raftwire.EngineResult{Applied: result.Applied, Error: result.Error, AppliedIndex: result.Index}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return response, nil
}

func (c *GroupClient) MirrorGeoBatch(ctx context.Context, target uint64, group string, encoded [][]byte) ([]GeoMirrorBatchResult, error) {
	engine, err := c.Engine(target, group)
	if err != nil {
		return nil, err
	}
	commands, err := decodeMirrorBatch(group, encoded)
	if err != nil {
		return nil, err
	}
	peer, err := c.enginePeer(engine.target)
	if err != nil {
		return nil, err
	}
	response, err := peer.MirrorGeoBatch(ctx, &raftwire.EngineMirrorBatchRequest{
		ClusterId: c.options.ClusterID, GroupId: group, FromRaftId: c.options.RaftID, ToRaftId: target, RoutedCommands: encoded,
	}, grpc.MaxCallSendMsgSize(geoMirrorBatchWireBytes), grpc.MaxCallRecvMsgSize(geoMirrorBatchWireBytes))
	if err != nil {
		return nil, err
	}
	return decodeMirrorBatchResults(commands, response)
}

func decodeMirrorBatchResults(commands []routedstate.Command, response *raftwire.EngineMirrorBatchResponse) ([]GeoMirrorBatchResult, error) {
	bad := func() ([]GeoMirrorBatchResult, error) {
		return nil, status.Error(codes.DataLoss, "invalid Geo mirror batch response")
	}
	if response == nil || len(response.Items) != len(commands) || len(response.ProtoReflect().GetUnknown()) != 0 {
		return bad()
	}
	results := make([]GeoMirrorBatchResult, len(commands))
	for i, row := range response.Items {
		if row == nil || len(row.ProtoReflect().GetUnknown()) != 0 || row.RequestId != commands[i].RequestID {
			return bad()
		}
		if row.ErrorCode != 0 {
			if row.Result != nil || row.Error == "" || row.ErrorCode > uint32(codes.Unauthenticated) {
				return bad()
			}
			results[i].Err = status.Error(codes.Code(row.ErrorCode), row.Error)
			continue
		}
		r := row.Result
		if row.Error != "" || r == nil || r.AppliedIndex == 0 || len(r.ProtoReflect().GetUnknown()) != 0 || len(r.Value) != 0 {
			return bad()
		}
		results[i].Result = enginestate.Result{RequestID: row.RequestId, Index: r.AppliedIndex, Applied: r.Applied, Error: r.Error}
	}
	return results, nil
}
