package raftrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type EngineResolver func(groupID string) (*enginestate.State, bool)

type EngineServer struct {
	raftwire.UnimplementedEngineServiceServer
	clusterID string
	raftID    uint64
	backend   GroupBackend
	states    EngineResolver
	authorize AuthorizePeer
	maxSize   int
}

func NewEngineServer(clusterID string, raftID uint64, backend GroupBackend,
	states EngineResolver, authorize AuthorizePeer, maxSize int,
) (*EngineServer, error) {
	if clusterID == "" || raftID == 0 || backend == nil || states == nil || authorize == nil {
		return nil, errors.New("Engine server requires ClusterID, RaftID, backend, state resolver and peer authorizer")
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxMessageSize
	}
	return &EngineServer{clusterID: clusterID, raftID: raftID, backend: backend,
		states: states, authorize: authorize, maxSize: maxSize}, nil
}

func (server *EngineServer) Mutate(
	ctx context.Context, request *raftwire.EngineMutateRequest,
) (*raftwire.EngineResult, error) {
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	if len(request.GetRoutedCommand()) == 0 || len(request.GetRoutedCommand()) > server.maxSize {
		return nil, status.Error(codes.ResourceExhausted, "routed engine command is empty or exceeds size limit")
	}
	command, err := routedstate.DecodeCommand(request.GetRoutedCommand())
	if err != nil || !validEngineCommand(command, request.GetGroupId()) {
		return nil, status.Error(codes.InvalidArgument, "invalid routed engine mutation")
	}
	state, found := server.states(request.GetGroupId())
	if !found || state == nil {
		return nil, status.Error(codes.NotFound, "engine Group is not hosted on this Node")
	}
	if err := server.backend.Submit(request.GetGroupId(), request.GetRoutedCommand()); err != nil {
		return nil, mutationError("submit engine mutation", err, codes.Unavailable)
	}
	var result enginestate.Result
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := server.backend.ReadIndex(ctx, request.GetGroupId()); err != nil {
			return nil, status.Errorf(codes.Unavailable, "confirm engine mutation: %v", err)
		}
		var found bool
		result, found, err = engineResult(state, command)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "read engine result: %v", err)
		}
		if found {
			break
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-ticker.C:
		}
	}
	groupStatus, err := server.backend.Status(request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read engine Group status: %v", err)
	}
	return &raftwire.EngineResult{Applied: result.Applied, Error: result.Error,
		Value: result.Value, AppliedIndex: result.Index, LeaderRaftId: groupStatus.LeaderID}, nil
}

// validEngineCommand keeps the existing authenticated engine lane for
// both opaque mutations and the platform-owned Geo metadata checkpoint. The
// latter is still scoped to the destination Group and is applied by the
// engine State's routed adapter; it cannot be used to write engine
// data or bypass the local Region fence.
func validEngineCommand(command routedstate.Command, groupID string) bool {
	switch command.Type {
	case routedstate.Mutation:
		return command.Target != nil && command.Target.OwnerGroupID == groupID
	case routedstate.MirrorGeo:
		return command.GeoMetadata != nil && command.GeoMetadata.DestinationGroupID == groupID
	default:
		return false
	}
}

func engineResult(state *enginestate.State, command routedstate.Command) (enginestate.Result, bool, error) {
	switch command.Type {
	case routedstate.Mutation:
		return state.ResultForMutation(command.RequestID, *command.Target, command.Payload)
	case routedstate.MirrorGeo:
		return state.ResultForGeoMetadataMirror(command.RequestID, *command.GeoMetadata)
	default:
		return enginestate.Result{}, false, errors.New("unsupported routed engine command")
	}
}

func (server *EngineServer) Read(
	ctx context.Context, request *raftwire.EngineReadRequest,
) (*raftwire.EngineReadResponse, error) {
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	if len(request.GetRouteTarget()) == 0 || len(request.GetRouteTarget()) > maxRangeDescriptorSize ||
		len(request.GetPayload()) == 0 || len(request.GetPayload()) > server.maxSize {
		return nil, status.Error(codes.ResourceExhausted, "engine read target or payload has invalid size")
	}
	var target router.Target
	decoder := json.NewDecoder(bytes.NewReader(request.GetRouteTarget()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&target); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid engine Route Target")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || target.Validate() != nil ||
		target.OwnerGroupID != request.GetGroupId() {
		return nil, status.Error(codes.InvalidArgument, "invalid engine Route Target")
	}
	state, found := server.states(request.GetGroupId())
	if !found || state == nil {
		return nil, status.Error(codes.NotFound, "engine Group is not hosted on this Node")
	}
	outcome, index, err := state.QueryRouted(ctx, server.backend, target, request.GetPayload())
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "read routed engine: %v", err)
	}
	groupStatus, err := server.backend.Status(request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read engine Group status: %v", err)
	}
	return &raftwire.EngineReadResponse{Error: outcome.Rejection, Value: outcome.Value,
		ConfirmedIndex: index, LeaderRaftId: groupStatus.LeaderID}, nil
}

func (server *EngineServer) validate(ctx context.Context, clusterID, groupID string, from, to uint64) error {
	if clusterID != server.clusterID {
		return status.Error(codes.PermissionDenied, "engine request belongs to a different Cluster")
	}
	if from == 0 || to != server.raftID {
		return status.Error(codes.FailedPrecondition, "engine request has an invalid sender or target")
	}
	if err := raft.ValidateGroupID(groupID); err != nil || groupID == raft.RootGroupID {
		return status.Error(codes.InvalidArgument, "invalid engine GroupID")
	}
	return authorizeSender(ctx, from, server.authorize)
}

type RemoteEngine struct {
	client  *GroupClient
	target  uint64
	groupID string
}

func (client *GroupClient) Engine(target uint64, groupID string) (*RemoteEngine, error) {
	if client == nil || target == 0 || target == client.options.RaftID || raft.ValidateGroupID(groupID) != nil ||
		groupID == raft.RootGroupID {
		return nil, errors.New("invalid remote engine Owner")
	}
	return &RemoteEngine{client: client, target: target, groupID: groupID}, nil
}

func (engine *RemoteEngine) Mutate(ctx context.Context, encoded []byte) (enginestate.Result, error) {
	command, err := routedstate.DecodeCommand(encoded)
	if err != nil || command.Type != routedstate.Mutation || command.Target == nil ||
		command.Target.OwnerGroupID != engine.groupID {
		return enginestate.Result{}, errors.New("invalid remote routed mutation")
	}
	peer, err := engine.client.enginePeer(engine.target)
	if err != nil {
		return enginestate.Result{}, err
	}
	response, err := peer.Mutate(ctx, &raftwire.EngineMutateRequest{
		ClusterId: engine.client.options.ClusterID, GroupId: engine.groupID,
		FromRaftId: engine.client.options.RaftID, ToRaftId: engine.target, RoutedCommand: encoded})
	if err != nil {
		return enginestate.Result{}, err
	}
	if response.GetAppliedIndex() == 0 {
		return enginestate.Result{}, errors.New("remote engine result has no applied index")
	}
	engine.client.observeLeader(engine.groupID, response.GetLeaderRaftId())
	return enginestate.Result{RequestID: command.RequestID, Index: response.GetAppliedIndex(),
		Applied: response.GetApplied(), Error: response.GetError(), Value: response.GetValue()}, nil
}

// MirrorGeo submits one authenticated, destination-Group Geo metadata
// checkpoint and waits until that Group has a durable Result. It uses the
// same engine RPC transport as ordinary routed writes, but the server
// accepts only the platform-owned MirrorGeo envelope and the local fence
// check decides whether it is applied.
func (engine *RemoteEngine) MirrorGeo(ctx context.Context, encoded []byte) (enginestate.Result, error) {
	command, err := routedstate.DecodeCommand(encoded)
	if err != nil || command.Type != routedstate.MirrorGeo || command.GeoMetadata == nil ||
		command.GeoMetadata.DestinationGroupID != engine.groupID {
		return enginestate.Result{}, errors.New("invalid remote Geo metadata mirror")
	}
	peer, err := engine.client.enginePeer(engine.target)
	if err != nil {
		return enginestate.Result{}, err
	}
	response, err := peer.Mutate(ctx, &raftwire.EngineMutateRequest{
		ClusterId: engine.client.options.ClusterID, GroupId: engine.groupID,
		FromRaftId: engine.client.options.RaftID, ToRaftId: engine.target, RoutedCommand: encoded})
	if err != nil {
		return enginestate.Result{}, err
	}
	if response.GetAppliedIndex() == 0 {
		return enginestate.Result{}, errors.New("remote Geo metadata result has no applied index")
	}
	engine.client.observeLeader(engine.groupID, response.GetLeaderRaftId())
	return enginestate.Result{RequestID: command.RequestID, Index: response.GetAppliedIndex(),
		Applied: response.GetApplied(), Error: response.GetError(), Value: response.GetValue()}, nil
}

func (engine *RemoteEngine) Read(
	ctx context.Context, target router.Target, payload []byte,
) (enginestate.Outcome, uint64, error) {
	if target.Validate() != nil || target.OwnerGroupID != engine.groupID || len(payload) == 0 {
		return enginestate.Outcome{}, 0, errors.New("invalid remote routed read")
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		return enginestate.Outcome{}, 0, err
	}
	peer, err := engine.client.enginePeer(engine.target)
	if err != nil {
		return enginestate.Outcome{}, 0, err
	}
	response, err := peer.Read(ctx, &raftwire.EngineReadRequest{
		ClusterId: engine.client.options.ClusterID, GroupId: engine.groupID,
		FromRaftId: engine.client.options.RaftID, ToRaftId: engine.target,
		RouteTarget: encoded, Payload: payload})
	if err != nil {
		return enginestate.Outcome{}, 0, err
	}
	if response.GetConfirmedIndex() == 0 {
		return enginestate.Outcome{}, 0, errors.New("remote engine read has no confirmed index")
	}
	engine.client.observeLeader(engine.groupID, response.GetLeaderRaftId())
	return enginestate.Outcome{Value: response.GetValue(), Rejection: response.GetError()},
		response.GetConfirmedIndex(), nil
}

type RoutedEngine struct {
	client  *GroupClient
	groupID string
}

func (client *GroupClient) RoutedEngine(groupID string) (*RoutedEngine, error) {
	if client == nil || client.options.GroupRoutes == nil || raft.ValidateGroupID(groupID) != nil ||
		groupID == raft.RootGroupID {
		return nil, errors.New("routed engine requires a valid Group directory")
	}
	return &RoutedEngine{client: client, groupID: groupID}, nil
}

func (engine *RoutedEngine) Mutate(ctx context.Context, encoded []byte) (enginestate.Result, error) {
	candidates := engine.client.options.GroupRoutes.Candidates(engine.groupID)
	if len(candidates) == 0 {
		return enginestate.Result{}, errors.New("engine Group has no candidate Nodes")
	}
	var last error
	for _, candidate := range candidates {
		if candidate == engine.client.options.RaftID {
			continue
		}
		remote, err := engine.client.Engine(candidate, engine.groupID)
		if err == nil {
			var result enginestate.Result
			result, err = remote.Mutate(ctx, encoded)
			if err == nil {
				return result, nil
			}
		}
		last = err
		if !retryableGroupRPC(ctx, err) {
			return enginestate.Result{}, err
		}
	}
	if last == nil {
		last = errors.New("engine Group has no remote candidate Nodes")
	}
	return enginestate.Result{}, last
}

func (engine *RoutedEngine) Read(
	ctx context.Context, target router.Target, payload []byte,
) (enginestate.Outcome, uint64, error) {
	candidates := engine.client.options.GroupRoutes.Candidates(engine.groupID)
	if len(candidates) == 0 {
		return enginestate.Outcome{}, 0, errors.New("engine Group has no candidate Nodes")
	}
	var last error
	for _, candidate := range candidates {
		if candidate == engine.client.options.RaftID {
			continue
		}
		remote, err := engine.client.Engine(candidate, engine.groupID)
		if err == nil {
			var outcome enginestate.Outcome
			var index uint64
			outcome, index, err = remote.Read(ctx, target, payload)
			if err == nil {
				return outcome, index, nil
			}
		}
		last = err
		if !retryableGroupRPC(ctx, err) {
			return enginestate.Outcome{}, 0, err
		}
	}
	if last == nil {
		last = errors.New("engine Group has no remote candidate Nodes")
	}
	return enginestate.Outcome{}, 0, last
}

func retryableGroupRPC(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	if errors.Is(err, ErrPeerUnavailable) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.NotFound:
		return true
	default:
		return false
	}
}

func (client *GroupClient) enginePeer(raftID uint64) (raftwire.EngineServiceClient, error) {
	if _, err := client.peer(raftID); err != nil {
		return nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed || client.peers[raftID] == nil {
		return nil, errors.New("Group client is closed")
	}
	return client.peers[raftID].engines, nil
}
