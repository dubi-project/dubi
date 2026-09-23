package raftrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CapabilityResolver func(groupID string) (*capstate.State, bool)

type CapabilityServer struct {
	raftwire.UnimplementedCapabilityServiceServer
	clusterID string
	raftID    uint64
	backend   GroupBackend
	states    CapabilityResolver
	authorize AuthorizePeer
	maxSize   int
}

func NewCapabilityServer(clusterID string, raftID uint64, backend GroupBackend,
	states CapabilityResolver, authorize AuthorizePeer, maxSize int,
) (*CapabilityServer, error) {
	if clusterID == "" || raftID == 0 || backend == nil || states == nil || authorize == nil {
		return nil, errors.New("Capability server requires ClusterID, RaftID, backend, state resolver and peer authorizer")
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxMessageSize
	}
	return &CapabilityServer{clusterID: clusterID, raftID: raftID, backend: backend,
		states: states, authorize: authorize, maxSize: maxSize}, nil
}

func (server *CapabilityServer) Mutate(
	ctx context.Context, request *raftwire.CapabilityMutateRequest,
) (*raftwire.CapabilityResult, error) {
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	if len(request.GetRoutedCommand()) == 0 || len(request.GetRoutedCommand()) > server.maxSize {
		return nil, status.Error(codes.ResourceExhausted, "routed capability command is empty or exceeds size limit")
	}
	command, err := routedstate.DecodeCommand(request.GetRoutedCommand())
	if err != nil || command.Type != routedstate.Mutation || command.Target == nil ||
		command.Target.OwnerGroupID != request.GetGroupId() {
		return nil, status.Error(codes.InvalidArgument, "invalid routed capability mutation")
	}
	state, found := server.states(request.GetGroupId())
	if !found || state == nil {
		return nil, status.Error(codes.NotFound, "capability Group is not hosted on this Node")
	}
	if err := server.backend.Submit(request.GetGroupId(), request.GetRoutedCommand()); err != nil {
		return nil, status.Errorf(codes.Unavailable, "submit capability mutation: %v", err)
	}
	if _, err := server.backend.ReadIndex(ctx, request.GetGroupId()); err != nil {
		return nil, status.Errorf(codes.Unavailable, "confirm capability mutation: %v", err)
	}
	result, found, err := state.ResultForMutation(command.RequestID, *command.Target, command.Payload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read capability result: %v", err)
	}
	if !found {
		return nil, status.Error(codes.Internal, "confirmed capability result is missing")
	}
	groupStatus, err := server.backend.Status(request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read capability Group status: %v", err)
	}
	return &raftwire.CapabilityResult{Applied: result.Applied, Error: result.Error,
		Value: result.Value, AppliedIndex: result.Index, LeaderRaftId: groupStatus.LeaderID}, nil
}

func (server *CapabilityServer) Read(
	ctx context.Context, request *raftwire.CapabilityReadRequest,
) (*raftwire.CapabilityReadResponse, error) {
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	if len(request.GetRouteTarget()) == 0 || len(request.GetRouteTarget()) > maxRangeDescriptorSize ||
		len(request.GetPayload()) == 0 || len(request.GetPayload()) > server.maxSize {
		return nil, status.Error(codes.ResourceExhausted, "capability read target or payload has invalid size")
	}
	var target router.Target
	decoder := json.NewDecoder(bytes.NewReader(request.GetRouteTarget()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&target); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid capability Route Target")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || target.Validate() != nil ||
		target.OwnerGroupID != request.GetGroupId() {
		return nil, status.Error(codes.InvalidArgument, "invalid capability Route Target")
	}
	state, found := server.states(request.GetGroupId())
	if !found || state == nil {
		return nil, status.Error(codes.NotFound, "capability Group is not hosted on this Node")
	}
	outcome, index, err := state.QueryRouted(ctx, server.backend, target, request.GetPayload())
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "read routed capability: %v", err)
	}
	groupStatus, err := server.backend.Status(request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read capability Group status: %v", err)
	}
	return &raftwire.CapabilityReadResponse{Error: outcome.Rejection, Value: outcome.Value,
		ConfirmedIndex: index, LeaderRaftId: groupStatus.LeaderID}, nil
}

func (server *CapabilityServer) validate(ctx context.Context, clusterID, groupID string, from, to uint64) error {
	if clusterID != server.clusterID {
		return status.Error(codes.PermissionDenied, "capability request belongs to a different Cluster")
	}
	if from == 0 || to != server.raftID {
		return status.Error(codes.FailedPrecondition, "capability request has an invalid sender or target")
	}
	if err := raft.ValidateGroupID(groupID); err != nil || groupID == raft.RootGroupID {
		return status.Error(codes.InvalidArgument, "invalid capability GroupID")
	}
	return authorizeSender(ctx, from, server.authorize)
}

type RemoteCapability struct {
	client  *GroupClient
	target  uint64
	groupID string
}

func (client *GroupClient) Capability(target uint64, groupID string) (*RemoteCapability, error) {
	if client == nil || target == 0 || target == client.options.RaftID || raft.ValidateGroupID(groupID) != nil ||
		groupID == raft.RootGroupID {
		return nil, errors.New("invalid remote capability Owner")
	}
	return &RemoteCapability{client: client, target: target, groupID: groupID}, nil
}

func (capability *RemoteCapability) Mutate(ctx context.Context, encoded []byte) (capstate.Result, error) {
	command, err := routedstate.DecodeCommand(encoded)
	if err != nil || command.Type != routedstate.Mutation || command.Target == nil ||
		command.Target.OwnerGroupID != capability.groupID {
		return capstate.Result{}, errors.New("invalid remote routed mutation")
	}
	peer, err := capability.client.capabilityPeer(capability.target)
	if err != nil {
		return capstate.Result{}, err
	}
	response, err := peer.Mutate(ctx, &raftwire.CapabilityMutateRequest{
		ClusterId: capability.client.options.ClusterID, GroupId: capability.groupID,
		FromRaftId: capability.client.options.RaftID, ToRaftId: capability.target, RoutedCommand: encoded})
	if err != nil {
		return capstate.Result{}, err
	}
	if response.GetAppliedIndex() == 0 {
		return capstate.Result{}, errors.New("remote capability result has no applied index")
	}
	capability.client.observeLeader(capability.groupID, response.GetLeaderRaftId())
	return capstate.Result{RequestID: command.RequestID, Index: response.GetAppliedIndex(),
		Applied: response.GetApplied(), Error: response.GetError(), Value: response.GetValue()}, nil
}

func (capability *RemoteCapability) Read(
	ctx context.Context, target router.Target, payload []byte,
) (capstate.Outcome, uint64, error) {
	if target.Validate() != nil || target.OwnerGroupID != capability.groupID || len(payload) == 0 {
		return capstate.Outcome{}, 0, errors.New("invalid remote routed read")
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		return capstate.Outcome{}, 0, err
	}
	peer, err := capability.client.capabilityPeer(capability.target)
	if err != nil {
		return capstate.Outcome{}, 0, err
	}
	response, err := peer.Read(ctx, &raftwire.CapabilityReadRequest{
		ClusterId: capability.client.options.ClusterID, GroupId: capability.groupID,
		FromRaftId: capability.client.options.RaftID, ToRaftId: capability.target,
		RouteTarget: encoded, Payload: payload})
	if err != nil {
		return capstate.Outcome{}, 0, err
	}
	if response.GetConfirmedIndex() == 0 {
		return capstate.Outcome{}, 0, errors.New("remote capability read has no confirmed index")
	}
	capability.client.observeLeader(capability.groupID, response.GetLeaderRaftId())
	return capstate.Outcome{Value: response.GetValue(), Rejection: response.GetError()},
		response.GetConfirmedIndex(), nil
}

type RoutedCapability struct {
	client  *GroupClient
	groupID string
}

func (client *GroupClient) RoutedCapability(groupID string) (*RoutedCapability, error) {
	if client == nil || client.options.GroupRoutes == nil || raft.ValidateGroupID(groupID) != nil ||
		groupID == raft.RootGroupID {
		return nil, errors.New("routed capability requires a valid Group directory")
	}
	return &RoutedCapability{client: client, groupID: groupID}, nil
}

func (capability *RoutedCapability) Mutate(ctx context.Context, encoded []byte) (capstate.Result, error) {
	candidates := capability.client.options.GroupRoutes.Candidates(capability.groupID)
	if len(candidates) == 0 {
		return capstate.Result{}, errors.New("capability Group has no candidate Nodes")
	}
	var last error
	for _, candidate := range candidates {
		if candidate == capability.client.options.RaftID {
			continue
		}
		remote, err := capability.client.Capability(candidate, capability.groupID)
		if err == nil {
			var result capstate.Result
			result, err = remote.Mutate(ctx, encoded)
			if err == nil {
				return result, nil
			}
		}
		last = err
		if !retryableGroupRPC(ctx, err) {
			return capstate.Result{}, err
		}
	}
	if last == nil {
		last = errors.New("capability Group has no remote candidate Nodes")
	}
	return capstate.Result{}, last
}

func (capability *RoutedCapability) Read(
	ctx context.Context, target router.Target, payload []byte,
) (capstate.Outcome, uint64, error) {
	candidates := capability.client.options.GroupRoutes.Candidates(capability.groupID)
	if len(candidates) == 0 {
		return capstate.Outcome{}, 0, errors.New("capability Group has no candidate Nodes")
	}
	var last error
	for _, candidate := range candidates {
		if candidate == capability.client.options.RaftID {
			continue
		}
		remote, err := capability.client.Capability(candidate, capability.groupID)
		if err == nil {
			var outcome capstate.Outcome
			var index uint64
			outcome, index, err = remote.Read(ctx, target, payload)
			if err == nil {
				return outcome, index, nil
			}
		}
		last = err
		if !retryableGroupRPC(ctx, err) {
			return capstate.Outcome{}, 0, err
		}
	}
	if last == nil {
		last = errors.New("capability Group has no remote candidate Nodes")
	}
	return capstate.Outcome{}, 0, last
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

func (client *GroupClient) capabilityPeer(raftID uint64) (raftwire.CapabilityServiceClient, error) {
	if _, err := client.peer(raftID); err != nil {
		return nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed || client.peers[raftID] == nil {
		return nil, errors.New("Group client is closed")
	}
	return client.peers[raftID].capabilities, nil
}
