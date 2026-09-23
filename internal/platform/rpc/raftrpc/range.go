package raftrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/dubi-project/dubi/internal/platform/cluster/rangebootstrap"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxRangeDescriptorSize = 64 << 10

type RangeControlServer struct {
	raftwire.UnimplementedRangeControlServiceServer
	clusterID string
	raftID    uint64
	owners    rangebootstrap.OwnerResolver
	authorize AuthorizePeer
}

func NewRangeControlServer(clusterID string, raftID uint64, owners rangebootstrap.OwnerResolver,
	authorize AuthorizePeer,
) (*RangeControlServer, error) {
	if clusterID == "" || raftID == 0 || owners == nil || authorize == nil {
		return nil, errors.New("Range control server requires ClusterID, RaftID, Owner resolver and peer authorizer")
	}
	return &RangeControlServer{clusterID: clusterID, raftID: raftID, owners: owners, authorize: authorize}, nil
}

func (server *RangeControlServer) EnsureInitialRange(
	ctx context.Context, request *raftwire.EnsureInitialRangeRequest,
) (*raftwire.EnsureInitialRangeResponse, error) {
	if request.GetClusterId() != server.clusterID {
		return nil, status.Error(codes.PermissionDenied, "Range control request belongs to a different Cluster")
	}
	if request.GetFromRaftId() == 0 || request.GetToRaftId() != server.raftID {
		return nil, status.Error(codes.FailedPrecondition, "Range control request has an invalid sender or target")
	}
	if err := raft.ValidateGroupID(request.GetGroupId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := authorizeSender(ctx, request.GetFromRaftId(), server.authorize); err != nil {
		return nil, err
	}
	if len(request.GetRangeDescriptor()) == 0 || len(request.GetRangeDescriptor()) > maxRangeDescriptorSize {
		return nil, status.Error(codes.ResourceExhausted, "Range descriptor is empty or exceeds size limit")
	}
	var descriptor rootstate.Range
	decoder := json.NewDecoder(bytes.NewReader(request.GetRangeDescriptor()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&descriptor); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decode Range descriptor: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, status.Error(codes.InvalidArgument, "trailing Range descriptor data")
	}
	if descriptor.Validate() != nil || descriptor.OwnerGroupID != request.GetGroupId() ||
		descriptor.State != rootstate.RangeStateActive || descriptor.Epoch != 1 ||
		len(descriptor.Start) != 0 || len(descriptor.End) != 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid initial Range descriptor")
	}
	owner, found := server.owners(request.GetGroupId())
	if !found || owner == nil {
		return nil, status.Error(codes.NotFound, "Range Owner Group is not hosted on this Node")
	}
	proof, err := owner.EnsureInitialRange(ctx, descriptor)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "ensure initial Range: %v", err)
	}
	if proof.RequestID != rangebootstrap.OwnerRequestID(descriptor) || proof.AppliedIndex == 0 {
		return nil, status.Error(codes.Internal, "Range Owner returned invalid proof")
	}
	return &raftwire.EnsureInitialRangeResponse{OwnerRequestId: proof.RequestID,
		OwnerAppliedIndex: proof.AppliedIndex}, nil
}

type remoteOwner struct {
	client  *GroupClient
	target  uint64
	groupID string
}

func (client *GroupClient) Owner(target uint64, groupID string) (rangebootstrap.OwnerControl, error) {
	if client == nil || target == 0 || target == client.options.RaftID || raft.ValidateGroupID(groupID) != nil ||
		groupID == raft.RootGroupID {
		return nil, errors.New("invalid remote Range Owner")
	}
	return &remoteOwner{client: client, target: target, groupID: groupID}, nil
}

func (owner *remoteOwner) EnsureInitialRange(
	ctx context.Context, descriptor rootstate.Range,
) (rangebootstrap.OwnerProof, error) {
	if descriptor.OwnerGroupID != owner.groupID || descriptor.Validate() != nil ||
		descriptor.State != rootstate.RangeStateActive || descriptor.Epoch != 1 {
		return rangebootstrap.OwnerProof{}, errors.New("invalid remote initial Range")
	}
	encoded, err := json.Marshal(descriptor)
	if err != nil {
		return rangebootstrap.OwnerProof{}, err
	}
	peer, err := owner.client.rangePeer(owner.target)
	if err != nil {
		return rangebootstrap.OwnerProof{}, err
	}
	response, err := peer.EnsureInitialRange(ctx, &raftwire.EnsureInitialRangeRequest{
		ClusterId: owner.client.options.ClusterID, GroupId: owner.groupID,
		FromRaftId: owner.client.options.RaftID, ToRaftId: owner.target, RangeDescriptor: encoded})
	if err != nil {
		return rangebootstrap.OwnerProof{}, err
	}
	proof := rangebootstrap.OwnerProof{RequestID: response.GetOwnerRequestId(),
		AppliedIndex: response.GetOwnerAppliedIndex()}
	if proof.RequestID != rangebootstrap.OwnerRequestID(descriptor) || proof.AppliedIndex == 0 {
		return rangebootstrap.OwnerProof{}, fmt.Errorf("remote Range Owner returned invalid proof")
	}
	return proof, nil
}

func (client *GroupClient) rangePeer(raftID uint64) (raftwire.RangeControlServiceClient, error) {
	if _, err := client.peer(raftID); err != nil {
		return nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed || client.peers[raftID] == nil {
		return nil, errors.New("Group client is closed")
	}
	return client.peers[raftID].ranges, nil
}
