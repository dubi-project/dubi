package raftrpc

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type MembershipBackend interface {
	ReadIndex(ctx context.Context, groupID string) (uint64, error)
	Membership(groupID string) (*raftpb.ConfState, error)
	Status(groupID string) (raft.GroupStatus, error)
	AddLearner(groupID string, raftID uint64) error
	PromoteLearner(groupID string, raftID uint64) error
	RemoveMember(groupID string, raftID uint64) error
	TransferLeadership(ctx context.Context, groupID string, target uint64) (raft.GroupStatus, error)
}

type MembershipServer struct {
	raftwire.UnimplementedGroupMembershipServiceServer
	clusterID string
	raftID    uint64
	backend   MembershipBackend
	joiner    groupmembership.JoiningControl
	authorize AuthorizePeer
}

func NewMembershipServer(clusterID string, raftID uint64, backend MembershipBackend,
	joiner groupmembership.JoiningControl, authorize AuthorizePeer,
) (*MembershipServer, error) {
	if clusterID == "" || raftID == 0 || backend == nil || joiner == nil || authorize == nil {
		return nil, errors.New("membership server requires ClusterID, RaftID, backend, join manager and peer authorizer")
	}
	return &MembershipServer{clusterID: clusterID, raftID: raftID, backend: backend,
		joiner: joiner, authorize: authorize}, nil
}

func (server *MembershipServer) PrepareJoining(
	ctx context.Context, request *raftwire.PrepareJoiningGroupRequest,
) (*raftwire.PrepareJoiningGroupResponse, error) {
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	if request.GetGroupId() == raft.RootGroupID {
		return nil, status.Error(codes.InvalidArgument, "Root Group uses a separate bootstrap workflow")
	}
	if err := server.joiner.PrepareJoining(ctx, request.GetGroupId(), server.raftID); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "prepare joining Group: %v", err)
	}
	return &raftwire.PrepareJoiningGroupResponse{}, nil
}

func (server *MembershipServer) Inspect(
	ctx context.Context, request *raftwire.GroupMembershipRequest,
) (*raftwire.GroupMembershipResponse, error) {
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	if _, err := server.backend.ReadIndex(ctx, request.GetGroupId()); err != nil {
		return nil, status.Errorf(codes.Unavailable, "confirm Group membership: %v", err)
	}
	return server.membershipResponse(request.GetGroupId())
}

func (server *MembershipServer) Change(
	ctx context.Context, request *raftwire.GroupMembershipChangeRequest,
) (*raftwire.GroupMembershipResponse, error) {
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	if request.GetMemberRaftId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "membership change has no member RaftID")
	}
	groupStatus, err := server.backend.Status(request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Group status: %v", err)
	}
	if groupStatus.Role != etcdraft.StateLeader || groupStatus.LeaderID != server.raftID {
		return nil, status.Errorf(codes.Unavailable, "membership change requires Group leader %d", groupStatus.LeaderID)
	}
	switch request.GetOperation() {
	case raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_ADD_LEARNER:
		err = server.backend.AddLearner(request.GetGroupId(), request.GetMemberRaftId())
	case raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_PROMOTE_LEARNER:
		err = server.backend.PromoteLearner(request.GetGroupId(), request.GetMemberRaftId())
	case raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_REMOVE_MEMBER:
		err = server.backend.RemoveMember(request.GetGroupId(), request.GetMemberRaftId())
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported membership operation")
	}
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "apply Group membership change: %v", err)
	}
	if _, err := server.backend.ReadIndex(ctx, request.GetGroupId()); err != nil {
		return nil, status.Errorf(codes.Unavailable, "confirm Group membership change: %v", err)
	}
	return server.membershipResponse(request.GetGroupId())
}

func (server *MembershipServer) TransferLeadership(
	ctx context.Context, request *raftwire.GroupLeadershipTransferRequest,
) (*raftwire.GroupLeadershipTransferResponse, error) {
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	if request.GetTargetRaftId() == 0 || request.GetTargetRaftId() == server.raftID {
		return nil, status.Error(codes.InvalidArgument, "invalid leadership transfer target")
	}
	groupStatus, err := server.backend.Status(request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Group status: %v", err)
	}
	if groupStatus.Role != etcdraft.StateLeader || groupStatus.LeaderID != server.raftID {
		return nil, status.Errorf(codes.Unavailable, "leadership transfer requires Group leader %d", groupStatus.LeaderID)
	}
	transferred, err := server.backend.TransferLeadership(ctx, request.GetGroupId(), request.GetTargetRaftId())
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "transfer Group leadership: %v", err)
	}
	if transferred.LeaderID != request.GetTargetRaftId() {
		return nil, status.Error(codes.Internal, "leadership transfer returned the wrong Leader")
	}
	return &raftwire.GroupLeadershipTransferResponse{LeaderRaftId: transferred.LeaderID}, nil
}

func (server *MembershipServer) membershipResponse(groupID string) (*raftwire.GroupMembershipResponse, error) {
	conf, err := server.backend.Membership(groupID)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Group membership: %v", err)
	}
	encoded, err := proto.Marshal(conf)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode Group membership: %v", err)
	}
	groupStatus, err := server.backend.Status(groupID)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Group status: %v", err)
	}
	return &raftwire.GroupMembershipResponse{ConfState: encoded, LeaderRaftId: groupStatus.LeaderID}, nil
}

func (server *MembershipServer) validate(ctx context.Context, clusterID, groupID string, from, to uint64) error {
	if clusterID != server.clusterID {
		return status.Error(codes.PermissionDenied, "membership request belongs to a different Cluster")
	}
	if from == 0 || to != server.raftID {
		return status.Error(codes.FailedPrecondition, "membership request has an invalid sender or target")
	}
	if err := raft.ValidateGroupID(groupID); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return authorizeSender(ctx, from, server.authorize)
}

func (client *GroupClient) inspectMembership(
	ctx context.Context, target uint64, groupID string,
) (*raftpb.ConfState, uint64, error) {
	peer, err := client.membershipPeer(target)
	if err != nil {
		return nil, 0, err
	}
	response, err := peer.Inspect(ctx, &raftwire.GroupMembershipRequest{ClusterId: client.options.ClusterID,
		GroupId: groupID, FromRaftId: client.options.RaftID, ToRaftId: target})
	if err != nil {
		return nil, 0, err
	}
	conf := new(raftpb.ConfState)
	if len(response.GetConfState()) == 0 || proto.Unmarshal(response.GetConfState(), conf) != nil {
		return nil, 0, errors.New("membership response has an invalid ConfState")
	}
	client.observeLeader(groupID, response.GetLeaderRaftId())
	return conf, response.GetLeaderRaftId(), nil
}

func (client *GroupClient) changeMembership(ctx context.Context, target uint64, groupID string,
	operation raftwire.GroupMembershipOperation, member uint64,
) (*raftpb.ConfState, uint64, error) {
	peer, err := client.membershipPeer(target)
	if err != nil {
		return nil, 0, err
	}
	response, err := peer.Change(ctx, &raftwire.GroupMembershipChangeRequest{ClusterId: client.options.ClusterID,
		GroupId: groupID, FromRaftId: client.options.RaftID, ToRaftId: target,
		Operation: operation, MemberRaftId: member})
	if err != nil {
		return nil, 0, err
	}
	conf := new(raftpb.ConfState)
	if len(response.GetConfState()) == 0 || proto.Unmarshal(response.GetConfState(), conf) != nil {
		return nil, 0, errors.New("membership change response has an invalid ConfState")
	}
	client.observeLeader(groupID, response.GetLeaderRaftId())
	return conf, response.GetLeaderRaftId(), nil
}

// RoutedMembershipControl resolves the current confirmed members plus members
// named by the active persistent task. The latter is required after leadership
// moves to a newly promoted Voter before Root publication completes.
type RoutedMembershipControl struct {
	client *GroupClient
	root   *rootstate.State
	local  groupmembership.GroupControl
}

func NewRoutedMembershipControl(client *GroupClient, root *rootstate.State,
	local groupmembership.GroupControl,
) (*RoutedMembershipControl, error) {
	if client == nil || root == nil || client.options.GroupRoutes == nil {
		return nil, errors.New("routed membership control requires Group client, Root state and routes")
	}
	return &RoutedMembershipControl{client: client, root: root, local: local}, nil
}

func (control *RoutedMembershipControl) PrepareJoining(ctx context.Context, groupID string, raftID uint64) error {
	if raftID == 0 {
		return errors.New("joining Group target has no RaftID")
	}
	if raftID == control.client.options.RaftID {
		if control.local == nil {
			return errors.New("local joining Group control is unavailable")
		}
		return control.local.PrepareJoining(ctx, groupID, raftID)
	}
	peer, err := control.client.membershipPeer(raftID)
	if err != nil {
		return err
	}
	_, err = peer.PrepareJoining(ctx, &raftwire.PrepareJoiningGroupRequest{
		ClusterId: control.client.options.ClusterID, GroupId: groupID,
		FromRaftId: control.client.options.RaftID, ToRaftId: raftID})
	return err
}

func (control *RoutedMembershipControl) Membership(ctx context.Context, groupID string) (*raftpb.ConfState, error) {
	var result *raftpb.ConfState
	err := control.route(ctx, groupID, func(candidate uint64) error {
		if candidate == control.client.options.RaftID && control.local != nil {
			var err error
			result, err = control.local.Membership(ctx, groupID)
			return err
		}
		conf, _, err := control.client.inspectMembership(ctx, candidate, groupID)
		result = conf
		return err
	})
	return result, err
}

func (control *RoutedMembershipControl) Status(ctx context.Context, groupID string) (raft.GroupStatus, error) {
	var result raft.GroupStatus
	err := control.route(ctx, groupID, func(candidate uint64) error {
		if candidate == control.client.options.RaftID && control.local != nil {
			localStatus, err := control.local.Status(ctx, groupID)
			if err == nil {
				result = raft.GroupStatus{RaftID: localStatus.LeaderID, LeaderID: localStatus.LeaderID}
			}
			return err
		}
		_, leader, err := control.client.inspectMembership(ctx, candidate, groupID)
		result = raft.GroupStatus{RaftID: leader, LeaderID: leader}
		return err
	})
	return result, err
}

func (control *RoutedMembershipControl) AddLearner(ctx context.Context, groupID string, raftID uint64) error {
	return control.change(ctx, groupID, raftID,
		raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_ADD_LEARNER)
}

func (control *RoutedMembershipControl) PromoteLearner(ctx context.Context, groupID string, raftID uint64) error {
	return control.change(ctx, groupID, raftID,
		raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_PROMOTE_LEARNER)
}

func (control *RoutedMembershipControl) RemoveMember(ctx context.Context, groupID string, raftID uint64) error {
	return control.change(ctx, groupID, raftID,
		raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_REMOVE_MEMBER)
}

func (control *RoutedMembershipControl) change(ctx context.Context, groupID string, raftID uint64,
	operation raftwire.GroupMembershipOperation,
) error {
	return control.route(ctx, groupID, func(candidate uint64) error {
		if candidate == control.client.options.RaftID && control.local != nil {
			localStatus, err := control.local.Status(ctx, groupID)
			if err != nil {
				return err
			}
			if localStatus.LeaderID != candidate {
				return status.Errorf(codes.Unavailable, "membership change requires Group leader %d", localStatus.LeaderID)
			}
			switch operation {
			case raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_ADD_LEARNER:
				return control.local.AddLearner(ctx, groupID, raftID)
			case raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_PROMOTE_LEARNER:
				return control.local.PromoteLearner(ctx, groupID, raftID)
			case raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_REMOVE_MEMBER:
				return control.local.RemoveMember(ctx, groupID, raftID)
			}
		}
		_, _, err := control.client.changeMembership(ctx, candidate, groupID, operation, raftID)
		return err
	})
}

func (control *RoutedMembershipControl) TransferLeadership(
	ctx context.Context, groupID string, target uint64,
) (raft.GroupStatus, error) {
	var result raft.GroupStatus
	err := control.route(ctx, groupID, func(candidate uint64) error {
		if candidate == control.client.options.RaftID && control.local != nil {
			var err error
			result, err = control.local.TransferLeadership(ctx, groupID, target)
			return err
		}
		peer, err := control.client.membershipPeer(candidate)
		if err != nil {
			return err
		}
		response, err := peer.TransferLeadership(ctx, &raftwire.GroupLeadershipTransferRequest{
			ClusterId: control.client.options.ClusterID, GroupId: groupID,
			FromRaftId: control.client.options.RaftID, ToRaftId: candidate, TargetRaftId: target})
		if err == nil {
			control.client.observeLeader(groupID, response.GetLeaderRaftId())
			result = raft.GroupStatus{RaftID: candidate, LeaderID: response.GetLeaderRaftId()}
		}
		return err
	})
	return result, err
}

func (control *RoutedMembershipControl) route(
	ctx context.Context, groupID string, call func(uint64) error,
) error {
	candidates := control.candidates(groupID)
	if len(candidates) == 0 {
		return errors.New("membership Group has no candidate Nodes")
	}
	var last error
	for _, candidate := range candidates {
		if candidate == control.client.options.RaftID && control.local == nil {
			continue
		}
		if err := call(candidate); err == nil {
			return nil
		} else {
			last = err
			if !retryableGroupRPC(ctx, err) {
				return err
			}
		}
	}
	if last == nil {
		last = ErrPeerUnavailable
	}
	return last
}

func (control *RoutedMembershipControl) candidates(groupID string) []uint64 {
	base := control.client.options.GroupRoutes.Candidates(groupID)
	seen := make(map[uint64]bool, len(base))
	result := make([]uint64, 0, len(base))
	for _, candidate := range base {
		if candidate != 0 && !seen[candidate] {
			seen[candidate] = true
			result = append(result, candidate)
		}
	}
	change, found, err := control.root.ActiveGroupChange(groupID)
	if err == nil && found {
		for _, member := range change.Members {
			if !seen[member.RaftID] {
				seen[member.RaftID] = true
				result = append(result, member.RaftID)
			}
		}
	}
	return result
}

func (client *GroupClient) membershipPeer(raftID uint64) (raftwire.GroupMembershipServiceClient, error) {
	if _, err := client.peer(raftID); err != nil {
		return nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed || client.peers[raftID] == nil {
		return nil, errors.New("Group client is closed")
	}
	return client.peers[raftID].memberships, nil
}

var _ groupmembership.GroupControl = (*RoutedMembershipControl)(nil)
