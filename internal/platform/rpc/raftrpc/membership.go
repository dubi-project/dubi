package raftrpc

import (
	"context"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type MembershipBackend interface {
	groupmembership.LocalBackend
}

const membershipConfBytes = 256 << 10
const membershipMessageBytes = membershipConfBytes + 4096

type MembershipServer struct {
	raftwire.UnimplementedGroupMembershipServiceServer
	clusterID string
	raftID    uint64
	backend   MembershipBackend
	joiner    groupmembership.JoiningControl
	authorize AuthorizePeer
	root      *rootstate.State
	changes   chan struct{}
}

func NewMembershipServer(clusterID string, raftID uint64, backend MembershipBackend,
	joiner groupmembership.JoiningControl, root *rootstate.State, authorize AuthorizePeer,
) (*MembershipServer, error) {
	if clusterID == "" || raftID == 0 || backend == nil || joiner == nil || root == nil || authorize == nil {
		return nil, errors.New("membership server requires ClusterID, RaftID, backend, join manager and peer authorizer")
	}
	return &MembershipServer{clusterID: clusterID, raftID: raftID, backend: backend,
		joiner: joiner, authorize: authorize, root: root, changes: make(chan struct{}, 8)}, nil
}

func (server *MembershipServer) PrepareJoining(
	ctx context.Context, request *raftwire.PrepareJoiningGroupRequest,
) (*raftwire.PrepareJoiningGroupResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "joining task missing")
	}
	if proto.Size(request) > 4096 {
		return nil, status.Error(codes.ResourceExhausted, "joining task too large")
	}
	ctx, release, err := server.admitAction(ctx, proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	task := rootstate.GroupTaskReference{GroupID: request.GroupId, ChangeID: request.ChangeId, ExpectedEpoch: request.ExpectedEpoch}
	if task.Validate() != nil {
		return nil, status.Error(codes.InvalidArgument, "joining request requires an exact task")
	}
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	if request.GetGroupId() == raft.RootGroupID {
		return nil, status.Error(codes.InvalidArgument, "Root Group uses a separate bootstrap workflow")
	}
	if err := server.authorizeAction(ctx, request.FromRaftId, task, server.raftID, false); err != nil {
		return nil, err
	}
	if err := server.joiner.PrepareJoining(ctx, task, server.raftID); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.FailedPrecondition, "prepare joining Group: %v", err)
	}
	if err := server.authorizeAction(ctx, request.FromRaftId, task, server.raftID, false); err != nil {
		return nil, err
	}
	return &raftwire.PrepareJoiningGroupResponse{ClusterId: server.clusterID, GroupId: task.GroupID, RaftId: server.raftID, ChangeId: task.ChangeID, ExpectedEpoch: task.ExpectedEpoch}, nil
}

func (server *MembershipServer) Inspect(
	ctx context.Context, request *raftwire.GroupMembershipRequest,
) (*raftwire.GroupMembershipResponse, error) {
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	index, err := server.backend.ReadIndex(ctx, request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "confirm Group membership: %v", err)
	}
	if err := server.validate(ctx, request.ClusterId, request.GroupId, request.FromRaftId, request.ToRaftId); err != nil {
		return nil, err
	}
	return server.membershipResponse(request.GetGroupId(), index)
}

func (server *MembershipServer) Change(
	ctx context.Context, request *raftwire.GroupMembershipChangeRequest,
) (*raftwire.GroupMembershipResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid membership mutation envelope")
	}
	if proto.Size(request) > membershipMessageBytes || len(request.ExpectedConfState) > membershipConfBytes {
		return nil, status.Error(codes.ResourceExhausted, "membership mutation envelope too large")
	}
	ctx, release, err := server.admitAction(ctx, proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	operation, err := membershipOperation(request.GetOperation())
	if err != nil {
		return nil, err
	}
	mutation := rootstate.GroupMutation{GroupID: request.GroupId, ChangeID: request.ChangeId, ExpectedEpoch: request.ExpectedEpoch, Member: request.MemberRaftId, Operation: operation}
	expected := raft.MembershipCut{Index: request.ExpectedMembershipIndex, Conf: new(raftpb.ConfState)}
	if mutation.Validate() != nil || expected.Index == 0 || len(request.ExpectedConfState) == 0 || proto.Unmarshal(request.ExpectedConfState, expected.Conf) != nil {
		return nil, status.Error(codes.InvalidArgument, "membership change requires a task and exact membership cut")
	}
	if _, err := server.backend.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return nil, observationWaitError(err)
	}
	if err := server.root.AuthorizeGroupMutation(request.FromRaftId, server.raftID, mutation); err != nil {
		if !errors.Is(err, rootstate.ErrGroupMutationUnauthorized) {
			return nil, status.Error(codes.Unavailable, "Root mutation authority is unavailable")
		}
		return nil, status.Error(codes.PermissionDenied, "membership mutation is not authorized by Root")
	}
	if err := authorizeSender(ctx, request.FromRaftId, server.authorize); err != nil {
		return nil, err
	}
	groupStatus, err := server.backend.Status(request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Group status: %v", err)
	}
	if groupStatus.Role != etcdraft.StateLeader || groupStatus.LeaderID != server.raftID {
		return nil, status.Errorf(codes.Unavailable, "membership change requires Group leader %d", groupStatus.LeaderID)
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	err = server.backend.ChangeMembership(request.GroupId, expected, mutation.Member, mutation.Operation)
	if err != nil {
		return nil, mutationError("apply Group membership change", err, codes.FailedPrecondition)
	}
	index, err := server.backend.ReadIndex(ctx, request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "confirm Group membership change: %v", err)
	}
	if _, err := server.backend.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return nil, observationWaitError(err)
	}
	if _, members, err := server.root.RootMembers(request.FromRaftId, server.raftID); err != nil {
		return nil, status.Error(codes.Unavailable, "Root response authority is unavailable")
	} else if !members {
		return nil, status.Error(codes.PermissionDenied, "membership response requires current Root members")
	}
	if err := authorizeSender(ctx, request.FromRaftId, server.authorize); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return server.membershipResponse(request.GetGroupId(), index)
}

func membershipOperation(operation raftwire.GroupMembershipOperation) (raftpb.ConfChangeType, error) {
	switch operation {
	case raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_ADD_LEARNER:
		return raftpb.ConfChangeType_ConfChangeAddLearnerNode, nil
	case raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_PROMOTE_LEARNER:
		return raftpb.ConfChangeType_ConfChangeAddNode, nil
	case raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_REMOVE_MEMBER:
		return raftpb.ConfChangeType_ConfChangeRemoveNode, nil
	default:
		return 0, status.Error(codes.InvalidArgument, "unsupported membership operation")
	}
}

func (server *MembershipServer) TransferLeadership(
	ctx context.Context, request *raftwire.GroupLeadershipTransferRequest,
) (*raftwire.GroupLeadershipTransferResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "transfer task missing")
	}
	ctx, release, err := server.admitAction(ctx, proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	task := rootstate.GroupTaskReference{GroupID: request.GroupId, ChangeID: request.ChangeId, ExpectedEpoch: request.ExpectedEpoch}
	expected := raft.MembershipCut{Index: request.ExpectedMembershipIndex, Conf: new(raftpb.ConfState)}
	if task.Validate() != nil || expected.Index == 0 || request.ExpectedLeaderTerm == 0 || len(request.ExpectedConfState) == 0 || len(request.ExpectedConfState) > membershipConfBytes || proto.Unmarshal(request.ExpectedConfState, expected.Conf) != nil {
		return nil, status.Error(codes.InvalidArgument, "transfer requires a task, membership cut and leader term")
	}
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	if request.GetGroupId() == raft.RootGroupID {
		return nil, status.Error(codes.InvalidArgument, "Root leadership uses the Root topology workflow")
	}
	if request.GetTargetRaftId() == 0 || request.GetTargetRaftId() == server.raftID {
		return nil, status.Error(codes.InvalidArgument, "invalid leadership transfer target")
	}
	if err := server.authorizeAction(ctx, request.FromRaftId, task, request.TargetRaftId, true); err != nil {
		return nil, err
	}
	groupStatus, err := server.backend.Status(request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Group status: %v", err)
	}
	if groupStatus.Role != etcdraft.StateLeader || groupStatus.LeaderID != server.raftID {
		return nil, status.Errorf(codes.Unavailable, "leadership transfer requires Group leader %d", groupStatus.LeaderID)
	}
	transferred, err := server.backend.TransferLeadershipFenced(ctx, request.GetGroupId(), request.GetTargetRaftId(), expected, request.ExpectedLeaderTerm)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Errorf(codes.FailedPrecondition, "transfer Group leadership: %v", err)
	}
	if transferred.LeaderID != request.GetTargetRaftId() {
		return nil, status.Error(codes.Internal, "leadership transfer returned the wrong Leader")
	}
	if err := server.authorizeAction(ctx, request.FromRaftId, task, request.TargetRaftId, true); err != nil {
		return nil, err
	}
	return &raftwire.GroupLeadershipTransferResponse{LeaderRaftId: transferred.LeaderID, ClusterId: server.clusterID, GroupId: task.GroupID, RaftId: server.raftID, ChangeId: task.ChangeID, ExpectedEpoch: task.ExpectedEpoch}, nil
}

func (server *MembershipServer) admitAction(ctx context.Context, size int) (context.Context, func(), error) {
	if size > membershipMessageBytes {
		return nil, nil, status.Error(codes.ResourceExhausted, "membership action too large")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, status.FromContextError(err).Err()
	}
	select {
	case server.changes <- struct{}{}:
	default:
		return nil, nil, status.Error(codes.ResourceExhausted, "membership action service is busy")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	return ctx, func() { cancel(); <-server.changes }, nil
}

func (server *MembershipServer) authorizeAction(ctx context.Context, caller uint64, task rootstate.GroupTaskReference, target uint64, transfer bool) error {
	if _, err := server.backend.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return observationWaitError(err)
	}
	var err error
	if transfer {
		err = server.root.AuthorizeGroupTransfer(caller, server.raftID, task, target)
	} else {
		err = server.root.AuthorizeGroupPreparation(caller, server.raftID, task, target)
	}
	if err != nil {
		if errors.Is(err, rootstate.ErrGroupMutationUnauthorized) {
			return status.Error(codes.PermissionDenied, "membership action is not authorized by the current Root task")
		}
		return status.Error(codes.Unavailable, "Root action authority unavailable")
	}
	if err := authorizeSender(ctx, caller, server.authorize); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

func (server *MembershipServer) membershipResponse(groupID string, index uint64) (*raftwire.GroupMembershipResponse, error) {
	cut, err := server.backend.MembershipCut(groupID)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Group membership: %v", err)
	}
	encoded, err := proto.Marshal(cut.Conf)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode Group membership: %v", err)
	}
	groupStatus, err := server.backend.Status(groupID)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Group status: %v", err)
	}
	if cut.Index == 0 || index == 0 || len(encoded) > membershipConfBytes {
		return nil, status.Error(codes.FailedPrecondition, "membership cut unavailable or too large")
	}
	return &raftwire.GroupMembershipResponse{ConfState: encoded, LeaderRaftId: groupStatus.LeaderID, MembershipIndex: cut.Index, ClusterId: server.clusterID, GroupId: groupID, RaftId: server.raftID, ConfirmedIndex: index, LeaderTerm: groupStatus.Term}, nil
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
	if err := authorizeSender(ctx, from, server.authorize); err != nil {
		return err
	}
	if _, err := server.backend.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return observationWaitError(err)
	}
	if _, members, err := server.root.RootMembers(from, server.raftID); err != nil {
		return status.Error(codes.Unavailable, "Root membership authority unavailable")
	} else if !members {
		return status.Error(codes.PermissionDenied, "membership service requires current Root members")
	}
	return authorizeSender(ctx, from, server.authorize)
}

func (client *GroupClient) inspectMembership(
	ctx context.Context, target uint64, groupID string,
) (*raftpb.ConfState, uint64, error) {
	cut, leader, err := client.inspectMembershipCut(ctx, target, groupID)
	return cut.Conf, leader, err
}

func (client *GroupClient) inspectMembershipCut(ctx context.Context, target uint64, groupID string) (raft.MembershipCut, uint64, error) {
	response, err := client.inspectMembershipResponse(ctx, target, groupID)
	if err != nil {
		return raft.MembershipCut{}, 0, err
	}
	cut, err := decodeMembershipResponse(client.options.ClusterID, target, groupID, response)
	return cut, response.LeaderRaftId, err
}

func (client *GroupClient) inspectMembershipResponse(ctx context.Context, target uint64, groupID string) (*raftwire.GroupMembershipResponse, error) {
	peer, err := client.membershipPeer(target)
	if err != nil {
		return nil, err
	}
	response, err := peer.Inspect(ctx, &raftwire.GroupMembershipRequest{ClusterId: client.options.ClusterID,
		GroupId: groupID, FromRaftId: client.options.RaftID, ToRaftId: target}, grpc.MaxCallRecvMsgSize(membershipMessageBytes))
	if err != nil {
		return nil, err
	}
	_, err = decodeMembershipResponse(client.options.ClusterID, target, groupID, response)
	if err != nil {
		return nil, err
	}
	client.observeLeader(groupID, response.GetLeaderRaftId())
	return response, nil
}

func decodeMembershipResponse(cluster string, target uint64, group string, r *raftwire.GroupMembershipResponse) (raft.MembershipCut, error) {
	bad := errors.New("invalid or unbound membership response")
	if r == nil || proto.Size(r) > membershipMessageBytes || r.ClusterId != cluster || r.GroupId != group || r.RaftId != target || r.MembershipIndex == 0 || r.ConfirmedIndex == 0 || r.LeaderTerm == 0 || len(r.ConfState) == 0 || len(r.ConfState) > membershipConfBytes {
		return raft.MembershipCut{}, bad
	}
	conf := new(raftpb.ConfState)
	if proto.Unmarshal(r.ConfState, conf) != nil {
		return raft.MembershipCut{}, bad
	}
	return raft.MembershipCut{Index: r.MembershipIndex, Conf: conf}, nil
}

// InspectMembership addresses a known authorized initial participant before a
// Group's routing descriptor has been published. It grants no membership.
func (client *GroupClient) InspectMembership(ctx context.Context, target uint64, groupID string) (*raftpb.ConfState, uint64, error) {
	if target == 0 || raft.ValidateGroupID(groupID) != nil {
		return nil, 0, errors.New("invalid Group inspection target")
	}
	return client.inspectMembership(ctx, target, groupID)
}

func (client *GroupClient) changeMembership(ctx context.Context, target uint64, mutation rootstate.GroupMutation,
) (*raftpb.ConfState, uint64, error) {
	if err := mutation.Validate(); err != nil {
		return nil, 0, err
	}
	cut, _, err := client.inspectMembershipCut(ctx, target, mutation.GroupID)
	if err != nil {
		return nil, 0, err
	}
	encoded, err := proto.Marshal(cut.Conf)
	if err != nil {
		return nil, 0, err
	}
	var operation raftwire.GroupMembershipOperation
	switch mutation.Operation {
	case raftpb.ConfChangeType_ConfChangeAddLearnerNode:
		operation = raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_ADD_LEARNER
	case raftpb.ConfChangeType_ConfChangeAddNode:
		operation = raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_PROMOTE_LEARNER
	case raftpb.ConfChangeType_ConfChangeRemoveNode:
		operation = raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_REMOVE_MEMBER
	}
	peer, err := client.membershipPeer(target)
	if err != nil {
		return nil, 0, err
	}
	response, err := peer.Change(ctx, &raftwire.GroupMembershipChangeRequest{ClusterId: client.options.ClusterID,
		GroupId: mutation.GroupID, FromRaftId: client.options.RaftID, ToRaftId: target,
		Operation: operation, MemberRaftId: mutation.Member, ChangeId: mutation.ChangeID, ExpectedEpoch: mutation.ExpectedEpoch, ExpectedMembershipIndex: cut.Index, ExpectedConfState: encoded}, grpc.MaxCallSendMsgSize(membershipMessageBytes), grpc.MaxCallRecvMsgSize(membershipMessageBytes))
	if err != nil {
		return nil, 0, err
	}
	result, err := decodeMembershipResponse(client.options.ClusterID, target, mutation.GroupID, response)
	if err != nil {
		return nil, 0, err
	}
	client.observeLeader(mutation.GroupID, response.GetLeaderRaftId())
	return result.Conf, response.GetLeaderRaftId(), nil
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

func (control *RoutedMembershipControl) PrepareJoining(ctx context.Context, task rootstate.GroupTaskReference, raftID uint64) error {
	if err := task.Validate(); err != nil {
		return err
	}
	if raftID == 0 {
		return errors.New("joining Group target has no RaftID")
	}
	if raftID == control.client.options.RaftID {
		if control.local == nil {
			return errors.New("local joining Group control is unavailable")
		}
		return control.local.PrepareJoining(ctx, task, raftID)
	}
	peer, err := control.client.membershipPeer(raftID)
	if err != nil {
		return err
	}
	response, err := peer.PrepareJoining(ctx, &raftwire.PrepareJoiningGroupRequest{
		ClusterId: control.client.options.ClusterID, GroupId: task.GroupID,
		FromRaftId: control.client.options.RaftID, ToRaftId: raftID, ChangeId: task.ChangeID, ExpectedEpoch: task.ExpectedEpoch}, grpc.MaxCallSendMsgSize(4096), grpc.MaxCallRecvMsgSize(4096))
	if err != nil {
		return err
	}
	return validatePreparationResponse(control.client.options.ClusterID, raftID, task, response)
}

func validatePreparationResponse(cluster string, target uint64, task rootstate.GroupTaskReference, response *raftwire.PrepareJoiningGroupResponse) error {
	if response == nil || proto.Size(response) > 4096 || response.ClusterId != cluster || response.GroupId != task.GroupID || response.RaftId != target || response.ChangeId != task.ChangeID || response.ExpectedEpoch != task.ExpectedEpoch {
		return errors.New("unbound joining response")
	}
	return nil
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

func (control *RoutedMembershipControl) ChangeMembership(ctx context.Context, mutation rootstate.GroupMutation) error {
	if err := mutation.Validate(); err != nil {
		return err
	}
	groupID := mutation.GroupID
	return control.route(ctx, groupID, func(candidate uint64) error {
		if candidate == control.client.options.RaftID && control.local != nil {
			localStatus, err := control.local.Status(ctx, groupID)
			if err != nil {
				return err
			}
			if localStatus.LeaderID != candidate {
				return status.Errorf(codes.Unavailable, "membership change requires Group leader %d", localStatus.LeaderID)
			}
			return control.local.ChangeMembership(ctx, mutation)
		}
		_, _, err := control.client.changeMembership(ctx, candidate, mutation)
		return err
	})
}

func (control *RoutedMembershipControl) TransferLeadership(
	ctx context.Context, task rootstate.GroupTaskReference, target uint64,
) (raft.GroupStatus, error) {
	if err := task.Validate(); err != nil {
		return raft.GroupStatus{}, err
	}
	groupID := task.GroupID
	var result raft.GroupStatus
	err := control.route(ctx, groupID, func(candidate uint64) error {
		if candidate == control.client.options.RaftID && control.local != nil {
			localStatus, err := control.local.Status(ctx, groupID)
			if err != nil {
				return err
			}
			if localStatus.LeaderID != candidate {
				return status.Errorf(codes.Unavailable, "leadership transfer requires Group leader %d", localStatus.LeaderID)
			}
			result, err = control.local.TransferLeadership(ctx, task, target)
			return err
		}
		peer, err := control.client.membershipPeer(candidate)
		if err != nil {
			return err
		}
		observed, err := control.client.inspectMembershipResponse(ctx, candidate, groupID)
		if err != nil {
			return err
		}
		if observed.LeaderRaftId != candidate {
			return status.Errorf(codes.Unavailable, "leadership transfer requires Group leader %d", observed.LeaderRaftId)
		}
		response, err := peer.TransferLeadership(ctx, &raftwire.GroupLeadershipTransferRequest{
			ClusterId: control.client.options.ClusterID, GroupId: groupID,
			FromRaftId: control.client.options.RaftID, ToRaftId: candidate, TargetRaftId: target, ChangeId: task.ChangeID, ExpectedEpoch: task.ExpectedEpoch, ExpectedMembershipIndex: observed.MembershipIndex, ExpectedConfState: observed.ConfState, ExpectedLeaderTerm: observed.LeaderTerm}, grpc.MaxCallSendMsgSize(membershipMessageBytes), grpc.MaxCallRecvMsgSize(4096))
		if err == nil {
			if err := validateTransferResponse(control.client.options.ClusterID, candidate, task, target, response); err != nil {
				return err
			}
			control.client.observeLeader(groupID, response.GetLeaderRaftId())
			result = raft.GroupStatus{RaftID: candidate, LeaderID: response.GetLeaderRaftId()}
		}
		return err
	})
	return result, err
}

func validateTransferResponse(cluster string, server uint64, task rootstate.GroupTaskReference, target uint64, response *raftwire.GroupLeadershipTransferResponse) error {
	if response == nil || proto.Size(response) > 4096 || response.ClusterId != cluster || response.GroupId != task.GroupID || response.RaftId != server || response.ChangeId != task.ChangeID || response.ExpectedEpoch != task.ExpectedEpoch || response.LeaderRaftId != target {
		return errors.New("unbound leadership transfer response")
	}
	return nil
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
