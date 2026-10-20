package raftrpc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	etcdraft "go.etcd.io/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var ErrPeerUnavailable = errors.New("Group RPC peer is unavailable")

type GroupBackend interface {
	Submit(groupID string, command []byte) error
	ReadIndex(ctx context.Context, groupID string) (uint64, error)
	Status(groupID string) (raft.GroupStatus, error)
}

type GroupServer struct {
	raftwire.UnimplementedGroupServiceServer
	clusterID string
	raftID    uint64
	backend   GroupBackend
	authorize AuthorizePeer
	maxSize   int
}

func NewGroupServer(clusterID string, raftID uint64, backend GroupBackend,
	authorize AuthorizePeer, maxSize int,
) (*GroupServer, error) {
	if clusterID == "" || raftID == 0 || backend == nil || authorize == nil {
		return nil, errors.New("Group server requires ClusterID, RaftID, backend and peer authorizer")
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxMessageSize
	}
	return &GroupServer{clusterID: clusterID, raftID: raftID, backend: backend,
		authorize: authorize, maxSize: maxSize}, nil
}

func (server *GroupServer) Submit(
	ctx context.Context, request *raftwire.GroupSubmitRequest,
) (*raftwire.GroupSubmitResponse, error) {
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	if len(request.GetCommand()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Group command is empty")
	}
	if len(request.GetCommand()) > server.maxSize {
		return nil, status.Error(codes.ResourceExhausted, "Group command exceeds size limit")
	}
	if err := server.backend.Submit(request.GetGroupId(), request.GetCommand()); err != nil {
		return nil, mutationError("submit Group command", err, codes.Unavailable)
	}
	index, err := server.backend.ReadIndex(ctx, request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "confirm Group command: %v", err)
	}
	groupStatus, err := server.backend.Status(request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Group status: %v", err)
	}
	return &raftwire.GroupSubmitResponse{ConfirmedIndex: index, LeaderRaftId: groupStatus.LeaderID}, nil
}

func (server *GroupServer) ReadIndex(
	ctx context.Context, request *raftwire.GroupReadIndexRequest,
) (*raftwire.GroupReadIndexResponse, error) {
	if err := server.validate(ctx, request.GetClusterId(), request.GetGroupId(),
		request.GetFromRaftId(), request.GetToRaftId()); err != nil {
		return nil, err
	}
	// A Root barrier must be established by the current leader: Root leaders
	// append and commit a coalesced barrier entry, while a follower's forwarded
	// ReadIndex can remain pending until a current-term entry is committed. Do
	// not route again from this handler (that can form cycles between followers)
	// and do not ask a follower to hold a pending ReadIndex. The caller already
	// has the voter list and will try another voter when this target is stale.
	var groupStatus raft.GroupStatus
	if request.GetGroupId() == raft.RootGroupID {
		var statusErr error
		groupStatus, statusErr = server.backend.Status(request.GetGroupId())
		if statusErr != nil {
			return nil, status.Errorf(codes.Unavailable, "read Root Group status: %v", statusErr)
		}
		if groupStatus.Role != etcdraft.StateLeader || groupStatus.LeaderID != server.raftID {
			if groupStatus.LeaderID != 0 {
				// This is only a transient routing hint. The client still needs a
				// non-zero quorum-confirmed index before it can accept the read.
				return &raftwire.GroupReadIndexResponse{LeaderRaftId: groupStatus.LeaderID}, nil
			}
			return nil, status.Error(codes.Unavailable, "Root Group has no known leader")
		}
	}
	// A Group RPC is already addressed to a concrete member of this Raft
	// Group. Ask that local Raft instance for its barrier; do not invoke the
	// Node-level voter router here, where it could recursively fan out again.
	index, err := server.backend.ReadIndex(ctx, request.GetGroupId())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "confirm Group read: %v", err)
	}
	if request.GetGroupId() != raft.RootGroupID {
		groupStatus, err = server.backend.Status(request.GetGroupId())
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read Group status: %v", err)
		}
	}
	return &raftwire.GroupReadIndexResponse{ConfirmedIndex: index, LeaderRaftId: groupStatus.LeaderID}, nil
}

func (server *GroupServer) validate(ctx context.Context, clusterID, groupID string, from, to uint64) error {
	if clusterID != server.clusterID {
		return status.Error(codes.PermissionDenied, "Group request belongs to a different Cluster")
	}
	if from == 0 || to != server.raftID {
		return status.Error(codes.FailedPrecondition, "Group request has an invalid sender or target")
	}
	if err := raft.ValidateGroupID(groupID); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return authorizeSender(ctx, from, server.authorize)
}

type GroupClientOptions struct {
	ClusterID   string
	RaftID      uint64
	Directory   Directory
	Credentials credentials.TransportCredentials
	MaxSize     int
	GroupRoutes GroupRoutes
}

type groupPeer struct {
	address      string
	conn         *grpc.ClientConn
	client       raftwire.GroupServiceClient
	ranges       raftwire.RangeControlServiceClient
	engines      raftwire.EngineServiceClient
	memberships  raftwire.GroupMembershipServiceClient
	recordConn   *grpc.ClientConn
	recordNodeID string
}

type GroupClient struct {
	mu            sync.Mutex
	options       GroupClientOptions
	peers         map[uint64]*groupPeer
	closed        bool
	recordSlots   chan struct{}
	recordReaders chan struct{}
	recordBuffers *resource.ByteBudget
}

func NewGroupClient(options GroupClientOptions) (*GroupClient, error) {
	if options.ClusterID == "" || options.RaftID == 0 || options.Directory == nil || options.Credentials == nil ||
		options.Credentials.Info().SecurityProtocol != "tls" {
		return nil, errors.New("Group client requires ClusterID, RaftID, Directory and TLS credentials")
	}
	if options.MaxSize <= 0 {
		options.MaxSize = DefaultMaxMessageSize
	}
	return &GroupClient{options: options, peers: make(map[uint64]*groupPeer), recordSlots: make(chan struct{}, recordTransferConcurrency), recordReaders: make(chan struct{}, recordTransferConcurrency), recordBuffers: resource.NewByteBudget(recordTransferBufferBytes)}, nil
}

func (client *GroupClient) Submit(ctx context.Context, target uint64, groupID string, command []byte) (uint64, error) {
	if len(command) == 0 || len(command) > client.options.MaxSize {
		return 0, errors.New("Group command is empty or exceeds size limit")
	}
	peer, err := client.peer(target)
	if err != nil {
		return 0, err
	}
	response, err := peer.Submit(ctx, &raftwire.GroupSubmitRequest{ClusterId: client.options.ClusterID,
		GroupId: groupID, FromRaftId: client.options.RaftID, ToRaftId: target, Command: command})
	if err != nil {
		return 0, err
	}
	if response.GetConfirmedIndex() == 0 {
		return 0, errors.New("Group command response has no confirmed index")
	}
	client.observeLeader(groupID, response.GetLeaderRaftId())
	return response.GetConfirmedIndex(), nil
}

func (client *GroupClient) ReadIndex(ctx context.Context, target uint64, groupID string) (uint64, error) {
	peer, err := client.peer(target)
	if err != nil {
		return 0, err
	}
	response, err := peer.ReadIndex(ctx, &raftwire.GroupReadIndexRequest{ClusterId: client.options.ClusterID,
		GroupId: groupID, FromRaftId: client.options.RaftID, ToRaftId: target})
	if err != nil {
		return 0, err
	}
	client.observeLeader(groupID, response.GetLeaderRaftId())
	if response.GetConfirmedIndex() == 0 {
		return 0, errors.New("Group read response has no confirmed index")
	}
	return response.GetConfirmedIndex(), nil
}

// MirrorGeo submits the platform-owned Geo metadata checkpoint through the
// destination engine Group. The routed engine endpoint waits for the
// destination StateMachine Result, so a successful return means the command
// is durably applied (or has a durable rejection), not merely queued.
func (client *GroupClient) MirrorGeo(ctx context.Context, target uint64, groupID string, encoded []byte) (enginestate.Result, error) {
	engine, err := client.Engine(target, groupID)
	if err != nil {
		return enginestate.Result{}, err
	}
	return engine.MirrorGeo(ctx, encoded)
}

func (client *GroupClient) observeLeader(groupID string, leader uint64) {
	if client.options.GroupRoutes != nil {
		client.options.GroupRoutes.ObserveLeader(groupID, leader)
	}
}

func (client *GroupClient) peer(raftID uint64) (raftwire.GroupServiceClient, error) {
	if raftID == 0 || raftID == client.options.RaftID {
		return nil, errors.New("Group RPC target must be a different non-zero RaftID")
	}
	address, found := client.options.Directory.Resolve(raftID)
	if !found {
		return nil, fmt.Errorf("%w: target %d has no address", ErrPeerUnavailable, raftID)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return nil, errors.New("Group client is closed")
	}
	peer := client.peers[raftID]
	if peer != nil && peer.address == address {
		return peer.client, nil
	}
	if peer != nil {
		_ = peer.conn.Close()
		if peer.recordConn != nil {
			_ = peer.recordConn.Close()
		}
	}
	// Group RPCs carry bounded control-plane deadlines. Disable gRPC's
	// transparent retry path so a broken HTTP/2 stream cannot keep a call
	// parked in waitOnHeader after the caller's context has expired; the
	// caller will retry against the current directory route instead.
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(client.options.Credentials),
		grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(client.options.MaxSize+1024)))
	if err != nil {
		return nil, err
	}
	peer = &groupPeer{address: address, conn: conn, client: raftwire.NewGroupServiceClient(conn),
		ranges: raftwire.NewRangeControlServiceClient(conn), engines: raftwire.NewEngineServiceClient(conn),
		memberships: raftwire.NewGroupMembershipServiceClient(conn)}
	client.peers[raftID] = peer
	return peer.client, nil
}

func (client *GroupClient) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return nil
	}
	client.closed = true
	var failures []error
	for _, peer := range client.peers {
		failures = append(failures, peer.conn.Close())
		if peer.recordConn != nil {
			failures = append(failures, peer.recordConn.Close())
		}
	}
	return errors.Join(failures...)
}
