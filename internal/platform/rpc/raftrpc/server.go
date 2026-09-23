package raftrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultMaxMessageSize  = 16 << 20
	DefaultMaxSnapshotSize = 64 << 20
	DefaultSnapshotFrame   = 1 << 20
)

type Stepper interface {
	Step(groupID string, message *raftpb.Message) error
}

type SnapshotInstaller interface {
	InstallSnapshot(groupID string, message *raftpb.Message) (uint64, error)
}

// AuthorizePeer must bind the authenticated TLS client certificate to the
// claimed RaftID using the cluster's membership/PKI policy.
type AuthorizePeer func(context.Context, uint64) error

type Server struct {
	raftwire.UnimplementedRaftServiceServer
	clusterID string
	raftID    uint64
	stepper   Stepper
	authorize AuthorizePeer
	maxSize   int
}

func NewServer(clusterID string, raftID uint64, stepper Stepper, authorize AuthorizePeer, maxSize int) (*Server, error) {
	if clusterID == "" || raftID == 0 || stepper == nil || authorize == nil {
		return nil, errors.New("Raft server requires ClusterID, RaftID, Stepper and peer authorizer")
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxMessageSize
	}
	return &Server{clusterID: clusterID, raftID: raftID, stepper: stepper, authorize: authorize, maxSize: maxSize}, nil
}

func (s *Server) Step(ctx context.Context, request *raftwire.StepRequest) (*raftwire.StepResponse, error) {
	if request == nil || request.GetClusterId() != s.clusterID {
		return nil, status.Error(codes.PermissionDenied, "Raft message belongs to a different Cluster")
	}
	if request.GetFromRaftId() == 0 || request.GetToRaftId() != s.raftID {
		return nil, status.Error(codes.FailedPrecondition, "Raft message has an invalid sender or target")
	}
	if err := raft.ValidateGroupID(request.GetGroupId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.authorizeSender(ctx, request.GetFromRaftId()); err != nil {
		return nil, err
	}
	if len(request.GetMessage()) == 0 || len(request.GetMessage()) > s.maxSize {
		return nil, status.Error(codes.ResourceExhausted, "Raft message is empty or exceeds size limit")
	}
	var message raftpb.Message
	if err := proto.Unmarshal(request.GetMessage(), &message); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decode Raft message: %v", err)
	}
	if message.GetFrom() != request.GetFromRaftId() || message.GetTo() != request.GetToRaftId() {
		return nil, status.Error(codes.InvalidArgument, "Raft envelope IDs do not match message")
	}
	if message.GetType() == raftpb.MessageType_MsgSnap {
		return nil, status.Error(codes.FailedPrecondition, "Raft snapshot requires the streamed RPC")
	}
	if err := s.stepper.Step(request.GetGroupId(), &message); err != nil {
		return nil, status.Error(codes.Unavailable, fmt.Sprintf("apply Raft message: %v", err))
	}
	return &raftwire.StepResponse{}, nil
}

func (s *Server) authorizeSender(ctx context.Context, raftID uint64) error {
	return authorizeSender(ctx, raftID, s.authorize)
}

func authorizeSender(ctx context.Context, raftID uint64, authorize AuthorizePeer) error {
	remote, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "Raft peer has no TLS identity")
	}
	tlsInfo, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.PeerCertificates) == 0 {
		return status.Error(codes.Unauthenticated, "Raft peer has no verified client certificate")
	}
	if err := authorize(ctx, raftID); err != nil {
		return status.Errorf(codes.PermissionDenied, "Raft peer is not authorized: %v", err)
	}
	return nil
}

func (s *Server) InstallSnapshot(stream raftwire.RaftService_InstallSnapshotServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "snapshot stream requires a header")
	}
	header := first.GetHeader()
	if header == nil || header.GetClusterId() != s.clusterID || header.GetFromRaftId() == 0 || header.GetToRaftId() != s.raftID {
		return status.Error(codes.FailedPrecondition, "invalid Raft snapshot header")
	}
	if err := raft.ValidateGroupID(header.GetGroupId()); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.authorizeSender(stream.Context(), header.GetFromRaftId()); err != nil {
		return err
	}
	installer, ok := s.stepper.(SnapshotInstaller)
	if !ok {
		return status.Error(codes.Unimplemented, "Raft snapshot installer is unavailable")
	}
	if header.GetEncodedSize() == 0 || header.GetEncodedSize() > DefaultMaxSnapshotSize || len(header.GetSha256Digest()) != sha256.Size {
		return status.Error(codes.ResourceExhausted, "Raft snapshot size or digest is invalid")
	}
	var encoded bytes.Buffer
	encoded.Grow(int(header.GetEncodedSize()))
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		chunk := frame.GetData()
		if len(chunk) == 0 || len(chunk) > DefaultSnapshotFrame || uint64(len(chunk)) > header.GetEncodedSize()-uint64(encoded.Len()) {
			return status.Error(codes.InvalidArgument, "invalid Raft snapshot frame")
		}
		_, _ = encoded.Write(chunk)
	}
	if uint64(encoded.Len()) != header.GetEncodedSize() {
		return status.Error(codes.InvalidArgument, "incomplete Raft snapshot stream")
	}
	digest := sha256.Sum256(encoded.Bytes())
	if !bytes.Equal(digest[:], header.GetSha256Digest()) {
		return status.Error(codes.DataLoss, "Raft snapshot SHA-256 mismatch")
	}
	var message raftpb.Message
	if err := proto.Unmarshal(encoded.Bytes(), &message); err != nil {
		return status.Errorf(codes.InvalidArgument, "decode Raft snapshot: %v", err)
	}
	if message.GetType() != raftpb.MessageType_MsgSnap || etcdraft.IsEmptySnap(message.GetSnapshot()) ||
		message.GetFrom() != header.GetFromRaftId() || message.GetTo() != header.GetToRaftId() {
		return status.Error(codes.InvalidArgument, "snapshot header does not match MsgSnap")
	}
	index, err := installer.InstallSnapshot(header.GetGroupId(), &message)
	if err != nil {
		return status.Errorf(codes.Unavailable, "install Raft snapshot: %v", err)
	}
	if index < message.GetSnapshot().GetMetadata().GetIndex() {
		return status.Error(codes.Internal, "Raft snapshot was not durably applied")
	}
	return stream.SendAndClose(&raftwire.InstallSnapshotResponse{AppliedIndex: index})
}
