package raftrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
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
	maxSnapshotReceivers   = 2
)

type Stepper interface {
	Step(groupID string, message *raftpb.Message) error
}

type SnapshotInstaller interface {
	InstallSnapshot(groupID string, message *raftpb.Message) (uint64, error)
}

// SnapshotStreamInstaller must verify and durably import the complete payload
// before handing its small reference to Raft. Returning an index certifies
// durable state installation, not just successful artifact receipt.
type SnapshotStreamInstaller interface {
	InstallSnapshotStream(context.Context, string, *raftpb.Message, io.Reader) (uint64, error)
}

// AuthorizePeer must bind the authenticated TLS client certificate to the
// claimed RaftID using the cluster's membership/PKI policy.
type AuthorizePeer func(context.Context, uint64) error

type Server struct {
	raftwire.UnimplementedRaftServiceServer
	clusterID             string
	raftID                uint64
	stepper               Stepper
	authorize             AuthorizePeer
	maxSize               int
	snapshotSlots         chan struct{}
	snapshotBudget        *resource.ByteBudget
	snapshotHeaders       chan struct{}
	snapshotReaders       chan struct{}
	snapshotHeaderTimeout time.Duration                  // Internal test override; zero uses automatic default.
	snapshotReceiveLimits func(uint64) snapshotDeadlines // Internal test clock policy only.
}

func NewServer(clusterID string, raftID uint64, stepper Stepper, authorize AuthorizePeer, maxSize int) (*Server, error) {
	if clusterID == "" || raftID == 0 || stepper == nil || authorize == nil {
		return nil, errors.New("Raft server requires ClusterID, RaftID, Stepper and peer authorizer")
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxMessageSize
	}
	return &Server{clusterID: clusterID, raftID: raftID, stepper: stepper, authorize: authorize, maxSize: maxSize,
		snapshotSlots: make(chan struct{}, maxSnapshotReceivers), snapshotBudget: resource.NewByteBudget(snapshotBudgetBytes),
		snapshotHeaders: make(chan struct{}, maxSnapshotHeaders), snapshotReaders: make(chan struct{}, maxSnapshotHeaders+maxSnapshotReceivers)}, nil
}

// SnapshotResources accounts for encoded snapshot buffers held through durable
// installation. It is not a bound on protobuf/state-machine decoded memory.
func (s *Server) SnapshotResources() resource.BudgetStatus { return s.snapshotBudget.Status() }

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
	if message.GetTo() != request.GetToRaftId() ||
		(message.GetFrom() != request.GetFromRaftId() && message.GetType() != raftpb.MessageType_MsgProp && message.GetType() != raftpb.MessageType_MsgReadIndex) {
		return nil, status.Error(codes.InvalidArgument, "Raft envelope IDs do not match message")
	}
	if message.GetType() == raftpb.MessageType_MsgSnap {
		return nil, status.Error(codes.FailedPrecondition, "Raft snapshot requires the streamed RPC")
	}
	if err := s.stepper.Step(request.GetGroupId(), &message); err != nil {
		return nil, mutationError("apply Raft message", err, codes.Unavailable)
	}
	return &raftwire.StepResponse{}, nil
}

func (s *Server) authorizeSender(ctx context.Context, raftID uint64) error {
	return authorizeSender(ctx, raftID, s.authorize)
}

func authorizeSender(ctx context.Context, raftID uint64, authorize AuthorizePeer) error {
	remote, ok := peer.FromContext(ctx)
	if !ok || remote == nil {
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
	first, err := s.receiveSnapshotHeader(stream)
	if err != nil {
		if code := status.Code(err); code != codes.Unknown && code != codes.OK {
			return err
		}
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
	if header.GetEncodedSize() == 0 || header.GetEncodedSize() > snapshot.MaxStreamBytes+uint64(maxSnapshotEnvelope+snapshotWirePrefixSize) || len(header.GetSha256Digest()) != sha256.Size {
		return status.Error(codes.ResourceExhausted, "Raft snapshot size or digest is invalid")
	}
	limits := deadlinesForSnapshot(header.GetEncodedSize())
	if s.snapshotReceiveLimits != nil {
		limits = s.snapshotReceiveLimits(header.GetEncodedSize())
	}
	ctx, cancel := context.WithTimeout(stream.Context(), limits.total)
	defer cancel()
	from := header.GetFromRaftId()
	stream = &boundedSnapshotServer{RaftService_InstallSnapshotServer: stream, ctx: ctx, idle: limits.idle, workers: s.snapshotReaders}
	stream = &authorizedSnapshotServer{RaftService_InstallSnapshotServer: stream,
		authorize: func() error { return s.authorizeSender(ctx, from) }}
	// Reject rather than queue more memory-intensive installs. These limits are
	// shared by all groups and senders; ordinary Step RPCs need neither lease.
	select {
	case s.snapshotSlots <- struct{}{}:
		defer func() { <-s.snapshotSlots }()
	default:
		return status.Error(codes.ResourceExhausted, "Raft snapshot receiver is busy")
	}
	// Reserve bounded framing space first. A legacy image acquires the rest
	// of its whole-message lease after format detection and before allocation.
	reserved := min(header.GetEncodedSize(), uint64(snapshotWireBufferBytes))
	release, err := s.snapshotBudget.TryAcquire(reserved)
	if err != nil {
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	defer release()
	reader := &snapshotFrameReader{stream: stream, remaining: header.GetEncodedSize()}
	prefix := make([]byte, min(header.GetEncodedSize(), uint64(snapshotWirePrefixSize)))
	if _, err := io.ReadFull(reader, prefix); err != nil {
		return err
	}
	if len(prefix) == snapshotWirePrefixSize && string(prefix[:len(snapshotWireMagic)]) == snapshotWireMagic {
		return s.installFileSnapshot(stream, header, reader, prefix)
	}
	if header.GetEncodedSize() > DefaultMaxSnapshotSize {
		return status.Error(codes.ResourceExhausted, "legacy Raft snapshot exceeds inline bound")
	}
	if header.GetEncodedSize() > reserved {
		extra, err := s.snapshotBudget.TryAcquire(header.GetEncodedSize() - reserved)
		if err != nil {
			return status.Error(codes.ResourceExhausted, err.Error())
		}
		defer extra()
	}
	encoded := make([]byte, int(header.GetEncodedSize()))
	copy(encoded, prefix)
	if _, err := io.ReadFull(reader, encoded[len(prefix):]); err != nil {
		return err
	}
	if err := reader.finish(); err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	if !bytes.Equal(digest[:], header.GetSha256Digest()) {
		return status.Error(codes.DataLoss, "Raft snapshot SHA-256 mismatch")
	}
	var message raftpb.Message
	if err := proto.Unmarshal(encoded, &message); err != nil {
		return status.Errorf(codes.InvalidArgument, "decode Raft snapshot: %v", err)
	}
	if message.GetType() != raftpb.MessageType_MsgSnap || etcdraft.IsEmptySnap(message.GetSnapshot()) ||
		message.GetFrom() != header.GetFromRaftId() || message.GetTo() != header.GetToRaftId() {
		return status.Error(codes.InvalidArgument, "snapshot header does not match MsgSnap")
	}
	if _, recognized, err := snapshot.DecodeReference(message.GetSnapshot().GetData()); recognized || err != nil {
		return status.Error(codes.InvalidArgument, "file snapshot reference requires its streamed artifact")
	}
	if err := s.authorizeSender(ctx, from); err != nil {
		return err
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
