// Package rootjoinrpc exposes Node-authenticated Root admission and discovery.
// No CA private key is needed, and an enrollment-role certificate cannot call it.
package rootjoinrpc

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/protocol"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinwire"
	"github.com/dubi-project/dubi/internal/platform/storage/diskpressure"
	etcdraft "go.etcd.io/raft/v3"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	MaxRequestSize        = 32 << 10
	MaxResponseSize       = 4 << 20
	MaxPeers              = rootjoin.MaxViewPeers
	maxRequestDocument    = 16 << 10
	maxConcurrentRequests = 4
	requestTimeout        = 30 * time.Second
)

type Server struct {
	rootjoinwire.UnimplementedRootJoinServiceServer
	host        rootjoin.Host
	root        *rootstate.State
	coordinator *rootjoin.Coordinator
	clusterID   string
	slots       chan struct{}
	readRoot    func(context.Context) (uint64, error)
}

func NewServer(host rootjoin.Host, root *rootstate.State, clusterID string) (*Server, error) {
	if identity.ValidateClusterID(clusterID) != nil {
		return nil, errors.New("Root join requires Cluster identity")
	}
	coordinator, err := rootjoin.New(host, root)
	if err != nil {
		return nil, err
	}
	return &Server{host: host, root: root, coordinator: coordinator, clusterID: clusterID, slots: make(chan struct{}, maxConcurrentRequests)}, nil
}

// SetRootReadIndex lets the owning Node route a Root barrier through an
// authenticated remote voter while this local process is catching up.
func (s *Server) SetRootReadIndex(read func(context.Context) (uint64, error)) {
	if s != nil {
		s.readRoot = read
	}
}

func (s *Server) readRootIndex(ctx context.Context) (uint64, error) {
	if s.readRoot != nil {
		return s.readRoot(ctx)
	}
	return s.host.ReadIndex(ctx, raft.RootGroupID)
}

// Like enrollment RPC, slots bound post-decode work. The listener's receive
// limit is the pre-decode bound; this is not a global connection budget.
func (s *Server) acquire(ctx context.Context, size int) (context.Context, func(), error) {
	if size > MaxRequestSize {
		return nil, nil, status.Error(codes.ResourceExhausted, "Root join request exceeds limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, status.FromContextError(err).Err()
	}
	select {
	case s.slots <- struct{}{}:
		ctx, cancel := context.WithTimeout(ctx, requestTimeout)
		return ctx, func() { cancel(); <-s.slots }, nil
	default:
		return nil, nil, retryError("Root join is busy", time.Second)
	}
}

func (s *Server) authorize(ctx context.Context, clusterID string, raftID uint64, linearizable bool) (*x509.Certificate, error) {
	if clusterID != s.clusterID || raftID == 0 {
		return nil, status.Error(codes.PermissionDenied, "Node identity does not match Cluster")
	}
	remote, ok := peer.FromContext(ctx)
	if !ok || remote == nil {
		return nil, status.Error(codes.Unauthenticated, "verified Node TLS required")
	}
	info, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
		return nil, status.Error(codes.Unauthenticated, "verified Node TLS required")
	}
	leaf := info.State.PeerCertificates[0]
	parsed, err := identity.ParseNodeCertificate(leaf)
	if err != nil || parsed.ClusterID != s.clusterID || parsed.RaftID != raftID {
		return nil, status.Error(codes.PermissionDenied, "Root join requires matching Node role")
	}
	if _, err := identity.VerifiedPeerCertificate(ctx, time.Now()); err != nil {
		return nil, status.Error(codes.PermissionDenied, "Node certificate chain is not currently valid")
	}
	if linearizable {
		if _, err := s.readRootIndex(ctx); err != nil {
			return nil, rpcError(err)
		}
	}
	// Refresh time after waiting for quorum, including on an existing TLS session.
	if _, err := identity.VerifiedPeerCertificate(ctx, time.Now()); err != nil {
		return nil, status.Error(codes.PermissionDenied, "Node certificate chain is not currently valid")
	}
	node, found, err := s.root.Node(raftID)
	if err != nil {
		return nil, rpcError(err)
	}
	if !found || node.NodeID != parsed.NodeID {
		return nil, status.Error(codes.PermissionDenied, "Node is not registered in Root")
	}
	if _, fenced, err := s.root.NodeFence(raftID); err != nil {
		return nil, rpcError(err)
	} else if fenced {
		return nil, status.Error(codes.PermissionDenied, "Node identity is fenced")
	}
	return leaf, nil
}

func (s *Server) JoinRoot(ctx context.Context, request *rootjoinwire.JoinRootRequest) (*rootjoinwire.JoinRootResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	ctx, release, err := s.acquire(ctx, proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	if len(request.RequestDocument) > maxRequestDocument {
		return nil, status.Error(codes.ResourceExhausted, "Root join document exceeds limit")
	}
	candidate, err := identityenrollment.DecodeRequest(request.RequestDocument)
	if err != nil || candidate.ServerName != nodepki.DefaultServerName || identity.ValidateJoinGrantID(request.GrantId) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Root join request")
	}
	canonical, err := identityenrollment.EncodeRequest(candidate)
	if err != nil || !bytes.Equal(canonical, request.RequestDocument) {
		return nil, status.Error(codes.InvalidArgument, "Root join request is not canonical")
	}
	// Determine the local role before choosing the authorization barrier. A
	// follower only redirects and must not propose a local read barrier; doing
	// so would mutate Root state despite the request being a read-only redirect.
	state, err := s.host.Status(raft.RootGroupID)
	if err != nil {
		return nil, rpcError(err)
	}
	leader := state.Role == etcdraft.StateLeader && state.LeaderID != 0 && state.LeaderID == state.RaftID
	// Authenticate and validate membership without a Raft barrier first.  The
	// CSR public key is untrusted input and must be checked before a leader
	// appends a read-index no-op; a certificate with a valid Node identity but
	// the wrong key must remain entirely read-only.
	leaf, err := s.authorize(ctx, request.ClusterId, candidate.RaftID, false)
	if err != nil {
		return nil, err
	}
	key, err := identityenrollment.RequestPublicKey(candidate)
	if err != nil || leaf.Subject.CommonName != candidate.NodeID || !bytes.Equal(key, leaf.RawSubjectPublicKeyInfo) {
		return nil, status.Error(codes.PermissionDenied, "Node certificate does not match enrolled CSR")
	}
	if !leader {
		view, err := s.view()
		if err != nil {
			return nil, rpcError(err)
		}
		return &rootjoinwire.JoinRootResponse{View: view}, nil
	}
	// Reconfirm membership linearly only after all request-bound checks pass.
	if _, err := s.authorize(ctx, request.ClusterId, candidate.RaftID, true); err != nil {
		return nil, err
	}
	task, err := s.coordinator.Ensure(ctx, rootstate.RootJoinStart{GrantID: request.GrantId, Request: candidate})
	if err != nil {
		return nil, rpcError(err)
	}
	// Reconfirm quorum after admission; a view from a stale leader is not a
	// license to initialize a joining runtime or shrink an existing quorum.
	if _, err := s.readRootIndex(ctx); err != nil {
		return nil, rpcError(err)
	}
	view, err := s.view()
	if err != nil {
		return nil, rpcError(err)
	}
	if !viewContains(view, task.RaftID) {
		return nil, rpcError(rootjoin.ErrMemberRemoved)
	}
	return &rootjoinwire.JoinRootResponse{View: view, AdmittedIndex: task.AdmittedIndex}, nil
}

func (s *Server) DiscoverRoot(ctx context.Context, request *rootjoinwire.DiscoverRootRequest) (*rootjoinwire.DiscoverRootResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	ctx, release, err := s.acquire(ctx, proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	if _, err := s.authorize(ctx, request.ClusterId, request.FromRaftId, true); err != nil {
		return nil, err
	}
	view, err := s.view()
	if err != nil {
		return nil, rpcError(err)
	}
	advertisement := protocol.CurrentAdvertisement()
	return &rootjoinwire.DiscoverRootResponse{View: view,
		ProtocolMin: uint32(advertisement.ProtocolMin), ProtocolMax: uint32(advertisement.ProtocolMax),
		StateFormat: uint32(advertisement.StateFormat), PackFormat: uint32(advertisement.PackFormat),
		RecordIndex: uint32(advertisement.RecordIndex)}, nil
}

func (s *Server) view() (*rootjoinwire.RootView, error) {
	cut, err := s.root.RootRoutingView(MaxPeers)
	if err != nil {
		return nil, err
	}
	// The applied Root cut remains useful for discovery and status even when
	// this Node has temporarily lost its local leader hint (for example while
	// a voter is being promoted or a peer reconnects).  LeaderID is only a
	// routing hint; never make an identity-bound membership view unavailable
	// merely because the local Raft status is transiently unknown.
	var leader uint64
	if state, statusErr := s.host.Status(raft.RootGroupID); statusErr == nil {
		leader = state.LeaderID
	}
	view, err := rootjoin.FromRootCut(s.clusterID, cut, leader)
	if err != nil {
		return nil, err
	}
	return encodeView(view)
}

func retryError(message string, delay time.Duration) error {
	s := status.New(codes.ResourceExhausted, message)
	withRetry, err := s.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(delay)})
	if err == nil {
		return withRetry.Err()
	}
	return s.Err()
}

func rpcError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if errors.Is(err, diskpressure.ErrPressure) || errors.Is(err, diskpressure.ErrProbe) {
		return retryError("Root storage is under pressure", diskpressure.RetryDelay)
	}
	if errors.Is(err, rootjoin.ErrMemberRemoved) || errors.Is(err, rootjoin.ErrJoinRejected) || errors.Is(err, rootstate.ErrRequestIDReused) {
		return status.Error(codes.FailedPrecondition, "Root admission rejected or member removed; fresh authorization required")
	}
	if errors.Is(err, rootstate.ErrRootRoutingLimit) {
		return status.Error(codes.ResourceExhausted, "Root routing view exceeds limit")
	}
	return status.Error(codes.Unavailable, "Root admission was not confirmed; retry the same request")
}
