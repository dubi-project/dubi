// Package enrollmentrpc exposes a narrowly scoped, verified-mTLS admission
// service. Temporary enrollment identities cannot manage grants or act as Nodes.
package enrollmentrpc

import (
	"context"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enrollment"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentwire"
	"github.com/dubi-project/dubi/internal/platform/storage/diskpressure"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	MaxMessageSize        = 32 << 10
	maxRequestDocument    = 16 << 10
	maxConcurrentRequests = 4
	requestTimeout        = 30 * time.Second
)

type Server struct {
	enrollmentwire.UnimplementedEnrollmentServiceServer
	host      enrollment.Host
	root      *rootstate.State
	clusterID string
	local     identity.Identity
	signer    *nodepki.Signer
	issuer    *enrollment.Issuer
	grants    *enrollment.GrantManager
	slots     chan struct{}
	readRoot  func(context.Context) (uint64, error)
}

func NewServer(host enrollment.Host, root *rootstate.State, signer *nodepki.Signer, local identity.Identity) (*Server, error) {
	grants, err := enrollment.NewGrantManager(host, root, signer, local)
	if err != nil {
		return nil, err
	}
	authorizer, err := enrollment.NewGrantAuthorizer(host, root, local.ClusterID)
	if err != nil {
		return nil, err
	}
	issuer, err := enrollment.New(host, root, signer, local.ClusterID, authorizer)
	if err != nil {
		return nil, err
	}
	return &Server{host: host, root: root, clusterID: local.ClusterID, issuer: issuer, grants: grants,
		local: local, signer: signer, slots: make(chan struct{}, maxConcurrentRequests)}, nil
}

func (s *Server) SetSigner(signer *nodepki.Signer) {
	s.signer = signer
	s.issuer.SetSigner(signer)
	s.grants.SetSigner(signer)
}

// SetRootReadIndex lets the owning Node route Root authorization barriers
// through an authenticated remote voter while local Raft is catching up.
func (s *Server) SetRootReadIndex(read func(context.Context) (uint64, error)) {
	if s != nil {
		s.readRoot = read
		s.issuer.SetRootReadIndex(read)
		s.grants.SetRootReadIndex(read)
	}
}

func (s *Server) readRootIndex(ctx context.Context) (uint64, error) {
	if s.readRoot != nil {
		return s.readRoot(ctx)
	}
	return s.host.ReadIndex(ctx, raft.RootGroupID)
}

// acquire bounds work after protobuf decoding. The listener's MaxRecvMsgSize
// remains the pre-decode bound; this is not a global connection/memory limit.
func (s *Server) acquire(ctx context.Context, size int) (context.Context, func(), error) {
	if size > MaxMessageSize {
		return nil, nil, status.Error(codes.ResourceExhausted, "enrollment request exceeds limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, status.FromContextError(err).Err()
	}
	select {
	case s.slots <- struct{}{}:
		ctx, cancel := context.WithTimeout(ctx, requestTimeout)
		return ctx, func() { cancel(); <-s.slots }, nil
	default:
		return nil, nil, busyError("enrollment is busy", time.Second)
	}
}

func (s *Server) authorizeNode(ctx context.Context, clusterID string, raftID uint64) error {
	if clusterID != s.clusterID || raftID == 0 {
		return status.Error(codes.PermissionDenied, "Node identity does not match Cluster")
	}
	remote, ok := peer.FromContext(ctx)
	if !ok || remote == nil {
		return status.Error(codes.Unauthenticated, "verified Node TLS required")
	}
	info, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
		return status.Error(codes.Unauthenticated, "verified Node TLS required")
	}
	leaf := info.State.PeerCertificates[0]
	parsed, err := identity.ParseNodeCertificate(leaf)
	if err != nil || parsed.ClusterID != s.clusterID || parsed.RaftID != raftID {
		return status.Error(codes.PermissionDenied, "grant management requires Node role")
	}
	if _, err := identity.VerifiedPeerCertificate(ctx, time.Now()); err != nil {
		return status.Error(codes.PermissionDenied, "Node certificate chain is not currently valid")
	}
	if _, err := s.readRootIndex(ctx); err != nil {
		return rpcError(err)
	}
	if _, err := identity.VerifiedPeerCertificate(ctx, time.Now()); err != nil {
		return status.Error(codes.PermissionDenied, "Node certificate chain is not currently valid")
	}
	node, found, err := s.root.RootMember(raftID)
	if err != nil {
		return rpcError(err)
	}
	if !found || node.NodeID != parsed.NodeID {
		return status.Error(codes.PermissionDenied, "Node is not a current Root member")
	}
	return nil
}

func (s *Server) CreateGrant(ctx context.Context, request *enrollmentwire.CreateGrantRequest) (*enrollmentwire.CreateGrantResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	ctx, release, err := s.acquire(ctx, proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.authorizeNode(ctx, request.GetClusterId(), request.GetFromRaftId()); err != nil {
		return nil, err
	}
	candidate := identityenrollment.GrantRequest{GrantID: request.GetGrantId(), SecretSHA256: request.GetSecretSha256(),
		PublicKeyDER: request.GetPublicKeyDer(), ExpiresUnix: request.GetExpiresUnix()}
	if err := candidate.Validate(time.Now().UTC()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid public Join Grant request")
	}
	bundle, err := s.grants.Create(ctx, candidate)
	if err != nil {
		return nil, rpcError(err)
	}
	return &enrollmentwire.CreateGrantResponse{ClusterId: bundle.ClusterID, GrantId: bundle.GrantID, IssuerAddress: bundle.IssuerAddress,
		ExpiresUnix: bundle.ExpiresUnix, CaCertificatePem: bundle.CACertificatePEM, CertificatePem: bundle.CertificatePEM}, nil
}

func (s *Server) RevokeGrant(ctx context.Context, request *enrollmentwire.RevokeGrantRequest) (*enrollmentwire.RevokeGrantResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	ctx, release, err := s.acquire(ctx, proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.authorizeNode(ctx, request.GetClusterId(), request.GetFromRaftId()); err != nil {
		return nil, err
	}
	if err := identity.ValidateJoinGrantID(request.GetGrantId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Join Grant identity")
	}
	if err := s.grants.Revoke(ctx, request.GetGrantId()); err != nil {
		return nil, rpcError(err)
	}
	return &enrollmentwire.RevokeGrantResponse{}, nil
}

func (s *Server) EnrollNode(ctx context.Context, request *enrollmentwire.EnrollNodeRequest) (*enrollmentwire.EnrollNodeResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	ctx, release, err := s.acquire(ctx, proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.authorizeSigner(ctx); err != nil {
		return nil, err
	}
	if request.GetClusterId() != s.clusterID {
		return nil, status.Error(codes.PermissionDenied, "enrollment belongs to another Cluster")
	}
	if len(request.GetRequestDocument()) > maxRequestDocument || len(request.GetAdmissionProof()) > enrollment.MaxAdmissionProofSize {
		return nil, status.Error(codes.ResourceExhausted, "enrollment document exceeds limit")
	}
	if len(request.GetAdmissionProof()) == 0 {
		return nil, status.Error(codes.PermissionDenied, "Node enrollment admission denied")
	}
	candidate, err := identityenrollment.DecodeRequest(request.GetRequestDocument())
	if err != nil || candidate.ServerName != nodepki.DefaultServerName {
		return nil, status.Error(codes.InvalidArgument, "invalid Node enrollment request")
	}
	bundle, err := s.issuer.Issue(ctx, candidate, request.GetAdmissionProof(), time.Time{})
	if err != nil {
		// Never echo proof/CSR/Root command errors across the enrollment boundary.
		if errors.Is(err, enrollment.ErrAdmissionDenied) {
			return nil, status.Error(codes.PermissionDenied, "Node enrollment admission denied")
		}
		return nil, rpcError(err)
	}
	encoded, err := identityenrollment.EncodeBundle(bundle)
	if err != nil || len(encoded) > MaxMessageSize-128 {
		return nil, status.Error(codes.Internal, "cannot encode issued Node bundle")
	}
	return &enrollmentwire.EnrollNodeResponse{BundleDocument: encoded}, nil
}

func (s *Server) authorizeSigner(ctx context.Context) error {
	if s.signer == nil {
		return status.Error(codes.PermissionDenied, "local Node has no enrollment authority")
	}
	if _, err := s.readRootIndex(ctx); err != nil {
		return rpcError(err)
	}
	local, found, err := s.root.Node(s.local.RaftID)
	if err != nil || !found {
		return status.Error(codes.PermissionDenied, "local Node is not a current enrollment authority")
	}
	if err := s.root.AuthorizeEnrollmentSigner(local, s.signer.AuthorityEpoch(), s.local.Origin == identity.OriginBootstrap); err != nil {
		if errors.Is(err, rootstate.ErrEnrollmentAuthorityDenied) {
			return status.Error(codes.PermissionDenied, err.Error())
		}
		return rpcError(err)
	}
	return nil
}

func busyError(message string, delay time.Duration) error {
	s := status.New(codes.ResourceExhausted, message)
	withRetry, err := s.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(delay)})
	if err == nil {
		return withRetry.Err()
	}
	return s.Err()
}

func rpcError(err error) error {
	if errors.Is(err, enrollment.ErrGrantRejected) {
		return status.Error(codes.FailedPrecondition, "Join Grant has conflicting, missing, expired or terminal state")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if errors.Is(err, diskpressure.ErrPressure) || errors.Is(err, diskpressure.ErrProbe) {
		return busyError("authority storage is under pressure", diskpressure.RetryDelay)
	}
	return status.Error(codes.Unavailable, "Join Grant operation was not confirmed; retry the same request")
}
