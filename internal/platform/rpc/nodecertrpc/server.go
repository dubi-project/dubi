// Package nodecertrpc renews existing Node leaves without enrollment grants,
// caller-selected keys, or distributing CA signing material.
package nodecertrpc

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/nodecertwire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	MaxMessageSize        = 32 << 10
	maxConcurrentRequests = 4
	requestTimeout        = 30 * time.Second
)

type RootReader interface {
	ReadIndex(context.Context, string) (uint64, error)
}

type Server struct {
	nodecertwire.UnimplementedNodeCertificateServiceServer
	host     RootReader
	root     *rootstate.State
	signer   *nodepki.Signer
	local    identity.Identity
	slots    chan struct{}
	now      func() time.Time
	readRoot func(context.Context) (uint64, error)
}

func NewServer(host RootReader, root *rootstate.State, signer *nodepki.Signer, local identity.Identity) (*Server, error) {
	if host == nil || root == nil {
		return nil, errors.New("Node renewal requires Root")
	}
	if _, err := identity.NodeIdentityURI(local); err != nil {
		return nil, err
	}
	if signer != nil {
		if _, err := nodepki.ValidateClusterCA(local.ClusterID, signer.CACertificatePEM(), time.Time{}); err != nil {
			return nil, err
		}
	}
	return &Server{host: host, root: root, signer: signer, local: local, slots: make(chan struct{}, maxConcurrentRequests), now: time.Now}, nil
}

func (s *Server) SetSigner(signer *nodepki.Signer) { s.signer = signer }

// SetRootReadIndex lets the owning Node route Root authorization barriers
// through an authenticated remote voter while local Raft is catching up.
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

func (s *Server) RenewNode(ctx context.Context, request *nodecertwire.RenewNodeRequest) (*nodecertwire.RenewNodeResponse, error) {
	if s.signer == nil {
		return nil, status.Error(codes.PermissionDenied, "local Node has no enrollment authority")
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "renewal request required")
	}
	if proto.Size(request) > MaxMessageSize {
		return nil, status.Error(codes.ResourceExhausted, "renewal request exceeds limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "Node renewal is busy")
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	leaf, id, err := s.peer(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := s.confirmMembership(ctx, id); err != nil {
		return nil, err
	}
	if err := s.confirmSigner(ctx); err != nil {
		return nil, err
	}
	// Root quorum waits do not extend an old TLS chain's authorization lifetime.
	if _, _, err := s.peer(ctx, request); err != nil {
		return nil, err
	}
	encoded := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.Raw})
	if nodepki.NeedsRenewal(leaf, s.now()) {
		encoded, err = s.signer.RenewNode(id, nodepki.DefaultServerName, encoded, s.now())
		if err != nil {
			return nil, status.Error(codes.FailedPrecondition, "Node leaf cannot be renewed under the current authority")
		}
	}
	// Never return newly signed material based solely on a pre-signing read.
	// This is a point-in-time authorization, not a durable global revocation fence.
	if err := s.confirmMembership(ctx, id); err != nil {
		return nil, err
	}
	if _, _, err := s.peer(ctx, request); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	response := &nodecertwire.RenewNodeResponse{ClusterId: id.ClusterID, NodeId: id.NodeID, RaftId: id.RaftID, CertificatePem: encoded}
	if proto.Size(response) > MaxMessageSize {
		return nil, status.Error(codes.ResourceExhausted, "renewal response exceeds limit")
	}
	return response, nil
}

func (s *Server) confirmSigner(ctx context.Context) error {
	local, found, err := s.root.Node(s.local.RaftID)
	if err != nil || !found {
		return status.Error(codes.PermissionDenied, "local Node is not a current enrollment authority")
	}
	if err := s.root.AuthorizeEnrollmentSigner(local, s.signer.AuthorityEpoch(), s.local.Origin == identity.OriginBootstrap); err != nil {
		if errors.Is(err, rootstate.ErrEnrollmentAuthorityDenied) {
			return status.Error(codes.PermissionDenied, err.Error())
		}
		return status.Error(codes.Unavailable, "enrollment authority unavailable")
	}
	return nil
}

func (s *Server) peer(ctx context.Context, request *nodecertwire.RenewNodeRequest) (*x509.Certificate, identity.Identity, error) {
	if request.ClusterId != s.local.ClusterID || request.FromRaftId == 0 {
		return nil, identity.Identity{}, status.Error(codes.PermissionDenied, "renewal identity does not match Cluster")
	}
	leaf, err := identity.VerifiedPeerCertificate(ctx, s.now())
	if err != nil {
		return nil, identity.Identity{}, status.Error(codes.Unauthenticated, "currently valid verified Node TLS required")
	}
	parsed, err := identity.ParseNodeCertificate(leaf)
	if err != nil || parsed.ClusterID != s.local.ClusterID || parsed.RaftID != request.FromRaftId {
		return nil, identity.Identity{}, status.Error(codes.PermissionDenied, "renewal requires matching Node role")
	}
	// This signing-only projection is never persisted as a Node identity. The
	// local creation timestamp is not a certificate claim or remote metadata.
	return leaf, identity.Identity{Version: 1, ClusterID: parsed.ClusterID, NodeID: parsed.NodeID,
		RaftID: parsed.RaftID, CreatedAt: time.Unix(1, 0).UTC()}, nil
}

func (s *Server) confirmMembership(ctx context.Context, id identity.Identity) error {
	// Reject identities that are clearly outside the current local Root
	// membership before issuing a linearizable barrier. A rejected renewal is
	// read-only; it must not append a Raft no-op merely to discover that the
	// caller is a pending or already-removed Node.
	for _, raftID := range []uint64{id.RaftID, s.local.RaftID} {
		_, member, err := s.root.RootMember(raftID)
		if err != nil {
			return status.Error(codes.Unavailable, "Root membership unavailable")
		}
		if !member {
			return status.Error(codes.PermissionDenied, "renewal requires current Node and issuer membership")
		}
	}
	if _, err := s.readRootIndex(ctx); err != nil {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		return status.Error(codes.Unavailable, "Root confirmation unavailable")
	}
	nodes, member, err := s.root.RootMembers(id.RaftID, s.local.RaftID)
	if err != nil {
		return status.Error(codes.Unavailable, "Root membership unavailable")
	}
	if !member || nodes[0].NodeID != id.NodeID || nodes[1].NodeID != s.local.NodeID {
		return status.Error(codes.PermissionDenied, "renewal requires current Node and issuer membership")
	}
	return nil
}
