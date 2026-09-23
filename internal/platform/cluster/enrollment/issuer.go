// Package enrollment coordinates authorized Node registration in the Root
// Group with Node CA leaf issuance. It does not define the external transport
// or admission-proof format.
package enrollment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

const MaxAdmissionProofSize = 4096

type Host interface {
	Submit(groupID string, command []byte) error
	ReadIndex(ctx context.Context, groupID string) (uint64, error)
}

// Authorizer validates a transport-independent admission proof. Implementors
// must make retries of the same request safe while rejecting reuse for a
// different request.
type Authorizer interface {
	Authorize(context.Context, identityenrollment.Request, []byte) error
}

type AuthorizeFunc func(context.Context, identityenrollment.Request, []byte) error

func (function AuthorizeFunc) Authorize(ctx context.Context, request identityenrollment.Request, proof []byte) error {
	return function(ctx, request, proof)
}

type Issuer struct {
	host       Host
	root       *rootstate.State
	signer     *nodepki.Signer
	clusterID  string
	authorizer Authorizer
}

func New(host Host, root *rootstate.State, signer *nodepki.Signer, clusterID string,
	authorizer Authorizer,
) (*Issuer, error) {
	if host == nil || root == nil || signer == nil || clusterID == "" || authorizer == nil {
		return nil, errors.New("Node enrollment issuer requires Host, Root state, signer, ClusterID, and authorizer")
	}
	return &Issuer{host: host, root: root, signer: signer, clusterID: clusterID, authorizer: authorizer}, nil
}

// Issue authorizes the request, durably registers the Node in Root, confirms
// that result through ReadIndex, and only then signs its CSR.
func (issuer *Issuer) Issue(ctx context.Context, request identityenrollment.Request,
	proof []byte, now time.Time,
) (identityenrollment.Bundle, error) {
	encodedRequest, err := identityenrollment.EncodeRequest(request)
	if err != nil {
		return identityenrollment.Bundle{}, err
	}
	if len(proof) == 0 || len(proof) > MaxAdmissionProofSize {
		return identityenrollment.Bundle{}, errors.New("Node enrollment admission proof is empty or too large")
	}
	if err := issuer.authorizer.Authorize(ctx, request, proof); err != nil {
		return identityenrollment.Bundle{}, fmt.Errorf("authorize Node enrollment: %w", err)
	}
	if _, err := issuer.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return identityenrollment.Bundle{}, fmt.Errorf("confirm Root before Node enrollment: %w", err)
	}
	descriptor := rootstate.Node{NodeID: request.NodeID, RaftID: request.RaftID,
		RegionID: request.RegionID, Address: request.Address}
	digest := sha256.Sum256(encodedRequest)
	command := rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: "enroll-node-" + hex.EncodeToString(digest[:12]), Type: rootstate.RegisterNode,
		Node: &descriptor}
	encodedCommand, err := rootstate.EncodeCommand(command)
	if err != nil {
		return identityenrollment.Bundle{}, err
	}
	current, found, err := issuer.root.Node(request.RaftID)
	if err != nil {
		return identityenrollment.Bundle{}, err
	}
	if found && current != descriptor {
		return identityenrollment.Bundle{}, errors.New("RaftID is already registered to different Node metadata")
	}
	if !found {
		if err := issuer.host.Submit(raft.RootGroupID, encodedCommand); err != nil {
			return identityenrollment.Bundle{}, fmt.Errorf("submit Root Node registration: %w", err)
		}
		if _, err := issuer.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			return identityenrollment.Bundle{}, fmt.Errorf("confirm Root Node registration: %w", err)
		}
	}
	result, registrationFound, err := issuer.root.ResultForCommand(encodedCommand)
	if err != nil {
		return identityenrollment.Bundle{}, err
	}
	if !registrationFound || result.Error != "" {
		return identityenrollment.Bundle{}, fmt.Errorf(
			"Root Node registration does not authorize this exact enrollment request: found=%v error=%q",
			registrationFound, result.Error)
	}
	bound := identity.Identity{Version: 1, NodeID: request.NodeID, RaftID: request.RaftID,
		ClusterID: issuer.clusterID, CreatedAt: time.Unix(1, 0).UTC()}
	certificatePEM, err := issuer.signer.IssueNode(bound, request.ServerName, []byte(request.CSRPEM), now)
	if err != nil {
		return identityenrollment.Bundle{}, fmt.Errorf("issue Node certificate: %w", err)
	}
	return identityenrollment.Bundle{Version: identityenrollment.FormatVersion,
		ClusterID: issuer.clusterID, NodeID: request.NodeID, RaftID: request.RaftID,
		ServerName: request.ServerName, CACertificatePEM: string(issuer.signer.CACertificatePEM()),
		CertificatePEM: string(certificatePEM)}, nil
}
