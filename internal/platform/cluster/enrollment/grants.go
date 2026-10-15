package enrollment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

// ErrGrantRejected identifies a confirmed terminal/precondition rejection, not
// a lost proposal response or unavailable Root quorum. Clients must not retry
// the same credential operation as though the authority were unreachable.
var ErrGrantRejected = errors.New("Join Grant operation rejected")

// GrantManager serves the authenticated management side of enrollment. Its
// caller MUST authorize a registered Node/operator before calling Create or
// Revoke. Enrollment-role clients may only call Issuer.Issue, never these.
type GrantManager struct {
	host   Host
	root   *rootstate.State
	signer *nodepki.Signer
	local  identity.Identity
	now    func() time.Time
	mu     sync.RWMutex
}

func NewGrantManager(host Host, root *rootstate.State, signer *nodepki.Signer, local identity.Identity) (*GrantManager, error) {
	if host == nil || root == nil || local.RaftID == 0 {
		return nil, errors.New("Join Grant manager requires Host, Root and local signer identity")
	}
	if _, err := identity.NodeIdentityURI(local); err != nil {
		return nil, err
	}
	if signer != nil {
		if _, err := nodepki.ValidateClusterCA(local.ClusterID, signer.CACertificatePEM(), time.Time{}); err != nil {
			return nil, err
		}
	}
	return &GrantManager{host: host, root: root, signer: signer, local: local}, nil
}

func (m *GrantManager) SetSigner(signer *nodepki.Signer) {
	m.mu.Lock()
	m.signer = signer
	m.mu.Unlock()
}

func (m *GrantManager) clock() time.Time {
	if m.now != nil {
		return m.now().UTC()
	}
	return time.Now().UTC()
}

// Create publishes only public metadata and returns the enrollment leaf only
// after a confirmed Root read proves the exact immutable grant. Same-request
// retries tolerate a lost Submit response, but never replace or reactivate a
// consumed/revoked grant. No secret or private key is accepted by this API.
func (m *GrantManager) Create(ctx context.Context, request identityenrollment.GrantRequest) (identityenrollment.GrantBundle, error) {
	m.mu.RLock()
	signer := m.signer
	m.mu.RUnlock()
	if signer == nil {
		return identityenrollment.GrantBundle{}, errors.New("Node enrollment authority is not installed")
	}
	if ctx == nil {
		return identityenrollment.GrantBundle{}, errors.New("admission context required")
	}
	if err := request.Validate(m.clock()); err != nil {
		return identityenrollment.GrantBundle{}, errors.Join(ErrGrantRejected, err)
	}
	if _, err := m.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return identityenrollment.GrantBundle{}, err
	}
	now := m.clock()
	if err := request.Validate(now); err != nil {
		return identityenrollment.GrantBundle{}, errors.Join(ErrGrantRejected, err)
	}
	node, found, err := m.root.Node(m.local.RaftID)
	if err != nil {
		return identityenrollment.GrantBundle{}, err
	}
	if !found || node.NodeID != m.local.NodeID || identityenrollment.ValidateIssuerAddress(node.Address) != nil {
		return identityenrollment.GrantBundle{}, errors.New("Join Grant issuer has no confirmed dialable Root identity")
	}
	// Grant creation is a management operation and may arrive through any
	// current Root member. The credential itself must be issued by a Node that
	// still holds the active enrollment authority, however. Return that
	// authority's address in the ticket so a learner or recently demoted Root
	// member can still be used as the operator's entry point.
	issuerAddress := node.Address
	if authority, authorityFound, err := m.root.EnrollmentAuthority(); err != nil {
		return identityenrollment.GrantBundle{}, err
	} else if authorityFound {
		for _, authorityID := range authority.Members {
			candidate, candidateFound, err := m.root.Node(authorityID)
			if err != nil {
				return identityenrollment.GrantBundle{}, err
			}
			if candidateFound && identityenrollment.ValidateIssuerAddress(candidate.Address) == nil {
				issuerAddress = candidate.Address
				if authorityID == m.local.RaftID {
					break
				}
			}
		}
	}
	keyDigest := sha256.Sum256(request.PublicKeyDER)
	grant := rootstate.JoinGrant{GrantID: request.GrantID, SecretSHA256: request.SecretSHA256,
		EnrollmentKeySHA256: hex.EncodeToString(keyDigest[:]), IssuerRaftID: m.local.RaftID,
		CreatedUnix: now.Unix(), ExpiresUnix: request.ExpiresUnix, State: rootstate.JoinGrantActive}
	current, found, err := m.root.JoinGrant(grant.GrantID)
	if err != nil {
		return identityenrollment.GrantBundle{}, err
	}
	matches := func(current rootstate.JoinGrant) bool {
		return current.State == rootstate.JoinGrantActive && current.SecretSHA256 == grant.SecretSHA256 &&
			current.EnrollmentKeySHA256 == grant.EnrollmentKeySHA256 && current.IssuerRaftID == grant.IssuerRaftID &&
			current.ExpiresUnix == grant.ExpiresUnix && current.CreatedUnix <= now.Unix()
	}
	if found && !matches(current) {
		return identityenrollment.GrantBundle{}, fmt.Errorf("%w: grant has different metadata or terminal state", ErrGrantRejected)
	}
	// Sign before publishing to catch key/CA/window failures without leaving an
	// unusable grant. This certificate is not returned until Root confirms it.
	certificate, err := signer.IssueEnrollment(request.GrantID, request.PublicKeyDER, time.Unix(request.ExpiresUnix, 0), now)
	if err != nil {
		return identityenrollment.GrantBundle{}, err
	}
	if !found {
		command, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.CreateJoinGrant,
			RequestID: "join-create-" + request.GrantID, JoinGrant: &grant})
		if err != nil {
			return identityenrollment.GrantBundle{}, err
		}
		submitErr := m.host.Submit(raft.RootGroupID, command)
		if _, err := m.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			return identityenrollment.GrantBundle{}, errors.Join(submitErr, err)
		}
		current, found, err = m.root.JoinGrant(grant.GrantID)
		if err == nil && found && !matches(current) {
			return identityenrollment.GrantBundle{}, fmt.Errorf("%w: Root confirmed a conflicting or terminal grant", ErrGrantRejected)
		}
		if err != nil || !found || !matches(current) {
			return identityenrollment.GrantBundle{}, errors.Join(submitErr, err, errors.New("Root did not confirm this exact active Join Grant"))
		}
	}
	if err := ctx.Err(); err != nil {
		return identityenrollment.GrantBundle{}, err
	}
	if err := request.Validate(m.clock()); err != nil {
		return identityenrollment.GrantBundle{}, errors.Join(ErrGrantRejected, err)
	}
	return identityenrollment.GrantBundle{ClusterID: m.local.ClusterID, GrantID: grant.GrantID,
		IssuerAddress: issuerAddress, ExpiresUnix: grant.ExpiresUnix,
		CACertificatePEM: string(signer.CACertificatePEM()), CertificatePEM: string(certificate)}, nil
}

func (m *GrantManager) Revoke(ctx context.Context, grantID string) error {
	if ctx == nil {
		return errors.New("admission context required")
	}
	if err := identity.ValidateJoinGrantID(grantID); err != nil {
		return err
	}
	if _, err := m.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	grant, found, err := m.root.JoinGrant(grantID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: grant does not exist", ErrGrantRejected)
	}
	if grant.State == rootstate.JoinGrantRevoked {
		return nil
	}
	if grant.State != rootstate.JoinGrantActive {
		return fmt.Errorf("%w: consumed grant cannot be revoked; Node revocation is a separate operation", ErrGrantRejected)
	}
	if m.clock().Unix() < grant.CreatedUnix {
		return errors.New("authority clock predates Join Grant creation")
	}
	command, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.RevokeJoinGrant,
		RequestID: "join-revoke-" + grantID, JoinRevocation: &rootstate.JoinGrantRevocation{GrantID: grantID, RevokedUnix: m.clock().Unix()}})
	if err != nil {
		return err
	}
	submitErr := m.host.Submit(raft.RootGroupID, command)
	if _, err := m.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return errors.Join(submitErr, err)
	}
	grant, found, err = m.root.JoinGrant(grantID)
	if err == nil && found && grant.State == rootstate.JoinGrantConsumed {
		return fmt.Errorf("%w: grant was consumed before revocation", ErrGrantRejected)
	}
	if err != nil || !found || grant.State != rootstate.JoinGrantRevoked {
		return errors.Join(submitErr, err, errors.New("Root did not confirm Join Grant revocation"))
	}
	return nil
}
