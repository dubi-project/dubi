package enrollment

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

var ErrAdmissionDenied = errors.New("enrollment admission denied")

func admissionDenied(reason string) error { return fmt.Errorf("%w: %s", ErrAdmissionDenied, reason) }

type grantProof struct {
	GrantID string `json:"grant_id"`
	Secret  []byte `json:"secret"`
}

// EncodeGrantProof is a private admission wire payload, never a Root command
// or a log message. The caller transports it only over verified mutual TLS.
func EncodeGrantProof(grantID string, secret []byte) ([]byte, error) {
	if identity.ValidateJoinGrantID(grantID) != nil || len(secret) != 32 {
		return nil, errors.New("invalid Join Grant proof")
	}
	return json.Marshal(grantProof{GrantID: grantID, Secret: secret})
}

type GrantAuthorizer struct {
	host      Host
	root      *rootstate.State
	clusterID string
	now       func() time.Time // nil uses local authority clock; Apply never reads a clock.
}

var _ Authorizer = (*GrantAuthorizer)(nil)

func NewGrantAuthorizer(host Host, root *rootstate.State, clusterID string) (*GrantAuthorizer, error) {
	if host == nil || root == nil || clusterID == "" {
		return nil, errors.New("Join Grant authorizer requires Host, Root and Cluster")
	}
	return &GrantAuthorizer{host: host, root: root, clusterID: clusterID}, nil
}

func (a *GrantAuthorizer) Authorize(ctx context.Context, request identityenrollment.Request, encoded []byte) error {
	if ctx == nil {
		return errors.New("admission context required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(encoded) == 0 || len(encoded) > MaxAdmissionProofSize {
		return admissionDenied("invalid Join Grant proof size")
	}
	var proof grantProof
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&proof); err != nil {
		return admissionDenied("invalid Join Grant proof")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return admissionDenied("trailing Join Grant proof data")
	}
	if identity.ValidateJoinGrantID(proof.GrantID) != nil || len(proof.Secret) != 32 {
		return admissionDenied("invalid Join Grant proof")
	}
	remote, ok := peer.FromContext(ctx)
	if !ok || remote == nil {
		return admissionDenied("enrollment requires a verified TLS client identity")
	}
	info, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
		return admissionDenied("enrollment requires a verified TLS client identity")
	}
	leaf := info.State.PeerCertificates[0]
	parsed, err := identity.ParseEnrollmentCertificate(leaf)
	if err != nil || parsed.ClusterID != a.clusterID || parsed.GrantID != proof.GrantID {
		return admissionDenied("enrollment certificate does not match Join Grant")
	}
	now := time.Now().UTC()
	if a.now != nil {
		now = a.now().UTC()
	}
	// Recheck per RPC, including calls on a TLS connection established before
	// expiry. Issuer signs a fresh leaf on retry, so an expired Grant must not
	// become a backdoor certificate-renewal credential.
	if _, err := identity.VerifiedPeerCertificate(ctx, now); err != nil {
		return admissionDenied("enrollment certificate chain expired or is not yet valid")
	}
	encodedRequest, err := identityenrollment.EncodeRequest(request)
	if err != nil {
		return err
	}
	requestDigest := sha256.Sum256(encodedRequest)
	requestHash := hex.EncodeToString(requestDigest[:])
	publicDER, err := x509.MarshalPKIXPublicKey(leaf.PublicKey)
	if err != nil {
		return err
	}
	publicDigest := sha256.Sum256(publicDER)
	publicHash := hex.EncodeToString(publicDigest[:])
	secretDigest := sha256.Sum256(proof.Secret)
	if _, err := a.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	grant, found, err := a.root.JoinGrant(proof.GrantID)
	if err != nil {
		return err
	}
	// A linearizable read may have waited for quorum. Do not authorize from
	// a clock sample taken before that wait crossed the certificate expiry.
	now = time.Now().UTC()
	if a.now != nil {
		now = a.now().UTC()
	}
	if _, err := identity.VerifiedPeerCertificate(ctx, now); err != nil {
		return admissionDenied("enrollment certificate chain expired during admission")
	}
	expectedSecret, _ := hex.DecodeString(grant.SecretSHA256)
	if !found || !hmac.Equal(expectedSecret, secretDigest[:]) || grant.EnrollmentKeySHA256 != publicHash ||
		grant.ExpiresUnix != leaf.NotAfter.Unix() || now.Unix() < grant.CreatedUnix || now.Unix() >= grant.ExpiresUnix {
		return admissionDenied("Join Grant proof, key or validity window does not match")
	}
	if grant.State == rootstate.JoinGrantConsumed {
		if grant.BoundRequestSHA256 == requestHash {
			return nil
		}
		return admissionDenied("Join Grant is bound to another enrollment request")
	}
	if grant.State != rootstate.JoinGrantActive {
		return admissionDenied("Join Grant is not active")
	}
	operationDigest := sha256.Sum256([]byte(proof.GrantID + "/" + requestHash))
	command := rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.ConsumeJoinGrant,
		RequestID:       "join-consume-" + hex.EncodeToString(operationDigest[:]),
		JoinConsumption: &rootstate.JoinGrantConsumption{GrantID: proof.GrantID, RequestSHA256: requestHash, AuthorizedUnix: now.Unix()}}
	data, err := rootstate.EncodeCommand(command)
	if err != nil {
		return err
	}
	submitErr := a.host.Submit(raft.RootGroupID, data)
	if _, err := a.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return errors.Join(submitErr, err)
	}
	current, found, err := a.root.JoinGrant(proof.GrantID)
	if err != nil {
		return errors.Join(submitErr, err)
	}
	if !found || current.State != rootstate.JoinGrantConsumed || current.BoundRequestSHA256 != requestHash {
		if found && (current.State == rootstate.JoinGrantConsumed || current.State == rootstate.JoinGrantRevoked) {
			return admissionDenied("Join Grant was consumed by another request or revoked")
		}
		return errors.Join(submitErr, errors.New("Root did not durably bind Join Grant to this request"))
	}
	// A lost response or concurrent same-request retry may have proposed a
	// different admission timestamp. The confirmed immutable binding is the
	// authority; it never authorizes a different request or changes expiry.
	return nil
}
