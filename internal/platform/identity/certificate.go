package identity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

const certificateTrustDomain = "dubi"

type CertificateIdentity struct {
	ClusterID string
	NodeID    string
	RaftID    uint64
}

// ParseNodeCertificate requires one canonical DUBI SPIFFE URI SAN. The TLS
// chain proves who signed the leaf; this URI binds that leaf to cluster IDs.
func ParseNodeCertificate(certificate *x509.Certificate) (CertificateIdentity, error) {
	if certificate == nil || len(certificate.URIs) != 1 {
		return CertificateIdentity{}, errors.New("Node certificate requires exactly one URI SAN")
	}
	uri := certificate.URIs[0]
	if uri == nil || uri.Scheme != "spiffe" || uri.Host != certificateTrustDomain ||
		uri.RawQuery != "" || uri.Fragment != "" {
		return CertificateIdentity{}, errors.New("Node certificate has an invalid DUBI identity URI")
	}
	parts := strings.Split(strings.TrimPrefix(uri.Path, "/"), "/")
	if len(parts) != 4 || !clusterRE.MatchString(parts[0]) || parts[1] != "node" ||
		!nodeIDRE.MatchString(parts[2]) {
		return CertificateIdentity{}, errors.New("Node certificate identity path is invalid")
	}
	raftID, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil || raftID == 0 || certificate.Subject.CommonName != parts[2] {
		return CertificateIdentity{}, errors.New("Node certificate identity does not match its subject")
	}
	canonical := &url.URL{Scheme: "spiffe", Host: certificateTrustDomain,
		Path: "/" + parts[0] + "/node/" + parts[2] + "/" + strconv.FormatUint(raftID, 10)}
	if uri.String() != canonical.String() {
		return CertificateIdentity{}, errors.New("Node certificate identity URI is not canonical")
	}
	return CertificateIdentity{ClusterID: parts[0], NodeID: parts[2], RaftID: raftID}, nil
}

func NodeIdentityURI(value Identity) (*url.URL, error) {
	if err := validate(value); err != nil || value.ClusterID == "" {
		return nil, errors.New("bound Node identity is required")
	}
	return url.Parse(fmt.Sprintf("spiffe://%s/%s/node/%s/%d",
		certificateTrustDomain, value.ClusterID, value.NodeID, value.RaftID))
}

// JoinIdentityURI binds a CSR to an unbound durable Node identity without
// pretending that the Node already belongs to a Cluster.
func JoinIdentityURI(value Identity) (*url.URL, error) {
	if err := validate(value); err != nil || value.ClusterID != "" {
		return nil, errors.New("unbound Node identity is required")
	}
	return url.Parse(fmt.Sprintf("spiffe://%s/join/node/%s/%d",
		certificateTrustDomain, value.NodeID, value.RaftID))
}

func ValidateNodeCertificate(certificate *x509.Certificate, value Identity) error {
	parsed, err := ParseNodeCertificate(certificate)
	if err != nil {
		return err
	}
	if parsed.ClusterID != value.ClusterID || parsed.NodeID != value.NodeID || parsed.RaftID != value.RaftID {
		return errors.New("Node certificate does not match the durable Node identity")
	}
	return nil
}

// VerifiedPeerCertificate requires a previously verified TLS chain that is
// still valid at now. TLS authenticates once per connection; its cached chain
// must not grant indefinite RPC authorization. This does not perform revocation
// or membership checks, nor replace handshake signature/usage verification.
func VerifiedPeerCertificate(ctx context.Context, now time.Time) (*x509.Certificate, error) {
	if ctx == nil {
		return nil, errors.New("peer has no transport context")
	}
	remote, ok := peer.FromContext(ctx)
	if !ok || remote == nil {
		return nil, errors.New("peer has no transport identity")
	}
	info, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, errors.New("peer has no verified certificate")
	}
	leaf, _, err := VerifiedTLSCertificate(info.State, now)
	return leaf, err
}

// VerifiedTLSCertificate rechecks historical TLS verification at now and returns
// the deadline of the longest currently valid verified path. Signature/usage
// verification remains TLS's responsibility; this is not a trust or revocation
// bypass. A shorter alternative path must not expire an otherwise valid peer.
func VerifiedTLSCertificate(state tls.ConnectionState, now time.Time) (*x509.Certificate, time.Time, error) {
	if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 || state.PeerCertificates[0] == nil {
		return nil, time.Time{}, errors.New("peer has no verified certificate")
	}
	if now.IsZero() {
		now = time.Now()
	}
	leaf := state.PeerCertificates[0]
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return nil, time.Time{}, errors.New("peer certificate is not currently valid")
	}
	var deadline time.Time
	for _, chain := range state.VerifiedChains {
		if len(chain) == 0 || chain[0] == nil || !leaf.Equal(chain[0]) {
			continue
		}
		valid := true
		pathDeadline := leaf.NotAfter
		for _, certificate := range chain {
			if certificate == nil || now.Before(certificate.NotBefore) || !now.Before(certificate.NotAfter) {
				valid = false
				break
			}
			if certificate.NotAfter.Before(pathDeadline) {
				pathDeadline = certificate.NotAfter
			}
		}
		if valid && pathDeadline.After(deadline) {
			deadline = pathDeadline
		}
	}
	if !deadline.IsZero() {
		return leaf, deadline, nil
	}
	return nil, time.Time{}, errors.New("peer certificate chain is not currently valid")
}

// PeerAuthorizer returns the callback consumed by internal RPC handlers. TLS
// verification must already have populated VerifiedChains. Validity is checked
// at every invocation, including requests on a reused connection.
func PeerAuthorizer(clusterID string) (func(context.Context, uint64) error, error) {
	return peerAuthorizer(clusterID, time.Now)
}

func peerAuthorizer(clusterID string, now func() time.Time) (func(context.Context, uint64) error, error) {
	if !clusterRE.MatchString(clusterID) {
		return nil, errors.New("valid ClusterID is required for peer authorization")
	}
	return func(ctx context.Context, claimedRaftID uint64) error {
		leaf, err := VerifiedPeerCertificate(ctx, now())
		if err != nil {
			return err
		}
		parsed, err := ParseNodeCertificate(leaf)
		if err != nil {
			return err
		}
		if parsed.ClusterID != clusterID || parsed.RaftID != claimedRaftID {
			return errors.New("peer certificate does not match ClusterID and claimed RaftID")
		}
		return nil
	}, nil
}
