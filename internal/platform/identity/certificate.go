package identity

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

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
	parts := strings.Split(strings.Trim(uri.Path, "/"), "/")
	if len(parts) != 4 || !clusterRE.MatchString(parts[0]) || parts[1] != "node" ||
		!nodeIDRE.MatchString(parts[2]) {
		return CertificateIdentity{}, errors.New("Node certificate identity path is invalid")
	}
	raftID, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil || raftID == 0 || certificate.Subject.CommonName != parts[2] {
		return CertificateIdentity{}, errors.New("Node certificate identity does not match its subject")
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

// PeerAuthorizer returns the callback consumed by internal RPC handlers. TLS
// verification must already have populated VerifiedChains.
func PeerAuthorizer(clusterID string) (func(context.Context, uint64) error, error) {
	if !clusterRE.MatchString(clusterID) {
		return nil, errors.New("valid ClusterID is required for peer authorization")
	}
	return func(ctx context.Context, claimedRaftID uint64) error {
		remote, ok := peer.FromContext(ctx)
		if !ok {
			return errors.New("peer has no transport identity")
		}
		tlsInfo, ok := remote.AuthInfo.(credentials.TLSInfo)
		if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.PeerCertificates) == 0 {
			return errors.New("peer has no verified certificate")
		}
		parsed, err := ParseNodeCertificate(tlsInfo.State.PeerCertificates[0])
		if err != nil {
			return err
		}
		if parsed.ClusterID != clusterID || parsed.RaftID != claimedRaftID {
			return errors.New("peer certificate does not match ClusterID and claimed RaftID")
		}
		return nil
	}, nil
}
