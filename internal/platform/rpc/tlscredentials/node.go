package tlscredentials

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// ForNode adds an exact endpoint identity check AFTER ordinary TLS verification
// but BEFORE gRPC can send headers or application bytes. expected must come
// from the caller's authorized Node selection, never the endpoint's response.
// It does not replace current membership authorization on either endpoint.
func ForNode(base credentials.TransportCredentials, expected identity.CertificateIdentity) (credentials.TransportCredentials, error) {
	if base == nil || base.Info().SecurityProtocol != "tls" || expected.ClusterID == "" || expected.NodeID == "" || expected.RaftID == 0 {
		return nil, errors.New("Node transport requires TLS and an exact identity")
	}
	return &nodeCredentials{base: base.Clone(), expected: expected}, nil
}

type nodeCredentials struct {
	base     credentials.TransportCredentials
	expected identity.CertificateIdentity
}

func (c *nodeCredentials) Info() credentials.ProtocolInfo { return c.base.Info() }
func (c *nodeCredentials) Clone() credentials.TransportCredentials {
	return &nodeCredentials{base: c.base.Clone(), expected: c.expected}
}
func (c *nodeCredentials) OverrideServerName(name string) error {
	return c.base.OverrideServerName(name)
}
func (c *nodeCredentials) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("Node-pinned credentials are client-only")
}
func (c *nodeCredentials) ClientHandshake(ctx context.Context, authority string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.base.ClientHandshake(ctx, authority, raw)
	if err != nil {
		return nil, nil, err
	}
	cert, err := identity.VerifiedPeerCertificate(peer.NewContext(ctx, &peer.Peer{AuthInfo: info}), time.Now())
	if err == nil {
		var actual identity.CertificateIdentity
		actual, err = identity.ParseNodeCertificate(cert)
		if err == nil && actual != c.expected {
			err = errors.New("Node transport responder identity mismatch")
		}
	}
	if err != nil {
		// Interrupt the raw socket first; TLS close-notify must not block a
		// failed identity check. Underlying credential wrappers own cleanup.
		_ = raw.Close()
		_ = conn.Close()
		return nil, nil, err
	}
	return conn, info, nil
}
