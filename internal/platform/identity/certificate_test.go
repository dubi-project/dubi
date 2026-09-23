package identity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/url"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func TestNodeCertificateIdentityAndPeerAuthorization(t *testing.T) {
	value := Identity{Version: formatVersion,
		ClusterID: "cluster_0123456789abcdef0123456789abcdef",
		NodeID:    "node_0123456789abcdef0123456789abcdef", RaftID: 42,
		CreatedAt: time.Now()}
	identityURI, err := NodeIdentityURI(value)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{Subject: pkix.Name{CommonName: value.NodeID}, URIs: []*url.URL{identityURI}}
	parsed, err := ParseNodeCertificate(certificate)
	if err != nil || parsed.ClusterID != value.ClusterID || parsed.NodeID != value.NodeID || parsed.RaftID != value.RaftID {
		t.Fatalf("parsed certificate = %+v, err %v", parsed, err)
	}
	if err := ValidateNodeCertificate(certificate, value); err != nil {
		t.Fatal(err)
	}
	authorize, err := PeerAuthorizer(value.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{
		State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate},
			VerifiedChains: [][]*x509.Certificate{{certificate}}}}})
	if err := authorize(ctx, value.RaftID); err != nil {
		t.Fatal(err)
	}
	if err := authorize(ctx, value.RaftID+1); err == nil {
		t.Fatal("certificate authorized a different claimed RaftID")
	}
}

func TestNodeCertificateRejectsAmbiguousOrMismatchedIdentity(t *testing.T) {
	valid, _ := url.Parse("spiffe://dubi/cluster_0123456789abcdef0123456789abcdef/node/node_0123456789abcdef0123456789abcdef/42")
	other, _ := url.Parse("spiffe://example/identity")
	for name, certificate := range map[string]*x509.Certificate{
		"missing":   {},
		"ambiguous": {Subject: pkix.Name{CommonName: "node_0123456789abcdef0123456789abcdef"}, URIs: []*url.URL{valid, other}},
		"subject":   {Subject: pkix.Name{CommonName: "another"}, URIs: []*url.URL{valid}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseNodeCertificate(certificate); err == nil {
				t.Fatal("invalid certificate identity was accepted")
			}
		})
	}
}
