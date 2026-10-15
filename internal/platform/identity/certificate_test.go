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
	certificate := &x509.Certificate{Subject: pkix.Name{CommonName: value.NodeID}, URIs: []*url.URL{identityURI},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
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

func TestNodeCertificateRejectsNoncanonicalURIAndNilPeer(t *testing.T) {
	canonical := "spiffe://dubi/cluster_0123456789abcdef0123456789abcdef/node/node_0123456789abcdef0123456789abcdef/42"
	for name, mutate := range map[string]func(*url.URL){
		"userinfo":       func(u *url.URL) { u.User = url.User("unexpected") },
		"leading-slash":  func(u *url.URL) { u.Path = "/" + u.Path },
		"trailing-slash": func(u *url.URL) { u.Path += "/" },
		"leading-zero":   func(u *url.URL) { u.Path = u.Path[:len(u.Path)-2] + "042" },
		"escaped-path": func(u *url.URL) {
			u.RawPath = "/%63luster_0123456789abcdef0123456789abcdef/node/node_0123456789abcdef0123456789abcdef/42"
		},
		"empty-query": func(u *url.URL) { u.ForceQuery = true },
	} {
		t.Run(name, func(t *testing.T) {
			uri, err := url.Parse(canonical)
			if err != nil {
				t.Fatal(err)
			}
			mutate(uri)
			cert := &x509.Certificate{Subject: pkix.Name{CommonName: "node_0123456789abcdef0123456789abcdef"}, URIs: []*url.URL{uri}}
			if _, err := ParseNodeCertificate(cert); err == nil {
				t.Fatal("noncanonical Node URI accepted")
			}
		})
	}
	authorize, err := PeerAuthorizer("cluster_0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if err := authorize(peer.NewContext(context.Background(), nil), 42); err == nil {
		t.Fatal("nil peer accepted")
	}
}

func TestNodePeerAuthorizationRejectsExpiredHandshakeIdentity(t *testing.T) {
	cluster := "cluster_0123456789abcdef0123456789abcdef"
	uri, _ := url.Parse("spiffe://dubi/" + cluster + "/node/node_0123456789abcdef0123456789abcdef/42")
	leaf := &x509.Certificate{Subject: pkix.Name{CommonName: "node_0123456789abcdef0123456789abcdef"}, URIs: []*url.URL{uri},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(-time.Second)}
	// VerifiedChains captures successful historical TLS verification, not a
	// promise that the same long-lived session is still authorized now.
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf}}}}})
	authorize, err := PeerAuthorizer(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := authorize(ctx, 42); err == nil {
		t.Fatal("expired TLS identity retained authorization")
	}
}
