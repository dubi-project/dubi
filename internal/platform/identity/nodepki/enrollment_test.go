package nodepki

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/url"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func TestEnrollmentCertificateCannotActAsNode(t *testing.T) {
	store := t.TempDir()
	id := mustBootstrap(t, store)
	now := time.Now().UTC().Truncate(time.Second)
	material, err := Ensure(store, id, DefaultServerName, now)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := OpenSigner(store, id, now)
	if err != nil {
		t.Fatal(err)
	}
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	grantID := "join_00000000000000000000000000000001"
	expires := now.Add(identity.DefaultJoinGrantTTL)
	encoded, err := signer.IssueEnrollment(grantID, der, expires, now)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(encoded)
	if block == nil {
		t.Fatal("no certificate")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(material.CACertificate)
	chains, err := leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := identity.ParseEnrollmentCertificate(leaf)
	if err != nil || parsed.GrantID != grantID || parsed.ClusterID != id.ClusterID || !leaf.NotAfter.Equal(expires) {
		t.Fatal(parsed, err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err == nil {
		t.Fatal("enrollment certificate may act as a server")
	}
	if _, err := identity.ParseNodeCertificate(leaf); err == nil {
		t.Fatal("enrollment certificate parsed as Node")
	}
	authorize, err := identity.PeerAuthorizer(id.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: chains}}})
	if err := authorize(ctx, id.RaftID); err == nil {
		t.Fatal("enrollment certificate authorized Node RPC")
	}
	for name, mutate := range map[string]func(*x509.Certificate){
		"missing":        func(c *x509.Certificate) { c.URIs = nil },
		"ambiguous":      func(c *x509.Certificate) { c.URIs = append(c.URIs, c.URIs[0]) },
		"subject":        func(c *x509.Certificate) { c.Subject.CommonName = id.NodeID },
		"node-uri":       func(c *x509.Certificate) { c.URIs[0], _ = identity.NodeIdentityURI(id) },
		"trailing-slash": func(c *x509.Certificate) { c.URIs[0].Path += "/" },
		"userinfo":       func(c *x509.Certificate) { c.URIs[0].User = url.User("unexpected") },
		"query":          func(c *x509.Certificate) { c.URIs[0].RawQuery = "role=node" },
		"server-role":    func(c *x509.Certificate) { c.ExtKeyUsage = append(c.ExtKeyUsage, x509.ExtKeyUsageServerAuth) },
		"dns":            func(c *x509.Certificate) { c.DNSNames = []string{DefaultServerName} },
		"ca":             func(c *x509.Certificate) { c.IsCA = true },
	} {
		t.Run(name, func(t *testing.T) {
			copy := *leaf
			uri := *leaf.URIs[0]
			copy.URIs = []*url.URL{&uri}
			copy.ExtKeyUsage = append([]x509.ExtKeyUsage(nil), leaf.ExtKeyUsage...)
			mutate(&copy)
			if _, err := identity.ParseEnrollmentCertificate(&copy); err == nil {
				t.Fatal("ambiguous/broadened identity accepted")
			}
		})
	}
	for _, deadline := range []time.Time{now, now.Add(-time.Second), now.Add(identity.MaxJoinGrantTTL + time.Second)} {
		if _, err := signer.IssueEnrollment(grantID, der, deadline, now); err == nil {
			t.Fatal("invalid expiry accepted")
		}
	}
	if _, err := signer.IssueEnrollment("node_00000000000000000000000000000001", der, expires, now); err == nil {
		t.Fatal("Node ID signed as enrollment")
	}
	if _, err := signer.IssueEnrollment(grantID, []byte("not a public key"), expires, now); err == nil {
		t.Fatal("invalid key accepted")
	}
}
