package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestVerifiedPeerCertificateValidityAndChainBinding(t *testing.T) {
	now := time.Now()
	for _, name := range []string{"valid", "leaf-expired", "leaf-future", "ca-expired", "ca-future", "nil-ca", "empty-chain", "other-leaf", "alternate-chain", "nil-leaf"} {
		t.Run(name, func(t *testing.T) {
			leaf := &x509.Certificate{Raw: []byte{1}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
			ca := &x509.Certificate{Raw: []byte{2}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
			chains := [][]*x509.Certificate{{leaf, ca}}
			switch name {
			case "leaf-expired":
				leaf.NotAfter = now
			case "leaf-future":
				leaf.NotBefore = now.Add(time.Nanosecond)
			case "ca-expired":
				ca.NotAfter = now
			case "ca-future":
				ca.NotBefore = now.Add(time.Nanosecond)
			case "nil-ca":
				chains[0][1] = nil
			case "empty-chain":
				chains = [][]*x509.Certificate{{}}
			case "other-leaf":
				other := *leaf
				other.Raw = []byte{3}
				chains[0][0] = &other
			case "alternate-chain":
				other := *ca
				ca.NotAfter = now
				chains = append(chains, []*x509.Certificate{leaf, &other})
			case "nil-leaf":
				leaf = nil
			}
			ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: chains}}})
			got, err := VerifiedPeerCertificate(ctx, now)
			wantValid := name == "valid" || name == "alternate-chain"
			if (err == nil) != wantValid || wantValid && got != leaf {
				t.Fatal("unexpected chain validity", err)
			}
		})
	}
	for _, ctx := range []context.Context{nil, context.Background(), peer.NewContext(context.Background(), nil)} {
		if _, err := VerifiedPeerCertificate(ctx, now); err == nil {
			t.Fatal("absent TLS accepted")
		}
	}
}

// The authorization clock advances without sleeping or changing TLS's own
// clock. The second RPC must fail on the SAME actual TCP/mTLS session, proving
// that a fresh handshake is not what enforces the expiry boundary.
func TestNodePeerAuthorizationExpiresOnExistingMTLSSession(t *testing.T) {
	now := time.Now().UTC()
	cluster := "cluster_0123456789abcdef0123456789abcdef"
	node := "node_0123456789abcdef0123456789abcdef"
	uri, _ := url.Parse("spiffe://dubi/" + cluster + "/node/" + node + "/42")
	caPub, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour)}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPub, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: node}, URIs: []*url.URL{uri}, DNSNames: []string{"localhost"},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, leafTemplate, ca, pub, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate := tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: key}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	var clock, handshakes atomic.Int64
	clock.Store(now.UnixNano())
	authorize, err := peerAuthorizer(cluster, func() time.Time { return time.Unix(0, clock.Load()) })
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert,
		VerifyConnection: func(tls.ConnectionState) error { handshakes.Add(1); return nil }})),
		grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := authorize(ctx, 42); err != nil {
				return nil, status.Error(codes.PermissionDenied, "peer expired")
			}
			return handler(ctx, req)
		}))
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	defer func() {
		s.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, ServerName: "localhost", RootCAs: pool, Certificates: []tls.Certificate{certificate}})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := grpc_health_v1.NewHealthClient(conn)
	if _, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	clock.Store(now.Add(2 * time.Hour).UnixNano())
	if _, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("expired session authorized", err)
	}
	if handshakes.Load() != 1 {
		t.Fatal("test reconnected instead of reusing TLS", handshakes.Load())
	}
}
