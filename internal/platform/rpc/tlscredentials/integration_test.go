package tlscredentials

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestTLSDeadlineReconnectsToRenewedLeafOnBothEnds(t *testing.T) {
	id, err := identity.Bootstrap(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uri, err := identity.NodeIdentityURI(id)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	caPublic, caPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test authority"}, IsCA: true, BasicConstraintsValid: true,
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPublic, caPrivate)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := func(serial int64, validity time.Duration) *tls.Certificate {
		t.Helper()
		template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: id.NodeID}, URIs: []*url.URL{uri}, DNSNames: []string{"dubi.internal"},
			NotBefore: now.Add(-time.Hour), NotAfter: now.Add(validity), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, public, caPrivate)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private, Leaf: parsed}
	}
	old, next := leaf(2, time.Hour), leaf(3, 3*time.Hour)
	var active atomic.Pointer[tls.Certificate]
	active.Store(old)
	var offset atomic.Int64
	clock := func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	var handshakes atomic.Int64
	serverTLS := &tls.Config{GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return active.Load(), nil }, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13,
		VerifyConnection: func(tls.ConnectionState) error { handshakes.Add(1); return nil }}
	clientTLS := &tls.Config{GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return active.Load(), nil }, RootCAs: pool, ServerName: "dubi.internal", MinVersion: tls.VersionTLS13}
	serverCredentials := NewTLS(serverTLS).(*boundedCredentials)
	serverCredentials.now = clock
	clientCredentials := NewTLS(clientTLS).(*boundedCredentials)
	clientCredentials.now = clock
	var observed atomic.Int64
	s := grpc.NewServer(grpc.Creds(serverCredentials), grpc.WaitForHandlers(true), grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		peer, err := identity.VerifiedPeerCertificate(ctx, clock())
		if err != nil {
			return nil, status.Error(codes.PermissionDenied, "expired peer")
		}
		if err := identity.ValidateNodeCertificate(peer, id); err != nil {
			return nil, err
		}
		observed.Store(peer.SerialNumber.Int64())
		return handler(ctx, req)
	}))
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	t.Cleanup(func() {
		s.Stop()
		if err := <-done; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	})
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(clientCredentials))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := grpc_health_v1.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	if observed.Load() != 2 || handshakes.Load() != 1 {
		t.Fatal("initial handshake", observed.Load(), handshakes.Load())
	}
	active.Store(next)
	// TLS's own clock stays unchanged. Both leaves are cryptographically valid
	// there; only the transport's current-validity clock expires the old session.
	offset.Store(int64(2 * time.Hour))
	for {
		_, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true))
		if err == nil && observed.Load() == 3 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("did not reconnect using renewed identity", ctx.Err(), err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if handshakes.Load() < 2 {
		t.Fatal("request reused expired TLS session")
	}
}
