package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/capabilityruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type serverTestHandler struct{}

func (*serverTestHandler) Apply(*capstate.Transaction, router.Target, []byte) (capstate.Outcome, error) {
	return capstate.Outcome{}, nil
}

func TestServerRunsInternalTLSRPCAndShutsDown(t *testing.T) {
	storeRoot := t.TempDir()
	id, err := identity.Bootstrap(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	mux := serverTestMux(t)
	serverTLS, clientTLS := serverTestTLS(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := Open(Options{StoreRoot: storeRoot, Listener: listener,
		ServerTLS: serverTLS, ClientTLS: clientTLS,
		AuthorizePeer: func(context.Context, uint64) error { return nil }, CapabilityMux: mux,
		RootBootstrapPeers: []uint64{id.RaftID}, TickInterval: time.Millisecond,
		MembershipInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- server.Run(ctx) }()
	select {
	case <-server.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("server did not become ready")
	}
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	if err != nil {
		t.Fatal(err)
	}
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), time.Second)
	response, err := raftwire.NewGroupServiceClient(conn).ReadIndex(rpcCtx,
		&raftwire.GroupReadIndexRequest{ClusterId: id.ClusterID, GroupId: raft.RootGroupID,
			FromRaftId: id.RaftID, ToRaftId: id.RaftID})
	rpcCancel()
	_ = conn.Close()
	if err != nil || response.GetConfirmedIndex() == 0 {
		t.Fatalf("Root ReadIndex RPC = %+v, err %v", response, err)
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}

	restartListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(Options{StoreRoot: storeRoot, Listener: restartListener,
		ServerTLS: serverTLS, ClientTLS: clientTLS,
		AuthorizePeer: func(context.Context, uint64) error { return nil }, CapabilityMux: mux,
		TickInterval: time.Hour, MembershipInterval: time.Second})
	if err != nil {
		t.Fatalf("server shutdown retained resources: %v", err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServerRejectsUnsafeTLSAndMissingBootstrapDirectory(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverTLS, clientTLS := serverTestTLS(t)
	unsafe := serverTLS.Clone()
	unsafe.ClientAuth = tls.NoClientCert
	options := Options{StoreRoot: t.TempDir(), Listener: listener,
		ServerTLS: unsafe, ClientTLS: clientTLS,
		AuthorizePeer: func(context.Context, uint64) error { return nil }, CapabilityMux: serverTestMux(t)}
	if _, err := Open(options); err == nil {
		t.Fatal("server accepted TLS without mandatory client certificates")
	}
	options.ServerTLS = serverTLS
	options.RootBootstrapPeers = []uint64{1, 2, 3}
	if _, err := Open(options); err == nil {
		t.Fatal("multi-node Root bootstrap without address directory was accepted")
	}
}

func serverTestMux(t *testing.T) *capabilityruntime.Mux {
	t.Helper()
	registry := capabilityruntime.NewRegistry()
	if err := registry.Register("record", &serverTestHandler{}); err != nil {
		t.Fatal(err)
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	return mux
}

func serverTestTLS(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	nodeKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nodeTemplate := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "node"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	nodeDER, err := x509.CreateCertificate(rand.Reader, nodeTemplate, ca, &nodeKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate := tls.Certificate{Certificate: [][]byte{nodeDER, caDER}, PrivateKey: nodeKey}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	serverTLS := &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}
	clientTLS := &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: pool,
		ServerName: "localhost", MinVersion: tls.VersionTLS13}
	return serverTLS, clientTLS
}
