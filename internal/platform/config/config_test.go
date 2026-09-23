package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
)

func TestLoadUsesBootstrapPKIDefaults(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "store")
	value, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	material, err := nodepki.Ensure(store, value, nodepki.DefaultServerName, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	configuration := fmt.Sprintf(`version: 1
store_root: store
listen: 127.0.0.1:7443
advertise: 127.0.0.1:7443
root_bootstrap:
  - raft_id: %d
    address: 127.0.0.1:7443
`, value.RaftID)
	path := filepath.Join(root, "dubi.yaml")
	if err := os.WriteFile(path, []byte(configuration), 0o640); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TLS.Certificate != material.Paths.Certificate || loaded.TLS.PrivateKey != material.Paths.PrivateKey ||
		loaded.TLS.CA != material.Paths.CACertificate || loaded.TLS.ServerName != nodepki.DefaultServerName {
		t.Fatalf("default TLS paths = %+v", loaded.TLS)
	}
	_, clientTLS, err := loaded.LoadTLS(value)
	if err != nil {
		t.Fatal(err)
	}
	if clientTLS.ServerName != nodepki.DefaultServerName {
		t.Fatalf("default TLS server name = %q", clientTLS.ServerName)
	}
}

func TestLoadStrictConfigurationAndTLSIdentity(t *testing.T) {
	root := t.TempDir()
	value := identity.Identity{Version: 1,
		ClusterID: "cluster_0123456789abcdef0123456789abcdef",
		NodeID:    "node_0123456789abcdef0123456789abcdef", RaftID: 42,
		CreatedAt: time.Now()}
	writeConfigTLS(t, root, value)
	configuration := fmt.Sprintf(`version: 1
store_root: store
listen: 127.0.0.1:7443
advertise: 127.0.0.1:7443
tls:
  certificate: node.crt
  private_key: node.key
  ca: ca.crt
  server_name: 127.0.0.1
root_bootstrap:
  - raft_id: %d
    address: 127.0.0.1:7443
tick_interval: 25ms
membership_interval: 2s
max_message_size: 1048576
`, value.RaftID)
	path := filepath.Join(root, "dubi.yaml")
	if err := os.WriteFile(path, []byte(configuration), 0o640); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.StoreRoot != filepath.Join(root, "store") || loaded.TickInterval != 25*time.Millisecond ||
		loaded.MembershipInterval != 2*time.Second || len(loaded.RootPeers()) != 1 || loaded.RootPeers()[0] != 42 {
		t.Fatalf("loaded configuration = %+v", loaded)
	}
	_, clientTLS, err := loaded.LoadTLS(value)
	if err != nil {
		t.Fatal(err)
	}
	if clientTLS.ServerName != "127.0.0.1" {
		t.Fatalf("expert TLS server name = %q", clientTLS.ServerName)
	}
	directory, err := loaded.BootstrapDirectory()
	if err != nil {
		t.Fatal(err)
	}
	if address, found := directory.Resolve(42); !found || address != "127.0.0.1:7443" {
		t.Fatalf("bootstrap address = %q, %v", address, found)
	}

	if err := os.WriteFile(path, append([]byte(configuration), []byte("unknown: true\n")...), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("unknown configuration field was accepted")
	}
}

func TestLoadTLSRejectsExposedPrivateKey(t *testing.T) {
	root := t.TempDir()
	value := identity.Identity{Version: 1,
		ClusterID: "cluster_0123456789abcdef0123456789abcdef",
		NodeID:    "node_0123456789abcdef0123456789abcdef", RaftID: 42,
		CreatedAt: time.Now()}
	writeConfigTLS(t, root, value)
	configuration := Config{Version: 1, StoreRoot: filepath.Join(root, "store"),
		Listen: "127.0.0.1:7443", Advertise: "127.0.0.1:7443",
		TLS: TLSFiles{Certificate: filepath.Join(root, "node.crt"), PrivateKey: filepath.Join(root, "node.key"),
			CA: filepath.Join(root, "ca.crt"), ServerName: "127.0.0.1"}, TickInterval: time.Second,
		MembershipInterval: time.Second, MaxMessageSize: 1024}
	if err := os.Chmod(configuration.TLS.PrivateKey, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := configuration.LoadTLS(value); err == nil {
		t.Fatal("group/world-readable Node private key was accepted")
	}
}

func writeConfigTLS(t *testing.T, root string, value identity.Identity) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	now := time.Now()
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "DUBI CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	nodeKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	identityURI, _ := identity.NodeIdentityURI(value)
	nodeTemplate := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: value.NodeID},
		URIs: []*url.URL{identityURI}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	nodeDER, err := x509.CreateCertificate(rand.Reader, nodeTemplate, ca, &nodeKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(nodeKey)
	if err != nil {
		t.Fatal(err)
	}
	writes := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{"ca.crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o644},
		{"node.crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: nodeDER}), 0o644},
		{"node.key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600},
	}
	for _, write := range writes {
		if err := os.WriteFile(filepath.Join(root, write.name), write.data, write.mode); err != nil {
			t.Fatal(err)
		}
	}
}
