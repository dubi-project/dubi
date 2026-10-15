package config

import (
	"crypto/tls"
	"os"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
)

func TestManagedTLSAndIdentity(t *testing.T) {
	store := t.TempDir()
	value, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	material, err := nodepki.Ensure(store, value, nodepki.DefaultServerName, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	server, client, err := LoadTLS(store, value)
	if err != nil {
		t.Fatal(err)
	}
	if server.ClientAuth != tls.RequireAndVerifyClientCert ||
		server.MinVersion != tls.VersionTLS13 || client.MinVersion != tls.VersionTLS13 ||
		client.ServerName != nodepki.DefaultServerName || client.InsecureSkipVerify {
		t.Fatal("managed TLS did not enforce mutual authentication and fixed server name")
	}
	if err := identity.ValidateNodeCertificate(client.Certificates[0].Leaf, value); err != nil {
		t.Fatal(err)
	}
	wrong := value
	wrong.RaftID++
	if _, _, err := LoadTLS(store, wrong); err == nil {
		t.Fatal("accepted TLS for another identity")
	}
	if err := os.Chmod(material.Paths.PrivateKey, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadTLS(store, value); err == nil {
		t.Fatal("accepted exposed private key")
	}
}

func TestManagedReloadableTLSUsesCallbacksOnly(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodepki.Ensure(store, id, nodepki.DefaultServerName, time.Time{}); err != nil {
		t.Fatal(err)
	}
	server, client, err := LoadReloadableTLS(store, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(server.Certificates) != 0 || len(client.Certificates) != 0 || server.GetCertificate == nil || client.GetClientCertificate == nil ||
		server.ClientAuth != tls.RequireAndVerifyClientCert || server.MinVersion != tls.VersionTLS13 || client.MinVersion != tls.VersionTLS13 ||
		client.InsecureSkipVerify || client.ServerName != nodepki.DefaultServerName {
		t.Fatal("invalid dynamic TLS policy")
	}
	for _, get := range []func() (*tls.Certificate, error){func() (*tls.Certificate, error) { return server.GetCertificate(nil) }, func() (*tls.Certificate, error) { return client.GetClientCertificate(nil) }} {
		cert, err := get()
		if err != nil {
			t.Fatal(err)
		}
		if err := identity.ValidateNodeCertificate(cert.Leaf, id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestManagedTLSRejectsMissingAndReplacedMaterial(t *testing.T) {
	store := t.TempDir()
	value, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadTLS(store, value); err == nil {
		t.Fatal("missing PKI was accepted")
	}
	material, err := nodepki.Ensure(store, value, nodepki.DefaultServerName, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	otherStore := t.TempDir()
	other, err := identity.Bootstrap(otherStore)
	if err != nil {
		t.Fatal(err)
	}
	otherPKI, err := nodepki.Ensure(otherStore, other, nodepki.DefaultServerName, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	ca, err := os.ReadFile(otherPKI.Paths.CACertificate)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(material.Paths.CACertificate, ca, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadTLS(store, value); err == nil {
		t.Fatal("accepted CA from another cluster")
	}
}

func TestProcessDefaultsAndListenValidation(t *testing.T) {
	t.Setenv("DUBI_STORE", "")
	if DefaultStore() != "/var/lib/dubi" {
		t.Fatal("unexpected default store")
	}
	t.Setenv("DUBI_STORE", "/data/dubi")
	if DefaultStore() != "/data/dubi" {
		t.Fatal("store environment ignored")
	}
	for _, address := range []string{DefaultListen, "127.0.0.1:0", "[::1]:7443"} {
		if err := ValidateListen(address); err != nil {
			t.Fatalf("%s: %v", address, err)
		}
	}
	for _, address := range []string{"", ":7443", "localhost", "127.0.0.1:65536", "127.0.0.1:-1"} {
		if err := ValidateListen(address); err == nil {
			t.Fatalf("accepted %q", address)
		}
	}
}
