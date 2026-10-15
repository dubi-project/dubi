package nodepki

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func TestLeafReloadUsesRenewedCertificateInBothTLSRoles(t *testing.T) {
	f := newRenewalFixture(t)
	now := f.now
	r, err := newLeafReloader(f.store, f.id, DefaultServerName, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	serverTLS := &tls.Config{GetCertificate: r.GetCertificate, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: r.TrustPool(), MinVersion: tls.VersionTLS13, Time: func() time.Time { return now }}
	clientTLS := &tls.Config{GetClientCertificate: r.GetClientCertificate, RootCAs: r.TrustPool(), ServerName: DefaultServerName, MinVersion: tls.VersionTLS13, Time: func() time.Time { return now }}
	handshake := func(want []byte) {
		t.Helper()
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		server, client := tls.Server(a, serverTLS), tls.Client(b, clientTLS)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- server.HandshakeContext(ctx) }()
		clientErr := client.HandshakeContext(ctx)
		serverErr := <-done
		if clientErr != nil || serverErr != nil {
			t.Fatal("handshake", clientErr, serverErr)
		}
		for _, state := range []tls.ConnectionState{server.ConnectionState(), client.ConnectionState()} {
			if len(state.PeerCertificates) != 1 || !bytes.Equal(state.PeerCertificates[0].Raw, want) {
				t.Fatal("TLS still used stale certificate")
			}
		}
	}
	handshake(f.original.Certificate.Raw)
	updated, err := InstallRenewedLeaf(f.store, f.id, DefaultServerName, f.leaf, now)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(leafReloadInterval)
	handshake(updated.Certificate.Raw)
	if r.LastError() != nil {
		t.Fatal(r.LastError())
	}
}

func TestLeafReloadPreservesValidCacheButNeverExpiredMaterial(t *testing.T) {
	for _, change := range []string{"broken", "rollback", "directory", "key-permissions"} {
		t.Run(change, func(t *testing.T) {
			f := newRenewalFixture(t)
			now := f.now
			r, err := newLeafReloader(f.store, f.id, DefaultServerName, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			updated, err := InstallRenewedLeaf(f.store, f.id, DefaultServerName, f.leaf, now)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Refresh(); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "broken":
				err = os.WriteFile(f.original.Paths.Certificate, []byte("broken"), 0o644)
			case "rollback":
				err = os.WriteFile(f.original.Paths.Certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.original.Certificate.Raw}), 0o644)
			case "key-permissions":
				err = os.Chmod(f.original.Paths.PrivateKey, 0o644)
			case "directory":
				moved := f.original.Paths.Directory + "-moved"
				err = os.Rename(f.original.Paths.Directory, moved)
				if err == nil {
					err = os.Symlink(moved, f.original.Paths.Directory)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Refresh(); err == nil {
				t.Fatal("invalid refresh accepted")
			}
			cached, err := r.GetCertificate(nil)
			if err != nil || !bytes.Equal(cached.Leaf.Raw, updated.Certificate.Raw) {
				t.Fatal("lost good cached leaf", err)
			}
			if r.LastError() == nil {
				t.Fatal("reload failure hidden from diagnostics")
			}
			now = updated.Certificate.NotAfter
			if _, err := r.GetClientCertificate(nil); err == nil {
				t.Fatal("served expired cached leaf")
			}
		})
	}
}

func TestLeafReloadConcurrentCallbacksAndRefresh(t *testing.T) {
	f := newRenewalFixture(t)
	r, err := newLeafReloader(f.store, f.id, DefaultServerName, func() time.Time { return f.now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InstallRenewedLeaf(f.store, f.id, DefaultServerName, f.leaf, f.now); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			for range 10 {
				if _, err := r.GetCertificate(nil); err != nil {
					t.Error(err)
				}
				if _, err := r.GetClientCertificate(nil); err != nil {
					t.Error(err)
				}
				if err := r.Refresh(); err != nil {
					t.Error(err)
				}
				_ = r.TrustPool()
				_ = r.LastError()
			}
		})
	}
	wg.Wait()
}

func TestLeafReloadRechecksTimeAfterStorageRead(t *testing.T) {
	f := newRenewalFixture(t)
	r, err := newLeafReloader(f.store, f.id, DefaultServerName, func() time.Time { return f.now })
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	r.now = func() time.Time {
		calls++
		if calls == 1 {
			return f.original.Certificate.NotAfter.Add(-time.Second)
		}
		return f.original.Certificate.NotAfter
	}
	if _, err := r.GetCertificate(nil); err == nil {
		t.Fatal("accepted certificate that expired during refresh")
	}
}

func TestLeafReloadRejectsValidlySignedKeyOrCARotation(t *testing.T) {
	for _, change := range []string{"key", "ca"} {
		t.Run(change, func(t *testing.T) {
			f := newRenewalFixture(t)
			r, err := newLeafReloader(f.store, f.id, DefaultServerName, func() time.Time { return f.now })
			if err != nil {
				t.Fatal(err)
			}
			public := f.original.Certificate.PublicKey.(ed25519.PublicKey)
			if change == "key" {
				var private ed25519.PrivateKey
				public, private, err = ed25519.GenerateKey(rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(f.original.Paths.PrivateKey, marshalPrivateKey(private), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				private := f.signer.privateKey.(ed25519.PrivateKey)
				der, err := createCA(f.id, f.now, private.Public().(ed25519.PublicKey), private)
				if err != nil {
					t.Fatal(err)
				}
				ca, err := x509.ParseCertificate(der)
				if err != nil {
					t.Fatal(err)
				}
				f.signer = &Signer{ca: ca, privateKey: private, clusterID: f.id.ClusterID}
				if err := os.WriteFile(f.original.Paths.CACertificate, f.signer.CACertificatePEM(), 0o644); err != nil {
					t.Fatal(err)
				}
				config := f.original.Config
				config.CAFingerprint = certificateFingerprint(ca)
				encoded, err := json.Marshal(config)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(f.original.Paths.Config, encoded, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(f.original.Paths.Certificate, f.issue(t, public, f.now), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(f.store, f.id, DefaultServerName, f.now); err != nil {
				t.Fatal("replacement must be independently valid", err)
			}
			if err := r.Refresh(); err == nil {
				t.Fatal("accepted uncoordinated rotation")
			}
			cached, err := r.GetCertificate(nil)
			if err != nil || !bytes.Equal(cached.Leaf.Raw, f.original.Certificate.Raw) {
				t.Fatal("evicted original leaf", err)
			}
		})
	}
}
