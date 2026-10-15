package nodepki

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
)

func TestSignerTransferBindsTargetAndAuthorityEpoch(t *testing.T) {
	sourceStore := filepath.Join(t.TempDir(), "source")
	source := mustBootstrap(t, sourceStore)
	now := time.Now().UTC()
	if _, err := Ensure(sourceStore, source, DefaultServerName, now); err != nil {
		t.Fatal(err)
	}
	signer, err := OpenSigner(sourceStore, source, now)
	if err != nil {
		t.Fatal(err)
	}
	target := mustBootstrap(t, filepath.Join(t.TempDir(), "target"))
	target.ClusterID = source.ClusterID
	wrapper, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := signer.SealFor(target, wrapper.PublicKey().Bytes(), 7)
	if err != nil {
		t.Fatal(err)
	}
	privatePEM := pem.EncodeToMemory(&pem.Block{Type: "DUBI SIGNER WRAPPING PRIVATE KEY", Bytes: wrapper.Bytes()})
	recovered, err := OpenTransferredSigner(target, signer.CACertificatePEM(), privatePEM, envelope, 7, now)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(recovered.CACertificatePEM(), signer.CACertificatePEM()) {
		t.Fatal("transferred signer changed the Cluster CA")
	}
	if _, err := OpenTransferredSigner(target, signer.CACertificatePEM(), privatePEM, envelope, 8, now); err == nil {
		t.Fatal("transferred signer accepted the wrong authority epoch")
	}
	sourceWrapping, err := SignerWrappingPublicKey(sourceStore, source, now)
	if err != nil {
		t.Fatal(err)
	}
	sourceEnvelope, err := signer.SealFor(source, sourceWrapping, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := InstallTransferredSigner(sourceStore, source, 7, sourceEnvelope, now); err != nil {
		t.Fatal(err)
	}
	installed, err := OpenSigner(sourceStore, source, now)
	if err != nil || installed.AuthorityEpoch() != 7 {
		t.Fatalf("persisted signer epoch = %d, err %v", installed.AuthorityEpoch(), err)
	}
}

func TestEnsureCreatesAndReopensBootstrapPKI(t *testing.T) {
	store := filepath.Join(t.TempDir(), "node")
	value, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	first, err := Ensure(store, value, "127.0.0.1", now)
	if err != nil {
		t.Fatal(err)
	}
	if first.Config.ClusterID != value.ClusterID || first.Config.NodeID != value.NodeID ||
		first.Config.RaftID != value.RaftID || first.Config.ServerName != "127.0.0.1" {
		t.Fatalf("PKI configuration = %+v", first.Config)
	}
	if err := first.Certificate.VerifyHostname("127.0.0.1"); err != nil {
		t.Fatalf("leaf hostname: %v", err)
	}
	if info, err := os.Lstat(first.Paths.Directory); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("PKI directory mode = %v, err %v", modeOf(info), err)
	}
	for _, path := range []string{first.Paths.PrivateKey, first.Paths.WrappingKey, first.Paths.Envelope, first.Paths.Config} {
		if info, err := os.Lstat(path); err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("secret %s mode = %v, err %v", filepath.Base(path), modeOf(info), err)
		}
	}
	if _, err := os.Stat(filepath.Join(first.Paths.Directory, "node-ca.key")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("plaintext Node CA key exists or cannot be checked: %v", err)
	}
	signer, err := OpenSigner(store, value, now)
	if err != nil || len(signer.CACertificatePEM()) == 0 {
		t.Fatalf("open signer: %v", err)
	}
	second, err := Ensure(store, value, "127.0.0.1", now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.CACertificate.Raw, second.CACertificate.Raw) ||
		!bytes.Equal(first.Certificate.Raw, second.Certificate.Raw) {
		t.Fatal("idempotent Ensure replaced existing PKI")
	}
}

func TestLoadRejectsIdentityServerNameAndPermissions(t *testing.T) {
	store := filepath.Join(t.TempDir(), "node")
	value, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	material, err := Ensure(store, value, "node-a.example.internal", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Load(store, value, "node-b.example.internal", time.Time{}); err == nil {
		t.Fatal("different server name was accepted")
	}
	other := value
	other.RaftID++
	if _, err := Load(store, other, "node-a.example.internal", time.Time{}); err == nil {
		t.Fatal("different durable identity was accepted")
	}
	if err := os.Chmod(material.Paths.PrivateKey, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(store, value, "node-a.example.internal", time.Time{}); err == nil {
		t.Fatal("exposed private key was accepted")
	}
}

func TestEnsureRejectsInvalidServerNameBeforePublishing(t *testing.T) {
	store := filepath.Join(t.TempDir(), "node")
	value := mustBootstrap(t, store)
	for _, serverName := range []string{"bad name", "bad_name", "-bad.example", "bad.example.", "host:7443"} {
		if _, err := Ensure(store, value, serverName, time.Now().UTC()); err == nil {
			t.Fatalf("invalid server name %q was accepted", serverName)
		}
		if _, err := os.Stat(StorePaths(store).Directory); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid server name %q published PKI: %v", serverName, err)
		}
	}
}

func TestOpenSignerRejectsTamperedEnvelopeAndUnknownConfig(t *testing.T) {
	store := filepath.Join(t.TempDir(), "node")
	value, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	material, err := Ensure(store, value, "localhost", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(material.Paths.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	var envelope signerEnvelope
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.Ciphertext[len(envelope.Ciphertext)-1] ^= 0xff
	encoded, err = json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(material.Paths.Envelope, append(encoded, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSigner(store, value, time.Time{}); err == nil {
		t.Fatal("tampered signer envelope was accepted")
	}

	strictStore := filepath.Join(t.TempDir(), "strict")
	strictIdentity := mustBootstrap(t, strictStore)
	strictMaterial, err := Ensure(strictStore, strictIdentity, "localhost", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := os.ReadFile(strictMaterial.Paths.Config)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(configuration, &fields); err != nil {
		t.Fatal(err)
	}
	fields["unknown"] = true
	configuration, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(strictMaterial.Paths.Config, configuration, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSigner(strictStore, strictIdentity, time.Time{}); err == nil {
		t.Fatal("unknown signer configuration field was accepted")
	}
}

func mustBootstrap(t *testing.T, store string) identity.Identity {
	t.Helper()
	value, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func modeOf(info os.FileInfo) os.FileMode {
	if info == nil {
		return 0
	}
	return info.Mode().Perm()
}
