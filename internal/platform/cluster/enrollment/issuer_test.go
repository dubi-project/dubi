package enrollment

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

func TestIssuerRegistersThenIssuesAndJoinerInstallsLeafOnlyPKI(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	bootstrapStore := filepath.Join(t.TempDir(), "bootstrap")
	bootstrapIdentity, err := identity.Bootstrap(bootstrapStore)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapPKI, err := nodepki.Ensure(bootstrapStore, bootstrapIdentity, "node-a.example.internal", now)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := nodepki.OpenSigner(bootstrapStore, bootstrapIdentity, now)
	if err != nil {
		t.Fatal(err)
	}
	root, err := rootstate.Open(filepath.Join(bootstrapStore, "root-state"))
	if err != nil {
		t.Fatal(err)
	}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: bootstrapStore, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	if err := host.AddGroup(raft.RootGroupID, []uint64{bootstrapIdentity.RaftID}, false, root); err != nil {
		t.Fatal(err)
	}
	if err := host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}

	joinStore := filepath.Join(t.TempDir(), "joiner")
	prepared, request, err := identityenrollment.Prepare(joinStore, "node-b.example.internal", "region-b", "node-b.example.internal:7443")
	if err != nil {
		t.Fatal(err)
	}
	proof := []byte("single-request-admission-proof")
	authorizations := 0
	issuer, err := New(host, root, signer, bootstrapIdentity.ClusterID,
		AuthorizeFunc(func(_ context.Context, candidate identityenrollment.Request, supplied []byte) error {
			authorizations++
			if candidate.NodeID != prepared.NodeID || !bytes.Equal(supplied, proof) {
				return errors.New("invalid admission proof")
			}
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bundle, err := issuer.Issue(ctx, request, proof, now)
	if err != nil {
		t.Fatal(err)
	}
	registered, found, err := root.Node(prepared.RaftID)
	if err != nil || !found || registered.NodeID != prepared.NodeID || registered.RegionID != request.RegionID ||
		registered.Address != request.Address {
		t.Fatalf("registered Node = %+v, found %v, err %v", registered, found, err)
	}
	material, err := identityenrollment.Install(joinStore, bundle, now)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := identity.Load(joinStore)
	if err != nil || bound.ClusterID != bootstrapIdentity.ClusterID || bound.NodeID != prepared.NodeID {
		t.Fatalf("bound identity = %+v, err %v", bound, err)
	}
	if !bytes.Equal(material.CACertificate.Raw, bootstrapPKI.CACertificate.Raw) {
		t.Fatal("joined Node installed a different Cluster CA")
	}
	for _, path := range []string{material.Paths.WrappingKey, material.Paths.Envelope} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("joined Node received CA signer material %s: %v", path, err)
		}
	}
	if _, err := os.Stat(identityenrollment.StorePaths(joinStore).Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed enrollment staging remains: %v", err)
	}
	if _, err := identityenrollment.Install(joinStore, bundle, now.Add(time.Minute)); err != nil {
		t.Fatalf("idempotent bundle install: %v", err)
	}
	if _, err := issuer.Issue(ctx, request, proof, now.Add(time.Minute)); err != nil {
		t.Fatalf("idempotent issue: %v", err)
	}
	if authorizations != 2 {
		t.Fatalf("authorization calls = %d", authorizations)
	}
}

func TestIssuerRejectsUnauthorizedOrConflictingRequestBeforeSigning(t *testing.T) {
	issuer, root, host, signerIdentity := openTestIssuer(t, func(_ context.Context,
		_ identityenrollment.Request, proof []byte,
	) error {
		if string(proof) != "allowed" {
			return errors.New("denied")
		}
		return nil
	})
	_ = host
	_ = signerIdentity
	joinStore := filepath.Join(t.TempDir(), "joiner")
	prepared, request, err := identityenrollment.Prepare(joinStore, "node-b.example.internal", "region-b", "node-b.example.internal:7443")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := issuer.Issue(ctx, request, []byte("denied"), time.Now().UTC()); err == nil {
		t.Fatal("unauthorized enrollment was issued")
	}
	if _, found, err := root.Node(prepared.RaftID); err != nil || found {
		t.Fatalf("unauthorized Node registration = found %v, err %v", found, err)
	}
	if _, err := issuer.Issue(ctx, request, []byte("allowed"), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	conflict := request
	conflict.Address = "node-b.example.internal:8443"
	if _, err := issuer.Issue(ctx, conflict, []byte("allowed"), time.Now().UTC()); err == nil {
		t.Fatal("conflicting registered Node metadata was issued")
	}
	differentKey := request
	differentKey.CSRPEM = newCSR(t, prepared, request.ServerName)
	if _, err := issuer.Issue(ctx, differentKey, []byte("allowed"), time.Now().UTC()); err == nil {
		t.Fatal("different CSR for an enrolled identity was issued")
	}
}

func newCSR(t *testing.T, value identity.Identity, serverName string) string {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	joinURI, err := identity.JoinIdentityURI(value)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: value.NodeID}, URIs: []*url.URL{joinURI}, DNSNames: []string{serverName},
	}, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: encoded}))
}

func openTestIssuer(t *testing.T, authorize AuthorizeFunc) (*Issuer, *rootstate.State, *raft.Host, identity.Identity) {
	t.Helper()
	store := filepath.Join(t.TempDir(), "issuer")
	value, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := nodepki.Ensure(store, value, "issuer.example.internal", now); err != nil {
		t.Fatal(err)
	}
	signer, err := nodepki.OpenSigner(store, value, now)
	if err != nil {
		t.Fatal(err)
	}
	root, err := rootstate.Open(filepath.Join(store, "root-state"))
	if err != nil {
		t.Fatal(err)
	}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: store, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	if err := host.AddGroup(raft.RootGroupID, []uint64{value.RaftID}, false, root); err != nil {
		t.Fatal(err)
	}
	if err := host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	issuer, err := New(host, root, signer, value.ClusterID, authorize)
	if err != nil {
		t.Fatal(err)
	}
	return issuer, root, host, value
}
