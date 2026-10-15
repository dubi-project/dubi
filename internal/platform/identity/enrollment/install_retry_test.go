package enrollment

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
)

func TestEnrollmentInstallRetryPinsExistingCAAndPublicKey(t *testing.T) {
	for _, entry := range []string{"enrollment", "pki"} {
		for _, change := range []string{"ca", "key", "equivalent"} {
			t.Run(entry+"/"+change, func(t *testing.T) {
				_, ticket, signer, now := ticketFixture(t)
				store := t.TempDir()
				id, request, err := Prepare(store, nodepki.DefaultServerName, "default", "127.0.0.1:7444")
				if err != nil {
					t.Fatal(err)
				}
				_, privatePEM, err := loadPrepared(store, id)
				if err != nil {
					t.Fatal(err)
				}
				id.ClusterID = ticket.ClusterID()
				certificate, err := signer.IssueNode(id, request.ServerName, []byte(request.CSRPEM), now)
				if err != nil {
					t.Fatal(err)
				}
				bundle := Bundle{Version: 1, ClusterID: id.ClusterID, NodeID: id.NodeID, RaftID: id.RaftID, ServerName: request.ServerName,
					CACertificatePEM: string(signer.CACertificatePEM()), CertificatePEM: string(certificate)}
				original, err := nodepki.InstallLeaf(store, id, id.ClusterID, request.ServerName, []byte(bundle.CACertificatePEM), certificate, privatePEM, now)
				if err != nil {
					t.Fatal(err)
				}
				// Keep staging to model a crash after PKI publication, before cleanup.
				candidate := bundle
				switch change {
				case "ca":
					caPEM, leafPEM := alternateClusterAuthority(t, original.CACertificate, original.Certificate)
					candidate.CACertificatePEM, candidate.CertificatePEM = caPEM, leafPEM
				case "key":
					csr, err := validateRequest(request)
					if err != nil {
						t.Fatal(err)
					}
					_, private, err := ed25519.GenerateKey(rand.Reader)
					if err != nil {
						t.Fatal(err)
					}
					csrDER, err := x509.CreateCertificateRequest(rand.Reader, csr, private)
					if err != nil {
						t.Fatal(err)
					}
					leaf, err := signer.IssueNode(id, request.ServerName, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}), now)
					if err != nil {
						t.Fatal(err)
					}
					candidate.CertificatePEM = string(leaf)
				case "equivalent":
					leaf, err := signer.IssueNode(id, request.ServerName, []byte(request.CSRPEM), now.Add(30*time.Second))
					if err != nil {
						t.Fatal(err)
					}
					candidate.CertificatePEM = string(leaf)
				}
				if err := nodepki.ValidateIssuedLeaf(id, request.ServerName, []byte(candidate.CACertificatePEM), []byte(candidate.CertificatePEM), now); err != nil {
					t.Fatal("fixture must be validly signed, not malformed", err)
				}
				var retry nodepki.Material
				if entry == "enrollment" {
					retry, err = Install(store, candidate, now)
				} else {
					retry, err = nodepki.InstallLeaf(store, id, id.ClusterID, request.ServerName, []byte(candidate.CACertificatePEM), []byte(candidate.CertificatePEM), privatePEM, now)
				}
				if change != "equivalent" {
					if err == nil {
						t.Fatal("retry accepted a different trust root or Node key")
					}
					if _, err := os.Stat(StorePaths(store).Directory); err != nil {
						t.Fatal("rejected retry destroyed prepared intent", err)
					}
				} else if err != nil || !bytes.Equal(retry.Certificate.Raw, original.Certificate.Raw) {
					t.Fatal("same-key retry replaced installed leaf or failed", err)
				}
				current, err := nodepki.Load(store, id, request.ServerName, now)
				if err != nil || !bytes.Equal(current.Certificate.Raw, original.Certificate.Raw) || !bytes.Equal(current.CACertificate.Raw, original.CACertificate.Raw) {
					t.Fatal("retry changed existing PKI", err)
				}
				if _, err := Install(store, bundle, now); err != nil {
					t.Fatal("correct retry no longer recoverable", err)
				}
			})
		}
	}
}

// A second fully valid self-signed authority for the same Cluster and a leaf
// for the same Node/key. Matching identity strings cannot make it trusted.
func alternateClusterAuthority(t *testing.T, ca, leaf *x509.Certificate) (string, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := *ca
	caTemplate.PublicKey = public
	caTemplate.SubjectKeyId, caTemplate.AuthorityKeyId = nil, nil
	caDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, public, private)
	if err != nil {
		t.Fatal(err)
	}
	alternate, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := *leaf
	leafTemplate.AuthorityKeyId = nil
	leafDER, err := x509.CreateCertificate(rand.Reader, &leafTemplate, alternate, leaf.PublicKey, private)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})), string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}))
}
