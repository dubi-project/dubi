package nodepki

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"net/url"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
)

// IssueEnrollment signs a short-lived client-only identity. The public Grant
// must be published in Root before the ticket is returned to its owner.
// Possessing this certificate never authorizes any Node/consensus RPC.
func (signer *Signer) IssueEnrollment(grantID string, publicDER []byte, expires, now time.Time) ([]byte, error) {
	if signer == nil || signer.ca == nil || signer.privateKey == nil {
		return nil, errors.New("active Node CA signer required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC().Truncate(time.Second)
	expires = expires.UTC().Truncate(time.Second)
	if !expires.After(now) || expires.Sub(now) > identity.MaxJoinGrantTTL || expires.After(signer.ca.NotAfter) ||
		now.Before(signer.ca.NotBefore) || len(publicDER) == 0 || len(publicDER) > 4096 {
		return nil, errors.New("invalid enrollment key or certificate validity window")
	}
	uri, err := identity.EnrollmentIdentityURI(signer.clusterID, grantID)
	if err != nil {
		return nil, err
	}
	public, err := x509.ParsePKIXPublicKey(publicDER)
	if err != nil {
		return nil, errors.New("invalid enrollment public key")
	}
	key, ok := public.(ed25519.PublicKey)
	if !ok || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("enrollment key must use Ed25519")
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: grantID}, URIs: []*url.URL{uri},
		NotBefore: now.Add(-clockSkewAllowance), NotAfter: expires, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	encoded, err := x509.CreateCertificate(rand.Reader, template, signer.ca, key, signer.privateKey)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: encoded}), nil
}
