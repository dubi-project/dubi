package nodepki

import (
	"crypto/x509"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
)

// ValidateClusterCA validates the pinned, self-signed Cluster trust anchor.
// It does not fetch roots or trust the operating system's public CA pool.
func ValidateClusterCA(clusterID string, encoded []byte, now time.Time) (*x509.Certificate, error) {
	// Reuse the identity grammar rather than accepting arbitrary URI paths.
	if err := identity.ValidateClusterID(clusterID); err != nil {
		return nil, errors.New("invalid Cluster identity")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	ca, err := parseCertificate(encoded)
	if err != nil || !ca.IsCA || caIdentityURI(clusterID).String() != onlyURI(ca) ||
		now.Before(ca.NotBefore) || !now.Before(ca.NotAfter) {
		return nil, errors.New("invalid or expired Node CA certificate")
	}
	if err := ca.CheckSignatureFrom(ca); err != nil {
		return nil, errors.New("Node CA is not self-signed")
	}
	return ca, nil
}
