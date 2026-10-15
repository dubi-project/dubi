package identity

import (
	"crypto/ed25519"
	"crypto/x509"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	DefaultJoinGrantTTL = 15 * time.Minute
	MaxJoinGrantTTL     = 24 * time.Hour
)

var joinGrantRE = regexp.MustCompile(`^join_[0-9a-f]{32}$`)

func ValidateJoinGrantID(value string) error {
	if !joinGrantRE.MatchString(value) {
		return errors.New("invalid Join Grant identity")
	}
	return nil
}

func EnrollmentIdentityURI(clusterID, grantID string) (*url.URL, error) {
	if !clusterRE.MatchString(clusterID) || ValidateJoinGrantID(grantID) != nil {
		return nil, errors.New("valid Cluster and Join Grant identities are required")
	}
	return &url.URL{Scheme: "spiffe", Host: certificateTrustDomain, Path: "/" + clusterID + "/enrollment/" + grantID}, nil
}

type EnrollmentCertificateIdentity struct {
	ClusterID string
	GrantID   string
}

// ParseEnrollmentCertificate verifies the narrow enrollment identity shape,
// not its trust chain or validity time. Its caller must verify both through
// TLS and recheck expiry at each admission RPC. It NEVER returns a Node ID.
func ParseEnrollmentCertificate(certificate *x509.Certificate) (EnrollmentCertificateIdentity, error) {
	if certificate == nil || certificate.IsCA || len(certificate.URIs) != 1 || certificate.URIs[0] == nil ||
		len(certificate.DNSNames) != 0 || len(certificate.IPAddresses) != 0 || len(certificate.EmailAddresses) != 0 ||
		certificate.KeyUsage != x509.KeyUsageDigitalSignature || len(certificate.ExtKeyUsage) != 1 ||
		certificate.ExtKeyUsage[0] != x509.ExtKeyUsageClientAuth || len(certificate.UnknownExtKeyUsage) != 0 {
		return EnrollmentCertificateIdentity{}, errors.New("certificate is not a client-only enrollment identity")
	}
	public, ok := certificate.PublicKey.(ed25519.PublicKey)
	if !ok || len(public) != ed25519.PublicKeySize {
		return EnrollmentCertificateIdentity{}, errors.New("enrollment identity requires Ed25519")
	}
	uri := certificate.URIs[0]
	parts := strings.Split(uri.Path, "/")
	if len(parts) != 4 || parts[0] != "" || parts[2] != "enrollment" || certificate.Subject.CommonName != parts[3] {
		return EnrollmentCertificateIdentity{}, errors.New("invalid enrollment identity path or subject")
	}
	canonical, err := EnrollmentIdentityURI(parts[1], parts[3])
	if err != nil || uri.String() != canonical.String() {
		return EnrollmentCertificateIdentity{}, errors.New("noncanonical enrollment identity URI")
	}
	return EnrollmentCertificateIdentity{ClusterID: parts[1], GrantID: parts[3]}, nil
}
