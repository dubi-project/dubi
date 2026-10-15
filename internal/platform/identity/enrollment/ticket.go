package enrollment

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
)

const (
	joinTicketPrefix  = "djt1."
	MaxJoinTicketSize = 32 << 10
)

// GrantRequest is public. The operator generates its key and secret locally;
// neither private value is sent to the authority when creating a grant.
type GrantRequest struct {
	GrantID      string `json:"grant_id"`
	SecretSHA256 string `json:"secret_sha256"`
	PublicKeyDER []byte `json:"public_key_der"`
	ExpiresUnix  int64  `json:"expires_unix"`
}

func (r GrantRequest) Validate(now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	digest, err := hex.DecodeString(r.SecretSHA256)
	if identity.ValidateJoinGrantID(r.GrantID) != nil || err != nil || len(digest) != sha256.Size ||
		hex.EncodeToString(digest) != r.SecretSHA256 || len(r.PublicKeyDER) == 0 || len(r.PublicKeyDER) > 4096 {
		return errors.New("invalid public Join Grant request")
	}
	key, err := x509.ParsePKIXPublicKey(r.PublicKeyDER)
	public, ok := key.(ed25519.PublicKey)
	if err != nil || !ok || len(public) != ed25519.PublicKeySize {
		return errors.New("Join Grant requires an Ed25519 public key")
	}
	expires := time.Unix(r.ExpiresUnix, 0)
	if r.ExpiresUnix <= 0 || !expires.After(now) || expires.Sub(now) > identity.MaxJoinGrantTTL {
		return errors.New("Join Grant expires outside the permitted window")
	}
	return nil
}

// GrantBundle is returned only after Root durably confirms the public grant.
type GrantBundle struct {
	ClusterID        string `json:"cluster_id"`
	GrantID          string `json:"grant_id"`
	IssuerAddress    string `json:"issuer_address"`
	ExpiresUnix      int64  `json:"expires_unix"`
	CACertificatePEM string `json:"ca_certificate_pem"`
	CertificatePEM   string `json:"certificate_pem"`
}

type ticketDocument struct {
	Version int `json:"version"`
	GrantBundle
	Secret        []byte `json:"secret"`
	PrivateKeyPEM string `json:"private_key_pem"`
}

// Ticket is a bearer credential. Its printable forms are deliberately redacted;
// EncodeTicket is the only intentional disclosure of its private material.
type Ticket struct{ document ticketDocument }

func (Ticket) String() string      { return "[DUBI join ticket: redacted]" }
func (Ticket) GoString() string    { return "[DUBI join ticket: redacted]" }
func (t Ticket) ClusterID() string { return t.document.ClusterID }
func (t Ticket) GrantID() string   { return t.document.GrantID }
func (t Ticket) Address() string   { return t.document.IssuerAddress }

type GrantDraft struct {
	request    GrantRequest
	secret     []byte
	privatePEM string
}

func (GrantDraft) String() string   { return "[DUBI join draft: redacted]" }
func (GrantDraft) GoString() string { return "[DUBI join draft: redacted]" }

func NewGrantDraft(ttl time.Duration, now time.Time) (*GrantDraft, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if ttl == 0 {
		ttl = identity.DefaultJoinGrantTTL
	}
	if ttl < time.Second || ttl > identity.MaxJoinGrantTTL {
		return nil, errors.New("invalid Join Grant TTL")
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	publicDER, err := x509.MarshalPKIXPublicKey(public)
	if err != nil {
		return nil, err
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(secret)
	return &GrantDraft{request: GrantRequest{GrantID: "join_" + hex.EncodeToString(id[:]),
		SecretSHA256: hex.EncodeToString(digest[:]), PublicKeyDER: publicDER, ExpiresUnix: now.Add(ttl).Unix()},
		secret: secret, privatePEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}))}, nil
}

func (d *GrantDraft) Request() GrantRequest {
	result := d.request
	result.PublicKeyDER = bytes.Clone(result.PublicKeyDER)
	return result
}

func (d *GrantDraft) Complete(bundle GrantBundle, now time.Time) (Ticket, error) {
	if d == nil || bundle.GrantID != d.request.GrantID || bundle.ExpiresUnix != d.request.ExpiresUnix {
		return Ticket{}, errors.New("Join Grant response does not match pending request")
	}
	ticket := Ticket{document: ticketDocument{Version: 1, GrantBundle: bundle,
		Secret: bytes.Clone(d.secret), PrivateKeyPEM: d.privatePEM}}
	if _, _, err := ticket.validate(now); err != nil {
		return Ticket{}, err
	}
	return ticket, nil
}

func EncodeTicket(ticket Ticket, now time.Time) (string, error) {
	if _, _, err := ticket.validate(now); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(ticket.document)
	if err != nil {
		return "", errors.New("cannot encode Join Ticket")
	}
	result := joinTicketPrefix + base64.RawURLEncoding.EncodeToString(encoded)
	if len(result) > MaxJoinTicketSize {
		return "", errors.New("Join Ticket exceeds size limit")
	}
	return result, nil
}

func DecodeTicket(value string, now time.Time) (Ticket, error) {
	if len(value) > MaxJoinTicketSize || !strings.HasPrefix(value, joinTicketPrefix) {
		return Ticket{}, errors.New("invalid Join Ticket version or size")
	}
	encoded, err := base64.RawURLEncoding.Strict().DecodeString(strings.TrimPrefix(value, joinTicketPrefix))
	if err != nil {
		return Ticket{}, errors.New("invalid Join Ticket encoding")
	}
	var document ticketDocument
	if err := decodeStrict(encoded, &document); err != nil {
		return Ticket{}, errors.New("invalid Join Ticket document")
	}
	canonical, err := json.Marshal(document)
	// Reject duplicate fields, unknown fields, alternate encodings and trailing
	// bytes. One byte representation avoids ambiguous credential interpretation.
	if err != nil || !bytes.Equal(canonical, encoded) || joinTicketPrefix+base64.RawURLEncoding.EncodeToString(encoded) != value {
		return Ticket{}, errors.New("noncanonical Join Ticket document")
	}
	ticket := Ticket{document: document}
	if _, _, err := ticket.validate(now); err != nil {
		return Ticket{}, err
	}
	return ticket, nil
}

func (t Ticket) validate(now time.Time) (*x509.Certificate, tls.Certificate, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	d := t.document
	invalid := func() (*x509.Certificate, tls.Certificate, error) {
		return nil, tls.Certificate{}, errors.New("invalid, mismatched or expired Join Ticket")
	}
	if d.Version != 1 || len(d.Secret) != 32 || identity.ValidateJoinGrantID(d.GrantID) != nil ||
		len(d.CACertificatePEM) > 8192 || len(d.CertificatePEM) > 8192 || len(d.PrivateKeyPEM) > 4096 ||
		ValidateIssuerAddress(d.IssuerAddress) != nil {
		return invalid()
	}
	ca, err := nodepki.ValidateClusterCA(d.ClusterID, []byte(d.CACertificatePEM), now)
	if err != nil {
		return invalid()
	}
	certificateBlock, rest := pem.Decode([]byte(d.CertificatePEM))
	if certificateBlock == nil || certificateBlock.Type != "CERTIFICATE" || len(certificateBlock.Headers) != 0 ||
		!strings.HasPrefix(strings.TrimSpace(d.CertificatePEM), "-----BEGIN CERTIFICATE-----") || len(bytes.TrimSpace(rest)) != 0 {
		return invalid()
	}
	privateBlock, rest := pem.Decode([]byte(d.PrivateKeyPEM))
	if privateBlock == nil || privateBlock.Type != "PRIVATE KEY" || len(privateBlock.Headers) != 0 ||
		!strings.HasPrefix(strings.TrimSpace(d.PrivateKeyPEM), "-----BEGIN PRIVATE KEY-----") || len(bytes.TrimSpace(rest)) != 0 {
		return invalid()
	}
	pair, err := tls.X509KeyPair([]byte(d.CertificatePEM), []byte(d.PrivateKeyPEM))
	if err != nil || len(pair.Certificate) != 1 {
		return invalid()
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return invalid()
	}
	parsed, err := identity.ParseEnrollmentCertificate(leaf)
	if err != nil || parsed.ClusterID != d.ClusterID || parsed.GrantID != d.GrantID ||
		leaf.NotAfter.Unix() != d.ExpiresUnix || !leaf.NotAfter.After(now) || leaf.NotAfter.Sub(now) > identity.MaxJoinGrantTTL {
		return invalid()
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return invalid()
	}
	pair.Leaf = leaf
	return ca, pair, nil
}

func ValidateIssuerAddress(address string) error {
	return ValidateNodeAddress(address)
}

// ValidateNodeAddress is shared by issuer targets, enrolled Node metadata and
// discovered peer routes. Reject an unusable route before consuming a grant,
// not after publishing the Node or adding it to Root membership.
func ValidateNodeAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	number, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || number < 1 || number > 65535 || strconv.Itoa(number) != port ||
		host == "" || strings.TrimSpace(host) != host || strings.ContainsAny(host, "/\\?#@%") || len(address) > 512 {
		return errors.New("invalid Node peer address")
	}
	if ip := net.ParseIP(host); ip != nil && (ip.IsUnspecified() || ip.IsMulticast()) {
		return errors.New("Node peer requires a dialable unicast address")
	}
	// Exclude resolver schemes, whitespace and malformed DNS names. IPv6 was
	// already parsed above; no URI/resolver target supplied by a ticket is used.
	if net.ParseIP(host) == nil {
		if len(strings.TrimSuffix(host, ".")) > 253 {
			return errors.New("Node peer hostname exceeds DNS length limit")
		}
		for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
			if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
				return errors.New("invalid issuer hostname")
			}
			for _, c := range label {
				if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
					return errors.New("invalid issuer hostname")
				}
			}
		}
	}
	return nil
}

func (t Ticket) ClientTLS(now time.Time) (*tls.Config, error) {
	ca, pair, err := t.validate(now)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	return &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots,
		ServerName: nodepki.DefaultServerName, Certificates: []tls.Certificate{pair}}, nil
}

func (t Ticket) AdmissionProof(now time.Time) ([]byte, error) {
	if _, _, err := t.validate(now); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		GrantID string `json:"grant_id"`
		Secret  []byte `json:"secret"`
	}{t.document.GrantID, t.document.Secret})
}

// ValidateNodeBundle pins the response to the ticket's authority and the local
// CSR before the client persists or installs any returned identity material.
func (t Ticket) ValidateNodeBundle(bundle Bundle, request Request, now time.Time) error {
	ca, _, err := t.validate(now)
	if err != nil {
		return err
	}
	if err := validateBundle(bundle); err != nil {
		return err
	}
	if bundle.ClusterID != t.ClusterID() || bundle.NodeID != request.NodeID || bundle.RaftID != request.RaftID ||
		bundle.ServerName != nodepki.DefaultServerName || request.ServerName != nodepki.DefaultServerName {
		return errors.New("issued Node bundle does not match Join Ticket or request")
	}
	issuedCA, err := nodepki.ValidateClusterCA(bundle.ClusterID, []byte(bundle.CACertificatePEM), now)
	if err != nil || !bytes.Equal(ca.Raw, issuedCA.Raw) {
		return errors.New("issued Node bundle replaces pinned Cluster CA")
	}
	return validateNodeBundleForRequest(bundle, request, now)
}

// Also used to validate a previously authenticated, durable receipt at its
// acceptance time, without retaining an expired ticket or temporary key.
func validateNodeBundleForRequest(bundle Bundle, request Request, now time.Time) error {
	if err := validateBundle(bundle); err != nil {
		return err
	}
	if bundle.NodeID != request.NodeID || bundle.RaftID != request.RaftID ||
		bundle.ServerName != nodepki.DefaultServerName || request.ServerName != nodepki.DefaultServerName {
		return errors.New("issued Node bundle does not match request identity")
	}
	value := identity.Identity{Version: 1, NodeID: request.NodeID, RaftID: request.RaftID, ClusterID: bundle.ClusterID}
	if err := nodepki.ValidateIssuedLeaf(value, bundle.ServerName, []byte(bundle.CACertificatePEM), []byte(bundle.CertificatePEM), now); err != nil {
		return err
	}
	csr, err := validateRequest(request)
	if err != nil {
		return err
	}
	block, _ := pem.Decode([]byte(bundle.CertificatePEM))
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return err
	}
	if !bytes.Equal(csr.RawSubjectPublicKeyInfo, leaf.RawSubjectPublicKeyInfo) {
		return errors.New("issued Node bundle does not match local CSR key")
	}
	return nil
}
