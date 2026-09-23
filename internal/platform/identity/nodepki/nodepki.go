// Package nodepki owns DUBI's bootstrap Node CA and serving certificate files.
// The CA signer is stored only as an authenticated envelope encrypted to a
// node-local X25519 wrapping key; joined nodes will receive a leaf, not signer.
package nodepki

import (
	"bytes"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
)

const (
	DirectoryName       = "dubi-node-pki"
	CACertificateName   = "node-ca.crt"
	NodeCertificateName = "node.crt"
	NodePrivateKeyName  = "node.key"
	WrappingKeyName     = "signer-wrap.key"
	SignerEnvelopeName  = "signer-envelope.json"
	ConfigName          = "config.json"
	DefaultServerName   = "dubi.internal"
	DefaultCAValidity   = 10 * 365 * 24 * time.Hour
	DefaultLeafValidity = 90 * 24 * time.Hour
	clockSkewAllowance  = 5 * time.Minute
	formatVersion       = 1
	envelopeDomain      = "dubi/node-ca-envelope/v1"
)

type Paths struct {
	Directory     string `json:"directory"`
	CACertificate string `json:"ca_certificate"`
	Certificate   string `json:"certificate"`
	PrivateKey    string `json:"private_key"`
	WrappingKey   string `json:"-"`
	Envelope      string `json:"-"`
	Config        string `json:"-"`
}

type Config struct {
	Version       int    `json:"version"`
	ClusterID     string `json:"cluster_id"`
	NodeID        string `json:"node_id"`
	RaftID        uint64 `json:"raft_id"`
	ServerName    string `json:"server_name"`
	CAFingerprint string `json:"ca_sha256"`
}

type Material struct {
	Paths         Paths
	Config        Config
	CACertificate *x509.Certificate
	Certificate   *x509.Certificate
}

type Signer struct {
	ca         *x509.Certificate
	privateKey crypto.Signer
	clusterID  string
}

type signerEnvelope struct {
	Version            int    `json:"version"`
	CAFingerprint      string `json:"ca_sha256"`
	EphemeralPublicKey []byte `json:"ephemeral_public_key"`
	Nonce              []byte `json:"nonce"`
	Ciphertext         []byte `json:"ciphertext"`
}

func StorePaths(store string) Paths {
	directory := filepath.Join(store, DirectoryName)
	return Paths{Directory: directory, CACertificate: filepath.Join(directory, CACertificateName),
		Certificate: filepath.Join(directory, NodeCertificateName), PrivateKey: filepath.Join(directory, NodePrivateKeyName),
		WrappingKey: filepath.Join(directory, WrappingKeyName), Envelope: filepath.Join(directory, SignerEnvelopeName),
		Config: filepath.Join(directory, ConfigName)}
}

func ValidateServerName(value string) error {
	if !validServerName(strings.TrimSpace(value)) {
		return errors.New("Node PKI server name must be one DNS name or IP address")
	}
	return nil
}

// ValidateIssuedLeaf verifies an enrollment bundle without requiring access
// to the Node private key or publishing any files.
func ValidateIssuedLeaf(value identity.Identity, serverName string, caPEM, certificatePEM []byte, now time.Time) error {
	_, _, err := validateIssuedLeaf(value, strings.TrimSpace(serverName), caPEM, certificatePEM, now)
	return err
}

// Ensure atomically creates bootstrap PKI or validates the existing material.
func Ensure(store string, value identity.Identity, serverName string, now time.Time) (Material, error) {
	serverName = strings.TrimSpace(serverName)
	if store == "" || value.ClusterID == "" || value.NodeID == "" || value.RaftID == 0 {
		return Material{}, errors.New("bootstrap Node PKI requires a bound durable identity")
	}
	if serverName == "" {
		serverName = DefaultServerName
	}
	if err := ValidateServerName(serverName); err != nil {
		return Material{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	paths := StorePaths(store)
	if _, err := os.Lstat(paths.Directory); err == nil {
		material, loadErr := Load(store, value, serverName, now)
		if loadErr != nil {
			return Material{}, fmt.Errorf("existing Node PKI is incomplete or invalid: %w", loadErr)
		}
		if _, err := OpenSigner(store, value, now); err != nil {
			return Material{}, err
		}
		return material, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Material{}, err
	}
	if err := os.MkdirAll(store, 0o750); err != nil {
		return Material{}, err
	}
	if err := validateStoreDirectory(store); err != nil {
		return Material{}, err
	}
	staging, err := os.MkdirTemp(store, ".dubi-node-pki-init-")
	if err != nil {
		return Material{}, err
	}
	defer os.RemoveAll(staging)
	if err := os.Chmod(staging, 0o700); err != nil {
		return Material{}, err
	}
	stagingPaths := pathsAt(staging)
	caPublic, caPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Material{}, err
	}
	caDER, err := createCA(value, now, caPublic, caPrivate)
	if err != nil {
		return Material{}, err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return Material{}, err
	}
	nodePublic, nodePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Material{}, err
	}
	nodeDER, err := createLeaf(value, serverName, now, nodePublic, ca, caPrivate)
	if err != nil {
		return Material{}, err
	}
	wrappingKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return Material{}, err
	}
	envelope, err := sealSigner(value, ca, caPrivate, wrappingKey.PublicKey())
	if err != nil {
		return Material{}, err
	}
	fingerprint := certificateFingerprint(ca)
	configuration := Config{Version: formatVersion, ClusterID: value.ClusterID, NodeID: value.NodeID,
		RaftID: value.RaftID, ServerName: serverName, CAFingerprint: fingerprint}
	encodedConfig, _ := json.Marshal(configuration)
	encodedEnvelope, _ := json.Marshal(envelope)
	writes := []struct {
		path string
		data []byte
		mode os.FileMode
	}{
		{stagingPaths.CACertificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o644},
		{stagingPaths.Certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: nodeDER}), 0o644},
		{stagingPaths.PrivateKey, marshalPrivateKey(nodePrivate), 0o600},
		{stagingPaths.WrappingKey, pem.EncodeToMemory(&pem.Block{Type: "DUBI SIGNER WRAPPING PRIVATE KEY", Bytes: wrappingKey.Bytes()}), 0o600},
		{stagingPaths.Envelope, append(encodedEnvelope, '\n'), 0o600},
		{stagingPaths.Config, append(encodedConfig, '\n'), 0o600},
	}
	for _, write := range writes {
		if err := writeExclusive(write.path, write.data, write.mode); err != nil {
			return Material{}, err
		}
	}
	if err := syncDirectory(staging); err != nil {
		return Material{}, err
	}
	if err := os.Rename(staging, paths.Directory); err != nil {
		if material, loadErr := Load(store, value, serverName, now); loadErr == nil {
			if _, signerErr := OpenSigner(store, value, now); signerErr == nil {
				return material, nil
			}
		}
		return Material{}, fmt.Errorf("publish Node PKI: %w", err)
	}
	if err := syncDirectory(store); err != nil {
		return Material{}, err
	}
	material, err := Load(store, value, serverName, now)
	if err != nil {
		return Material{}, err
	}
	if _, err := OpenSigner(store, value, now); err != nil {
		return Material{}, err
	}
	return material, nil
}

// InstallLeaf validates enrollment output, binds the prepared identity to the
// Cluster, and atomically publishes a leaf-only PKI directory. It never stores
// the CA signer envelope on a joined Node.
func InstallLeaf(store string, value identity.Identity, clusterID, serverName string,
	caPEM, certificatePEM, privateKeyPEM []byte, now time.Time,
) (Material, error) {
	serverName = strings.TrimSpace(serverName)
	if store == "" || value.NodeID == "" || value.RaftID == 0 || clusterID == "" {
		return Material{}, errors.New("leaf installation requires Store, prepared identity, and ClusterID")
	}
	if value.ClusterID != "" && value.ClusterID != clusterID {
		return Material{}, errors.New("prepared Node is already bound to a different Cluster")
	}
	if err := ValidateServerName(serverName); err != nil {
		return Material{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	candidate := value
	candidate.ClusterID = clusterID
	if _, err := identity.NodeIdentityURI(candidate); err != nil {
		return Material{}, err
	}
	ca, _, err := validateIssuedLeaf(candidate, serverName, caPEM, certificatePEM, now)
	if err != nil {
		return Material{}, err
	}
	paths := StorePaths(store)
	if _, err := os.Lstat(paths.Directory); err == nil {
		material, loadErr := Load(store, candidate, serverName, now)
		if loadErr != nil {
			return Material{}, fmt.Errorf("existing Node PKI is incomplete or invalid: %w", loadErr)
		}
		bound, bindErr := identity.BindCluster(store, clusterID)
		if bindErr != nil || bound.ClusterID != clusterID {
			return Material{}, errors.Join(bindErr, errors.New("bind prepared Node to enrolled Cluster"))
		}
		return material, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Material{}, err
	}
	if err := validateStoreDirectory(store); err != nil {
		return Material{}, err
	}
	staging, err := os.MkdirTemp(store, ".dubi-node-pki-enroll-")
	if err != nil {
		return Material{}, err
	}
	defer os.RemoveAll(staging)
	if err := os.Chmod(staging, 0o700); err != nil {
		return Material{}, err
	}
	configuration := Config{Version: formatVersion, ClusterID: clusterID, NodeID: value.NodeID,
		RaftID: value.RaftID, ServerName: serverName, CAFingerprint: certificateFingerprint(ca)}
	encodedConfig, err := json.Marshal(configuration)
	if err != nil {
		return Material{}, err
	}
	stagingPaths := pathsAt(staging)
	for _, write := range []struct {
		path string
		data []byte
		mode os.FileMode
	}{
		{stagingPaths.CACertificate, caPEM, 0o644},
		{stagingPaths.Certificate, certificatePEM, 0o644},
		{stagingPaths.PrivateKey, privateKeyPEM, 0o600},
		{stagingPaths.Config, append(encodedConfig, '\n'), 0o600},
	} {
		if err := writeExclusive(write.path, write.data, write.mode); err != nil {
			return Material{}, err
		}
	}
	if _, err := loadPaths(stagingPaths, candidate, serverName, now); err != nil {
		return Material{}, fmt.Errorf("validate enrolled Node PKI: %w", err)
	}
	bound, err := identity.BindCluster(store, clusterID)
	if err != nil || bound.ClusterID != clusterID {
		return Material{}, errors.Join(err, errors.New("bind prepared Node to enrolled Cluster"))
	}
	if err := syncDirectory(staging); err != nil {
		return Material{}, err
	}
	if err := os.Rename(staging, paths.Directory); err != nil {
		if material, loadErr := Load(store, bound, serverName, now); loadErr == nil {
			return material, nil
		}
		return Material{}, fmt.Errorf("publish enrolled Node PKI: %w", err)
	}
	if err := syncDirectory(store); err != nil {
		return Material{}, err
	}
	return Load(store, bound, serverName, now)
}

func Load(store string, value identity.Identity, serverName string, now time.Time) (Material, error) {
	serverName = strings.TrimSpace(serverName)
	if serverName == "" {
		serverName = DefaultServerName
	}
	if err := ValidateServerName(serverName); err != nil {
		return Material{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return loadPaths(StorePaths(store), value, serverName, now)
}

func loadPaths(paths Paths, value identity.Identity, serverName string, now time.Time) (Material, error) {
	if err := validateDirectory(paths.Directory); err != nil {
		return Material{}, err
	}
	encodedConfig, err := readFile(paths.Config, 0o600)
	if err != nil {
		return Material{}, err
	}
	var configuration Config
	decoder := json.NewDecoder(bytes.NewReader(encodedConfig))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&configuration); err != nil || decoder.Decode(new(any)) != io.EOF {
		return Material{}, errors.New("invalid Node PKI configuration")
	}
	if configuration.Version != formatVersion || configuration.ClusterID != value.ClusterID ||
		configuration.NodeID != value.NodeID || configuration.RaftID != value.RaftID ||
		configuration.ServerName != serverName || len(configuration.CAFingerprint) != sha256.Size*2 {
		return Material{}, errors.New("Node PKI configuration does not match durable identity")
	}
	caPEM, err := readFile(paths.CACertificate, 0o644)
	if err != nil {
		return Material{}, err
	}
	ca, err := parseCertificate(caPEM)
	if err != nil || !ca.IsCA || certificateFingerprint(ca) != configuration.CAFingerprint ||
		caIdentityURI(value.ClusterID).String() != onlyURI(ca) || now.Before(ca.NotBefore) || !now.Before(ca.NotAfter) {
		return Material{}, errors.New("invalid or expired Node CA certificate")
	}
	if err := ca.CheckSignatureFrom(ca); err != nil {
		return Material{}, errors.New("Node CA is not self-signed")
	}
	certificatePEM, err := readFile(paths.Certificate, 0o644)
	if err != nil {
		return Material{}, err
	}
	certificate, err := parseCertificate(certificatePEM)
	if err != nil {
		return Material{}, err
	}
	if err := validateLeaf(certificate, ca, value, serverName, now); err != nil {
		return Material{}, err
	}
	privatePEM, err := readFile(paths.PrivateKey, 0o600)
	if err != nil {
		return Material{}, err
	}
	privateKey, err := parseEd25519PrivateKey(privatePEM, "PRIVATE KEY")
	certificatePublic, publicOK := certificate.PublicKey.(ed25519.PublicKey)
	if err != nil || !publicOK || !bytes.Equal(privateKey.Public().(ed25519.PublicKey), certificatePublic) {
		return Material{}, errors.New("Node private key does not match certificate")
	}
	return Material{Paths: paths, Config: configuration, CACertificate: ca, Certificate: certificate}, nil
}

func OpenSigner(store string, value identity.Identity, now time.Time) (*Signer, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	paths := StorePaths(store)
	encodedConfig, err := readFile(paths.Config, 0o600)
	if err != nil {
		return nil, err
	}
	var configuration Config
	decoder := json.NewDecoder(bytes.NewReader(encodedConfig))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&configuration); err != nil || decoder.Decode(new(any)) != io.EOF ||
		configuration.Version != formatVersion || configuration.ClusterID != value.ClusterID ||
		configuration.NodeID != value.NodeID || configuration.RaftID != value.RaftID {
		return nil, errors.New("Node PKI signer configuration does not match identity")
	}
	material, err := Load(store, value, configuration.ServerName, now)
	if err != nil {
		return nil, fmt.Errorf("validate Node PKI before opening signer: %w", err)
	}
	ca := material.CACertificate
	wrappingPEM, err := readFile(paths.WrappingKey, 0o600)
	if err != nil {
		return nil, err
	}
	wrappingKey, err := parseWrappingKey(wrappingPEM)
	if err != nil {
		return nil, err
	}
	encodedEnvelope, err := readFile(paths.Envelope, 0o600)
	if err != nil {
		return nil, err
	}
	var envelope signerEnvelope
	envelopeDecoder := json.NewDecoder(bytes.NewReader(encodedEnvelope))
	envelopeDecoder.DisallowUnknownFields()
	if err := envelopeDecoder.Decode(&envelope); err != nil || envelopeDecoder.Decode(new(any)) != io.EOF ||
		envelope.Version != formatVersion || envelope.CAFingerprint != configuration.CAFingerprint {
		return nil, errors.New("invalid Node CA signer envelope")
	}
	privateKey, err := openSigner(value, envelope, wrappingKey)
	if err != nil {
		return nil, err
	}
	caPublic, publicOK := ca.PublicKey.(ed25519.PublicKey)
	if !publicOK || !bytes.Equal(privateKey.Public().(ed25519.PublicKey), caPublic) ||
		now.Before(ca.NotBefore) || !now.Before(ca.NotAfter) {
		return nil, errors.New("Node CA signer does not match active certificate")
	}
	return &Signer{ca: ca, privateKey: privateKey, clusterID: value.ClusterID}, nil
}

func (signer *Signer) CACertificatePEM() []byte {
	if signer == nil || signer.ca == nil {
		return nil
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: signer.ca.Raw})
}

// IssueNode signs one CSR only after the caller has supplied the bound Node
// identity. Root registration and admission authorization remain the caller's
// responsibility and must happen before this method is invoked.
func (signer *Signer) IssueNode(value identity.Identity, serverName string, encodedCSR []byte, now time.Time) ([]byte, error) {
	if signer == nil || signer.ca == nil || signer.privateKey == nil || value.ClusterID != signer.clusterID {
		return nil, errors.New("active Cluster signer and matching bound Node identity are required")
	}
	serverName = strings.TrimSpace(serverName)
	if err := ValidateServerName(serverName); err != nil {
		return nil, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	csr, err := parseCSR(encodedCSR)
	if err != nil {
		return nil, err
	}
	if err := validateCSR(csr, value, serverName); err != nil {
		return nil, err
	}
	publicKey, ok := csr.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("Node CSR must use Ed25519")
	}
	privateKey, ok := signer.privateKey.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("Node CA signer must use Ed25519")
	}
	encoded, err := createLeaf(value, serverName, now, publicKey, signer.ca, privateKey)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: encoded}), nil
}

func createCA(value identity.Identity, now time.Time, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey) ([]byte, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "DUBI Node CA"},
		URIs: []*url.URL{caIdentityURI(value.ClusterID)}, NotBefore: now.Add(-clockSkewAllowance), NotAfter: now.Add(DefaultCAValidity),
		IsCA: true, BasicConstraintsValid: true, MaxPathLen: 0, MaxPathLenZero: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature}
	return x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
}

func createLeaf(value identity.Identity, serverName string, now time.Time, publicKey ed25519.PublicKey,
	ca *x509.Certificate, caPrivate ed25519.PrivateKey,
) ([]byte, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	uri, err := identity.NodeIdentityURI(value)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: value.NodeID},
		URIs: []*url.URL{uri}, NotBefore: now.Add(-clockSkewAllowance),
		NotAfter: minTime(now.Add(DefaultLeafValidity), ca.NotAfter), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	if address := net.ParseIP(serverName); address != nil {
		template.IPAddresses = []net.IP{address}
	} else {
		template.DNSNames = []string{serverName}
	}
	return x509.CreateCertificate(rand.Reader, template, ca, publicKey, caPrivate)
}

func validateLeaf(certificate, ca *x509.Certificate, value identity.Identity, serverName string, now time.Time) error {
	if err := identity.ValidateNodeCertificate(certificate, value); err != nil {
		return err
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		options := x509.VerifyOptions{Roots: roots, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{usage}}
		if usage == x509.ExtKeyUsageServerAuth {
			options.DNSName = serverName
		}
		if _, err := certificate.Verify(options); err != nil {
			return fmt.Errorf("verify Node certificate: %w", err)
		}
	}
	return nil
}

func validateIssuedLeaf(value identity.Identity, serverName string, caPEM, certificatePEM []byte,
	now time.Time,
) (*x509.Certificate, *x509.Certificate, error) {
	if err := ValidateServerName(serverName); err != nil {
		return nil, nil, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	ca, err := parseCertificate(caPEM)
	if err != nil || !ca.IsCA || caIdentityURI(value.ClusterID).String() != onlyURI(ca) ||
		now.Before(ca.NotBefore) || !now.Before(ca.NotAfter) {
		return nil, nil, errors.New("invalid or expired Node CA certificate")
	}
	if err := ca.CheckSignatureFrom(ca); err != nil {
		return nil, nil, errors.New("Node CA is not self-signed")
	}
	certificate, err := parseCertificate(certificatePEM)
	if err != nil {
		return nil, nil, err
	}
	if err := validateLeaf(certificate, ca, value, serverName, now); err != nil {
		return nil, nil, err
	}
	return ca, certificate, nil
}

func sealSigner(value identity.Identity, ca *x509.Certificate, privateKey ed25519.PrivateKey,
	wrappingPublic *ecdh.PublicKey,
) (signerEnvelope, error) {
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return signerEnvelope{}, err
	}
	shared, err := ephemeral.ECDH(wrappingPublic)
	if err != nil {
		return signerEnvelope{}, err
	}
	defer clear(shared)
	fingerprint := certificateFingerprint(ca)
	aad := envelopeAAD(value, fingerprint)
	key, err := hkdf.Key(sha256.New, shared, []byte(envelopeDomain), string(aad), 32)
	if err != nil {
		return signerEnvelope{}, err
	}
	defer clear(key)
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return signerEnvelope{}, err
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return signerEnvelope{}, err
	}
	defer clear(privateDER)
	return signerEnvelope{Version: formatVersion, CAFingerprint: fingerprint,
		EphemeralPublicKey: ephemeral.PublicKey().Bytes(), Nonce: nonce,
		Ciphertext: aead.Seal(nil, nonce, privateDER, aad)}, nil
}

func openSigner(value identity.Identity, envelope signerEnvelope, wrappingKey *ecdh.PrivateKey) (ed25519.PrivateKey, error) {
	ephemeral, err := ecdh.X25519().NewPublicKey(envelope.EphemeralPublicKey)
	if err != nil {
		return nil, errors.New("invalid signer envelope ephemeral key")
	}
	shared, err := wrappingKey.ECDH(ephemeral)
	if err != nil {
		return nil, errors.New("derive signer envelope key")
	}
	defer clear(shared)
	aad := envelopeAAD(value, envelope.CAFingerprint)
	key, err := hkdf.Key(sha256.New, shared, []byte(envelopeDomain), string(aad), 32)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	if len(envelope.Nonce) != aead.NonceSize() || len(envelope.Ciphertext) <= aead.Overhead() {
		return nil, errors.New("invalid signer envelope ciphertext")
	}
	privateDER, err := aead.Open(nil, envelope.Nonce, envelope.Ciphertext, aad)
	if err != nil {
		return nil, errors.New("decrypt signer envelope")
	}
	defer clear(privateDER)
	parsed, err := x509.ParsePKCS8PrivateKey(privateDER)
	privateKey, ok := parsed.(ed25519.PrivateKey)
	if err != nil || !ok {
		return nil, errors.New("decode signer envelope private key")
	}
	return privateKey, nil
}

func pathsAt(directory string) Paths {
	return Paths{Directory: directory, CACertificate: filepath.Join(directory, CACertificateName),
		Certificate: filepath.Join(directory, NodeCertificateName), PrivateKey: filepath.Join(directory, NodePrivateKeyName),
		WrappingKey: filepath.Join(directory, WrappingKeyName), Envelope: filepath.Join(directory, SignerEnvelopeName),
		Config: filepath.Join(directory, ConfigName)}
}

func validateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return errors.Join(err, errors.New("Node PKI directory must be a real 0700 directory"))
	}
	return nil
}

func validateStoreDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(err, errors.New("Node PKI store must be a real directory"))
	}
	return nil
}

func validServerName(value string) bool {
	if net.ParseIP(value) != nil {
		return true
	}
	if len(value) == 0 || len(value) > 253 || strings.HasSuffix(value, ".") {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
				(character < '0' || character > '9') && character != '-' {
				return false
			}
		}
	}
	return true
}

func readFile(path string, mode os.FileMode) ([]byte, error) {
	info, err := os.Lstat(path)
	permissionsOK := info != nil && info.Mode().Perm() == mode
	if mode == 0o644 && info != nil && info.Mode().Perm() == 0o600 {
		permissionsOK = true
	}
	if err != nil || info == nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || !permissionsOK {
		return nil, errors.Join(err, fmt.Errorf("Node PKI file %s must be a regular %04o file", filepath.Base(path), mode))
	}
	encoded, err := os.ReadFile(path)
	if err != nil || len(encoded) == 0 || len(encoded) > 1<<20 {
		return nil, errors.Join(err, errors.New("Node PKI file is empty or too large"))
	}
	return encoded, nil
}

func parseCertificate(encoded []byte) (*x509.Certificate, error) {
	block, rest := pem.Decode(encoded)
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("Node PKI certificate must contain one PEM certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}

func parseCSR(encoded []byte) (*x509.CertificateRequest, error) {
	block, rest := pem.Decode(encoded)
	if block == nil || block.Type != "CERTIFICATE REQUEST" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("Node enrollment request must contain one PEM CSR")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil || csr.CheckSignature() != nil {
		return nil, errors.New("Node enrollment CSR signature is invalid")
	}
	return csr, nil
}

func validateCSR(csr *x509.CertificateRequest, value identity.Identity, serverName string) error {
	joinValue := value
	joinValue.ClusterID = ""
	joinURI, err := identity.JoinIdentityURI(joinValue)
	if err != nil {
		return err
	}
	if csr.Subject.CommonName != value.NodeID || len(csr.URIs) != 1 || csr.URIs[0].String() != joinURI.String() ||
		len(csr.EmailAddresses) != 0 {
		return errors.New("Node enrollment CSR does not match the durable Node identity")
	}
	if address := net.ParseIP(serverName); address != nil {
		if len(csr.IPAddresses) != 1 || !csr.IPAddresses[0].Equal(address) || len(csr.DNSNames) != 0 {
			return errors.New("Node enrollment CSR does not match its server IP")
		}
	} else if len(csr.DNSNames) != 1 || csr.DNSNames[0] != serverName || len(csr.IPAddresses) != 0 {
		return errors.New("Node enrollment CSR does not match its server DNS name")
	}
	return nil
}

func parseEd25519PrivateKey(encoded []byte, blockType string) (ed25519.PrivateKey, error) {
	block, rest := pem.Decode(encoded)
	if block == nil || block.Type != blockType || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("invalid Node PKI private key PEM")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	privateKey, ok := parsed.(ed25519.PrivateKey)
	if err != nil || !ok {
		return nil, errors.New("Node PKI private key must use Ed25519")
	}
	return privateKey, nil
}

func parseWrappingKey(encoded []byte) (*ecdh.PrivateKey, error) {
	block, rest := pem.Decode(encoded)
	if block == nil || block.Type != "DUBI SIGNER WRAPPING PRIVATE KEY" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("invalid signer wrapping key")
	}
	privateKey, err := ecdh.X25519().NewPrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.New("invalid signer wrapping key")
	}
	return privateKey, nil
}

func marshalPrivateKey(privateKey crypto.PrivateKey) []byte {
	encoded, _ := x509.MarshalPKCS8PrivateKey(privateKey)
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
}

func writeExclusive(path string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	return errors.Join(err, file.Close())
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}

func certificateFingerprint(certificate *x509.Certificate) string {
	digest := sha256.Sum256(certificate.Raw)
	return hex.EncodeToString(digest[:])
}

func caIdentityURI(clusterID string) *url.URL {
	uri, _ := url.Parse(fmt.Sprintf("spiffe://dubi/%s/node-ca", clusterID))
	return uri
}

func onlyURI(certificate *x509.Certificate) string {
	if certificate == nil || len(certificate.URIs) != 1 {
		return ""
	}
	return certificate.URIs[0].String()
}

func envelopeAAD(value identity.Identity, fingerprint string) []byte {
	return []byte(strings.Join([]string{envelopeDomain, value.ClusterID, value.NodeID,
		fmt.Sprintf("%d", value.RaftID), fingerprint}, "\x00"))
}

func minTime(first, second time.Time) time.Time {
	if first.Before(second) {
		return first
	}
	return second
}
