package nodepki

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
)

const leafReloadInterval = 30 * time.Second

// LeafReloader reloads only the leaf of an existing managed identity. CA and key
// rotation require a different coordinated protocol. No background goroutine or
// open descriptor is retained: checks are amortized across new handshakes.
type LeafReloader struct {
	mu                 sync.Mutex
	store              string
	id                 identity.Identity
	serverName         string
	storeInfo, pkiInfo os.FileInfo
	ca                 *x509.Certificate
	certificate        *tls.Certificate
	checked            time.Time
	lastError          error
	now                func() time.Time
}

func NewLeafReloader(store string, value identity.Identity, serverName string) (*LeafReloader, error) {
	return newLeafReloader(store, value, serverName, time.Now)
}

func newLeafReloader(store string, value identity.Identity, serverName string, now func() time.Time) (*LeafReloader, error) {
	storeInfo, err := os.Lstat(store)
	if err != nil || !storeInfo.IsDir() {
		return nil, errors.Join(err, errors.New("TLS Store must be a physical directory"))
	}
	pkiInfo, err := os.Lstat(StorePaths(store).Directory)
	if err != nil || !pkiInfo.IsDir() {
		return nil, errors.Join(err, errors.New("TLS PKI must be a physical directory"))
	}
	r := &LeafReloader{store: store, id: value, serverName: serverName, storeInfo: storeInfo, pkiInfo: pkiInfo, now: now}
	if err := r.Refresh(); err != nil {
		return nil, err
	}
	return r, nil
}

// TrustPool returns a distinct pool for each TLS configuration. The immutable
// CA chosen at construction cannot change through a leaf refresh.
func (r *LeafReloader) TrustPool() *x509.CertPool {
	r.mu.Lock()
	defer r.mu.Unlock()
	pool := x509.NewCertPool()
	pool.AddCert(r.ca)
	return pool
}

// Refresh bypasses the normal check interval after successful publication.
// An invalid replacement is reported but never evicts a still-valid old leaf.
func (r *LeafReloader) Refresh() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.refresh(r.now().UTC())
}

func (r *LeafReloader) LastError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastError
}

func (r *LeafReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return r.current()
}
func (r *LeafReloader) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return r.current()
}

func (r *LeafReloader) current() (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now().UTC()
	if r.checked.IsZero() || now.Before(r.checked) || !now.Before(r.checked.Add(leafReloadInterval)) {
		_ = r.refresh(now)
	}
	// Reading a replacement can block on storage. Re-sample before releasing
	// cached credentials; the pre-read time is not an indefinite validity lease.
	now = r.now().UTC()
	// A reload failure is tolerated only while the cached identity and CA are
	// still valid. Serving an expired last-known-good leaf is not recovery.
	if r.certificate == nil || now.Before(r.certificate.Leaf.NotBefore) || !now.Before(r.certificate.Leaf.NotAfter) ||
		now.Before(r.ca.NotBefore) || !now.Before(r.ca.NotAfter) {
		return nil, errors.New("no currently valid managed TLS certificate")
	}
	return r.certificate, nil // immutable after publication; TLS callbacks must not mutate it
}

func (r *LeafReloader) refresh(now time.Time) (err error) {
	r.checked = now
	defer func() { r.lastError = err }()
	if err := r.verifyDirectories(); err != nil {
		return err
	}
	material, certificate, err := LoadTLSCertificate(r.store, r.id, r.serverName, now)
	if err != nil {
		return err
	}
	if err := r.verifyDirectories(); err != nil {
		return err
	}
	if r.certificate != nil {
		if !bytes.Equal(material.CACertificate.Raw, r.ca.Raw) || !bytes.Equal(certificate.Leaf.RawSubjectPublicKeyInfo, r.certificate.Leaf.RawSubjectPublicKeyInfo) {
			return errors.New("TLS refresh cannot rotate CA or Node key")
		}
		if bytes.Equal(certificate.Leaf.Raw, r.certificate.Leaf.Raw) {
			return nil
		}
		if !certificate.Leaf.NotAfter.After(r.certificate.Leaf.NotAfter) || certificate.Leaf.NotBefore.Before(r.certificate.Leaf.NotBefore) {
			return errors.New("TLS refresh cannot roll back certificate validity")
		}
	}
	r.ca, r.certificate = material.CACertificate, &certificate
	return nil
}

func (r *LeafReloader) verifyDirectories() error {
	for _, entry := range []struct {
		path string
		info os.FileInfo
	}{{r.store, r.storeInfo}, {StorePaths(r.store).Directory, r.pkiInfo}} {
		current, err := os.Lstat(entry.path)
		if err != nil || !current.IsDir() || !os.SameFile(entry.info, current) {
			return errors.Join(err, errors.New("managed TLS directory was replaced"))
		}
	}
	return nil
}

// LoadTLSCertificate bounds and checks actual file descriptors before parsing
// the pair. Re-reading a changing leaf fails closed instead of combining a
// certificate with an identity validation performed on different bytes.
func LoadTLSCertificate(store string, value identity.Identity, serverName string, now time.Time) (Material, tls.Certificate, error) {
	material, err := Load(store, value, serverName, now)
	if err != nil {
		return Material{}, tls.Certificate{}, err
	}
	leafPEM, err := readFile(material.Paths.Certificate, 0o644)
	if err != nil {
		return Material{}, tls.Certificate{}, err
	}
	keyPEM, err := readFile(material.Paths.PrivateKey, 0o600)
	if err != nil {
		return Material{}, tls.Certificate{}, err
	}
	defer clear(keyPEM)
	certificate, err := tls.X509KeyPair(leafPEM, keyPEM)
	if err != nil {
		return Material{}, tls.Certificate{}, err
	}
	if len(certificate.Certificate) != 1 || !bytes.Equal(certificate.Certificate[0], material.Certificate.Raw) {
		return Material{}, tls.Certificate{}, errors.New("managed TLS leaf changed during validation")
	}
	certificate.Leaf = material.Certificate
	return material, certificate, nil
}
