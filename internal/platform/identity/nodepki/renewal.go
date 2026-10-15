package nodepki

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"golang.org/x/sys/unix"
)

const (
	RenewBefore     = 30 * 24 * time.Hour
	renewalLockName = ".leaf-renewal.lock"
)

// NeedsRenewal is an internal scheduling policy, not a deployment parameter.
func NeedsRenewal(certificate *x509.Certificate, now time.Time) bool {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return certificate == nil || !now.Add(RenewBefore).Before(certificate.NotAfter)
}

// RenewNode renews an existing, currently valid Node certificate under the same
// CA and public key. The caller must first prove possession of the old key and
// confirm current Root membership/renewal eligibility. A public certificate
// alone is not authorization, and enrollment grants cannot call this path.
func (signer *Signer) RenewNode(value identity.Identity, serverName string, currentPEM []byte, now time.Time) ([]byte, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if signer == nil || signer.ca == nil || signer.privateKey == nil || signer.clusterID != value.ClusterID ||
		now.Before(signer.ca.NotBefore) || !now.Before(signer.ca.NotAfter) {
		return nil, errors.New("renewal requires an active matching Cluster signer")
	}
	if err := ValidateServerName(serverName); err != nil {
		return nil, err
	}
	if len(currentPEM) == 0 || len(currentPEM) > 1<<20 {
		return nil, errors.New("current leaf is empty or too large")
	}
	current, err := parseCertificate(currentPEM)
	if err != nil {
		return nil, err
	}
	if err := validateLeaf(current, signer.ca, value, serverName, now); err != nil {
		return nil, err
	}
	public, publicOK := current.PublicKey.(ed25519.PublicKey)
	private, privateOK := signer.privateKey.(ed25519.PrivateKey)
	if !publicOK || !privateOK {
		return nil, errors.New("Node renewal requires Ed25519")
	}
	// Near CA expiry a leaf cannot be extended; silently reissuing the same
	// expiry would hide the need for a distinct, coordinated CA rotation.
	if !minTime(now.Add(DefaultLeafValidity), signer.ca.NotAfter).Truncate(time.Second).After(current.NotAfter) {
		return nil, errors.New("Node certificate validity cannot be extended under this CA")
	}
	der, err := createLeaf(value, serverName, now, public, signer.ca, private)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

// RenewWithSigner is the local signer variant. It never distributes the signer
// or modifies its envelope. Server scheduling and TLS reload remain separate.
func RenewWithSigner(store string, value identity.Identity, serverName string, signer *Signer, now time.Time) (Material, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	current, err := Load(store, value, serverName, now)
	if err != nil {
		return Material{}, err
	}
	if signer == nil || signer.ca == nil || !bytes.Equal(signer.ca.Raw, current.CACertificate.Raw) {
		return Material{}, errors.New("renewal signer does not match the installed CA")
	}
	leaf, err := signer.RenewNode(value, serverName, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: current.Certificate.Raw}), now)
	if err != nil {
		return Material{}, err
	}
	return InstallRenewedLeaf(store, value, serverName, leaf, now)
}

// InstallRenewedLeaf publishes only a renewed serving certificate. The caller
// must own the runtime Store lease; a separate short-lived PKI lease serializes
// renewal writers without reacquiring that lease. Both the existing material
// and the new leaf must be currently valid. This is not expired-node recovery,
// key/CA rotation, or authorization to issue a certificate.
//
// A failure after rename can leave the new certificate visible; retrying the
// exact same certificate re-syncs it and its directory before reporting success.
func InstallRenewedLeaf(store string, value identity.Identity, serverName string,
	certificatePEM []byte, now time.Time,
) (Material, error) {
	return installRenewedLeaf(store, value, serverName, certificatePEM, now, nil)
}

func installRenewedLeaf(store string, value identity.Identity, serverName string,
	certificatePEM []byte, now time.Time, hook func(string) error,
) (material Material, err error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if len(certificatePEM) == 0 || len(certificatePEM) > 1<<20 {
		return Material{}, errors.New("renewed leaf is empty or too large")
	}
	if err := validateStoreDirectory(store); err != nil {
		return Material{}, err
	}
	storeRoot, err := os.OpenRoot(store)
	if err != nil {
		return Material{}, err
	}
	defer func() { err = errors.Join(err, storeRoot.Close()) }()
	actual, err := identity.Load(store)
	if err != nil || actual.NodeID != value.NodeID || actual.RaftID != value.RaftID ||
		actual.ClusterID == "" || actual.ClusterID != value.ClusterID {
		return Material{}, errors.Join(err, errors.New("renewal does not match durable Node identity"))
	}
	if err := validateDirectory(StorePaths(store).Directory); err != nil {
		return Material{}, err
	}
	pkiRoot, err := storeRoot.OpenRoot(DirectoryName)
	if err != nil {
		return Material{}, err
	}
	defer func() { err = errors.Join(err, pkiRoot.Close()) }()
	verify := func() error { return verifyRenewalRoots(store, storeRoot, pkiRoot) }
	if err := verify(); err != nil {
		return Material{}, err
	}
	lock, err := pkiRoot.OpenFile(renewalLockName, os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return Material{}, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	info, err := lock.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return Material{}, errors.Join(err, errors.New("leaf renewal lock must be a regular 0600 file"))
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return Material{}, fmt.Errorf("acquire leaf renewal lease: %w", err)
	}
	read := func(path string, mode os.FileMode) ([]byte, error) {
		file, err := pkiRoot.OpenFile(filepath.Base(path), os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
		if err != nil {
			return nil, err
		}
		return readOpenedFile(file, mode)
	}
	current, err := loadMaterial(StorePaths(store), value, serverName, now, read)
	if err != nil {
		return Material{}, err
	}
	leaf, err := parseCertificate(certificatePEM)
	if err != nil {
		return Material{}, err
	}
	if err := validateLeaf(leaf, current.CACertificate, value, serverName, now); err != nil {
		return Material{}, err
	}
	if !bytes.Equal(leaf.RawSubjectPublicKeyInfo, current.Certificate.RawSubjectPublicKeyInfo) {
		return Material{}, errors.New("renewal cannot replace the Node public key")
	}
	if bytes.Equal(leaf.Raw, current.Certificate.Raw) {
		if err := verify(); err != nil {
			return Material{}, err
		}
		if err := syncRenewalLeaf(pkiRoot); err != nil {
			return Material{}, err
		}
		return current, verify()
	}
	if !leaf.NotAfter.After(current.Certificate.NotAfter) || leaf.NotBefore.Before(current.Certificate.NotBefore) {
		return Material{}, errors.New("renewal must extend validity without rolling back issuance time")
	}
	stage := ".leaf-renewal-" + rand.Text()
	file, err := pkiRoot.OpenFile(stage, os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0o644)
	if err != nil {
		return Material{}, err
	}
	defer pkiRoot.Remove(stage)
	_, writeErr := file.Write(certificatePEM)
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err != nil {
		return Material{}, err
	}
	checkpoint := func(phase string) error {
		if hook != nil {
			if err := hook(phase); err != nil {
				return err
			}
		}
		return verify()
	}
	if err := checkpoint("staged"); err != nil {
		return Material{}, err
	}
	if err := pkiRoot.Rename(stage, NodeCertificateName); err != nil {
		return Material{}, err
	}
	if err := checkpoint("published"); err != nil {
		return Material{}, err
	}
	if err := syncRenewalDirectory(pkiRoot); err != nil {
		return Material{}, err
	}
	if err := checkpoint("synced"); err != nil {
		return Material{}, err
	}
	current.Certificate = leaf
	return current, nil
}

func verifyRenewalRoots(store string, storeRoot, pkiRoot *os.Root) error {
	held, err := storeRoot.Stat(".")
	current, statErr := os.Lstat(store)
	if err != nil || statErr != nil || !current.IsDir() || !os.SameFile(held, current) {
		return errors.Join(err, statErr, errors.New("renewal Store directory was replaced"))
	}
	held, err = pkiRoot.Stat(".")
	current, statErr = storeRoot.Lstat(DirectoryName)
	if err != nil || statErr != nil || !current.IsDir() || current.Mode().Perm() != 0o700 || !os.SameFile(held, current) {
		return errors.Join(err, statErr, errors.New("renewal PKI directory was replaced"))
	}
	return nil
}

func syncRenewalLeaf(root *os.Root) error {
	file, err := root.OpenFile(NodeCertificateName, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	return syncRenewalDirectory(root)
}

func syncRenewalDirectory(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
