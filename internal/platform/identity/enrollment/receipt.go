package enrollment

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
	"golang.org/x/sys/unix"
)

const JoinReceiptName = "DUBI_JOIN_RECEIPT"
const maxReceiptBytes = 64 << 10

// Receipt is an immutable, secret-free record of an authenticated enrollment
// response. It precedes PKI installation and survives removal of the prepared
// CSR/private-key staging directory. It does NOT certify Root membership.
type Receipt struct {
	Version       int     `json:"version"`
	GrantID       string  `json:"grant_id"`
	IssuerAddress string  `json:"issuer_address"`
	AcceptedUnix  int64   `json:"accepted_unix"`
	RequestSHA256 string  `json:"request_sha256"`
	Request       Request `json:"request"`
	Bundle        Bundle  `json:"bundle"`
}

// SaveReceipt durably publishes the authenticated public response before any
// PKI/Cluster binding. It holds the Store process lock and never overwrites a
// different receipt. The first valid received leaf wins same-request retries.
func SaveReceipt(store string, ticket Ticket, request Request, bundle Bundle, now time.Time) (Receipt, error) {
	return saveReceipt(store, ticket, request, bundle, now, nil)
}

func saveReceipt(store string, ticket Ticket, request Request, bundle Bundle, now time.Time, hook func(string) error) (Receipt, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := ticket.ValidateNodeBundle(bundle, request, now); err != nil {
		return Receipt{}, err
	}
	root, id, release, err := lockedReceiptRoot(store)
	if err != nil {
		return Receipt{}, err
	}
	defer release.Release()
	defer root.Close()
	requestBytes, err := EncodeRequest(request)
	if err != nil {
		return Receipt{}, err
	}
	requestDigest := sha256.Sum256(requestBytes)
	candidate := Receipt{Version: 1, GrantID: ticket.GrantID(), IssuerAddress: ticket.Address(), AcceptedUnix: now.Unix(),
		RequestSHA256: hex.EncodeToString(requestDigest[:]), Request: request, Bundle: bundle}
	if err := candidate.validate(id); err != nil {
		return Receipt{}, err
	}
	current, err := loadReceipt(root, id)
	if err == nil {
		if current.GrantID != candidate.GrantID || current.IssuerAddress != candidate.IssuerAddress ||
			current.Request != candidate.Request || current.Bundle.ClusterID != candidate.Bundle.ClusterID ||
			current.Bundle.CACertificatePEM != candidate.Bundle.CACertificatePEM {
			return Receipt{}, errors.New("another authenticated enrollment receipt already exists")
		}
		// Reaffirm a prior link visible after an uncertain publication sync.
		if err := syncReceipt(root); err != nil {
			return Receipt{}, err
		}
		return current, verifyReceiptRoot(root, store)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return Receipt{}, err
	}
	// A caller cannot substitute an arbitrary CSR for the one prepared with
	// the local durable private key. Existing bound receipts resume separately.
	prepared, _, err := loadPrepared(store, id)
	if err != nil || prepared != request {
		return Receipt{}, errors.Join(err, errors.New("receipt does not match locally prepared enrollment"))
	}
	encoded, err := json.Marshal(candidate)
	if err != nil || len(encoded)+1 > maxReceiptBytes {
		return Receipt{}, errors.New("enrollment receipt exceeds size limit")
	}
	encoded = append(encoded, '\n')
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return Receipt{}, err
	}
	temporary := ".dubi-join-receipt-" + hex.EncodeToString(nonce[:])
	file, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return Receipt{}, err
	}
	defer root.Remove(temporary) // This call owns only this exclusive temporary file.
	stage := func(name string) error {
		if hook != nil {
			return hook(name)
		}
		return nil
	}
	if _, err := file.Write(encoded); err != nil {
		return Receipt{}, errors.Join(err, file.Close())
	}
	if err := stage("after-write"); err != nil {
		return Receipt{}, errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return Receipt{}, errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return Receipt{}, err
	}
	if err := stage("after-file-sync"); err != nil {
		return Receipt{}, err
	}
	if err := verifyReceiptRoot(root, store); err != nil {
		return Receipt{}, err
	}
	if err := root.Link(temporary, JoinReceiptName); err != nil {
		return Receipt{}, err
	}
	if err := stage("after-link"); err != nil {
		return Receipt{}, err
	}
	if err := syncReceipt(root); err != nil {
		return Receipt{}, err
	}
	if err := stage("after-directory-sync"); err != nil {
		return Receipt{}, err
	}
	if err := verifyReceiptRoot(root, store); err != nil {
		return Receipt{}, err
	}
	return candidate, nil
}

// LoadReceipt validates identity and the original acceptance-time trust chain.
// An expired temporary grant does not erase an already authenticated response.
// Starting a server still requires currently valid managed Node credentials.
func LoadReceipt(store string) (Receipt, error) {
	root, id, err := receiptRoot(store)
	if err != nil {
		return Receipt{}, err
	}
	defer root.Close()
	receipt, err := loadReceipt(root, id)
	if err != nil {
		return Receipt{}, err
	}
	return receipt, verifyReceiptRoot(root, store)
}

// ResumeReceipt installs the saved leaf-only response without a live grant or
// network dependency. It never starts/bootstraps Root or changes Node IDs.
func ResumeReceipt(store string, now time.Time) (Receipt, nodepki.Material, error) {
	root, id, release, err := lockedReceiptRoot(store)
	if err != nil {
		return Receipt{}, nodepki.Material{}, err
	}
	defer release.Release()
	defer root.Close()
	receipt, err := loadReceipt(root, id)
	if err != nil {
		return Receipt{}, nodepki.Material{}, err
	}
	if err := syncReceipt(root); err != nil {
		return Receipt{}, nodepki.Material{}, err
	}
	if err := verifyReceiptRoot(root, store); err != nil {
		return Receipt{}, nodepki.Material{}, err
	}
	material, err := Install(store, receipt.Bundle, now)
	if err != nil {
		return Receipt{}, nodepki.Material{}, err
	}
	block, _ := pem.Decode([]byte(receipt.Bundle.CACertificatePEM))
	ca, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !bytes.Equal(ca.Raw, material.CACertificate.Raw) {
		return Receipt{}, nodepki.Material{}, errors.New("installed Node authority differs from saved receipt")
	}
	csr, err := validateRequest(receipt.Request)
	if err != nil || !bytes.Equal(csr.RawSubjectPublicKeyInfo, material.Certificate.RawSubjectPublicKeyInfo) {
		return Receipt{}, nodepki.Material{}, errors.New("installed Node key differs from saved receipt")
	}
	if err := verifyReceiptRoot(root, store); err != nil {
		return Receipt{}, nodepki.Material{}, err
	}
	return receipt, material, nil
}

func (r Receipt) validate(id identity.Identity) error {
	if r.Version != 1 || id.Origin != identity.OriginJoin || identity.ValidateJoinGrantID(r.GrantID) != nil ||
		ValidateIssuerAddress(r.IssuerAddress) != nil || r.AcceptedUnix <= 0 ||
		r.Request.NodeID != id.NodeID || r.Request.RaftID != id.RaftID || (id.ClusterID != "" && id.ClusterID != r.Bundle.ClusterID) {
		return errors.New("enrollment receipt does not match joining Node identity")
	}
	requestBytes, err := EncodeRequest(r.Request)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(requestBytes)
	if r.RequestSHA256 != hex.EncodeToString(digest[:]) {
		return errors.New("enrollment receipt request digest mismatch")
	}
	return validateNodeBundleForRequest(r.Bundle, r.Request, time.Unix(r.AcceptedUnix, 0))
}

func loadReceipt(root *os.Root, id identity.Identity) (Receipt, error) {
	info, err := root.Lstat(JoinReceiptName)
	if err != nil {
		return Receipt{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() <= 0 || info.Size() > maxReceiptBytes {
		return Receipt{}, errors.New("enrollment receipt must be a bounded regular 0600 file")
	}
	file, err := root.OpenFile(JoinReceiptName, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return Receipt{}, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return Receipt{}, errors.Join(err, file.Close(), errors.New("enrollment receipt changed while opening"))
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maxReceiptBytes+1))
	err = errors.Join(err, file.Close())
	if err != nil || len(encoded) > maxReceiptBytes {
		return Receipt{}, errors.Join(err, errors.New("cannot read bounded enrollment receipt"))
	}
	var receipt Receipt
	if err := decodeStrict(encoded, &receipt); err != nil {
		return Receipt{}, errors.New("invalid enrollment receipt document")
	}
	canonical, err := json.Marshal(receipt)
	if err != nil || !bytes.Equal(append(canonical, '\n'), encoded) {
		return Receipt{}, errors.New("noncanonical enrollment receipt")
	}
	if err := receipt.validate(id); err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

func receiptRoot(store string) (*os.Root, identity.Identity, error) {
	if strings.TrimSpace(store) == "" {
		return nil, identity.Identity{}, errors.New("enrollment Store path required")
	}
	absolute, err := filepath.Abs(store)
	if err != nil {
		return nil, identity.Identity{}, err
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.IsDir() {
		return nil, identity.Identity{}, errors.Join(err, errors.New("enrollment Store must be an existing directory, not a symlink"))
	}
	root, err := os.OpenRoot(absolute)
	if err != nil {
		return nil, identity.Identity{}, err
	}
	id, err := identity.Load(absolute)
	if err == nil {
		err = verifyReceiptRoot(root, absolute)
	}
	if err != nil {
		return nil, identity.Identity{}, errors.Join(err, root.Close())
	}
	return root, id, nil
}

func lockedReceiptRoot(store string) (*os.Root, identity.Identity, *storelock.Lock, error) {
	// Validate an existing managed Store before creating its process lock; an
	// invalid path must not initialize files in an unrelated directory.
	root, _, err := receiptRoot(store)
	if err != nil {
		return nil, identity.Identity{}, nil, err
	}
	lock, err := storelock.Acquire(store)
	if err != nil {
		return nil, identity.Identity{}, nil, errors.Join(err, root.Close())
	}
	id, err := identity.Load(store)
	if err == nil {
		err = verifyReceiptRoot(root, store)
	}
	if err != nil {
		return nil, identity.Identity{}, nil, errors.Join(err, lock.Release(), root.Close())
	}
	return root, id, lock, nil
}

func verifyReceiptRoot(root *os.Root, store string) error {
	held, err := root.Stat(".")
	if err != nil {
		return err
	}
	current, err := os.Lstat(store)
	if err != nil || !current.IsDir() || !os.SameFile(held, current) {
		return errors.Join(err, errors.New("enrollment Store directory was replaced"))
	}
	return nil
}

func syncReceipt(root *os.Root) error {
	file, err := root.OpenFile(JoinReceiptName, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	err = errors.Join(file.Sync(), file.Close())
	if err != nil {
		return err
	}
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
