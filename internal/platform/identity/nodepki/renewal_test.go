package nodepki

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"golang.org/x/sys/unix"
)

type renewalFixture struct {
	store    string
	id       identity.Identity
	original Material
	signer   *Signer
	now      time.Time
	leaf     []byte
}

func newRenewalFixture(t *testing.T) renewalFixture {
	t.Helper()
	store := t.TempDir()
	id := mustBootstrap(t, store)
	issued := time.Now().UTC().Truncate(time.Second)
	material, err := Ensure(store, id, DefaultServerName, issued)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := OpenSigner(store, id, issued)
	if err != nil {
		t.Fatal(err)
	}
	f := renewalFixture{store: store, id: id, original: material, signer: signer, now: issued.Add(60 * 24 * time.Hour)}
	f.leaf = f.issue(t, material.Certificate.PublicKey.(ed25519.PublicKey), f.now)
	return f
}

func (f renewalFixture) issue(t *testing.T, public ed25519.PublicKey, now time.Time) []byte {
	t.Helper()
	der, err := createLeaf(f.id, DefaultServerName, now, public, f.signer.ca, f.signer.privateKey.(ed25519.PrivateKey))
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestLeafRenewalOnlyReplacesCertificate(t *testing.T) {
	f := newRenewalFixture(t)
	protected := map[string][]byte{}
	for _, path := range []string{f.original.Paths.PrivateKey, f.original.Paths.CACertificate, f.original.Paths.Config, f.original.Paths.WrappingKey, f.original.Paths.Envelope} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		protected[path] = data
	}
	if NeedsRenewal(f.original.Certificate, f.now.Add(-time.Second)) || !NeedsRenewal(f.original.Certificate, f.now) || !NeedsRenewal(nil, f.now) {
		t.Fatal("renewal threshold")
	}
	updated, err := InstallRenewedLeaf(f.store, f.id, DefaultServerName, f.leaf, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.Certificate.NotAfter.After(f.original.Certificate.NotAfter) || NeedsRenewal(updated.Certificate, f.now) {
		t.Fatal("leaf not extended")
	}
	for path, want := range protected {
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("changed protected file %s: %v", path, err)
		}
	}
	for i := 0; i < 2; i++ {
		retry, err := InstallRenewedLeaf(f.store, f.id, DefaultServerName, f.leaf, f.now)
		if err != nil || !bytes.Equal(retry.Certificate.Raw, updated.Certificate.Raw) {
			t.Fatal("retry", err)
		}
	}
	if _, err := OpenSigner(f.store, f.id, f.now); err != nil {
		t.Fatal("signer no longer readable", err)
	}
	if _, err := InstallRenewedLeaf(f.store, f.id, DefaultServerName, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.original.Certificate.Raw}), f.now); err == nil {
		t.Fatal("rolled back renewed leaf")
	}
}

func TestLeafRenewalWithSigner(t *testing.T) {
	f := newRenewalFixture(t)
	// A reissued CA with the same key is still a trust-root change, not ordinary
	// leaf renewal. Cryptographic signature verification alone cannot detect it.
	private := f.signer.privateKey.(ed25519.PrivateKey)
	der, err := createCA(f.id, f.now, private.Public().(ed25519.PublicKey), private)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	reissued := &Signer{ca: ca, privateKey: private, clusterID: f.id.ClusterID}
	for _, signer := range []*Signer{nil, newRenewalFixture(t).signer, reissued} {
		if _, err := RenewWithSigner(f.store, f.id, DefaultServerName, signer, f.now); err == nil {
			t.Fatal("accepted absent or foreign signer")
		}
	}
	updated, err := RenewWithSigner(f.store, f.id, DefaultServerName, f.signer, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(updated.CACertificate.Raw, f.original.CACertificate.Raw) ||
		!bytes.Equal(updated.Certificate.RawSubjectPublicKeyInfo, f.original.Certificate.RawSubjectPublicKeyInfo) ||
		!updated.Certificate.NotAfter.After(f.original.Certificate.NotAfter) {
		t.Fatal("renewal changed key/CA or did not extend validity")
	}
	oldPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.original.Certificate.Raw})
	if _, err := f.signer.RenewNode(f.id, DefaultServerName, oldPEM, f.original.Certificate.NotAfter); err == nil {
		t.Fatal("renewed expired certificate")
	}
	if _, err := RenewWithSigner(f.store, f.id, DefaultServerName, f.signer, f.now); err == nil {
		t.Fatal("reissued without extending validity")
	}
}

func TestLeafRenewalRejectsInvalidReplacement(t *testing.T) {
	for _, change := range []string{"key", "ca", "identity", "future", "expired", "same-expiry", "malformed", "oversize", "expired-current"} {
		t.Run(change, func(t *testing.T) {
			f := newRenewalFixture(t)
			candidate, now := f.leaf, f.now
			switch change {
			case "key":
				public, _, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				candidate = f.issue(t, public, now)
			case "ca":
				other := newRenewalFixture(t)
				f.signer = other.signer
				candidate = f.issue(t, f.original.Certificate.PublicKey.(ed25519.PublicKey), now)
			case "identity":
				f.id.RaftID++
			case "future":
				candidate = f.issue(t, f.original.Certificate.PublicKey.(ed25519.PublicKey), now.Add(time.Hour))
			case "expired":
				candidate = f.issue(t, f.original.Certificate.PublicKey.(ed25519.PublicKey), now.Add(-100*24*time.Hour))
			case "same-expiry":
				candidate = f.issue(t, f.original.Certificate.PublicKey.(ed25519.PublicKey), now.Add(-60*24*time.Hour))
			case "malformed":
				candidate = []byte("not a certificate")
			case "oversize":
				candidate = make([]byte, (1<<20)+1)
			case "expired-current":
				now = f.original.Certificate.NotAfter
			}
			if _, err := InstallRenewedLeaf(f.store, f.id, DefaultServerName, candidate, now); err == nil {
				t.Fatal("accepted", change)
			}
			encoded, err := os.ReadFile(f.original.Paths.Certificate)
			if err != nil {
				t.Fatal(err)
			}
			leaf, err := parseCertificate(encoded)
			if err != nil || !bytes.Equal(leaf.Raw, f.original.Certificate.Raw) {
				t.Fatal("changed original", err)
			}
		})
	}
}

func TestLeafRenewalPublicationFailuresRecover(t *testing.T) {
	for _, phase := range []string{"staged", "published", "synced"} {
		t.Run(phase, func(t *testing.T) {
			f := newRenewalFixture(t)
			injected := errors.New("injected renewal publication failure")
			_, err := installRenewedLeaf(f.store, f.id, DefaultServerName, f.leaf, f.now, func(at string) error {
				if at == phase {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) {
				t.Fatal(err)
			}
			current, err := Load(f.store, f.id, DefaultServerName, f.now)
			if err != nil {
				t.Fatal(err)
			}
			old := bytes.Equal(current.Certificate.Raw, f.original.Certificate.Raw)
			if old != (phase == "staged") {
				t.Fatal("unexpected publication boundary")
			}
			if _, err := InstallRenewedLeaf(f.store, f.id, DefaultServerName, f.leaf, f.now); err != nil {
				t.Fatal("retry", err)
			}
		})
	}
}

func TestLeafRenewalProcessCrash(t *testing.T) {
	if store := os.Getenv("DUBI_TEST_RENEWAL_STORE"); store != "" {
		id, err := identity.Load(store)
		if err != nil {
			t.Fatal(err)
		}
		leaf, err := os.ReadFile(os.Getenv("DUBI_TEST_RENEWAL_INPUT"))
		if err != nil {
			t.Fatal(err)
		}
		now, err := time.Parse(time.RFC3339, os.Getenv("DUBI_TEST_RENEWAL_TIME"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = installRenewedLeaf(store, id, DefaultServerName, leaf, now, func(phase string) error {
			if phase == os.Getenv("DUBI_TEST_RENEWAL_PHASE") {
				os.Exit(91)
			}
			return nil
		})
		t.Fatalf("crash hook not reached: %v", err)
	}
	for _, phase := range []string{"staged", "published", "synced"} {
		t.Run(phase, func(t *testing.T) {
			f := newRenewalFixture(t)
			input := filepath.Join(t.TempDir(), "renewed.crt")
			if err := os.WriteFile(input, f.leaf, 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestLeafRenewalProcessCrash$")
			cmd.Env = append(os.Environ(), "DUBI_TEST_RENEWAL_STORE="+f.store, "DUBI_TEST_RENEWAL_INPUT="+input, "DUBI_TEST_RENEWAL_TIME="+f.now.Format(time.RFC3339), "DUBI_TEST_RENEWAL_PHASE="+phase)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 91 {
				t.Fatalf("crash failed: %v %s", err, output)
			}
			current, err := Load(f.store, f.id, DefaultServerName, f.now)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(current.Certificate.Raw, f.original.Certificate.Raw) != (phase == "staged") {
				t.Fatal("unexpected crash state")
			}
			if _, err := InstallRenewedLeaf(f.store, f.id, DefaultServerName, f.leaf, f.now); err != nil {
				t.Fatal("crash retry", err)
			}
		})
	}
}

func TestLeafRenewalSerializesWriters(t *testing.T) {
	f := newRenewalFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := installRenewedLeaf(f.store, f.id, DefaultServerName, f.leaf, f.now, func(phase string) error {
			if phase == "staged" {
				close(entered)
				<-release
			}
			return nil
		})
		done <- err
	}()
	<-entered
	_, err := InstallRenewedLeaf(f.store, f.id, DefaultServerName, f.leaf, f.now)
	close(release)
	if err == nil {
		t.Fatal("concurrent writer admitted")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestLeafRenewalRejectsReplacedDirectories(t *testing.T) {
	for _, target := range []string{"store", "pki"} {
		t.Run(target, func(t *testing.T) {
			f := newRenewalFixture(t)
			path := f.store
			if target == "pki" {
				path = f.original.Paths.Directory
			}
			moved := path + "-moved"
			t.Cleanup(func() { _ = os.Remove(path); _ = os.Rename(moved, path) })
			_, err := installRenewedLeaf(f.store, f.id, DefaultServerName, f.leaf, f.now, func(phase string) error {
				if phase != "staged" {
					return nil
				}
				if err := os.Rename(path, moved); err != nil {
					return err
				}
				return os.Mkdir(path, 0o700)
			})
			if err == nil {
				t.Fatal("accepted replaced directory")
			}
			entries, err := os.ReadDir(path)
			if err != nil || len(entries) != 0 {
				t.Fatal("wrote into replacement", err)
			}
		})
	}
}

func TestNodePKIReadRejectsNonregularAndLargeFiles(t *testing.T) {
	for _, kind := range []string{"symlink", "fifo", "directory", "large", "empty"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "node.crt")
			switch kind {
			case "symlink":
				if err := os.Symlink("missing", path); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := unix.Mkfifo(path, 0o644); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0o644); err != nil {
					t.Fatal(err)
				}
			default:
				file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "large" {
					if err := file.Truncate(1 << 30); err != nil {
						t.Fatal(err)
					}
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := readFile(path, 0o644); err == nil {
				t.Fatal("accepted", kind)
			}
		})
	}
}
