package enrollment

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
)

func receiptFixture(t *testing.T) (string, Ticket, Request, Bundle, time.Time) {
	t.Helper()
	_, ticket, signer, now := ticketFixture(t)
	store := t.TempDir()
	id, request, err := Prepare(store, nodepki.DefaultServerName, "local", "127.0.0.1:7444")
	if err != nil {
		t.Fatal(err)
	}
	id.ClusterID = ticket.ClusterID()
	cert, err := signer.IssueNode(id, request.ServerName, []byte(request.CSRPEM), now)
	if err != nil {
		t.Fatal(err)
	}
	bundle := Bundle{Version: 1, ClusterID: ticket.ClusterID(), NodeID: id.NodeID, RaftID: id.RaftID, ServerName: request.ServerName,
		CACertificatePEM: ticket.document.CACertificatePEM, CertificatePEM: string(cert)}
	return store, ticket, request, bundle, now
}

func TestJoinReceiptResumesLeafInstallWithoutLiveGrant(t *testing.T) {
	store, ticket, request, bundle, now := receiptFixture(t)
	original, err := identity.Load(store)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := SaveReceipt(store, ticket, request, bundle, now)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		retry, err := SaveReceipt(store, ticket, request, bundle, now.Add(time.Minute))
		if err != nil || retry != saved {
			t.Fatal("receipt was replaced on retry", err)
		}
	}
	loaded, err := LoadReceipt(store)
	if err != nil || loaded != saved {
		t.Fatal("receipt changed on reopen", err)
	}
	before, err := identity.Load(store)
	if err != nil || before != original {
		t.Fatal("receipt prematurely bound identity", err)
	}
	encoded, err := os.ReadFile(filepath.Join(store, JoinReceiptName))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("PRIVATE KEY")) || bytes.Contains(encoded, []byte(`"secret"`)) || bytes.Contains(encoded, []byte(joinTicketPrefix)) {
		t.Fatal("receipt persisted private grant material")
	}
	for range 2 {
		resumed, material, err := ResumeReceipt(store, now.Add(time.Hour))
		if err != nil || resumed != saved {
			t.Fatal("expired ticket blocked durable receipt", err)
		}
		bound, err := identity.Load(store)
		if err != nil || bound.NodeID != original.NodeID || bound.RaftID != original.RaftID || bound.ClusterID != ticket.ClusterID() || bound.CreatedAt != original.CreatedAt {
			t.Fatal("resume changed Node identity", err)
		}
		for _, path := range []string{material.Paths.WrappingKey, material.Paths.Envelope, StorePaths(store).Directory, filepath.Join(store, "dubi-raft-groups")} {
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("receipt install created signer/Root or retained staging", path, err)
			}
		}
	}
	if _, _, err := ResumeReceipt(store, now.Add(100*24*time.Hour)); err == nil {
		t.Fatal("expired Node credential installed")
	}
	if _, err := LoadReceipt(store); err != nil {
		t.Fatal("historical receipt lost because current Node certificate expired", err)
	}
}

func TestJoinReceiptRejectsReplacementCorruptionAndConcurrentStore(t *testing.T) {
	for _, damage := range []string{"symlink", "permissions", "truncated", "duplicate-field", "request-address", "foreign-node", "locked-store", "replaced-root"} {
		t.Run(damage, func(t *testing.T) {
			store, ticket, request, bundle, now := receiptFixture(t)
			if damage == "locked-store" {
				lock, err := storelock.Acquire(store)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Release()
				if _, err := SaveReceipt(store, ticket, request, bundle, now); err == nil {
					t.Fatal("concurrent Store writer accepted")
				}
				return
			}
			if damage == "replaced-root" {
				moved := store + "-original"
				t.Cleanup(func() {
					if err := os.Remove(store); err != nil {
						t.Error(err)
					}
					if err := os.Rename(moved, store); err != nil {
						t.Error(err)
					}
				})
				_, err := saveReceipt(store, ticket, request, bundle, now, func(stage string) error {
					if stage != "after-file-sync" {
						return nil
					}
					if err := os.Rename(store, moved); err != nil {
						return err
					}
					return os.Mkdir(store, 0o700)
				})
				if err == nil {
					t.Fatal("replaced Store accepted publication")
				}
				if _, err := os.Stat(filepath.Join(store, JoinReceiptName)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("replacement Store was written", err)
				}
				return
			}
			if _, err := SaveReceipt(store, ticket, request, bundle, now); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(store, JoinReceiptName)
			encoded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "symlink":
				other := filepath.Join(t.TempDir(), "receipt")
				if err := os.Rename(path, other); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(other, path); err != nil {
					t.Fatal(err)
				}
			case "permissions":
				if err := os.Chmod(path, 0o644); err != nil {
					t.Fatal(err)
				}
			case "truncated":
				if err := os.WriteFile(path, encoded[:len(encoded)/2], 0o600); err != nil {
					t.Fatal(err)
				}
			case "duplicate-field":
				if err := os.WriteFile(path, append([]byte(`{"version":1,`), encoded[1:]...), 0o600); err != nil {
					t.Fatal(err)
				}
			case "request-address":
				var receipt Receipt
				if err := json.Unmarshal(encoded, &receipt); err != nil {
					t.Fatal(err)
				}
				receipt.Request.Address = "127.0.0.1:9555"
				mutated, err := json.Marshal(receipt)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, append(mutated, '\n'), 0o600); err != nil {
					t.Fatal(err)
				}
			case "foreign-node":
				other := t.TempDir()
				if _, _, err := Prepare(other, nodepki.DefaultServerName, "local", request.Address); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(other, JoinReceiptName), encoded, 0o600); err != nil {
					t.Fatal(err)
				}
				store = other
			}
			if _, err := LoadReceipt(store); err == nil {
				t.Fatal("damaged receipt loaded")
			}
			if _, _, err := ResumeReceipt(store, now); err == nil {
				t.Fatal("damaged receipt installed")
			}
			value, err := identity.Load(store)
			if err != nil || value.ClusterID != "" {
				t.Fatal("damaged receipt bound Node", err)
			}
		})
	}
}

func TestJoinReceiptInterruptedPublicationRetry(t *testing.T) {
	for _, phase := range []string{"after-write", "after-file-sync", "after-link", "after-directory-sync"} {
		t.Run(phase, func(t *testing.T) {
			store, ticket, request, bundle, now := receiptFixture(t)
			injected := errors.New("injected receipt publication failure")
			if _, err := saveReceipt(store, ticket, request, bundle, now, func(stage string) error {
				if phase == stage {
					return injected
				}
				return nil
			}); !errors.Is(err, injected) {
				t.Fatal("failure not surfaced", err)
			}
			if _, err := SaveReceipt(store, ticket, request, bundle, now); err != nil {
				t.Fatal("exact retry failed", err)
			}
			if _, _, err := ResumeReceipt(store, now.Add(time.Hour)); err != nil {
				t.Fatal("receipt did not resume", err)
			}
		})
	}
}

func TestJoinReceiptInterruptedLeafInstallation(t *testing.T) {
	for _, phase := range []string{"bound-before-pki", "pki-before-staging-cleanup"} {
		t.Run(phase, func(t *testing.T) {
			store, ticket, request, bundle, now := receiptFixture(t)
			if _, err := SaveReceipt(store, ticket, request, bundle, now); err != nil {
				t.Fatal(err)
			}
			id, err := identity.Load(store)
			if err != nil {
				t.Fatal(err)
			}
			if phase == "bound-before-pki" {
				if _, err := identity.BindCluster(store, bundle.ClusterID); err != nil {
					t.Fatal(err)
				}
			} else {
				_, private, err := loadPrepared(store, id)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := nodepki.InstallLeaf(store, id, bundle.ClusterID, bundle.ServerName,
					[]byte(bundle.CACertificatePEM), []byte(bundle.CertificatePEM), private, now); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := ResumeReceipt(store, now.Add(time.Hour)); err != nil {
				t.Fatal("interrupted leaf installation did not resume", err)
			}
			bound, err := identity.Load(store)
			if err != nil || bound.NodeID != id.NodeID || bound.RaftID != id.RaftID || bound.ClusterID != bundle.ClusterID {
				t.Fatal("interrupted installation changed identity", err)
			}
			if _, err := os.Stat(StorePaths(store).Directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("completed staging retained", err)
			}
		})
	}
}

func TestJoinReceiptCannotSubstitutePreparedRequestOrExistingReceipt(t *testing.T) {
	store, ticket, request, bundle, now := receiptFixture(t)
	changed := request
	changed.Address = "127.0.0.1:9444"
	if _, err := SaveReceipt(store, ticket, changed, bundle, now); err == nil {
		t.Fatal("substituted prepared metadata accepted")
	}
	if _, err := LoadReceipt(store); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid request published receipt", err)
	}
	if _, err := SaveReceipt(store, ticket, request, bundle, now); err != nil {
		t.Fatal(err)
	}
	if _, err := SaveReceipt(store, ticket, changed, bundle, now); err == nil {
		t.Fatal("existing receipt replaced")
	}
	loaded, err := LoadReceipt(store)
	if err != nil || loaded.Request != request {
		t.Fatal("original receipt altered", err)
	}
}

func TestJoinReceiptDoesNotInitializeUnmanagedOrSymlinkStore(t *testing.T) {
	store, ticket, request, bundle, now := receiptFixture(t)
	for _, mode := range []string{"unmanaged", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			path := t.TempDir()
			if mode == "symlink" {
				path = filepath.Join(path, "linked-store")
				if err := os.Symlink(store, path); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := SaveReceipt(path, ticket, request, bundle, now); err == nil {
				t.Fatal("invalid Store adopted")
			}
			if _, _, err := ResumeReceipt(path, now); err == nil {
				t.Fatal("invalid Store resumed")
			}
			if _, err := os.Stat(filepath.Join(path, storelock.Filename)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid path created lock file", err)
			}
		})
	}
}

type receiptCrashInput struct {
	Store, Ticket string
	Request       Request
	Bundle        Bundle
	Now           time.Time
}

func TestJoinReceiptProcessCrashPublication(t *testing.T) {
	if inputPath := os.Getenv("DUBI_RECEIPT_TEST_INPUT"); inputPath != "" {
		encoded, err := os.ReadFile(inputPath)
		if err != nil {
			t.Fatal(err)
		}
		var input receiptCrashInput
		if err := json.Unmarshal(encoded, &input); err != nil {
			t.Fatal(err)
		}
		ticket, err := DecodeTicket(input.Ticket, input.Now)
		if err != nil {
			t.Fatal(err)
		}
		_, err = saveReceipt(input.Store, ticket, input.Request, input.Bundle, input.Now, func(stage string) error {
			if stage == os.Getenv("DUBI_RECEIPT_TEST_STAGE") {
				os.Exit(76)
			}
			return nil
		})
		t.Fatal("crash stage not reached", err)
	}
	for _, stage := range []string{"after-write", "after-file-sync", "after-link", "after-directory-sync"} {
		t.Run(stage, func(t *testing.T) {
			store, ticket, request, bundle, now := receiptFixture(t)
			encodedTicket, err := EncodeTicket(ticket, now)
			if err != nil {
				t.Fatal(err)
			}
			input, err := json.Marshal(receiptCrashInput{Store: store, Ticket: encodedTicket, Request: request, Bundle: bundle, Now: now})
			if err != nil {
				t.Fatal(err)
			}
			inputPath := filepath.Join(t.TempDir(), "test-input.json")
			if err := os.WriteFile(inputPath, input, 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestJoinReceiptProcessCrashPublication$")
			command.Env = append(os.Environ(), "DUBI_RECEIPT_TEST_INPUT="+inputPath, "DUBI_RECEIPT_TEST_STAGE="+stage)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 76 {
				t.Fatalf("child failed outside injected crash: %v\n%s", err, output)
			}
			if _, err := SaveReceipt(store, ticket, request, bundle, now); err != nil {
				t.Fatal("crash retry failed", err)
			}
			if _, _, err := ResumeReceipt(store, now.Add(time.Hour)); err != nil {
				t.Fatal("crash receipt install failed", err)
			}
		})
	}
}
