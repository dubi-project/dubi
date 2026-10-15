package snapshot

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
)

const testNodeStoreID = "store_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func snapshotNodeRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	lock, err := storelock.Acquire(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lock.Release() })
	return root
}

func TestNodeSnapshotStoreAutomaticRegistrationAndReopen(t *testing.T) {
	root := snapshotNodeRoot(t)
	store, err := OpenNodeStore(root, testNodeStoreID)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ref, err := createTestArtifact(store)
	if err != nil {
		t.Fatal(err)
	}
	want := readTestArtifact(t, store, ref)
	registry, err := os.ReadFile(filepath.Join(root, nodeStoreRegistry))
	if err != nil {
		t.Fatal(err)
	}
	if other, err := OpenNodeStore(root, testNodeStoreID); err == nil {
		other.Close()
		t.Fatal("concurrent artifact Store accepted")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenNodeStore(root, testNodeStoreID)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if got := readTestArtifact(t, store, ref); !bytes.Equal(got, want) {
		t.Fatal("artifact changed during registration recovery")
	}
	again, err := os.ReadFile(filepath.Join(root, nodeStoreRegistry))
	if err != nil || !bytes.Equal(registry, again) {
		t.Fatal("reopen regenerated resource identity", err)
	}
}

func TestNodeSnapshotStoreRejectsMissingReplacedAndForeignResources(t *testing.T) {
	for _, damage := range []string{"directory-missing", "directory-replaced", "directory-symlink", "registry-missing", "owner-missing", "owner-corrupt", "wrong-store", "wrong-node"} {
		t.Run(damage, func(t *testing.T) {
			root := snapshotNodeRoot(t)
			store, err := OpenNodeStore(root, testNodeStoreID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := createTestArtifact(store); err != nil {
				t.Fatal(err)
			}
			store.Close()
			directory := filepath.Join(root, nodeStoreDirectory)
			storeID := testNodeStoreID
			switch damage {
			case "directory-missing", "directory-replaced", "directory-symlink":
				if err := os.Rename(directory, directory+".retained"); err != nil {
					t.Fatal(err)
				}
				if damage == "directory-replaced" {
					err = os.Mkdir(directory, 0o750)
				} else if damage == "directory-symlink" {
					err = os.Symlink(directory+".retained", directory)
				}
			case "registry-missing":
				err = os.Rename(filepath.Join(root, nodeStoreRegistry), filepath.Join(root, nodeStoreRegistry+".retained"))
			case "owner-missing":
				err = os.Rename(filepath.Join(directory, nodeStoreOwner), filepath.Join(directory, nodeStoreOwner+".retained"))
			case "owner-corrupt":
				err = os.WriteFile(filepath.Join(directory, nodeStoreOwner), []byte("{}"), 0o600)
			case "wrong-store":
				storeID = "store_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			case "wrong-node":
				other := snapshotNodeRoot(t)
				metadata, readErr := os.ReadFile(filepath.Join(root, nodeStoreRegistry))
				if readErr != nil {
					t.Fatal(readErr)
				}
				err = os.WriteFile(filepath.Join(other, nodeStoreRegistry), metadata, 0o600)
				root = other
			}
			if err != nil {
				t.Fatal(err)
			}
			if reopened, err := OpenNodeStore(root, storeID); err == nil {
				reopened.Close()
				t.Fatal("lost/foreign snapshot resource silently adopted")
			}
			if damage == "directory-missing" {
				if _, err := os.Lstat(directory); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("missing registered directory recreated", err)
				}
			}
		})
	}
}

func TestNodeSnapshotStoreChecksOwnershipWhileOpen(t *testing.T) {
	root := snapshotNodeRoot(t)
	store, err := OpenNodeStore(root, testNodeStoreID)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ref, err := createTestArtifact(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, nodeStoreRegistry), filepath.Join(root, nodeStoreRegistry+".retained")); err != nil {
		t.Fatal(err)
	}
	if reader, err := store.Open(ref); err == nil {
		reader.Close()
		t.Fatal("opened artifact after resource registration disappeared")
	}
	if _, err := createTestArtifact(store); err == nil {
		t.Fatal("published artifact after resource registration disappeared")
	}
}

func TestNodeSnapshotStorePendingIntentRejectsDirectorySubstitution(t *testing.T) {
	root := snapshotNodeRoot(t)
	_, err := openNodeStore(root, testNodeStoreID, func(stage string) error {
		if stage == "after-intent" {
			return errors.New("stop after intent")
		}
		return nil
	})
	if err == nil {
		t.Fatal("fault not reached")
	}
	directory := filepath.Join(root, nodeStoreDirectory)
	if err := os.Rename(directory, directory+".retained"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(directory, 0o750); err != nil {
		t.Fatal(err)
	}
	if store, err := OpenNodeStore(root, testNodeStoreID); err == nil {
		store.Close()
		t.Fatal("pending registration adopted replacement directory")
	}
	if _, err := os.Lstat(filepath.Join(directory, nodeStoreOwner)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("wrote owner into substituted directory", err)
	}
}

func TestNodeSnapshotStoreCrashHelper(t *testing.T) {
	root := os.Getenv("DUBI_SNAPSHOT_REGISTRATION_ROOT")
	if root == "" {
		return
	}
	lock, err := storelock.Acquire(root)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	_, err = openNodeStore(root, testNodeStoreID, func(stage string) error {
		if stage == os.Getenv("DUBI_SNAPSHOT_REGISTRATION_POINT") {
			os.Exit(74)
		}
		return nil
	})
	t.Fatal("did not reach registration crash point", err)
}

func TestNodeSnapshotStoreProcessCrashRegistration(t *testing.T) {
	for _, point := range []string{"after-directory", "after-intent", "after-owner", "after-active"} {
		t.Run(point, func(t *testing.T) {
			root := t.TempDir()
			if _, err := identity.Bootstrap(root); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNodeSnapshotStoreCrashHelper$")
			child.Env = append(os.Environ(), "DUBI_SNAPSHOT_REGISTRATION_ROOT="+root, "DUBI_SNAPSHOT_REGISTRATION_POINT="+point)
			output, err := child.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 74 {
				t.Fatalf("registration crash: %v\n%s", err, output)
			}
			lock, err := storelock.Acquire(root)
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Release()
			before, beforeErr := os.ReadFile(filepath.Join(root, nodeStoreRegistry))
			store, err := OpenNodeStore(root, testNodeStoreID)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			ref, err := createTestArtifact(store)
			if err != nil {
				t.Fatal(err)
			}
			readTestArtifact(t, store, ref)
			parent, err := os.OpenRoot(root)
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Close()
			var registration nodeRegistration
			if err := readOwnerJSON(parent, nodeStoreRegistry, &registration); err != nil || registration.State != "active" {
				t.Fatal("registration did not reach active", err)
			}
			if beforeErr == nil {
				// The persisted intent's identity survives all recovery phases.
				if !bytes.Contains(before, []byte(registration.Owner.ArtifactID)) {
					t.Fatal("crash recovery regenerated persisted resource identity")
				}
			} else if point != "after-directory" {
				t.Fatal("crash lost previously durable intent", beforeErr)
			}
		})
	}
}
