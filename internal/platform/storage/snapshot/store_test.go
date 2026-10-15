package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openTestStore(t *testing.T, root string) *Store {
	t.Helper()
	store, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func createTestArtifact(store *Store) (Reference, error) {
	return store.Create(context.Background(), streamHeader(), 1<<20, func(encoder *Encoder) error {
		return encoder.Add([]byte("item"), []byte("value"))
	})
}

func readTestArtifact(t *testing.T, store *Store, ref Reference) []byte {
	t.Helper()
	reader, err := store.Open(ref)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err := errors.Join(err, reader.Close()); err != nil {
		t.Fatal(err)
	}
	return data
}

func TestStorePublicationReopenImportAndDeduplication(t *testing.T) {
	root := t.TempDir()
	store := openTestStore(t, root)
	if other, err := OpenStore(root); err == nil {
		other.Close()
		t.Fatal("competing Store accepted")
	}
	ref, err := createTestArtifact(store)
	if err != nil {
		t.Fatal(err)
	}
	data := readTestArtifact(t, store, ref)
	if err := consumeStream(data); err != nil {
		t.Fatal(err)
	}
	again, err := createTestArtifact(store)
	if err != nil || again != ref {
		t.Fatal("immutable publication is not idempotent", again, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, root)
	if got := readTestArtifact(t, store, ref); !bytes.Equal(got, data) {
		t.Fatal("artifact changed after reopen")
	}
	destination := openTestStore(t, t.TempDir())
	for range 2 {
		if err := destination.Import(context.Background(), ref, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	if got := readTestArtifact(t, destination, ref); !bytes.Equal(got, data) {
		t.Fatal("import differs")
	}
	entries, err := os.ReadDir(destination.path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatal("expected one artifact and lock, no pending files", entries)
	}
}

func TestStoreWriterDoesNotBlockImmutableReadsOrCanceledWaiters(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	ref, err := createTestArtifact(store)
	if err != nil {
		t.Fatal(err)
	}
	entered, finished := make(chan struct{}), make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, err := store.Create(ctx, streamHeader(), 1<<20, func(*Encoder) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		})
		finished <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not start")
	}
	read := make(chan error, 1)
	go func() {
		r, err := store.Open(ref)
		if err == nil {
			_, err = io.Copy(io.Discard, r)
			err = errors.Join(err, r.Close())
		}
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		cancel()
		<-finished
		t.Fatal("active writer blocked immutable artifact read")
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	queued := make(chan error, 1)
	go func() {
		_, err := store.Create(canceled, streamHeader(), 1<<20, func(*Encoder) error {
			return errors.New("canceled callback executed")
		})
		queued <- err
	}()
	select {
	case err := <-queued:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		cancel()
		<-finished
		t.Fatal("queued writer ignored cancellation")
	}
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestStoreRejectsBadImportAndNeverOverwritesExistingArtifact(t *testing.T) {
	source := openTestStore(t, t.TempDir())
	ref, err := createTestArtifact(source)
	if err != nil {
		t.Fatal(err)
	}
	data := readTestArtifact(t, source, ref)
	for _, damage := range []string{"truncated", "trailing", "digest", "body", "group", "index", "canceled"} {
		t.Run(damage, func(t *testing.T) {
			copy := bytes.Clone(data)
			changed := ref
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch damage {
			case "truncated":
				copy = copy[:len(copy)-1]
			case "trailing":
				copy = append(copy, 0)
			case "digest":
				changed.SHA256 = strings.Repeat("0", 64)
			case "body":
				copy[len(copy)-14] ^= 1
			case "group":
				changed.GroupID = "other"
			case "index":
				changed.Index++
			case "canceled":
				cancel()
			}
			destination := openTestStore(t, t.TempDir())
			if err := destination.Import(ctx, changed, bytes.NewReader(copy)); err == nil {
				t.Fatal("invalid import accepted")
			}
			entries, err := os.ReadDir(destination.path)
			if err != nil || len(entries) != 1 {
				t.Fatal("invalid import published or retained its pending file", entries, err)
			}
		})
	}
	destination := openTestStore(t, t.TempDir())
	path := filepath.Join(destination.path, ref.filename())
	bad := bytes.Clone(data)
	bad[len(bad)-14] ^= 1
	if err := os.WriteFile(path, bad, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := destination.Import(context.Background(), ref, bytes.NewReader(data)); err == nil {
		t.Fatal("silently overwrote damaged artifact")
	}
	retained, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(retained, bad) {
		t.Fatal("existing artifact was overwritten", err)
	}
}

func TestStoreRejectsChangedPathsAndIncompleteReads(t *testing.T) {
	root := t.TempDir()
	store := openTestStore(t, root)
	ref, err := createTestArtifact(store)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Open(ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err == nil {
		t.Fatal("partial read claimed verified")
	}
	for _, damage := range []string{"group", "index", "size", "digest"} {
		changed := ref
		switch damage {
		case "group":
			changed.GroupID = "other"
		case "index":
			changed.Index++
		case "size":
			changed.Size++
		case "digest":
			changed.SHA256 = "../escape"
		}
		if reader, err := store.Open(changed); err == nil {
			reader.Close()
			t.Fatal("invalid reference opened", damage)
		}
	}
	// Reads hold the original immutable file, never a later replacement path.
	data := readTestArtifact(t, store, ref)
	artifact := filepath.Join(root, ref.filename())
	if err := os.Rename(artifact, filepath.Join(root, "saved")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("saved", artifact); err != nil {
		t.Fatal(err)
	}
	if reader, err := store.Open(ref); err == nil {
		reader.Close()
		t.Fatal("artifact symlink accepted")
	}
	if err := store.Import(context.Background(), ref, bytes.NewReader(data)); err == nil {
		t.Fatal("import replaced artifact symlink")
	}
	// Move only this test-created directory, retaining everything for cleanup.
	moved := root + "-moved"
	if err := os.Rename(root, moved); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Rename(moved, root) })
	if _, err := createTestArtifact(store); err == nil {
		t.Fatal("wrote after Store path disappeared")
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing Store was recreated")
	}
}

func TestReferenceCanonicalEncodingAndNoPaths(t *testing.T) {
	ref := Reference{Version: StreamVersion, GroupID: "root", Index: 10, Size: 100, SHA256: strings.Repeat("a", 64)}
	encoded, err := ref.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, recognized, err := DecodeReference(encoded)
	if err != nil || !recognized || got != ref {
		t.Fatal(got, recognized, err)
	}
	if _, recognized, err := DecodeReference([]byte("legacy inline")); recognized || err != nil {
		t.Fatal("legacy input misclassified")
	}
	for _, encoded := range [][]byte{
		append(bytes.Clone(encoded), ' '),
		bytes.Replace(encoded, []byte(`"index":10`), []byte(`"index":10,"index":10`), 1),
		bytes.Replace(encoded, []byte(`"index":10`), []byte(`"path":"/tmp/file","index":10`), 1),
		[]byte(referencePrefix + strings.Repeat("x", MaxReferenceBytes)),
	} {
		if _, recognized, err := DecodeReference(encoded); !recognized || err == nil {
			t.Fatal("bad reference accepted or fell back to inline")
		}
	}
}

func TestStorePublicationFailureRetainsOnlyUnreferencedArtifacts(t *testing.T) {
	for _, stage := range []string{"after-file-sync", "after-link", "after-directory-sync"} {
		t.Run(stage, func(t *testing.T) {
			store := openTestStore(t, t.TempDir())
			injected := errors.New("injected durability failure")
			store.hook = func(current string) error {
				if current == stage {
					return injected
				}
				return nil
			}
			ref, err := createTestArtifact(store)
			if !errors.Is(err, injected) || ref != (Reference{}) {
				t.Fatal("failed publication returned reference", ref, err)
			}
			store.hook = nil
			ref, err = createTestArtifact(store)
			if err != nil {
				t.Fatal("retry after unpublished artifact failed", err)
			}
			_ = readTestArtifact(t, store, ref)
		})
	}
}

func TestSnapshotStoreCrashHelper(t *testing.T) {
	root := os.Getenv("DUBI_SNAPSHOT_CRASH_ROOT")
	if root == "" {
		return
	}
	store, err := OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	store.hook = func(stage string) error {
		if stage == os.Getenv("DUBI_SNAPSHOT_CRASH_STAGE") {
			os.Exit(71)
		}
		return nil
	}
	if _, err := createTestArtifact(store); err != nil {
		t.Fatal(err)
	}
	t.Fatal("did not reach requested crash stage")
}

func TestSnapshotStoreProcessCrashPublication(t *testing.T) {
	for _, stage := range []string{"after-file-sync", "after-link", "after-directory-sync"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSnapshotStoreCrashHelper$")
			command.Env = append(os.Environ(), "DUBI_SNAPSHOT_CRASH_ROOT="+root, "DUBI_SNAPSHOT_CRASH_STAGE="+stage)
			output, err := command.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 71 {
				t.Fatalf("crash helper: %v\n%s", err, output)
			}
			store := openTestStore(t, root)
			before, err := os.ReadDir(root)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := createTestArtifact(store)
			if err != nil {
				t.Fatal(err)
			}
			data := readTestArtifact(t, store, ref)
			digest := sha256.Sum256(data)
			if hex.EncodeToString(digest[:]) != ref.SHA256 {
				t.Fatal("recovered artifact corrupt")
			}
			// Unknown crash leftovers are retained for the later resource/GC
			// protocol, not guessed safe to delete at startup.
			for _, entry := range before {
				if _, err := os.Lstat(filepath.Join(root, entry.Name())); err != nil {
					t.Fatal("startup deleted retained resource", err)
				}
			}
		})
	}
}
