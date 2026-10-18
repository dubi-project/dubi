package recovery

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
)

func TestRunFileBackupPublishesDeduplicatedRecoveryPoint(t *testing.T) {
	root := t.TempDir()
	config := repository.Repository{Name: "local", Type: repository.TypeFile, Path: root}
	store, err := repository.OpenFile(config)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("same immutable bytes")
	sources := []Source{
		{Name: "root-state", Kind: "root-state", NodeID: "node-a", Size: int64(len(content)), Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(string(content))), nil
		}},
		{Name: "pack-catalog", Kind: "pack", NodeID: "node-b", Size: int64(len(content)), Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(string(content))), nil
		}},
	}
	result, err := RunFileBackup(context.Background(), store, Plan{Repository: config, ClusterID: "cluster-a", Epoch: 1}, sources)
	if err != nil {
		t.Fatal(err)
	}
	if result.Objects != 1 || result.Artifacts != 2 || len(result.Manifest.Objects) != 1 || len(result.Manifest.Artifacts) != 2 {
		t.Fatalf("unexpected backup result: %+v", result)
	}
	read, err := store.ReadManifest(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := read.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRunFileBackupDoesNotPublishOnSourceFailure(t *testing.T) {
	config := repository.Repository{Name: "local", Type: repository.TypeFile, Path: t.TempDir()}
	store, err := repository.OpenFile(config)
	if err != nil {
		t.Fatal(err)
	}
	_, err = RunFileBackup(context.Background(), store, Plan{Repository: config, ClusterID: "cluster-a", Epoch: 2}, []Source{
		{Name: "broken", Kind: "pack", Size: -1, Open: func(context.Context) (io.ReadCloser, error) {
			return nil, context.DeadlineExceeded
		}},
	})
	if err == nil {
		t.Fatal("broken source backup succeeded")
	}
	if _, err := store.ReadManifest(2); err == nil {
		t.Fatal("failed backup was published")
	}
}

func TestRunFileBackupRetriesPublishedManifestIdempotently(t *testing.T) {
	config := repository.Repository{Name: "local", Type: repository.TypeFile, Path: t.TempDir()}
	store, err := repository.OpenFile(config)
	if err != nil {
		t.Fatal(err)
	}
	sources := []Source{{Name: "root-state", Kind: "root", NodeID: "node-a", StoreID: "store-a", Size: 7,
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("payload")), nil
		}}}
	first, err := RunFileBackup(context.Background(), store, Plan{Repository: config, ClusterID: "cluster-a", Epoch: 6}, sources)
	if err != nil {
		t.Fatal(err)
	}
	second, err := RunFileBackup(context.Background(), store, Plan{Repository: config, ClusterID: "cluster-a", Epoch: 6}, sources)
	if err != nil {
		t.Fatal(err)
	}
	if !sameManifestContent(first.Manifest, second.Manifest) || second.Objects != 1 || second.Bytes != 7 {
		t.Fatalf("idempotent retry changed result: first=%+v second=%+v", first, second)
	}
}

func TestSortSourcesReturnsIndependentDeterministicOrder(t *testing.T) {
	sources := []Source{{Name: "z", Kind: "x", NodeID: "b"}, {Name: "a", Kind: "x", NodeID: "a"}}
	sorted := SortSources(sources)
	if sorted[0].NodeID != "a" || sources[0].NodeID != "b" {
		t.Fatalf("sort mutated input or was not deterministic: %#v %#v", sources, sorted)
	}
}

func TestFileSourceUsesCheckpointedFileSizeAndStreamsBytes(t *testing.T) {
	path := t.TempDir() + "/pack.bin"
	content := []byte("sealed pack bytes")
	if err := os.WriteFile(path, content, 0o640); err != nil {
		t.Fatal(err)
	}
	source, err := FileSource("volume/v1/pack/p1", "pack", "node_a", "store_a", "DUBIPK02", path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := source.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(got, content) || source.Size != int64(len(content)) {
		t.Fatalf("source = %+v, bytes = %q, read=%v close=%v", source, got, readErr, closeErr)
	}
}

func TestRestoreFileStreamsVerifiedArtifacts(t *testing.T) {
	root := t.TempDir()
	config := repository.Repository{Name: "local", Type: repository.TypeFile, Path: root}
	store, err := repository.OpenFile(config)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("restore me")
	if _, err := RunFileBackup(context.Background(), store, Plan{Repository: config, ClusterID: "cluster-a", Epoch: 3}, []Source{{
		Name: "root-state", Kind: "root", NodeID: "node-a", Size: int64(len(content)), Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(content)), nil
		},
	}}); err != nil {
		t.Fatal(err)
	}
	var restored []byte
	if _, err := RestoreFile(context.Background(), store, 3, []Target{{Name: "root-state", NodeID: "node-a", Apply: func(_ context.Context, reader io.Reader, size int64) error {
		if size != int64(len(content)) {
			t.Fatalf("restore size = %d", size)
		}
		var err error
		restored, err = io.ReadAll(reader)
		return err
	}}}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, content) {
		t.Fatalf("restored bytes = %q", restored)
	}
}

func TestRestoreFileRejectsPartialTarget(t *testing.T) {
	config := repository.Repository{Name: "local", Type: repository.TypeFile, Path: t.TempDir()}
	store, err := repository.OpenFile(config)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("restore me")
	if _, err := RunFileBackup(context.Background(), store, Plan{Repository: config, ClusterID: "cluster-a", Epoch: 4}, []Source{{
		Name: "root-state", Kind: "root", NodeID: "node-a", Size: int64(len(content)), Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(content)), nil
		},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreFile(context.Background(), store, 4, []Target{{Name: "root-state", NodeID: "node-a", Apply: func(_ context.Context, _ io.Reader, _ int64) error { return nil }}}); err == nil {
		t.Fatal("partial restore target was accepted")
	}
}

func TestRestorePreflightsAllTargetsBeforeApplying(t *testing.T) {
	config := repository.Repository{Name: "local", Type: repository.TypeFile, Path: t.TempDir()}
	store, err := repository.OpenFile(config)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("restore me")
	if _, err := RunFileBackup(context.Background(), store, Plan{Repository: config, ClusterID: "cluster-a", Epoch: 5}, []Source{
		{Name: "first", Kind: "pack", NodeID: "node-a", Size: int64(len(content)), Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(content)), nil
		}},
		{Name: "second", Kind: "pack", NodeID: "node-a", Size: int64(len(content)), Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(content)), nil
		}},
	}); err != nil {
		t.Fatal(err)
	}
	applied := 0
	_, err = RestoreFile(context.Background(), store, 5, []Target{{Name: "first", NodeID: "node-a", Apply: func(_ context.Context, _ io.Reader, _ int64) error {
		applied++
		return nil
	}}})
	if err == nil || applied != 0 {
		t.Fatalf("restore applied before target preflight: err=%v applied=%d", err, applied)
	}
}
