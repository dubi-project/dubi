package enginestate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

func fileArtifacts(t *testing.T) (*snapshot.Store, string) {
	t.Helper()
	root := t.TempDir()
	store, err := snapshot.OpenStore(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store, root
}

func TestFileSnapshotCapturesScopedLogicalCutIndependentOfPacking(t *testing.T) {
	artifacts, _ := fileArtifacts(t)
	old := bytes.Repeat([]byte("a"), PackValueThreshold+1)
	handler := &packedHandler{action: func(tx *Transaction, payload []byte) (Outcome, error) {
		if err := tx.Set([]byte("z-packed"), payload); err != nil {
			return Outcome{}, err
		}
		return Outcome{}, tx.Set([]byte("a-inline"), []byte("small"))
	}}
	source, err := open(t.TempDir(), "data-1", handler, &memoryPacks{id: "physical-store"})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	a := activateKeyspace(t, source, rootstate.Keyspace{TenantID: "a", EngineID: "record", DatasetID: "items"})
	b := activateKeyspace(t, source, rootstate.Keyspace{TenantID: "b", EngineID: "search", DatasetID: "items"})
	if err := source.ApplyRouted(3, "write-a", a, old); err != nil {
		t.Fatal(err)
	}
	if err := source.ApplyRouted(4, "write-b", b, old); err != nil {
		t.Fatal(err)
	}
	inlineImage, index, conf, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := source.PrepareSnapshotTo(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	// Publication after capture must not enter the image or its request results.
	if err := source.ApplyRouted(5, "after-capture", a, bytes.Repeat([]byte("b"), len(old))); err != nil {
		t.Fatal(err)
	}
	encoded, err := prepared.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ref, recognized, err := snapshot.DecodeReference(encoded)
	if err != nil || !recognized || ref.Index != index {
		t.Fatal(ref, recognized, err)
	}
	reader, err := artifacts.Open(ref)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := snapshot.NewDecoder(reader, ref.Size)
	if err != nil {
		reader.Close()
		t.Fatal(err)
	}
	if header := decoder.Header(); header.Index != index || header.GroupID != "data-1" || header.Format != "enginestate-v7" {
		t.Fatal(header)
	}
	values := make(map[string][]byte)
	for {
		key, value, err := decoder.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			reader.Close()
			t.Fatal(err)
		}
		if bytes.HasPrefix(key, []byte("local/")) || bytes.HasPrefix(key, []byte("meta/")) {
			t.Fatal("local storage reference leaked")
		}
		values[string(key)] = value
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []rootstate.Keyspace{a.Keyspace, b.Keyspace} {
		if !bytes.Equal(values[string(engineKey(scope, []byte("z-packed")))], old) ||
			string(values[string(engineKey(scope, []byte("a-inline")))]) != "small" {
			t.Fatal("captured logical values differ")
		}
	}
	if _, exists := values["result/after-capture"]; exists {
		t.Fatal("post-capture result leaked")
	}
	inline, err := Open(t.TempDir(), "data-1", &packedHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer inline.Close()
	if err := inline.Restore(index, inlineImage, conf); err != nil {
		t.Fatal(err)
	}
	other, err := inline.PrepareSnapshotTo(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	inlineReference, err := other.Build(context.Background())
	if err != nil || !bytes.Equal(inlineReference, encoded) {
		t.Fatal("physical placement changed snapshot digest", err)
	}
	if _, err := prepared.Build(context.Background()); err == nil {
		t.Fatal("preparation built twice")
	}
}

func TestFileSnapshotRejectsCorruptViewWithoutPublishing(t *testing.T) {
	for _, damage := range []string{"missing-fence", "unknown-key", "conflicting-inline", "canceled"} {
		t.Run(damage, func(t *testing.T) {
			artifacts, root := fileArtifacts(t)
			state, err := open(t.TempDir(), "data-1", &packedHandler{}, &memoryPacks{id: "s"})
			if err != nil {
				t.Fatal(err)
			}
			defer state.Close()
			target := packedTarget(t, state, "record", "t")
			if err := state.ApplyRouted(2, "write", target, bytes.Repeat([]byte("x"), PackValueThreshold)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch damage {
			case "missing-fence":
				err = state.db.Delete(fenceKeyspaceKey(target.Keyspace), pebble.Sync)
			case "unknown-key":
				err = state.db.Set([]byte("data/unsupported"), []byte("x"), pebble.Sync)
			case "conflicting-inline":
				err = state.db.Set(engineKey(target.Keyspace, []byte("item")), []byte("x"), pebble.Sync)
			case "canceled":
				cancel()
			}
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := state.PrepareSnapshotTo(artifacts)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			if encoded, err := prepared.Build(ctx); err == nil || encoded != nil {
				t.Fatal("invalid source published a reference", err)
			}
			entries, err := os.ReadDir(root)
			if err != nil || len(entries) != 1 {
				t.Fatal("failed build left a published artifact", entries, err)
			}
		})
	}
}

func TestFileSnapshotPreparationPinsViewUntilClosed(t *testing.T) {
	artifacts, _ := fileArtifacts(t)
	state, err := Open(t.TempDir(), "data-1", &packedHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Advance(1); err != nil {
		t.Fatal(err)
	}
	prepared, err := state.PrepareSnapshotTo(artifacts)
	if err != nil {
		state.Close()
		t.Fatal(err)
	}
	defer prepared.Close()
	closed := make(chan error, 1)
	go func() { closed <- state.Close() }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		state.mu.RLock()
		closing := state.closed
		state.mu.RUnlock()
		if closing {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("state did not begin closing")
		case <-tick.C:
		}
	}
	select {
	case err := <-closed:
		t.Fatal("state closed with a captured view", err)
	default:
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.C:
		t.Fatal("state did not finish closing")
	}
	if _, err := prepared.Build(context.Background()); err == nil {
		t.Fatal("build used a released view")
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileSnapshotExceedsInlineLimitUsingBoundedRecordStreaming(t *testing.T) {
	artifacts, _ := fileArtifacts(t)
	value := bytes.Repeat([]byte("x"), 1<<20)
	handler := &packedHandler{action: func(tx *Transaction, key []byte) (Outcome, error) { return Outcome{}, tx.Set(key, value) }}
	state, err := open(t.TempDir(), "data-1", handler, &memoryPacks{id: "source"})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	target := packedTarget(t, state, "record", "t")
	for i := range 80 {
		if err := state.ApplyRouted(state.AppliedIndex()+1, fmt.Sprintf("write-%03d", i), target, []byte(fmt.Sprintf("item-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	prepared, err := state.PrepareSnapshotTo(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	encoded, err := prepared.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ref, recognized, err := snapshot.DecodeReference(encoded)
	if err != nil || !recognized || ref.Size <= maxSnapshotBytes {
		t.Fatal("file snapshot did not exceed inline limit", ref, err)
	}
	reader, err := artifacts.Open(ref)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := snapshot.NewDecoder(reader, ref.Size)
	if err != nil {
		reader.Close()
		t.Fatal(err)
	}
	count := 0
	for {
		key, got, err := decoder.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			reader.Close()
			t.Fatal(err)
		}
		if bytes.HasPrefix(key, valuePrefix) {
			count++
			if !bytes.Equal(got, value) {
				t.Fatal("streamed engine payload changed")
			}
		}
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if count != 80 {
		t.Fatal("file snapshot lost values", count)
	}
	// Exercise the actual disk Pack backend on restoration, not just codec
	// iteration. The complete logical image exceeds the old inline ceiling.
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	volumes, err := volume.Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer volumes.Close()
	pool, err := pack.OpenPool(volumes)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	destination, err := OpenWithPacks(filepath.Join(root, "state"), "data-1", &packedHandler{}, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if err := destination.RestoreFrom(context.Background(), artifacts, prepared.Index, encoded, prepared.Conf); err != nil {
		t.Fatal(err)
	}
	if destination.AppliedIndex() != state.AppliedIndex() {
		t.Fatal("large restore lost applied cut")
	}
	for i := range 80 {
		got, found, err := destination.Value(target.Keyspace, []byte(fmt.Sprintf("item-%03d", i)))
		if err != nil || !found || !bytes.Equal(got, value) {
			t.Fatal("large Pack-backed restore lost value", i, err)
		}
	}
}
