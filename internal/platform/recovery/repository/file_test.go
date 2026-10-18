package repository

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStoreContentAddressingAndManifestCommit(t *testing.T) {
	root := t.TempDir()
	store, err := OpenFile(Repository{Name: "local", Type: TypeFile, Path: root})
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("immutable backup object")
	first, err := store.PutObject(context.Background(), bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.PutObject(context.Background(), bytes.NewReader(content))
	if err != nil || first != second {
		t.Fatalf("content identity changed: %+v %+v %v", first, second, err)
	}
	reader, err := store.OpenObject(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil || closeErr != nil || !bytes.Equal(decoded, content) {
		t.Fatalf("object read = %q, %v, %v", decoded, err, closeErr)
	}
	manifest := Manifest{Version: 1, Repository: "local", ClusterID: "cluster_test", Epoch: 1,
		CreatedAt: time.Now().UTC(), Objects: []ObjectRef{first}}
	if err := store.CommitManifest(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.ReadManifest(1)
	if err != nil || loaded.Repository != manifest.Repository || len(loaded.Objects) != 1 {
		t.Fatalf("manifest = %+v, %v", loaded, err)
	}
	manifests, err := store.ListManifests()
	if err != nil || len(manifests) != 1 || manifests[0].Epoch != manifest.Epoch {
		t.Fatalf("manifest list = %+v, %v", manifests, err)
	}
	if err := store.CommitManifest(context.Background(), manifest); err == nil {
		t.Fatal("duplicate Recovery Point committed")
	}
}

func TestFileStoreRejectsIncompleteOrMissingObjectManifest(t *testing.T) {
	store, err := OpenFile(Repository{Name: "local", Type: TypeFile, Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Version: 1, Repository: "local", ClusterID: "cluster_test", Epoch: 1,
		CreatedAt: time.Now().UTC(), Objects: []ObjectRef{{ID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Size: 1}}}
	if err := store.CommitManifest(context.Background(), manifest); err == nil {
		t.Fatal("missing object accepted")
	}
	if _, err := store.ReadManifest(1); err == nil {
		t.Fatal("incomplete manifest was readable")
	}
}

func TestFileStoreManifestReadVerifiesObjectHash(t *testing.T) {
	root := t.TempDir()
	config := Repository{Name: "local", Type: TypeFile, Path: filepath.Join(root, "backup")}
	store, err := OpenFile(config)
	if err != nil {
		t.Fatal(err)
	}
	object, err := store.PutObject(context.Background(), bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Version: 1, Repository: config.Name, ClusterID: "cluster", Epoch: 11, CreatedAt: time.Unix(11, 0).UTC(), Objects: []ObjectRef{object}}
	if err := store.CommitManifest(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(config.Path, objectsDirectory, object.ID[:2], object.ID)
	if err := os.WriteFile(path, []byte("tampered"), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadManifest(manifest.Epoch); err == nil {
		t.Fatal("tampered object was accepted")
	}
}

func TestFileStoreShardCommitAndRead(t *testing.T) {
	config := Repository{Name: "local", Type: TypeFile, Path: t.TempDir()}
	store, err := OpenFile(config)
	if err != nil {
		t.Fatal(err)
	}
	object, err := store.PutObject(context.Background(), bytes.NewReader([]byte("node bytes")))
	if err != nil {
		t.Fatal(err)
	}
	shard := Shard{Version: 1, JobID: "backup_01", Repository: config.Name, ClusterID: "cluster_test", Epoch: 7,
		CreatedAt: time.Unix(7, 0).UTC(), NodeID: "node_a", StoreID: "store_a", Objects: []ObjectRef{object},
		Artifacts: []Artifact{{Name: "pack/pack_01", Kind: "pack", NodeID: "node_a", StoreID: "store_a", Object: object}}}
	if err := store.CommitShard(context.Background(), shard); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.ReadShard(shard.JobID, shard.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.JobID != shard.JobID || loaded.NodeID != shard.NodeID || len(loaded.Artifacts) != 1 {
		t.Fatalf("loaded shard = %+v", loaded)
	}
	shards, err := store.ListShards(shard.JobID)
	if err != nil || len(shards) != 1 || shards[0].NodeID != shard.NodeID {
		t.Fatalf("shard list = %+v, %v", shards, err)
	}
	if err := store.CommitShard(context.Background(), shard); err == nil {
		t.Fatal("duplicate backup shard committed")
	}
}
