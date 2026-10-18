package recovery

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
)

func TestRunNodeBackupAndFinalizeShards(t *testing.T) {
	config := repository.Repository{Name: "local", Type: repository.TypeFile, Path: t.TempDir()}
	store, err := repository.OpenFile(config)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Unix(10, 0).UTC()
	makeSource := func(name, value string) Source {
		return Source{Name: name, Kind: "pack", NodeID: "node_a", StoreID: "store_a", Size: int64(len(value)), Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewBufferString(value)), nil
		}}
	}
	result, shard, err := RunFileNodeBackup(context.Background(), store, ShardPlan{Repository: config, JobID: "backup_01", ClusterID: "cluster_test", Epoch: 10, CreatedAt: created, NodeID: "node_a", StoreID: "store_a"}, []Source{makeSource("pack-a", "same")})
	if err != nil {
		t.Fatal(err)
	}
	if result.Objects != 1 || result.Artifacts != 1 || shard.JobID != "backup_01" {
		t.Fatalf("unexpected node backup result: %+v %+v", result, shard)
	}
	second := shard
	second.NodeID = "node_b"
	second.StoreID = "store_b"
	second.Artifacts = []repository.Artifact{{Name: "pack-b", Kind: "pack", NodeID: "node_b", StoreID: "store_b", Object: shard.Objects[0]}}
	if err := store.CommitShard(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	final, err := FinalizeShards(context.Background(), store, Plan{Repository: config, ClusterID: "cluster_test", Epoch: 10, CreatedAt: created}, []repository.Shard{shard, second})
	if err != nil {
		t.Fatal(err)
	}
	if final.Objects != 1 || final.Artifacts != 2 {
		t.Fatalf("unexpected final result: %+v", final)
	}
	loaded, err := store.ReadManifest(10)
	if err != nil || len(loaded.Artifacts) != 2 {
		t.Fatalf("manifest = %+v, %v", loaded, err)
	}
}

func TestFinalizeShardsRejectsDuplicateWorker(t *testing.T) {
	config := repository.Repository{Name: "local", Type: repository.TypeFile, Path: t.TempDir()}
	store, err := repository.OpenFile(config)
	if err != nil {
		t.Fatal(err)
	}
	shard := repository.Shard{Version: 1, JobID: "backup_02", Repository: config.Name, ClusterID: "cluster_test", Epoch: 11,
		CreatedAt: time.Unix(11, 0).UTC(), NodeID: "node_a", StoreID: "store_a", Objects: []repository.ObjectRef{{ID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Size: 0}}}
	if _, err := FinalizeShards(context.Background(), store, Plan{Repository: config, ClusterID: "cluster_test", Epoch: 11}, []repository.Shard{shard, shard}); err == nil {
		t.Fatal("duplicate worker shard accepted")
	}
}
