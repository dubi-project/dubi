package identity

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestBootstrapIsPersistentAndIdempotent(t *testing.T) {
	root := t.TempDir()
	first, err := Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	if first.NodeID == "" || first.ClusterID == "" || first.RaftID == 0 {
		t.Fatalf("incomplete bootstrap identity: %+v", first)
	}
	second, err := Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatalf("bootstrap changed identity: first=%+v second=%+v", first, second)
	}
	loaded, err := Load(root)
	if err != nil || loaded != first {
		t.Fatalf("loaded identity = %+v, %v; want %+v", loaded, err, first)
	}
	if _, err := os.Stat(filepath.Join(root, "NODE_IDENTITY")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Byfile identity path unexpectedly used: %v", err)
	}
}

func TestJoinIdentityCanBindExactlyOneCluster(t *testing.T) {
	root := t.TempDir()
	prepared, err := PrepareJoin(root)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.ClusterID != "" {
		t.Fatalf("prepared join already has Cluster %q", prepared.ClusterID)
	}
	cluster, err := Bootstrap(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindCluster(root, cluster.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if bound.NodeID != prepared.NodeID || bound.RaftID != prepared.RaftID {
		t.Fatal("binding changed the local Node identity")
	}
	if _, err := BindCluster(root, cluster.ClusterID); err != nil {
		t.Fatalf("idempotent binding: %v", err)
	}
	other, err := Bootstrap(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BindCluster(root, other.ClusterID); err == nil {
		t.Fatal("rebinding to another Cluster unexpectedly succeeded")
	}
}

func TestPreparedJoinCannotBootstrap(t *testing.T) {
	root := t.TempDir()
	if _, err := PrepareJoin(root); err != nil {
		t.Fatal(err)
	}
	if _, err := Bootstrap(root); err == nil {
		t.Fatal("prepared join unexpectedly bootstrapped a new Cluster")
	}
}

func TestLoadRejectsMissingAndCorruptIdentity(t *testing.T) {
	root := t.TempDir()
	if _, err := Load(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing identity error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, filename), []byte("{}"), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(root); err == nil {
		t.Fatal("corrupt identity unexpectedly loaded")
	}
}
