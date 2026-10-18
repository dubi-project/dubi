package repository

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRepositoryKeyCreateLoadAndReuse(t *testing.T) {
	store := t.TempDir()
	first, err := EnsureRepositoryKey(store)
	if err != nil {
		t.Fatal(err)
	}
	second, err := EnsureRepositoryKey(store)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadRepositoryKey(store)
	if err != nil || !bytes.Equal(first, second) || !bytes.Equal(first, loaded) || len(loaded) != repositoryKeyBytes {
		t.Fatalf("key mismatch: first=%x second=%x loaded=%x err=%v", first, second, loaded, err)
	}
	info, err := os.Stat(filepath.Join(store, RepositoryKeyFileName))
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("key permissions = %v, err=%v", info.Mode().Perm(), err)
	}
}

func TestRepositoryKeyDoesNotSilentlyReplaceMissingKey(t *testing.T) {
	store := t.TempDir()
	if _, err := EnsureRepositoryKey(store); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(store, RepositoryKeyFileName)); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRepositoryKey(store); err == nil {
		t.Fatal("missing Repository key was accepted")
	}
}

func TestRepositoryKeyRejectsWeakPermissions(t *testing.T) {
	store := t.TempDir()
	if _, err := EnsureRepositoryKey(store); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store, RepositoryKeyFileName)
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRepositoryKey(store); err == nil {
		t.Fatal("weak Repository key permissions were accepted")
	}
}

func TestRepositoryKeyInstallIsIdempotentAndRejectsConflict(t *testing.T) {
	store := t.TempDir()
	key := bytes.Repeat([]byte{0x55}, repositoryKeyBytes)
	if err := InstallRepositoryKey(store, key); err != nil {
		t.Fatal(err)
	}
	if err := InstallRepositoryKey(store, key); err != nil {
		t.Fatal(err)
	}
	if err := InstallRepositoryKey(store, bytes.Repeat([]byte{0x56}, repositoryKeyBytes)); err == nil {
		t.Fatal("conflicting Repository key was accepted")
	}
}
