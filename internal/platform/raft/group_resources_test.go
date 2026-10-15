package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHasGroupResourcesNeverInitializesOrIgnoresPartialState(t *testing.T) {
	store := t.TempDir()
	if found, err := HasGroupResources(store); err != nil || found {
		t.Fatal(found, err)
	}
	entries, err := os.ReadDir(store)
	if err != nil || len(entries) != 0 {
		t.Fatal("read-only guard created state", err)
	}
	parent := filepath.Join(store, groupDirectory)
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if found, err := HasGroupResources(store); err != nil || found {
		t.Fatal(found, err)
	}
	if err := os.Mkdir(filepath.Join(parent, RootGroupID), 0o700); err != nil {
		t.Fatal(err)
	}
	if found, err := HasGroupResources(store); err != nil || !found {
		t.Fatal("partial group ignored", err)
	}
	other := t.TempDir()
	if err := os.Symlink(parent, filepath.Join(other, groupDirectory)); err != nil {
		t.Fatal(err)
	}
	if _, err := HasGroupResources(other); err == nil {
		t.Fatal("symlink parent accepted")
	}
}
