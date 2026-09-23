package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareGroupRootIsolatesAndReopensGroups(t *testing.T) {
	storeRoot := t.TempDir()
	root, err := PrepareGroupRoot(storeRoot, RootGroupID)
	if err != nil {
		t.Fatal(err)
	}
	other, err := PrepareGroupRoot(storeRoot, "regional-west")
	if err != nil {
		t.Fatal(err)
	}
	if root == other || root == storeRoot || other == storeRoot {
		t.Fatalf("Group roots are not isolated: root=%q other=%q", root, other)
	}
	if reopened, err := PrepareGroupRoot(storeRoot, RootGroupID); err != nil || reopened != root {
		t.Fatalf("reopen Group root = %q, %v", reopened, err)
	}
	if _, err := os.Stat(filepath.Join(root, identityFile)); err != nil {
		t.Fatalf("Group identity marker: %v", err)
	}
}

func TestPrepareGroupRootRejectsInvalidOrUnidentifiedDirectory(t *testing.T) {
	storeRoot := t.TempDir()
	if _, err := PrepareGroupRoot(storeRoot, "../escape"); err == nil {
		t.Fatal("path traversal GroupID was accepted")
	}
	for _, id := range []string{".", ".."} {
		if _, err := PrepareGroupRoot(storeRoot, id); err == nil {
			t.Fatalf("path traversal GroupID %q was accepted", id)
		}
	}
	unidentified := filepath.Join(storeRoot, groupDirectory, "old")
	if err := os.MkdirAll(unidentified, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unidentified, "raft"), []byte("unknown"), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareGroupRoot(storeRoot, "old"); err == nil {
		t.Fatal("non-empty unidentified Group directory was adopted")
	}
}

func TestPrepareGroupRootRejectsCorruptMarker(t *testing.T) {
	storeRoot := t.TempDir()
	root, err := PrepareGroupRoot(storeRoot, "group-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, identityFile), []byte("{}"), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareGroupRoot(storeRoot, "group-a"); err == nil {
		t.Fatal("corrupt Group marker was accepted")
	}
}
