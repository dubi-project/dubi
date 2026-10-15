package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStagingViewValidationAndCleanup(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	ref, err := createTestArtifact(store)
	if err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	staged, err := Stage(context.Background(), parent, store, ref, func(header Header) error {
		if !reflect.DeepEqual(header, streamHeader()) {
			return errors.New("wrong header")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	path := staged.path
	count := 0
	if err := Walk(context.Background(), staged.View(), func(key, value []byte) error {
		count++
		if string(key) != "item" || string(value) != "value" {
			return errors.New("wrong staged record")
		}
		return nil
	}); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	if err := staged.Close(); err != nil {
		t.Fatal(err)
	}
	if err := staged.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("owned stage not removed", err)
	}
	sentinel := errors.New("header rejected")
	if _, err := Stage(context.Background(), parent, store, ref, func(Header) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 {
		t.Fatal("rejected stage leaked", entries, err)
	}
}

func TestStagingCleanupRefusesReplacedDirectory(t *testing.T) {
	parent := t.TempDir()
	path, err := os.MkdirTemp(parent, ".restore-stage-")
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	staged := &Staged{path: path, identity: info}
	original := path + ".retained"
	if err := os.Rename(path, original); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(path, "must-keep")
	if err := os.WriteFile(sentinel, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := staged.Close(); err == nil {
		t.Fatal("replaced directory treated as owned")
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "replacement" {
		t.Fatal("replacement removed", err)
	}
	if _, err := os.Lstat(original); err != nil {
		t.Fatal("retained original removed", err)
	}
}
