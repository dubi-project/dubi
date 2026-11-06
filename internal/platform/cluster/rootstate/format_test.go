package rootstate

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
)

func TestOldDatasetStateRejectedWithoutReinterpretation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "dubi-root-state")
	db, err := pebbleutil.Open(path, &pebble.Options{Logger: pebbleutil.QuietLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("data/dataset/tenant/record/items")
	value := []byte(`{"tenant_id":"tenant","dataset_id":"items"}`)
	if err = db.Set(key, value, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	state, err := Open(root)
	if !errors.Is(err, ErrStateFormat) {
		if state != nil {
			state.Close()
		}
		t.Fatal("old state must require explicit migration", err)
	}
	db, err = pebbleutil.Open(path, &pebble.Options{Logger: pebbleutil.QuietLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	actual, closer, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	if !bytes.Equal(actual, value) {
		t.Fatal("rejected old data was altered")
	}
	if _, c, e := db.Get(stateFormatKey); e == nil {
		c.Close()
		t.Fatal("old data relabelled as new format")
	} else if !errors.Is(e, pebble.ErrNotFound) {
		t.Fatal(e)
	}
}
