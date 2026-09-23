package pebbleutil

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestRecoverOpenFatalConvertsOnlyPebbleLoggerFailure(t *testing.T) {
	db, err := triggerLoggerFatal()
	if db != nil || err == nil || err.Error() != "disk full: ENOSPC" {
		t.Fatalf("fatal recovery = db %v err %v", db, err)
	}
	deferred := func() (recovered any) {
		defer func() { recovered = recover() }()
		func() (db *pebble.DB, err error) {
			defer recoverOpenFatal(&db, &err)
			panic("programming bug")
		}()
		return nil
	}()
	if deferred != "programming bug" {
		t.Fatalf("unrelated panic was not propagated: %v", deferred)
	}
}

func triggerLoggerFatal() (db *pebble.DB, err error) {
	defer recoverOpenFatal(&db, &err)
	QuietLogger{}.Fatalf("disk full: %s", "ENOSPC")
	return nil, nil
}
