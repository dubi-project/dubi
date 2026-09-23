package pebbleutil

import (
	"fmt"

	"github.com/cockroachdb/pebble"
)

// QuietLogger keeps expected Pebble recovery messages out of CLI output.
// Pebble fatal conditions still abort the current operation through a panic.
type QuietLogger struct{}

func (QuietLogger) Infof(string, ...any) {}

func (QuietLogger) Fatalf(format string, args ...any) {
	panic(FatalError{Message: fmt.Sprintf(format, args...)})
}

// FatalError is the typed panic required by Pebble's Logger contract. Open
// converts only this expected storage failure back into an ordinary error;
// unrelated programming panics continue to crash loudly.
type FatalError struct{ Message string }

func (e FatalError) Error() string { return e.Message }

// Open prevents ENOSPC and similar Pebble startup failures from terminating a
// CLI or node process through Logger.Fatalf.
func Open(path string, options *pebble.Options) (db *pebble.DB, err error) {
	defer recoverOpenFatal(&db, &err)
	return pebble.Open(path, options)
}

func recoverOpenFatal(db **pebble.DB, err *error) {
	if recovered := recover(); recovered != nil {
		fatal, ok := recovered.(FatalError)
		if !ok {
			panic(recovered)
		}
		*db = nil
		*err = fatal
	}
}
