package pebbleutil

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
)

const (
	// The Node owns several small Pebble databases (Root, Raft logs, Engine,
	// repair ledgers and records). A private default cache per DB multiplies
	// memory on small Nodes, while an unbounded table-file budget makes startup
	// recovery compete with the process heap. Keep one bounded cache and modest
	// per-DB file/memtable budgets instead.
	sharedCacheSize = 16 << 20
	maxOpenFiles    = 128
	memTableSize    = 1 << 20
)

var shared = struct {
	sync.Once
	cache *pebble.Cache
}{}

func sharedPebbleCache() *pebble.Cache {
	shared.Do(func() { shared.cache = pebble.NewCache(sharedCacheSize) })
	return shared.cache
}

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
	if options == nil {
		options = &pebble.Options{}
	}
	// Pebble takes its own reference to a supplied Cache and releases it when
	// the DB closes. The package-level reference intentionally keeps the small
	// shared cache alive for the Node lifetime.
	if options.Cache == nil {
		options.Cache = sharedPebbleCache()
	}
	if options.MaxOpenFiles == 0 || options.MaxOpenFiles > maxOpenFiles {
		options.MaxOpenFiles = maxOpenFiles
	}
	if options.MemTableSize == 0 || options.MemTableSize > memTableSize {
		options.MemTableSize = memTableSize
	}
	if options.MaxConcurrentCompactions == nil {
		options.MaxConcurrentCompactions = func() int { return 1 }
	}
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
