package snapshot

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/storage/diskpressure"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
)

const StagingBatchBytes = 1 << 20

// Staged is a private disk-backed logical image, never a live state database.
// View remains valid until Close; callers must complete semantic validation
// against it before changing live state. It is NOT a recovery authority: the
// durable Reference must survive installation failures and process crashes.
type Staged struct {
	path     string
	identity os.FileInfo
	db       *pebble.DB
	cache    *pebble.Cache
	view     *pebble.Snapshot
	once     sync.Once
	closeErr error
}

func (s *Staged) View() *pebble.Snapshot { return s.view }

func (s *Staged) Close() error {
	s.once.Do(func() {
		if s.view != nil {
			s.closeErr = s.view.Close()
		}
		if s.db != nil {
			s.closeErr = errors.Join(s.closeErr, s.db.Close())
		}
		if s.cache != nil {
			s.cache.Unref()
		}
		if s.closeErr == nil {
			// Only this invocation's generated temporary directory is removed.
			// Unknown crash leftovers or replaced paths are never swept here.
			current, err := os.Lstat(s.path)
			if err != nil || s.identity == nil || !current.IsDir() || !os.SameFile(current, s.identity) {
				s.closeErr = errors.Join(err, errors.New("snapshot staging identity changed; retained without cleanup"))
			} else {
				s.closeErr = os.RemoveAll(s.path)
			}
		}
	})
	return s.closeErr
}

// Stage verifies the entire artifact (including its reader.Close digest
// result) while building bounded batches in a private Pebble DB. Its caller's
// header validator binds state format/membership. No live state is modified.
func Stage(ctx context.Context, parent string, artifacts *Store, ref Reference, validateHeader func(Header) error) (_ *Staged, returnErr error) {
	if ctx == nil || parent == "" || artifacts == nil || validateHeader == nil {
		return nil, errors.New("snapshot staging requires context, directory, artifacts and header validation")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	disk, err := diskpressure.Open(parent)
	if err != nil {
		return nil, err
	}
	defer disk.Close()
	if err := disk.Check(); err != nil {
		return nil, err
	}
	path, err := os.MkdirTemp(parent, ".restore-stage-")
	if err != nil {
		return nil, err
	}
	staged := &Staged{path: path, cache: pebble.NewCache(8 << 20)}
	success := false
	defer func() {
		if !success {
			returnErr = errors.Join(returnErr, staged.Close())
		}
	}()
	staged.identity, err = os.Lstat(path)
	if err != nil || !staged.identity.IsDir() {
		return nil, errors.Join(err, errors.New("snapshot staging directory cannot be verified"))
	}
	staged.db, err = pebbleutil.Open(path, &pebble.Options{Logger: pebbleutil.QuietLogger{}, Cache: staged.cache,
		MemTableSize: 8 << 20, MemTableStopWritesThreshold: 2, DisableWAL: true})
	if err != nil {
		return nil, err
	}
	if err := stageRecords(ctx, disk, artifacts, ref, validateHeader, staged.db); err != nil {
		return nil, err
	}
	staged.view = staged.db.NewSnapshot()
	success = true
	return staged, nil
}

func stageRecords(ctx context.Context, disk *diskpressure.Guard, artifacts *Store, ref Reference,
	validateHeader func(Header) error, staged *pebble.DB) (returnErr error) {
	reader, err := artifacts.Open(ref)
	if err != nil {
		return err
	}
	// io.ReadFull may suppress a digest error alongside the final bytes.
	// Close must succeed before Stage hands any view to the caller.
	defer func() { returnErr = errors.Join(returnErr, reader.Close()) }()
	decoder, err := NewDecoder(&contextReader{ctx: ctx, reader: reader}, ref.Size)
	if err != nil {
		return err
	}
	if err := validateHeader(decoder.Header()); err != nil {
		return err
	}
	batch := staged.NewBatch()
	defer batch.Close()
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := disk.Check(); err != nil {
			return err
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			return err
		}
		batch.Reset()
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		key, value, err := decoder.Next()
		if errors.Is(err, io.EOF) {
			return flush()
		}
		if err != nil {
			return err
		}
		if uint64(batch.Len())+uint64(len(key)+len(value)) > StagingBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		if err := batch.Set(key, value, nil); err != nil {
			return err
		}
	}
}

// Walk visits borrowed key/value slices in order without collecting records.
// A visitor that retains them must copy and independently bound its memory.
func Walk(ctx context.Context, view *pebble.Snapshot, visit func([]byte, []byte) error) (returnErr error) {
	if ctx == nil || view == nil || visit == nil {
		return errors.New("snapshot walk requires context, view and visitor")
	}
	iterator, err := view.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, iterator.Close()) }()
	for iterator.First(); iterator.Valid(); iterator.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(iterator.Key(), iterator.Value()); err != nil {
			return err
		}
	}
	return iterator.Error()
}
