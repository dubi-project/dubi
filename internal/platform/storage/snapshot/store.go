package snapshot

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/storage/diskpressure"
	"golang.org/x/sys/unix"
)

const storeLockName = ".snapshot-lock"

// Store owns immutable artifacts in an existing, node-owned directory. Its
// caller initializes and registers that directory; OpenStore never recreates
// a missing directory. Completed or crash-orphaned artifacts are not pruned.
// One writer/importer operates at a time; opened readers own independent FDs.
type Store struct {
	mu         sync.RWMutex
	writers    chan struct{}
	path       string
	root       *os.Root
	info       os.FileInfo
	lock       *os.File
	disk       *diskpressure.Guard
	closed     bool
	hook       func(string) error // Test-only durable-publication fault points.
	nodeRoot   *os.Root
	ownerCheck func() error
}

func OpenStore(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("snapshot Store path is empty")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(absolute)
	if err != nil {
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			root.Close()
		}
	}()
	info, err := root.Stat(".")
	if err != nil {
		return nil, err
	}
	lock, err := root.OpenFile(storeLockName, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() {
		if failed {
			lock.Close()
		}
	}()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock snapshot Store: %w", err)
	}
	disk, err := diskpressure.Open(absolute)
	if err != nil {
		return nil, err
	}
	s := &Store{path: absolute, root: root, info: info, lock: lock, disk: disk, writers: make(chan struct{}, 1)}
	if err := s.check(); err != nil {
		disk.Close()
		return nil, err
	}
	failed = false
	return s, nil
}

func (s *Store) check() error {
	if s.closed {
		return errors.New("snapshot Store is closed")
	}
	info, err := os.Lstat(s.path)
	if err != nil || !info.IsDir() || !os.SameFile(info, s.info) {
		return errors.Join(err, errors.New("snapshot Store directory changed or disappeared"))
	}
	current, err := s.root.Lstat(storeLockName)
	if err != nil {
		return err
	}
	held, err := s.lock.Stat()
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(current, held) {
		return errors.Join(err, errors.New("snapshot Store lock was replaced"))
	}
	if s.ownerCheck != nil {
		return s.ownerCheck()
	}
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var parentErr error
	if s.nodeRoot != nil {
		parentErr = s.nodeRoot.Close()
	}
	return errors.Join(s.disk.Close(), s.lock.Close(), s.root.Close(), parentErr)
}

// Create calls write with a bounded encoder and publishes only after Finish,
// buffered writes, file fsync and directory fsync succeed. The callback must
// not re-enter Store. Disk headroom is checked throughout, not merely at start;
// this is not a shared reservation against unrelated disk writers.
func (s *Store) Create(ctx context.Context, header Header, limit uint64, write func(*Encoder) error) (Reference, error) {
	if ctx == nil || write == nil {
		return Reference{}, errors.New("snapshot creation requires context and writer")
	}
	if err := header.Validate(); err != nil {
		return Reference{}, err
	}
	if limit == 0 || limit > MaxStreamBytes {
		return Reference{}, errors.New("invalid snapshot creation bound")
	}
	if err := s.acquireWriter(ctx); err != nil {
		return Reference{}, err
	}
	defer func() { <-s.writers }()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.check(); err != nil {
		return Reference{}, err
	}
	file, pending, err := s.temporary(ctx)
	if err != nil {
		return Reference{}, err
	}
	defer func() { file.Close(); _ = s.root.Remove(pending) }()
	hasher := sha256.New()
	writer := bufio.NewWriterSize(&guardedWriter{ctx: ctx, disk: s.disk, writer: io.MultiWriter(file, hasher)}, 256<<10)
	encoder, err := NewEncoder(writer, header, limit)
	if err != nil {
		return Reference{}, err
	}
	if err := write(encoder); err != nil {
		return Reference{}, err
	}
	if err := encoder.Finish(); err != nil {
		return Reference{}, err
	}
	if err := writer.Flush(); err != nil {
		return Reference{}, err
	}
	ref := Reference{Version: StreamVersion, GroupID: header.GroupID, Index: header.Index,
		Size: encoder.Written(), SHA256: hex.EncodeToString(hasher.Sum(nil))}
	if err := s.publish(ctx, file, pending, ref); err != nil {
		return Reference{}, err
	}
	return ref, nil
}

// Import validates bounded framing, the complete SHA-256 and exact EOF before
// publishing a remote artifact. It never interprets a remote path or overwrites
// an existing artifact. Application semantic validation is a later, separate
// restore gate; a successful Import is not a Raft installation ACK.
func (s *Store) Import(ctx context.Context, ref Reference, source io.Reader) error {
	if ctx == nil || source == nil {
		return errors.New("snapshot import requires context and reader")
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	if err := s.acquireWriter(ctx); err != nil {
		return err
	}
	defer func() { <-s.writers }()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.check(); err != nil {
		return err
	}
	file, pending, err := s.temporary(ctx)
	if err != nil {
		return err
	}
	defer func() { file.Close(); _ = s.root.Remove(pending) }()
	hasher := sha256.New()
	writer := bufio.NewWriterSize(&guardedWriter{ctx: ctx, disk: s.disk, writer: io.MultiWriter(file, hasher)}, 256<<10)
	decoder, err := NewDecoder(io.TeeReader(&contextReader{ctx: ctx, reader: source}, writer), ref.Size)
	if err != nil {
		return err
	}
	header := decoder.Header()
	if header.GroupID != ref.GroupID || header.Index != ref.Index {
		return errors.New("snapshot reference does not match stream cut")
	}
	for {
		_, _, err := decoder.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	var trailing [1]byte
	if n, err := (&contextReader{ctx: ctx, reader: source}).Read(trailing[:]); n != 0 || !errors.Is(err, io.EOF) {
		// Keep cancellation/authorization causes from a streamed source. Losing
		// them here would turn a permanent denial into a retryable IO failure.
		return errors.Join(err, errors.New("snapshot import lacks exact EOF"))
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if hex.EncodeToString(hasher.Sum(nil)) != ref.SHA256 {
		return errors.New("snapshot import SHA-256 mismatch")
	}
	return s.publish(ctx, file, pending, ref)
}

// Serialize publication without blocking readers of immutable, already
// published files. Waiting creators/importers honor cancellation, and Close
// still waits for the active writer's directory handles to be released.
func (s *Store) acquireWriter(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.writers <- struct{}{}:
		return nil
	}
}

func (s *Store) temporary(ctx context.Context) (*os.File, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if err := s.disk.Check(); err != nil {
		return nil, "", err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, "", err
	}
	name := ".pending-" + hex.EncodeToString(id[:])
	file, err := s.root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	return file, name, err
}

func (s *Store) publish(ctx context.Context, file *os.File, pending string, ref Reference) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil || info.Size() < 0 || uint64(info.Size()) != ref.Size {
		return errors.Join(err, errors.New("snapshot file size differs from encoded size"))
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := s.stage("after-file-sync"); err != nil {
		return err
	}
	if err := s.check(); err != nil {
		return err
	}
	// Hard-link publication is atomic and never replaces a pre-existing name.
	// The pending link is removed separately; crashes may leave it behind.
	if err := s.root.Link(pending, ref.filename()); errors.Is(err, os.ErrExist) {
		reader, openErr := s.open(ref)
		if openErr != nil {
			return openErr
		}
		_, verifyErr := io.Copy(io.Discard, &contextReader{ctx: ctx, reader: reader})
		if err := errors.Join(verifyErr, reader.Close()); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if err := s.stage("after-link"); err != nil {
		return err
	}
	directory, err := s.root.Open(".")
	if err != nil {
		return err
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		return err
	}
	if err := s.stage("after-directory-sync"); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.check()
}

func (s *Store) stage(name string) error {
	if s.hook != nil {
		return s.hook(name)
	}
	return nil
}

// Open returns a streaming verifier. Read to completion before trusting the
// image: checksum failure may be returned alongside the last bytes. Close
// before completion returns an error rather than claiming verification.
func (s *Store) Open(ref Reference) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	return s.open(ref)
}

func (s *Store) open(ref Reference) (io.ReadCloser, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	info, err := s.root.Lstat(ref.filename())
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != ref.Size {
		return nil, errors.New("snapshot artifact is not a regular file of the expected size")
	}
	file, err := s.root.Open(ref.filename())
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		file.Close()
		return nil, errors.Join(err, errors.New("snapshot artifact changed while opening"))
	}
	decoder, err := NewDecoder(file, ref.Size)
	if err != nil {
		file.Close()
		return nil, err
	}
	header := decoder.Header()
	if header.GroupID != ref.GroupID || header.Index != ref.Index {
		file.Close()
		return nil, errors.New("snapshot reference does not match artifact cut")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	return &verifiedReader{file: file, ref: ref, hash: sha256.New()}, nil
}

type verifiedReader struct {
	file     *os.File
	ref      Reference
	hash     hash.Hash
	read     uint64
	verified bool
	failed   error
	closed   bool
}

func (r *verifiedReader) Read(data []byte) (int, error) {
	if r.closed {
		return 0, os.ErrClosed
	}
	if r.failed != nil {
		return 0, r.failed
	}
	if r.verified {
		return 0, io.EOF
	}
	if uint64(len(data)) > r.ref.Size-r.read {
		data = data[:int(r.ref.Size-r.read)]
	}
	n, err := r.file.Read(data)
	_, _ = r.hash.Write(data[:n])
	r.read += uint64(n)
	if r.read == r.ref.Size {
		if hex.EncodeToString(r.hash.Sum(nil)) != r.ref.SHA256 {
			r.failed = errors.New("snapshot artifact SHA-256 mismatch")
			return n, r.failed
		}
		r.verified = true
	} else if errors.Is(err, io.EOF) {
		r.failed = io.ErrUnexpectedEOF
		return n, r.failed
	}
	if err != nil && !errors.Is(err, io.EOF) {
		r.failed = err
	}
	return n, err
}

func (r *verifiedReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	err := r.failed
	if !r.verified && err == nil {
		err = errors.New("snapshot artifact was not fully verified")
	}
	return errors.Join(err, r.file.Close())
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(data)
}

type guardedWriter struct {
	ctx        context.Context
	disk       *diskpressure.Guard
	writer     io.Writer
	sinceCheck uint64
}

func (w *guardedWriter) Write(data []byte) (int, error) {
	total := 0
	for len(data) > 0 {
		if err := w.ctx.Err(); err != nil {
			return total, err
		}
		if w.sinceCheck == 0 || w.sinceCheck >= 4<<20 {
			if err := w.disk.Check(); err != nil {
				return total, err
			}
			w.sinceCheck = 0
		}
		chunk := data[:min(len(data), 256<<10)]
		n, err := w.writer.Write(chunk)
		if n < 0 || n > len(chunk) {
			return total, errors.New("snapshot writer returned invalid count")
		}
		total += n
		w.sinceCheck += uint64(n)
		if err != nil {
			return total, err
		}
		if n != len(chunk) {
			return total, io.ErrShortWrite
		}
		data = data[n:]
	}
	return total, nil
}
