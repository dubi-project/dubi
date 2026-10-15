package rootjoin

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"golang.org/x/sys/unix"
)

const LocalStateFile = "DUBI_ROOT_JOIN"
const MaxLocalStateBytes = 4 << 20

// Only absence of the join record itself has this sentinel. A present record
// referencing a missing receipt/resource is corruption, not a legacy Node.
var ErrLocalStateMissing = errors.New("local Root join record is absent")

// LocalStore durably owns one enrolled Node's Root admission/activation fence
// and latest confirmed peer cut. It creates no Raft state or storage volumes.
// The caller MUST hold the Node Store process lock for its entire lifetime.
// SaveLocalAdmission provides that lock for the offline joining workflow.
type LocalStore struct {
	mu       sync.Mutex
	root     *os.Root
	path     string
	id       identity.Identity
	storeID  string
	failed   error
	closed   bool
	closeErr error
	hook     func(string) error // Test-only publication/crash boundaries.
}

// OpenLocalStore opens only an existing enrolled Node and registered Store.
// Its expected StoreID normally comes from the already-open volume registry.
func OpenLocalStore(store, storeID string) (*LocalStore, error) {
	path, err := filepath.Abs(store)
	if err != nil || store == "" {
		return nil, errors.Join(err, errors.New("existing Node Store path required"))
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() {
		return nil, errors.Join(err, errors.New("Root join Store must be an existing physical directory"))
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*LocalStore, error) { return nil, errors.Join(err, root.Close()) }
	id, err := identity.Load(path)
	if err != nil {
		return fail(err)
	}
	if _, err := identity.NodeIdentityURI(id); err != nil || id.Origin != identity.OriginJoin {
		return fail(errors.Join(err, errors.New("bound enrolled Node required")))
	}
	report, err := volume.Inspect(path)
	if err != nil || report.StoreID != storeID || !storeName.MatchString(storeID) {
		return fail(errors.Join(err, errors.New("Root join Store identity differs from registry")))
	}
	s := &LocalStore{root: root, path: path, id: id, storeID: storeID}
	if err := s.usable(); err != nil {
		return fail(err)
	}
	return s, nil
}

// SaveLocalAdmission holds the Store lock while publishing a validated remote
// admission result. The caller must supply a view from the authenticated Join
// RPC, not manually authored routing configuration. Receipt/Node/Store bindings
// are independently rechecked. This function does not start a runtime.
func SaveLocalAdmission(store string, view View, admittedIndex uint64) (result LocalState, err error) {
	// Validate before creating even a lock in an unmanaged or symlinked path.
	if store == "" {
		return LocalState{}, errors.New("managed Node Store path required")
	}
	store, err = filepath.Abs(store)
	if err != nil {
		return LocalState{}, err
	}
	info, statErr := os.Lstat(store)
	if statErr != nil || !info.IsDir() {
		return LocalState{}, errors.Join(statErr, errors.New("managed physical Node Store required"))
	}
	id, loadErr := identity.Load(store)
	if loadErr != nil || id.Origin != identity.OriginJoin || id.ClusterID == "" {
		return LocalState{}, errors.Join(loadErr, errors.New("bound enrolled Node required"))
	}
	lock, err := storelock.Acquire(store)
	if err != nil {
		return LocalState{}, err
	}
	defer func() { err = errors.Join(err, lock.Release()) }()
	report, err := volume.Inspect(store)
	if err != nil {
		return LocalState{}, err
	}
	s, err := OpenLocalStore(store, report.StoreID)
	if err != nil {
		return LocalState{}, err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	return s.Start(view, admittedIndex)
}

// Load reaffirms file and directory durability before handing a recovery
// authorization to the caller. It is not a lock-free inspection API.
func (s *LocalStore) Load() (LocalState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return LocalState{}, err
	}
	state, _, err := s.read()
	if err != nil {
		return LocalState{}, err
	}
	if err := s.syncPublished(); err != nil {
		return LocalState{}, s.poison(err)
	}
	return state, nil
}

// Start never resets an existing activation or routing cut. Exact admission
// retries preserve the first durable record, even if the RPC response is newer.
func (s *LocalStore) Start(view View, admittedIndex uint64) (LocalState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return LocalState{}, err
	}
	receipt, err := identityenrollment.LoadReceipt(s.path)
	if err != nil {
		return LocalState{}, err
	}
	candidate, err := NewLocalState(s.id, s.storeID, receipt, view, admittedIndex)
	if err != nil {
		return LocalState{}, err
	}
	current, _, err := s.read()
	if err == nil {
		if current.GrantID != candidate.GrantID || current.RequestSHA256 != candidate.RequestSHA256 || current.AdmittedIndex != candidate.AdmittedIndex {
			return LocalState{}, errors.New("different Root admission already exists")
		}
		if err := s.syncPublished(); err != nil {
			return LocalState{}, s.poison(err)
		}
		return current, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return LocalState{}, err
	}
	if err := s.publish(candidate, nil); err != nil {
		return LocalState{}, s.poison(err)
	}
	return candidate, nil
}

func (s *LocalStore) Activate(view View, task rootstate.RootJoin) (LocalState, error) {
	return s.transition(func(state LocalState) (LocalState, error) { return state.Activate(view, task) })
}

func (s *LocalStore) Observe(view View) (LocalState, error) {
	return s.transition(func(state LocalState) (LocalState, error) { return state.Observe(view) })
}

func (s *LocalStore) transition(change func(LocalState) (LocalState, error)) (LocalState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return LocalState{}, err
	}
	current, info, err := s.read()
	if err != nil {
		return LocalState{}, err
	}
	updated, err := change(current)
	if err != nil {
		return LocalState{}, err
	}
	if err := updated.Validate(s.id, s.storeID); err != nil {
		return LocalState{}, err
	}
	old, err := json.Marshal(current)
	if err != nil {
		return LocalState{}, err
	}
	next, err := json.Marshal(updated)
	if err != nil {
		return LocalState{}, err
	}
	if bytes.Equal(old, next) {
		if err := s.syncPublished(); err != nil {
			return LocalState{}, s.poison(err)
		}
	} else if err := s.publish(updated, info); err != nil {
		return LocalState{}, s.poison(err)
	}
	return updated, nil
}

func (s *LocalStore) read() (LocalState, os.FileInfo, error) {
	file, err := s.root.OpenFile(LocalStateFile, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return LocalState{}, nil, errors.Join(ErrLocalStateMissing, err)
		}
		return LocalState{}, nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() <= 0 || info.Size() > MaxLocalStateBytes {
		return LocalState{}, nil, errors.Join(err, file.Close(), errors.New("invalid Root join state file type, permissions or size"))
	}
	encoded, readErr := io.ReadAll(io.LimitReader(file, MaxLocalStateBytes+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return LocalState{}, nil, err
	}
	if len(encoded) > MaxLocalStateBytes {
		return LocalState{}, nil, errors.New("Root join state exceeds limit")
	}
	current, err := s.root.Lstat(LocalStateFile)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(info, current) {
		return LocalState{}, nil, errors.Join(err, errors.New("Root join file replaced while reading"))
	}
	var state LocalState
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return LocalState{}, nil, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return LocalState{}, nil, errors.New("trailing Root join state document")
	}
	canonical, err := json.Marshal(state)
	if err != nil || !bytes.Equal(append(canonical, '\n'), encoded) {
		return LocalState{}, nil, errors.New("noncanonical Root join state document")
	}
	if err := state.Validate(s.id, s.storeID); err != nil {
		return LocalState{}, nil, err
	}
	receipt, err := identityenrollment.LoadReceipt(s.path)
	if err != nil || state.GrantID != receipt.GrantID || state.RequestSHA256 != receipt.RequestSHA256 {
		return LocalState{}, nil, errors.Join(err, errors.New("Root join state differs from authenticated enrollment receipt"))
	}
	if err := s.usable(); err != nil {
		return LocalState{}, nil, err
	}
	return state, info, nil
}

func (s *LocalStore) publish(state LocalState, previous os.FileInfo) error {
	encoded, err := json.Marshal(state)
	if err != nil || len(encoded)+1 > MaxLocalStateBytes {
		return errors.Join(err, errors.New("Root join state exceeds limit"))
	}
	encoded = append(encoded, '\n')
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	name := ".dubi-root-join-" + hex.EncodeToString(random[:])
	file, err := s.root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer s.root.Remove(name) // Only the exclusive temporary file owned by this call.
	if _, err := file.Write(encoded); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := s.stage("after-write"); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := s.stage("after-file-sync"); err != nil {
		return err
	}
	if err := s.usable(); err != nil {
		return err
	}
	if previous == nil {
		if err := s.root.Link(name, LocalStateFile); err != nil {
			return err
		}
	} else {
		info, err := s.root.Lstat(LocalStateFile)
		if err != nil || !info.Mode().IsRegular() || !os.SameFile(previous, info) {
			return errors.Join(err, errors.New("Root join record changed before update"))
		}
		if err := s.root.Rename(name, LocalStateFile); err != nil {
			return err
		}
	}
	if err := s.stage("after-publish"); err != nil {
		return err
	}
	if err := s.syncPublished(); err != nil {
		return err
	}
	return s.stage("after-directory-sync")
}

func (s *LocalStore) syncPublished() error {
	file, err := s.root.OpenFile(LocalStateFile, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	directory, err := s.root.Open(".")
	if err != nil {
		return err
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		return err
	}
	return s.usable()
}

func (s *LocalStore) usable() error {
	if s.closed {
		return errors.New("Root join local Store is closed")
	}
	if s.failed != nil {
		return errors.Join(errors.New("Root join publication is uncertain; reopen required"), s.failed)
	}
	held, err := s.root.Stat(".")
	if err != nil {
		return err
	}
	current, err := os.Lstat(s.path)
	if err != nil || !current.IsDir() || !os.SameFile(held, current) {
		return errors.Join(err, errors.New("Root join Store directory was replaced"))
	}
	return nil
}

func (s *LocalStore) stage(name string) error {
	if s.hook != nil {
		return s.hook(name)
	}
	return nil
}
func (s *LocalStore) poison(err error) error {
	s.failed = err
	return errors.Join(errors.New("Root join publication is uncertain; reopen required"), err)
}

func (s *LocalStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.closeErr = s.root.Close()
	return s.closeErr
}
