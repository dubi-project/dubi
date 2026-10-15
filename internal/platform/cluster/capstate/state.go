// Package capstate provides the persistent, capability-neutral transaction
// core for a routed DUBI data Group.
package capstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const FormatVersion = 7

var ErrRequestIDReused = errors.New("capability RequestID was reused for a different command")

var appliedKey = []byte("meta/applied")
var confKey = []byte("meta/conf")
var groupKey = []byte("meta/group")

type Outcome struct {
	Value     []byte
	Rejection string
}

// Handler owns capability payload semantics. It may only mutate state through
// Transaction. A non-empty Rejection discards all staged capability changes.
type Handler interface {
	Apply(transaction *Transaction, target router.Target, payload []byte) (Outcome, error)
}

type Result struct {
	RequestID string `json:"request_id"`
	Hash      string `json:"hash"`
	Index     uint64 `json:"index"`
	Applied   bool   `json:"applied"`
	Error     string `json:"error,omitempty"`
	Value     []byte `json:"value,omitempty"`
}

type State struct {
	mu              sync.RWMutex
	db              *pebble.DB
	groupID         string
	handler         Handler
	applied         uint64
	conf            *raftpb.ConfState
	closed          bool
	packs           packStorage
	failed          error
	commit          func(*pebble.Batch) error // nil uses pebble.Sync; fault injection in tests.
	snapshotWG      sync.WaitGroup
	recordCutActive bool
	closeDone       chan struct{}
	closeErr        error
	root            string
	restoring       *restoreFence
	restoreHook     func(string) error // Test-only streaming restore fault points.
}

var _ routedstate.Machine = (*State)(nil)

func Open(root, groupID string, handler Handler) (*State, error) {
	return open(root, groupID, handler, nil)
}

// OpenWithPacks uses the node-owned Pool for large transaction values. State
// does not own Pool; close all Groups before closing it. No location enters a
// replicated command, result or snapshot.
func OpenWithPacks(root, groupID string, handler Handler, packs *pack.Pool) (*State, error) {
	if packs == nil {
		return Open(root, groupID, handler)
	}
	return open(root, groupID, handler, packs)
}

func open(root, groupID string, handler Handler, packs packStorage) (*State, error) {
	if root == "" || handler == nil || raft.ValidateGroupID(groupID) != nil || groupID == raft.RootGroupID {
		return nil, errors.New("invalid capability state options")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	db, err := pebbleutil.Open(filepath.Join(root, "dubi-capability-state"),
		&pebble.Options{Logger: pebbleutil.QuietLogger{}})
	if err != nil {
		return nil, err
	}
	state := &State{db: db, root: root, groupID: groupID, handler: handler, packs: packs, conf: &raftpb.ConfState{}, closeDone: make(chan struct{})}
	if err := state.checkStateFormat(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := state.checkPackBinding(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if encoded, found, err := state.get(groupKey); err != nil {
		_ = db.Close()
		return nil, err
	} else if found && string(encoded) != groupID {
		_ = db.Close()
		return nil, fmt.Errorf("capability state belongs to Group %q, not %q", encoded, groupID)
	} else if !found {
		if err := db.Set(groupKey, []byte(groupID), pebble.Sync); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("bind capability state Group: %w", err)
		}
	}
	if encoded, found, err := state.get(appliedKey); err != nil {
		_ = db.Close()
		return nil, err
	} else if found {
		if len(encoded) != 8 {
			_ = db.Close()
			return nil, errors.New("invalid capability applied index")
		}
		state.applied = binary.BigEndian.Uint64(encoded)
	}
	if encoded, found, err := state.get(confKey); err != nil {
		_ = db.Close()
		return nil, err
	} else if found {
		if err := proto.Unmarshal(encoded, state.conf); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("decode capability membership: %w", err)
		}
	}
	if err := state.loadRestoreFence(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return state, nil
}

func (s *State) AppliedIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

func (s *State) GroupID() string {
	if s == nil {
		return ""
	}
	return s.groupID // Immutable from Open.
}

func (s *State) ConfState() *raftpb.ConfState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return proto.Clone(s.conf).(*raftpb.ConfState)
}

func (s *State) ApplyRange(index uint64, requestID string, descriptor rootstate.Range) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkNext(index); err != nil {
		return err
	}
	hash := commandHash("activate_range", requestID, descriptor, nil)
	batch := s.db.NewBatch()
	defer batch.Close()
	if _, found, err := s.result(requestID); err != nil {
		return err
	} else if found {
		// Preserve the original result; callers compare its stored hash.
		return s.commitIndex(batch, index)
	}
	result := Result{RequestID: requestID, Hash: hash, Index: index}
	if rootstate.ValidateRequestID(requestID) != nil || descriptor.Validate() != nil ||
		descriptor.OwnerGroupID != s.groupID || descriptor.Epoch != 1 ||
		len(descriptor.Start) != 0 || len(descriptor.End) != 0 || descriptor.State != rootstate.RangeStateActive {
		result.Error = "invalid initial Range activation"
	} else if _, found, err := s.get(fenceRangeKey(descriptor.RangeID)); err != nil {
		return err
	} else if found {
		result.Error = "RangeID is already active"
	} else if _, found, err := s.get(fenceKeyspaceKey(descriptor.Keyspace())); err != nil {
		return err
	} else if found {
		result.Error = "Keyspace already has an active Range"
	} else {
		result.Applied = true
		if err := setJSON(batch, fenceRangeKey(descriptor.RangeID), descriptor); err != nil {
			return err
		}
		if err := batch.Set(fenceKeyspaceKey(descriptor.Keyspace()), []byte(descriptor.RangeID), nil); err != nil {
			return err
		}
	}
	if err := setJSON(batch, resultKey(requestID), result); err != nil {
		return err
	}
	return s.commitIndex(batch, index)
}

func (s *State) ApplyRouted(index uint64, requestID string, target router.Target, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkNext(index); err != nil {
		return err
	}
	hash := commandHash("mutation", requestID, target, payload)
	batch := s.db.NewBatch()
	defer batch.Close()
	if _, found, err := s.result(requestID); err != nil {
		return err
	} else if found {
		// Preserve the first result while advancing the Raft apply index.
		return s.commitIndex(batch, index)
	}
	result := Result{RequestID: requestID, Hash: hash, Index: index}
	descriptor, found, err := s.fence(target.RangeID)
	if err != nil {
		return err
	}
	blocked, err := s.regionalWriteRejection()
	if err != nil {
		return err
	}
	if rootstate.ValidateRequestID(requestID) != nil || target.Validate() != nil || len(payload) == 0 {
		result.Error = "invalid routed mutation"
	} else if !found {
		result.Error = router.ErrNoRoute.Error()
	} else if err := router.CheckOwner(s.groupID, descriptor, target); err != nil {
		result.Error = err.Error()
	} else if blocked != "" {
		result.Error = blocked
	} else {
		transaction := newTransaction(s, batch, target, requestID, hash, index)
		outcome, applyErr := s.handler.Apply(transaction, target, bytes.Clone(payload))
		if applyErr != nil {
			return applyErr
		}
		if outcome.Rejection != "" {
			// Discard staged capability writes and persist only the rejection.
			batch.Reset()
			result.Error = outcome.Rejection
		} else {
			result.Applied = true
			result.Value = outcome.Value
		}
		encoded, err := encodeHandlerResult(result, MaxResultBytes)
		if errors.Is(err, ErrOutcomeTooLarge) {
			// This is a deterministic application rejection, not a storage
			// failure: do not poison an already committed Raft log entry.
			batch.Reset()
			result.Applied, result.Value, result.Error = false, nil, ErrOutcomeTooLarge.Error()
		} else {
			if err != nil {
				return err
			}
			if result.Applied {
				if err := transaction.flush(); err != nil {
					return err
				}
			}
			if err := batch.Set(resultKey(requestID), encoded, nil); err != nil {
				return err
			}
			return s.commitIndex(batch, index)
		}
	}
	if err := setJSON(batch, resultKey(requestID), result); err != nil {
		return err
	}
	return s.commitIndex(batch, index)
}

func (s *State) Advance(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkNext(index); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	return s.commitIndex(batch, index)
}

func (s *State) ApplyMembership(index uint64, conf *raftpb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkNext(index); err != nil {
		return err
	}
	if conf == nil {
		return errors.New("capability membership is nil")
	}
	encoded, err := proto.Marshal(conf)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(confKey, encoded, nil); err != nil {
		return err
	}
	var position [8]byte
	binary.BigEndian.PutUint64(position[:], index)
	if err := batch.Set(membershipIndexKey, position[:], nil); err != nil {
		return err
	}
	if err := s.commitIndex(batch, index); err != nil {
		return err
	}
	s.conf = proto.Clone(conf).(*raftpb.ConfState)
	return nil
}

func (s *State) Result(requestID string) (Result, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return Result{}, false, err
	}
	if rootstate.ValidateRequestID(requestID) != nil {
		return Result{}, false, errors.New("capability RequestID is invalid")
	}
	return s.result(requestID)
}

func (s *State) Range(rangeID string) (rootstate.Range, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return rootstate.Range{}, false, err
	}
	if rootstate.ValidateRangeID(rangeID) != nil {
		return rootstate.Range{}, false, errors.New("capability RangeID is invalid")
	}
	return s.fence(rangeID)
}

// Value is an internal local read, not an authorized or linearizable public
// read. Even this diagnostic API requires the complete Keyspace explicitly.
func (s *State) Value(keyspace rootstate.Keyspace, key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return nil, false, err
	}
	if keyspace.Validate() != nil || validateCapabilityKey(key) != nil {
		return nil, false, errors.New("capability identity/key is invalid")
	}
	return s.getValue(capabilityKey(keyspace, key))
}

func (s *State) ResultForMutation(requestID string, target router.Target, payload []byte) (Result, bool, error) {
	result, found, err := s.Result(requestID)
	if err != nil || !found {
		return result, found, err
	}
	if result.Hash != commandHash("mutation", requestID, target, payload) {
		return Result{}, true, ErrRequestIDReused
	}
	return result, true, nil
}

func (s *State) ResultForRange(requestID string, descriptor rootstate.Range) (Result, bool, error) {
	result, found, err := s.Result(requestID)
	if err != nil || !found {
		return result, found, err
	}
	if result.Hash != commandHash("activate_range", requestID, descriptor, nil) {
		return Result{}, true, ErrRequestIDReused
	}
	return result, true, nil
}

func (s *State) checkNext(index uint64) error {
	if err := s.usable(); err != nil {
		return err
	}
	if index != s.applied+1 {
		return fmt.Errorf("capability applied index gap: current %d, next %d", s.applied, index)
	}
	return nil
}

func (s *State) commitIndex(batch *pebble.Batch, index uint64) error {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], index)
	if err := batch.Set(appliedKey, encoded[:], nil); err != nil {
		return err
	}
	if err := s.commitBatch(batch); err != nil {
		return err
	}
	s.applied = index
	return nil
}

func (s *State) usable() error {
	if s.closed {
		return errors.New("capability state is closed")
	}
	if s.failed != nil {
		return fmt.Errorf("capability state needs reopen after uncertain commit: %w", s.failed)
	}
	if s.restoring != nil {
		return errors.New("capability state requires completion of its fenced snapshot restore")
	}
	return nil
}

func (s *State) commitBatch(batch *pebble.Batch) error {
	var err error
	if s.commit != nil {
		err = s.commit(batch)
	} else {
		err = batch.Commit(pebble.Sync)
	}
	if err != nil {
		s.failed = err
		return fmt.Errorf("commit capability state (outcome uncertain): %w", err)
	}
	return nil
}

func (s *State) result(requestID string) (Result, bool, error) {
	encoded, found, err := s.get(resultKey(requestID))
	if !found || err != nil {
		return Result{}, found, err
	}
	var result Result
	err = json.Unmarshal(encoded, &result)
	return result, err == nil, err
}

func (s *State) fence(rangeID string) (rootstate.Range, bool, error) {
	encoded, found, err := s.get(fenceRangeKey(rangeID))
	if !found || err != nil {
		return rootstate.Range{}, found, err
	}
	var descriptor rootstate.Range
	err = json.Unmarshal(encoded, &descriptor)
	return descriptor, err == nil, err
}

func (s *State) get(key []byte) ([]byte, bool, error) {
	return s.getBounded(key, MaxResultBytes)
}

// Inspect the borrowed Pebble value before cloning. A corrupt or older
// oversized row must not allocate an unbounded second copy on a live read.
func (s *State) getBounded(key []byte, limit int) ([]byte, bool, error) {
	value, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if len(value) > limit {
		return nil, false, errors.Join(errors.New("capability state row exceeds read limit"), closer.Close())
	}
	copy := bytes.Clone(value)
	return copy, true, closer.Close()
}

func (s *State) Close() error {
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return s.closeErr
	}
	s.closed = true
	s.mu.Unlock()
	s.snapshotWG.Wait()
	err := s.db.Close()
	s.mu.Lock()
	s.closeErr = err
	close(s.closeDone)
	s.mu.Unlock()
	return err
}

func setJSON(batch *pebble.Batch, key []byte, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return batch.Set(key, encoded, nil)
}

func commandHash(kind, requestID string, value any, payload []byte) string {
	encoded, _ := json.Marshal(struct {
		Kind      string `json:"kind"`
		RequestID string `json:"request_id"`
		Value     any    `json:"value"`
		Payload   []byte `json:"payload,omitempty"`
	}{kind, requestID, value, payload})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func fenceRangeKey(rangeID string) []byte { return []byte("data/fence/range/" + rangeID) }
func fenceKeyspaceKey(value rootstate.Keyspace) []byte {
	return []byte("data/fence/keyspace/" + value.TenantID + "/" + value.CapabilityID + "/" + value.DatasetID)
}
func capabilityKey(keyspace rootstate.Keyspace, key []byte) []byte {
	// Identity components cannot contain '/' and the final key is opaque.
	prefix := []byte("data/keyspace/" + keyspace.TenantID + "/" + keyspace.CapabilityID + "/" + keyspace.DatasetID + "/")
	return append(prefix, key...)
}
func resultKey(requestID string) []byte { return []byte("result/" + requestID) }
