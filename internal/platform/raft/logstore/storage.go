package logstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	hardStateKey = []byte("meta/hard-state")
	confStateKey = []byte("meta/conf-state")
	snapshotKey  = []byte("meta/snapshot")
	baseKey      = []byte("meta/base")
	entryPrefix  = []byte("entry/")
	entryUpper   = []byte("entry0")
)

type Storage struct {
	mu sync.RWMutex

	db        *pebble.DB
	hardState *raftpb.HardState
	confState *raftpb.ConfState
	snapshot  *raftpb.Snapshot
	baseIndex uint64
	baseTerm  uint64
	lastIndex uint64
	closed    bool
}

func Open(root string) (*Storage, error) {
	db, err := pebbleutil.Open(filepath.Join(root, "dubi-raft"), &pebble.Options{Logger: pebbleutil.QuietLogger{}})
	if err != nil {
		return nil, fmt.Errorf("open Raft storage: %w", err)
	}
	storage := &Storage{
		db:        db,
		confState: &raftpb.ConfState{},
		snapshot:  &raftpb.Snapshot{},
	}
	if err := storage.recover(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return storage, nil
}

func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close Raft storage: %w", err)
	}
	return nil
}

func (s *Storage) Checkpoint(destination string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("Raft storage is closed")
	}
	if err := s.db.Checkpoint(destination, pebble.WithFlushedWAL()); err != nil {
		return fmt.Errorf("checkpoint Raft storage: %w", err)
	}
	return nil
}

func (s *Storage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, nil, errors.New("Raft storage is closed")
	}
	return cloneHardState(s.hardState), cloneConfState(s.confState), nil
}

func (s *Storage) Entries(lo, hi, maxSize uint64) ([]*raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("Raft storage is closed")
	}
	if lo <= s.baseIndex {
		return nil, raft.ErrCompacted
	}
	if hi > s.lastIndex+1 || lo > hi {
		return nil, raft.ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}
	entries := make([]*raftpb.Entry, 0, hi-lo)
	var size uint64
	for index := lo; index < hi; index++ {
		entry := new(raftpb.Entry)
		if err := getProto(s.db, entryKey(index), entry); err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil, raft.ErrUnavailable
			}
			return nil, err
		}
		entrySize := uint64(proto.Size(entry))
		if len(entries) > 0 && size+entrySize > maxSize {
			break
		}
		entries = append(entries, entry)
		size += entrySize
	}
	return entries, nil
}

func (s *Storage) Term(index uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, errors.New("Raft storage is closed")
	}
	return s.term(index)
}

func (s *Storage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, errors.New("Raft storage is closed")
	}
	return s.lastIndex, nil
}

func (s *Storage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, errors.New("Raft storage is closed")
	}
	return s.baseIndex + 1, nil
}

func (s *Storage) Snapshot() (*raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("Raft storage is closed")
	}
	return cloneSnapshot(s.snapshot), nil
}

// Save atomically persists one Ready batch before messages are sent or entries
// are applied to the state machine.
func (s *Storage) Save(hardState *raftpb.HardState, entries []*raftpb.Entry, snapshot *raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("Raft storage is closed")
	}
	if err := validateEntries(entries); err != nil {
		return err
	}

	newBaseIndex := s.baseIndex
	newBaseTerm := s.baseTerm
	newLastIndex := s.lastIndex
	newSnapshot := s.snapshot
	newConfState := s.confState
	if !raft.IsEmptySnap(snapshot) {
		metadata := snapshot.GetMetadata()
		if metadata.GetIndex() <= s.snapshot.GetMetadata().GetIndex() {
			return raft.ErrSnapOutOfDate
		}
		newBaseIndex = metadata.GetIndex()
		newBaseTerm = metadata.GetTerm()
		if newLastIndex < newBaseIndex {
			newLastIndex = newBaseIndex
		}
		newSnapshot = cloneSnapshot(snapshot)
		newConfState = cloneConfState(metadata.GetConfState())
	}

	trimmed := entries
	for len(trimmed) > 0 && trimmed[0].GetIndex() <= newBaseIndex {
		trimmed = trimmed[1:]
	}
	if len(trimmed) > 0 {
		first := trimmed[0].GetIndex()
		if first > newLastIndex+1 {
			return fmt.Errorf("Raft entries contain a gap: first new index %d, last stored index %d", first, newLastIndex)
		}
		newLastIndex = trimmed[len(trimmed)-1].GetIndex()
	}
	if !raft.IsEmptyHardState(hardState) {
		if s.hardState != nil && hardState.GetTerm() < s.hardState.GetTerm() {
			return fmt.Errorf("Raft term would move backward from %d to %d", s.hardState.GetTerm(), hardState.GetTerm())
		}
		if s.hardState != nil && hardState.GetCommit() < s.hardState.GetCommit() {
			return fmt.Errorf("Raft commit would move backward from %d to %d", s.hardState.GetCommit(), hardState.GetCommit())
		}
		if hardState.GetCommit() > newLastIndex {
			return fmt.Errorf("Raft commit %d exceeds last index %d", hardState.GetCommit(), newLastIndex)
		}
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if !raft.IsEmptySnap(snapshot) {
		if err := setProto(batch, snapshotKey, snapshot); err != nil {
			return err
		}
		if err := setProto(batch, confStateKey, newConfState); err != nil {
			return err
		}
		if err := batch.Set(baseKey, encodeBase(newBaseIndex, newBaseTerm), nil); err != nil {
			return err
		}
		if err := batch.DeleteRange(entryPrefix, entryKeyAfter(newBaseIndex), nil); err != nil {
			return err
		}
	}
	if len(trimmed) > 0 {
		if err := batch.DeleteRange(entryKey(trimmed[0].GetIndex()), entryUpper, nil); err != nil {
			return err
		}
		for _, entry := range trimmed {
			if err := setProto(batch, entryKey(entry.GetIndex()), entry); err != nil {
				return err
			}
		}
	}
	if !raft.IsEmptyHardState(hardState) {
		if err := setProto(batch, hardStateKey, hardState); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit Raft Ready: %w", err)
	}
	s.baseIndex = newBaseIndex
	s.baseTerm = newBaseTerm
	s.lastIndex = newLastIndex
	s.snapshot = newSnapshot
	s.confState = newConfState
	if !raft.IsEmptyHardState(hardState) {
		s.hardState = cloneHardState(hardState)
	}
	return nil
}

func (s *Storage) SaveConfState(confState *raftpb.ConfState) error {
	if confState == nil {
		return errors.New("Raft ConfState must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("Raft storage is closed")
	}
	if err := s.setProtoSync(confStateKey, confState); err != nil {
		return err
	}
	s.confState = cloneConfState(confState)
	return nil
}

func (s *Storage) Compact(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("Raft storage is closed")
	}
	if index <= s.baseIndex {
		return raft.ErrCompacted
	}
	if index > s.lastIndex {
		return raft.ErrUnavailable
	}
	term, err := s.term(index)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.DeleteRange(entryPrefix, entryKeyAfter(index), nil); err != nil {
		return err
	}
	if err := batch.Set(baseKey, encodeBase(index, term), nil); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("compact Raft log: %w", err)
	}
	s.baseIndex = index
	s.baseTerm = term
	return nil
}

func (s *Storage) recover() error {
	hardState := new(raftpb.HardState)
	if err := getProto(s.db, hardStateKey, hardState); err == nil {
		s.hardState = hardState
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	if err := getProto(s.db, confStateKey, s.confState); err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	if err := getProto(s.db, snapshotKey, s.snapshot); err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	encodedBase, closer, err := s.db.Get(baseKey)
	if err == nil {
		if len(encodedBase) != 16 {
			_ = closer.Close()
			return fmt.Errorf("invalid Raft base metadata length %d", len(encodedBase))
		}
		s.baseIndex = binary.BigEndian.Uint64(encodedBase[:8])
		s.baseTerm = binary.BigEndian.Uint64(encodedBase[8:])
		if err := closer.Close(); err != nil {
			return err
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	s.lastIndex = s.baseIndex
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: entryPrefix, UpperBound: entryUpper})
	if err != nil {
		return err
	}
	if iterator.Last() {
		index, err := decodeEntryKey(iterator.Key())
		if err != nil {
			_ = iterator.Close()
			return err
		}
		if index > s.lastIndex {
			s.lastIndex = index
		}
	}
	if err := iterator.Close(); err != nil {
		return err
	}
	if s.hardState != nil && s.hardState.GetCommit() > s.lastIndex {
		return fmt.Errorf("persisted Raft commit %d exceeds last index %d", s.hardState.GetCommit(), s.lastIndex)
	}
	return nil
}

func (s *Storage) term(index uint64) (uint64, error) {
	if index < s.baseIndex {
		return 0, raft.ErrCompacted
	}
	if index == s.baseIndex {
		return s.baseTerm, nil
	}
	if index > s.lastIndex {
		return 0, raft.ErrUnavailable
	}
	entry := new(raftpb.Entry)
	if err := getProto(s.db, entryKey(index), entry); err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, raft.ErrUnavailable
		}
		return 0, err
	}
	return entry.GetTerm(), nil
}

func (s *Storage) setProtoSync(key []byte, value proto.Message) error {
	encoded, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	return s.db.Set(key, encoded, pebble.Sync)
}

func validateEntries(entries []*raftpb.Entry) error {
	for index, entry := range entries {
		if entry == nil || entry.GetIndex() == 0 {
			return errors.New("Raft entry index must be non-zero")
		}
		if index > 0 && entry.GetIndex() != entries[index-1].GetIndex()+1 {
			return fmt.Errorf("Raft entries are not contiguous at index %d", entry.GetIndex())
		}
	}
	return nil
}

func entryKey(index uint64) []byte {
	key := make([]byte, len(entryPrefix)+8)
	copy(key, entryPrefix)
	binary.BigEndian.PutUint64(key[len(entryPrefix):], index)
	return key
}

func entryKeyAfter(index uint64) []byte {
	if index == math.MaxUint64 {
		return entryUpper
	}
	return entryKey(index + 1)
}

func decodeEntryKey(key []byte) (uint64, error) {
	if len(key) != len(entryPrefix)+8 {
		return 0, fmt.Errorf("invalid Raft entry key length %d", len(key))
	}
	return binary.BigEndian.Uint64(key[len(entryPrefix):]), nil
}

func encodeBase(index, term uint64) []byte {
	encoded := make([]byte, 16)
	binary.BigEndian.PutUint64(encoded[:8], index)
	binary.BigEndian.PutUint64(encoded[8:], term)
	return encoded
}

func setProto(batch *pebble.Batch, key []byte, value proto.Message) error {
	// Encode directly into Pebble's batch representation. Raft entries can
	// carry multi-megabyte metadata commands; marshaling into a temporary slice
	// and then passing it to Batch.Set made two full copies before the WAL write.
	// SetDeferred preserves the exact protobuf bytes (and therefore the existing
	// on-disk format) while removing that transient allocation and copy.
	encodedSize := proto.Size(value)
	operation := batch.SetDeferred(len(key), encodedSize)
	copy(operation.Key, key)
	encoded, err := (proto.MarshalOptions{}).MarshalAppend(operation.Value[:0], value)
	if err != nil {
		return err
	}
	if len(encoded) != encodedSize {
		return fmt.Errorf("encoded protobuf size %d differs from planned size %d", len(encoded), encodedSize)
	}
	if encodedSize > 0 && &encoded[0] != &operation.Value[0] {
		return errors.New("protobuf encoder did not use deferred Raft batch storage")
	}
	return operation.Finish()
}

func getProto(db *pebble.DB, key []byte, target proto.Message) error {
	encoded, closer, err := db.Get(key)
	if err != nil {
		return err
	}
	defer func() { _ = closer.Close() }()
	if err := proto.Unmarshal(encoded, target); err != nil {
		return fmt.Errorf("decode Raft record %q: %w", key, err)
	}
	return nil
}

func cloneHardState(value *raftpb.HardState) *raftpb.HardState {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*raftpb.HardState)
}

func cloneConfState(value *raftpb.ConfState) *raftpb.ConfState {
	if value == nil {
		return &raftpb.ConfState{}
	}
	return proto.Clone(value).(*raftpb.ConfState)
}

func cloneSnapshot(value *raftpb.Snapshot) *raftpb.Snapshot {
	if value == nil {
		return &raftpb.Snapshot{}
	}
	return proto.Clone(value).(*raftpb.Snapshot)
}

var _ raft.Storage = (*Storage)(nil)
