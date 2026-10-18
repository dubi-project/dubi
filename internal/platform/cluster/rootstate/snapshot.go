package rootstate

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type snapshotEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type snapshotImage struct {
	Version int             `json:"version"`
	Applied uint64          `json:"applied"`
	Conf    []byte          `json:"conf"`
	Entries []snapshotEntry `json:"entries"`
}

const maxSnapshotBytes = 64 << 20

// The inline image is already bounded and decoded. Reference its values rather
// than copying them into a second database; share the file snapshot validator.
type inlineRootView map[string][]byte
type inlineRootCloser struct{}

func (inlineRootCloser) Close() error { return nil }
func (v inlineRootView) Get(key []byte) ([]byte, io.Closer, error) {
	value, found := v[string(key)]
	if !found {
		return nil, nil, pebble.ErrNotFound
	}
	return value, inlineRootCloser{}, nil
}

func (s *State) Snapshot() ([]byte, uint64, *raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return nil, 0, nil, err
	}
	conf, err := proto.Marshal(s.conf)
	if err != nil {
		return nil, 0, nil, err
	}
	image := snapshotImage{Version: FormatVersion, Applied: s.applied, Conf: conf}
	view := s.db.NewSnapshot()
	defer view.Close()
	iterator, err := view.NewIter(nil)
	if err != nil {
		return nil, 0, nil, err
	}
	defer iterator.Close()
	total := uint64(0)
	for iterator.First(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		if !bytes.HasPrefix(key, []byte("data/")) && !bytes.HasPrefix(key, []byte("result/")) {
			continue
		}
		keySize, valueSize := uint64(len(key)), uint64(len(iterator.Value()))
		if keySize > maxSnapshotBytes || valueSize > maxSnapshotBytes || keySize+valueSize+32 > maxSnapshotBytes-total {
			return nil, 0, nil, errors.New("Root snapshot exceeds inline limit")
		}
		total += keySize + valueSize + 32
		if err := validateStreamEntry(view, s.applied, key, iterator.Value()); err != nil {
			return nil, 0, nil, err
		}
		image.Entries = append(image.Entries, snapshotEntry{Key: bytes.Clone(key), Value: bytes.Clone(iterator.Value())})
	}
	if err := iterator.Error(); err != nil {
		return nil, 0, nil, err
	}
	encoded, err := json.Marshal(image)
	if len(encoded) > maxSnapshotBytes {
		return nil, 0, nil, errors.New("Root snapshot exceeds inline limit")
	}
	return encoded, s.applied, proto.Clone(s.conf).(*raftpb.ConfState), err
}

func (s *State) Restore(index uint64, encoded []byte, conf *raftpb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if conf == nil || index <= s.applied {
		return errors.New("Root snapshot is stale or lacks membership")
	}
	var image snapshotImage
	if len(encoded) > maxSnapshotBytes {
		return errors.New("Root snapshot exceeds inline limit")
	}
	if err := decodeStreamJSON(encoded, &image); err != nil {
		return fmt.Errorf("decode Root snapshot: %w", err)
	}
	storedConf := new(raftpb.ConfState)
	if err := proto.Unmarshal(image.Conf, storedConf); err != nil {
		return fmt.Errorf("decode Root snapshot membership: %w", err)
	}
	if image.Version != FormatVersion || image.Applied != index || !proto.Equal(storedConf, conf) {
		return fmt.Errorf("Root snapshot metadata does not match Raft: version=%d applied=%d index=%d stored_conf=%v raft_conf=%v", image.Version, image.Applied, index, storedConf, conf)
	}
	view := make(inlineRootView, len(image.Entries))
	for _, entry := range image.Entries {
		key := string(entry.Key)
		if _, seen := view[key]; seen || !(strings.HasPrefix(key, "data/") || strings.HasPrefix(key, "result/")) || len(entry.Value) == 0 {
			return errors.New("Root snapshot contains invalid or duplicate key")
		}
		view[key] = entry.Value
	}
	for _, entry := range image.Entries {
		if err := validateStreamEntry(view, index, entry.Key, entry.Value); err != nil {
			return err
		}
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange([]byte("data/"), []byte("data0"), nil); err != nil {
		return err
	}
	if err := batch.DeleteRange([]byte("result/"), []byte("result0"), nil); err != nil {
		return err
	}
	for _, entry := range image.Entries {
		if err := batch.Set(entry.Key, entry.Value, nil); err != nil {
			return err
		}
	}
	confBytes, err := proto.Marshal(conf)
	if err != nil {
		return err
	}
	if err := batch.Set(confKey, confBytes, nil); err != nil {
		return err
	}
	var applied [8]byte
	binary.BigEndian.PutUint64(applied[:], index)
	if err := batch.Set(appliedKey, applied[:], nil); err != nil {
		return err
	}
	if err := s.commitBatch(batch); err != nil {
		return fmt.Errorf("install Root snapshot: %w", err)
	}
	s.applied = index
	s.conf = proto.Clone(conf).(*raftpb.ConfState)
	return nil
}
