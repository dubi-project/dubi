package capstate

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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
	GroupID string          `json:"group_id"`
	Applied uint64          `json:"applied"`
	Conf    []byte          `json:"conf"`
	Entries []snapshotEntry `json:"entries"`
}

func (s *State) Snapshot() ([]byte, uint64, *raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, 0, nil, errors.New("capability state is closed")
	}
	conf, err := proto.Marshal(s.conf)
	if err != nil {
		return nil, 0, nil, err
	}
	image := snapshotImage{Version: FormatVersion, GroupID: s.groupID, Applied: s.applied, Conf: conf}
	view := s.db.NewSnapshot()
	defer view.Close()
	iterator, err := view.NewIter(nil)
	if err != nil {
		return nil, 0, nil, err
	}
	defer iterator.Close()
	for iterator.First(); iterator.Valid(); iterator.Next() {
		if !bytes.HasPrefix(iterator.Key(), []byte("data/")) && !bytes.HasPrefix(iterator.Key(), []byte("result/")) {
			continue
		}
		image.Entries = append(image.Entries, snapshotEntry{
			Key: bytes.Clone(iterator.Key()), Value: bytes.Clone(iterator.Value())})
	}
	if err := iterator.Error(); err != nil {
		return nil, 0, nil, err
	}
	encoded, err := json.Marshal(image)
	return encoded, s.applied, proto.Clone(s.conf).(*raftpb.ConfState), err
}

func (s *State) Restore(index uint64, encoded []byte, conf *raftpb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("capability state is closed")
	}
	if conf == nil || index <= s.applied {
		return errors.New("capability snapshot is stale or lacks membership")
	}
	var image snapshotImage
	if err := json.Unmarshal(encoded, &image); err != nil {
		return fmt.Errorf("decode capability snapshot: %w", err)
	}
	storedConf := new(raftpb.ConfState)
	if err := proto.Unmarshal(image.Conf, storedConf); err != nil {
		return fmt.Errorf("decode capability snapshot membership: %w", err)
	}
	if image.Version != FormatVersion || image.GroupID != s.groupID || image.Applied != index ||
		!proto.Equal(storedConf, conf) {
		return errors.New("capability snapshot metadata does not match Raft Group")
	}
	seen := make(map[string]bool, len(image.Entries))
	for _, entry := range image.Entries {
		key := string(entry.Key)
		if seen[key] || !(strings.HasPrefix(key, "data/") || strings.HasPrefix(key, "result/")) {
			return errors.New("capability snapshot contains invalid or duplicate key")
		}
		seen[key] = true
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
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("install capability snapshot: %w", err)
	}
	s.applied = index
	s.conf = proto.Clone(conf).(*raftpb.ConfState)
	return nil
}
