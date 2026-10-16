package enginestate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
)

const (
	// These are internal storage defaults, not deployment settings. The size
	// limit is independent of whether a node has a Pack backend attached.
	PackValueThreshold = 64 << 10
	MaxValueBytes      = pack.MaxPayloadSize
)

var packBindingKey = []byte("meta/pack-store")
var packRefPrefix = []byte("local/pack/")
var packRefEnd = []byte("local/pack0")
var valuePrefix = []byte("data/keyspace/")

type packStorage interface {
	StoreID() string
	Append(context.Context, pack.Record) (pack.Location, error)
	Read(pack.Location) (pack.Record, error)
}

type packBinding struct {
	Version int    `json:"version"`
	StoreID string `json:"store_id"`
}

type packReference struct {
	Version  int           `json:"version"`
	TenantID string        `json:"tenant_id"`
	Location pack.Location `json:"location"`
	Digest   [32]byte      `json:"sha256"`
}

// Inline keys and local references are mutually exclusive and changed in the
// same Pebble batch as the applied index/result. Local references are NEVER
// copied to another node by a Raft snapshot.
func referenceKey(dbKey []byte) []byte {
	return append(bytes.Clone(packRefPrefix), dbKey...)
}

func (s *State) checkPackBinding() error {
	encoded, found, err := s.get(packBindingKey)
	if err != nil {
		return err
	}
	if found {
		var binding packBinding
		if err := decodePackJSON(encoded, &binding); err != nil {
			return err
		}
		if binding.Version != 1 || s.packs == nil || binding.StoreID != s.packs.StoreID() {
			return errors.New("engine Pack state requires its original Store and supported reference format")
		}
		return nil
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: packRefPrefix, UpperBound: packRefEnd})
	if err != nil {
		return err
	}
	defer iter.Close()
	if iter.First() {
		return errors.New("engine Pack references lack a Store binding")
	}
	return iter.Error()
}

func splitValueKey(key []byte) (rootstate.Keyspace, error) {
	if !bytes.HasPrefix(key, valuePrefix) {
		return rootstate.Keyspace{}, errors.New("invalid engine value namespace")
	}
	parts := bytes.SplitN(key[len(valuePrefix):], []byte{'/'}, 4)
	if len(parts) != 4 {
		return rootstate.Keyspace{}, errors.New("invalid engine value key")
	}
	keyspace := rootstate.Keyspace{TenantID: string(parts[0]), EngineID: string(parts[1]), DatasetID: string(parts[2])}
	if keyspace.Validate() != nil || validateEngineKey(parts[3]) != nil {
		return rootstate.Keyspace{}, errors.New("invalid engine value key")
	}
	return keyspace, nil
}

// The opaque Pack key binds the logical key to this Group. An index pointing
// to another Group, engine, tenant or logical key is an error, not a miss.
func (s *State) recordKey(dbKey []byte) []byte {
	key := make([]byte, 4+len(s.groupID)+len(dbKey))
	binary.BigEndian.PutUint32(key[:4], uint32(len(s.groupID)))
	copy(key[4:], s.groupID)
	copy(key[4+len(s.groupID):], dbKey)
	return key
}

func (s *State) getValue(dbKey []byte) ([]byte, bool, error) {
	inline, present, err := s.get(dbKey)
	if err != nil {
		return nil, false, err
	}
	encoded, packed, err := s.get(referenceKey(dbKey))
	if err != nil {
		return nil, false, err
	}
	if !packed {
		return inline, present, nil
	}
	if present {
		return nil, false, errors.New("engine value has conflicting inline and Pack entries")
	}
	record, _, err := s.readReference(dbKey, encoded)
	if err != nil {
		return nil, false, err
	}
	return record.Payload, true, nil
}

func (s *State) readReference(dbKey, encoded []byte) (pack.Record, packReference, error) {
	var ref packReference
	keyspace, err := splitValueKey(dbKey)
	if err != nil {
		return pack.Record{}, ref, err
	}
	if err := decodePackJSON(encoded, &ref); err != nil {
		return pack.Record{}, ref, err
	}
	if ref.Version != 1 || ref.TenantID != keyspace.TenantID || s.packs == nil || ref.Location.StoreID != s.packs.StoreID() {
		return pack.Record{}, ref, errors.New("invalid or unavailable engine Pack reference")
	}
	record, err := s.packs.Read(ref.Location)
	if err != nil {
		return pack.Record{}, ref, fmt.Errorf("read engine Pack value: %w", err)
	}
	if record.EngineID != keyspace.EngineID || record.TenantID != keyspace.TenantID ||
		!bytes.Equal(record.Key, s.recordKey(dbKey)) || sha256.Sum256(record.Payload) != ref.Digest {
		return pack.Record{}, ref, errors.New("engine Pack reference does not match record identity or digest")
	}
	return record, ref, nil
}

func (s *State) deleteValue(batch *pebble.Batch, dbKey []byte) error {
	if err := batch.Delete(dbKey, nil); err != nil {
		return err
	}
	return batch.Delete(referenceKey(dbKey), nil)
}

func (s *State) stageValue(batch *pebble.Batch, dbKey []byte, value []byte) error {
	keyspace, err := splitValueKey(dbKey)
	if err != nil {
		return err
	}
	if s.packs == nil || len(value) < PackValueThreshold {
		if err := batch.Delete(referenceKey(dbKey), nil); err != nil {
			return err
		}
		return batch.Set(dbKey, value, nil)
	}
	// Apply is driven by a committed Raft entry, not by a client deadline.
	// A failure leaves applied unchanged and must be retried through recovery.
	loc, err := s.packs.Append(context.Background(), pack.Record{EngineID: keyspace.EngineID,
		TenantID: keyspace.TenantID, Key: s.recordKey(dbKey), Payload: value})
	if err != nil {
		return fmt.Errorf("persist engine Pack value: %w", err)
	}
	if err := batch.Delete(dbKey, nil); err != nil {
		return err
	}
	if err := setJSON(batch, referenceKey(dbKey), packReference{Version: 1, TenantID: keyspace.TenantID,
		Location: loc, Digest: sha256.Sum256(value)}); err != nil {
		return err
	}
	return setJSON(batch, packBindingKey, packBinding{Version: 1, StoreID: s.packs.StoreID()})
}

func decodePackJSON(encoded []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing Pack reference data")
	}
	return nil
}
