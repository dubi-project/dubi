package enginestate

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// LogicalValue describes the current size of one logical item. Zero is a
// retained deletion tombstone. It carries no quota settings or reservations.
type LogicalValue struct {
	Key   []byte
	Bytes uint64
}

func (v LogicalValue) Validate() error {
	if len(v.Key) == 0 || len(v.Key) > 2048 || v.Bytes > 1<<30 {
		return errors.New("invalid logical value key")
	}
	return nil
}

// PublishRecord atomically couples the current reference and its logical size.
// Runtime owns accounting indexes and background reporting; Engines describe
// the value and their business outcome only.
func (tx *Transaction) PublishRecord(key []byte, descriptor records.Descriptor, requestID string, value LogicalValue) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if err := tx.SetRecordReference(key, descriptor); err != nil {
		return err
	}
	return tx.SetLogicalUsage(key, value.Key, requestID, value.Bytes)
}

var usageIndexPrefix = []byte("data/usage-index/")

func usageIndexKey(ref RecordReference) []byte {
	h := sha256.Sum256(recordReferenceKey(ref.Descriptor.Keyspace, ref.Descriptor.Key))
	return []byte(fmt.Sprintf("data/usage-index/%020d/%x", ref.Index, h))
}

// SetLogicalUsage joins accounting to the SAME atomic Engine publication.
// The index retains only the newest size for a key, including zero tombstones.
func (tx *Transaction) SetLogicalUsage(recordKey, logicalKey []byte, requestID string, size uint64) error {
	staged, ok := tx.recordReferences[string(recordKey)]
	if !ok || staged.deleted {
		return errors.New("usage requires a staged record reference")
	}
	r := staged.reference
	u := rootstate.UsageReport{Space: tx.keyspace, Key: bytes.Clone(logicalKey), RequestID: requestID, RangeID: r.RangeID, OwnerGroupID: r.OwnerGroupID, Epoch: r.RangeEpoch, Index: r.Index, Bytes: size}
	if err := u.Validate(); err != nil {
		return err
	}
	staged.reference.Usage = &u
	tx.recordReferences[string(recordKey)] = staged
	return nil
}
func validateUsageIndex(key, value []byte, get func([]byte) ([]byte, bool, error), group string, index uint64) error {
	if !bytes.HasPrefix(value, recordReferencePrefix) || len(value) > maxLiveReferenceKeyBytes() {
		return errors.New("invalid usage index reference")
	}
	b, ok, err := get(value)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("usage index has no reference")
	}
	ref, err := decodeRecordReference(value, b, group, index)
	if err != nil {
		return err
	}
	if ref.Usage == nil || !bytes.Equal(key, usageIndexKey(ref)) {
		return errors.New("usage index differs from publication")
	}
	return nil
}

// UsagePage reads already applied metadata locally, in publication order. No
// network reads or Pack scans. The worker keeps its cursor only after Root ACK;
// restart/snapshot restore replay is harmless because reports are absolute.
func (s *State) UsagePage(after []byte, limit int) ([]rootstate.UsageReport, []byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return nil, nil, err
	}
	if limit < 1 || limit > rootstate.MaxUsageReports || len(after) > 128 {
		return nil, nil, errors.New("invalid usage page")
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: usageIndexPrefix, UpperBound: []byte("data/usage-index0")})
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	if len(after) == 0 {
		it.First()
	} else {
		it.SeekGE(append(bytes.Clone(after), 0))
	}
	out := []rootstate.UsageReport{}
	cursor := bytes.Clone(after)
	for it.Valid() && len(out) < limit {
		if err := validateUsageIndex(it.Key(), it.Value(), s.get, s.groupID, s.applied); err != nil {
			return nil, nil, err
		}
		b, _, err := s.get(it.Value())
		if err != nil {
			return nil, nil, err
		}
		var ref RecordReference
		if err = json.Unmarshal(b, &ref); err != nil {
			return nil, nil, err
		}
		out = append(out, *ref.Usage)
		cursor = bytes.Clone(it.Key())
		it.Next()
	}
	return out, cursor, it.Error()
}
