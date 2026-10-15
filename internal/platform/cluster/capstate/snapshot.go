package capstate

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type snapshotEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
	// Attribution only; physical Location is always reconstructed locally.
	TenantID string `json:"tenant_id,omitempty"`
}

const maxSnapshotBytes = 64 << 20

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
	if err := s.usable(); err != nil {
		return nil, 0, nil, err
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
	total := 0
	for iterator.First(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		entry := snapshotEntry{Key: bytes.Clone(key), Value: bytes.Clone(iterator.Value())}
		if bytes.HasPrefix(key, packRefPrefix) {
			dbKey := key[len(packRefPrefix):]
			if _, present, err := s.get(dbKey); err != nil || present {
				return nil, 0, nil, errors.Join(err, errors.New("conflicting capability snapshot value"))
			}
			record, ref, err := s.readReference(dbKey, iterator.Value())
			if err != nil {
				return nil, 0, nil, err
			}
			entry = snapshotEntry{Key: bytes.Clone(dbKey), Value: record.Payload, TenantID: ref.TenantID}
		} else if !bytes.HasPrefix(key, []byte("data/")) && !bytes.HasPrefix(key, []byte("result/")) {
			continue
		}
		// Attribution comes from the logical key, not the local placement. An
		// inline-only node and a Pack-backed node produce the same image.
		if bytes.HasPrefix(entry.Key, valuePrefix) {
			keyspace, err := splitValueKey(entry.Key)
			if err != nil {
				return nil, 0, nil, err
			}
			entry.TenantID = keyspace.TenantID
		} else if bytes.HasPrefix(entry.Key, recordReferencePrefix) {
			keyspace, _, err := splitRecordReferenceKey(entry.Key)
			if err != nil {
				return nil, 0, nil, err
			}
			entry.TenantID = keyspace.TenantID
		} else if bytes.HasPrefix(entry.Key, localConfirmationPrefix) {
			keyspace, _, err := splitLocalConfirmationKey(entry.Key)
			if err != nil {
				return nil, 0, nil, err
			}
			entry.TenantID = keyspace.TenantID
		} else if bytes.HasPrefix(entry.Key, geoCoveragePrefix) {
			keyspace, _, err := splitGeoCoverageKey(entry.Key)
			if err != nil {
				return nil, 0, nil, err
			}
			entry.TenantID = keyspace.TenantID
		} else if bytes.HasPrefix(entry.Key, geoMetadataPrefix) {
			keyspace, _, err := splitGeoMetadataKey(entry.Key)
			if err != nil {
				return nil, 0, nil, err
			}
			entry.TenantID = keyspace.TenantID
		}
		total += len(entry.Key) + len(entry.Value) + len(entry.TenantID)
		if total > maxSnapshotBytes {
			return nil, 0, nil, errors.New("capability snapshot exceeds inline size limit")
		}
		image.Entries = append(image.Entries, entry)
	}
	if err := iterator.Error(); err != nil {
		return nil, 0, nil, err
	}
	if err := s.validateSnapshotEntries(image); err != nil {
		return nil, 0, nil, err
	}
	sort.Slice(image.Entries, func(i, j int) bool { return bytes.Compare(image.Entries[i].Key, image.Entries[j].Key) < 0 })
	encoded, err := json.Marshal(image)
	if len(encoded) > maxSnapshotBytes {
		return nil, 0, nil, errors.New("capability snapshot exceeds inline size limit")
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
		return errors.New("capability snapshot is stale or lacks membership")
	}
	var image snapshotImage
	if len(encoded) > maxSnapshotBytes {
		return errors.New("capability snapshot exceeds inline size limit")
	}
	if err := decodePackJSON(encoded, &image); err != nil {
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
	if err := s.validateSnapshotEntries(image); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange([]byte("data/"), []byte("data0"), nil); err != nil {
		return err
	}
	if err := batch.DeleteRange([]byte("result/"), []byte("result0"), nil); err != nil {
		return err
	}
	if err := batch.DeleteRange(packRefPrefix, packRefEnd, nil); err != nil {
		return err
	}
	if err := batch.Delete(packBindingKey, nil); err != nil {
		return err
	}
	for _, entry := range image.Entries {
		if bytes.HasPrefix(entry.Key, valuePrefix) {
			if err := s.stageValue(batch, entry.Key, entry.Value); err != nil {
				return err
			}
		} else if err := batch.Set(entry.Key, entry.Value, nil); err != nil {
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
		return fmt.Errorf("install capability snapshot: %w", err)
	}
	s.applied = index
	s.conf = proto.Clone(conf).(*raftpb.ConfState)
	return nil
}

// Validate the entire logical image before any Pack append or state change.
// Values must be scoped to a matching Range/Keyspace fence pair in this Group.
func (s *State) validateSnapshotEntries(image snapshotImage) error {
	seen := make(map[string][]byte, len(image.Entries))
	for _, entry := range image.Entries {
		key := string(entry.Key)
		if _, exists := seen[key]; exists {
			return errors.New("capability snapshot contains duplicate key")
		}
		seen[key] = entry.Value
	}
	ranges := make(map[string]rootstate.Keyspace)
	keyspaces := make(map[rootstate.Keyspace]string)
	values := make(map[rootstate.Keyspace]bool)
	for _, entry := range image.Entries {
		key := string(entry.Key)
		if !bytes.HasPrefix(entry.Key, valuePrefix) && !bytes.HasPrefix(entry.Key, recordReferencePrefix) && !bytes.HasPrefix(entry.Key, localConfirmationPrefix) && !bytes.HasPrefix(entry.Key, geoCoveragePrefix) && !bytes.HasPrefix(entry.Key, geoMetadataPrefix) && entry.TenantID != "" {
			return errors.New("capability snapshot attributes metadata to a Tenant")
		}
		switch {
		case bytes.Equal(entry.Key, regionFenceKey), bytes.Equal(entry.Key, membershipIndexKey), bytes.Equal(entry.Key, regionHandoffKey):
			// The aggregate validator below checks both records and their result.
		case bytes.HasPrefix(entry.Key, valuePrefix):
			keyspace, err := splitValueKey(entry.Key)
			if err != nil || len(entry.Value) > MaxValueBytes || entry.TenantID != keyspace.TenantID {
				return errors.New("capability snapshot contains invalid value key, size or attribution")
			}
			values[keyspace] = true
		case bytes.HasPrefix(entry.Key, recordReferencePrefix):
			ref, err := decodeRecordReference(entry.Key, entry.Value, s.groupID, image.Applied)
			if err != nil || entry.TenantID != ref.Descriptor.Keyspace.TenantID {
				return errors.Join(err, errors.New("invalid snapshot record reference attribution"))
			}
			if err := validateRecordReferenceLinks(ref, func(key []byte) ([]byte, bool, error) { value, ok := seen[string(key)]; return value, ok, nil }); err != nil {
				return err
			}
		case bytes.HasPrefix(entry.Key, localConfirmationPrefix):
			record, err := decodeLocalConfirmation(entry.Key, entry.Value, s.groupID, image.Applied)
			if err != nil || entry.TenantID != record.Claim.Target.Keyspace.TenantID {
				return errors.Join(err, errors.New("invalid snapshot local confirmation attribution"))
			}
			if err := validateLocalConfirmationLinks(record, func(key []byte) ([]byte, bool, error) { value, ok := seen[string(key)]; return value, ok, nil }, s.groupID, image.Applied); err != nil {
				return err
			}
		case bytes.HasPrefix(entry.Key, geoCoveragePrefix):
			record, err := decodeGeoCoverage(entry.Key, entry.Value, s.groupID, image.Applied)
			if err != nil || entry.TenantID != record.Claim.Target.Keyspace.TenantID {
				return errors.Join(err, errors.New("invalid snapshot geo coverage attribution"))
			}
			if err := validateGeoCoverageLinks(record, func(key []byte) ([]byte, bool, error) { value, ok := seen[string(key)]; return value, ok, nil }, s.groupID, image.Applied); err != nil {
				return err
			}
		case bytes.HasPrefix(entry.Key, geoMetadataPrefix):
			record, err := decodeGeoMetadata(entry.Key, entry.Value, s.groupID, image.Applied)
			if err != nil || entry.TenantID != record.Mirror.Target.Keyspace.TenantID {
				return errors.Join(err, errors.New("invalid snapshot geo metadata attribution"))
			}
		case strings.HasPrefix(key, "data/fence/range/"):
			var descriptor rootstate.Range
			if decodePackJSON(entry.Value, &descriptor) != nil || descriptor.Validate() != nil ||
				descriptor.OwnerGroupID != s.groupID || descriptor.State != rootstate.RangeStateActive ||
				descriptor.Epoch != 1 || len(descriptor.Start) != 0 || len(descriptor.End) != 0 ||
				!bytes.Equal(entry.Key, fenceRangeKey(descriptor.RangeID)) {
				return errors.New("capability snapshot contains invalid Range fence")
			}
			ranges[descriptor.RangeID] = descriptor.Keyspace()
		case strings.HasPrefix(key, "data/fence/keyspace/"):
			parts := strings.Split(strings.TrimPrefix(key, "data/fence/keyspace/"), "/")
			if len(parts) != 3 {
				return errors.New("capability snapshot contains invalid Keyspace fence")
			}
			keyspace := rootstate.Keyspace{TenantID: parts[0], CapabilityID: parts[1], DatasetID: parts[2]}
			if keyspace.Validate() != nil || rootstate.ValidateRangeID(string(entry.Value)) != nil {
				return errors.New("capability snapshot contains invalid Keyspace fence")
			}
			keyspaces[keyspace] = string(entry.Value)
		case strings.HasPrefix(key, "result/"):
			var result Result
			if decodePackJSON(entry.Value, &result) != nil || rootstate.ValidateRequestID(result.RequestID) != nil ||
				!bytes.Equal(entry.Key, resultKey(result.RequestID)) || result.Index == 0 || result.Index > image.Applied ||
				(result.Applied && result.Error != "") || (!result.Applied && result.Error == "") {
				return errors.New("capability snapshot contains invalid request result")
			}
			digest, err := hex.DecodeString(result.Hash)
			if err != nil || len(digest) != 32 {
				return errors.New("capability snapshot contains invalid result digest")
			}
		default:
			return errors.New("capability snapshot contains unsupported namespace")
		}
	}
	conf := new(raftpb.ConfState)
	if err := proto.Unmarshal(image.Conf, conf); err != nil {
		return err
	}
	if _, _, _, err := s.validateRegionMetadata(image.Applied, conf, func(key []byte) ([]byte, bool, error) {
		for _, entry := range image.Entries {
			if bytes.Equal(entry.Key, key) {
				return entry.Value, true, nil
			}
		}
		return nil, false, nil
	}); err != nil {
		return err
	}
	for rangeID, keyspace := range ranges {
		if keyspaces[keyspace] != rangeID {
			return errors.New("capability snapshot has conflicting or missing Keyspace fence")
		}
	}
	for keyspace, rangeID := range keyspaces {
		if scope, exists := ranges[rangeID]; !exists || scope != keyspace {
			return errors.New("capability snapshot has conflicting or missing Range fence")
		}
	}
	for keyspace := range values {
		if _, exists := keyspaces[keyspace]; !exists {
			return errors.New("capability snapshot value lacks Keyspace fence")
		}
	}
	return nil
}
