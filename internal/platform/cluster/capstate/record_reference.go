package capstate

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

const MaxRecordReferencesPerTransaction = 256
const maxRecordReferenceBytes = 16 << 10

var recordReferencePrefix = []byte("data/record-reference/")

// RecordReference is authoritative metadata, NOT a physical holding or a
// durability receipt. The originating Group in Descriptor is distinct from
// the current metadata OwnerGroupID. No Store, Volume or Pack location enters
// this replicated record. Index identifies this publication, including ABA.
type RecordReference struct {
	Version      int                `json:"version"`
	Descriptor   records.Descriptor `json:"descriptor"`
	OwnerGroupID string             `json:"owner_group_id"`
	RangeID      string             `json:"range_id"`
	RangeEpoch   uint64             `json:"range_epoch"`
	RoutingKey   []byte             `json:"routing_key"`
	RequestID    string             `json:"request_id"`
	CommandHash  string             `json:"command_hash"`
	Index        uint64             `json:"index"`
}

func (r RecordReference) clone() RecordReference {
	r.Descriptor.Key = bytes.Clone(r.Descriptor.Key)
	r.RoutingKey = bytes.Clone(r.RoutingKey)
	return r
}
func (r RecordReference) validate() error {
	if r.Version != 2 || r.Descriptor.Validate() != nil || raft.ValidateGroupID(r.OwnerGroupID) != nil || r.OwnerGroupID == raft.RootGroupID || rootstate.ValidateRangeID(r.RangeID) != nil || r.RangeEpoch == 0 || len(r.RoutingKey) > 4096 || rootstate.ValidateRequestID(r.RequestID) != nil || r.Index == 0 || len(r.CommandHash) != 64 {
		return errors.New("invalid authoritative record reference")
	}
	h, err := hex.DecodeString(r.CommandHash)
	if err != nil || hex.EncodeToString(h) != r.CommandHash {
		return errors.New("invalid record reference command hash")
	}
	return nil
}
func recordReferenceKey(scope rootstate.Keyspace, key []byte) []byte {
	logical := capabilityKey(scope, key)
	return append(bytes.Clone(recordReferencePrefix), logical[len(valuePrefix):]...)
}
func splitRecordReferenceKey(key []byte) (rootstate.Keyspace, []byte, error) {
	if !bytes.HasPrefix(key, recordReferencePrefix) {
		return rootstate.Keyspace{}, nil, errors.New("invalid record reference namespace")
	}
	logical := append(bytes.Clone(valuePrefix), key[len(recordReferencePrefix):]...)
	scope, err := splitValueKey(logical)
	if err != nil {
		return scope, nil, err
	}
	parts := bytes.SplitN(key[len(recordReferencePrefix):], []byte{'/'}, 4)
	return scope, parts[3], nil
}
func decodeRecordReference(key, value []byte, group string, index uint64) (RecordReference, error) {
	var ref RecordReference
	scope, logical, err := splitRecordReferenceKey(key)
	if err != nil || len(value) > maxRecordReferenceBytes || decodePackJSON(value, &ref) != nil || ref.validate() != nil || ref.OwnerGroupID != group || ref.Index > index || ref.Descriptor.Keyspace != scope || !bytes.Equal(ref.Descriptor.Key, logical) {
		return ref, errors.New("record reference does not match its key, Group, scope or applied cut")
	}
	return ref, nil
}

// Point lookups must refer to the SAME state cut as the reference. Result
// payloads are intentionally not interpreted here; only their publication
// header is relevant. Full snapshot validation separately checks result rows.
func validateRecordReferenceLinks(ref RecordReference, lookup func([]byte) ([]byte, bool, error)) error {
	read := func(key []byte) ([]byte, error) {
		v, ok, err := lookup(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New("record reference has a missing metadata backlink")
		}
		return v, nil
	}
	encoded, err := read(fenceRangeKey(ref.RangeID))
	if err != nil {
		return err
	}
	var fence rootstate.Range
	if len(encoded) > 64<<10 || decodePackJSON(encoded, &fence) != nil || fence.Validate() != nil || fence.State != rootstate.RangeStateActive || fence.RangeID != ref.RangeID || fence.OwnerGroupID != ref.OwnerGroupID || fence.Epoch != ref.RangeEpoch || fence.Keyspace() != ref.Descriptor.Keyspace {
		return errors.New("record reference differs from current Range fence")
	}
	if err := router.CheckOwner(ref.OwnerGroupID, fence, router.Target{Keyspace: ref.Descriptor.Keyspace,
		RangeID: ref.RangeID, OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}); err != nil {
		return errors.New("record reference routing key differs from current Range fence")
	}
	rangeID, err := read(fenceKeyspaceKey(ref.Descriptor.Keyspace))
	if err != nil {
		return err
	}
	if string(rangeID) != ref.RangeID {
		return errors.New("record reference differs from Keyspace fence")
	}
	encoded, err = read(resultKey(ref.RequestID))
	if err != nil {
		return err
	}
	var result struct {
		RequestID string `json:"request_id"`
		Hash      string `json:"hash"`
		Index     uint64 `json:"index"`
		Applied   bool   `json:"applied"`
		Error     string `json:"error"`
	}
	if len(encoded) > MaxResultBytes || json.Unmarshal(encoded, &result) != nil || result.RequestID != ref.RequestID || result.Hash != ref.CommandHash || result.Index != ref.Index || !result.Applied || result.Error != "" {
		return errors.New("record reference lacks its successful publication result")
	}
	return nil
}

func (s *State) recordReference(scope rootstate.Keyspace, key []byte) (RecordReference, bool, error) {
	dbKey := recordReferenceKey(scope, key)
	encoded, ok, err := s.getBounded(dbKey, maxRecordReferenceBytes)
	if err != nil || !ok {
		return RecordReference{}, false, err
	}
	ref, err := decodeRecordReference(dbKey, encoded, s.groupID, s.applied)
	if err != nil {
		return RecordReference{}, false, err
	}
	if err := validateRecordReferenceLinks(ref, func(key []byte) ([]byte, bool, error) {
		limit := 64 << 10
		if bytes.HasPrefix(key, []byte("result/")) {
			limit = MaxResultBytes
		}
		return s.getBounded(key, limit)
	}); err != nil {
		return RecordReference{}, false, err
	}
	return ref.clone(), true, nil
}

// RecordReference reads authoritative metadata within an already fenced
// ReadRouted view. Existence does not assert local bytes or a data quorum.
func (view ReadView) RecordReference(key []byte) (RecordReference, bool, error) {
	if view.state == nil || validateCapabilityKey(key) != nil {
		return RecordReference{}, false, errors.New("invalid record reference read key")
	}
	return view.state.recordReference(view.keyspace, key)
}

type stagedRecordReference struct {
	reference RecordReference
	deleted   bool
}

// SetRecordReference declares one opaque data key's current immutable content.
// A descriptor never makes the data locally available by itself. Data transfer,
// holding checks and durability confirmation are separate protocols. Metadata
// application must not depend on which participant happens to hold the bytes.
func (tx *Transaction) SetRecordReference(key []byte, descriptor records.Descriptor) error {
	if validateCapabilityKey(key) != nil || descriptor.Validate() != nil || descriptor.Keyspace != tx.keyspace || !bytes.Equal(descriptor.Key, key) {
		return errors.New("record reference escaped transaction Keyspace/key")
	}
	if err := tx.reserveRecordReference(key); err != nil {
		return err
	}
	ref := RecordReference{Version: 2, Descriptor: descriptor, OwnerGroupID: tx.state.groupID, RangeID: tx.target.RangeID, RangeEpoch: tx.target.Epoch,
		RoutingKey: bytes.Clone(tx.target.RoutingKey), RequestID: tx.requestID, CommandHash: tx.commandHash, Index: tx.index}
	if err := ref.validate(); err != nil {
		return err
	}
	tx.recordReferences[string(key)] = stagedRecordReference{reference: ref.clone()}
	return nil
}
func (tx *Transaction) DeleteRecordReference(key []byte) error {
	if validateCapabilityKey(key) != nil {
		return errors.New("invalid record reference deletion key")
	}
	if err := tx.reserveRecordReference(key); err != nil {
		return err
	}
	tx.recordReferences[string(key)] = stagedRecordReference{deleted: true}
	return nil
}
func (tx *Transaction) RecordReference(key []byte) (RecordReference, bool, error) {
	if validateCapabilityKey(key) != nil {
		return RecordReference{}, false, errors.New("invalid record reference read key")
	}
	if staged, ok := tx.recordReferences[string(key)]; ok {
		return staged.reference.clone(), !staged.deleted, nil
	}
	return tx.state.recordReference(tx.keyspace, key)
}
func (tx *Transaction) reserveRecordReference(key []byte) error {
	if _, ok := tx.recordReferences[string(key)]; !ok && len(tx.recordReferences) >= MaxRecordReferencesPerTransaction {
		return errors.New("transaction record reference limit exceeded")
	}
	return nil
}
