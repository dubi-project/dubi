package capstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

const maxLocalConfirmationBytes = routedstate.MaxLocalConfirmationBytes

var localConfirmationPrefix = []byte("data/local-confirmation/")

// LocalConfirmationRecord is an Owner Group's durable account of a verified
// physical majority for one exact reference publication and Region fence.
// It is not by itself a live LocalReady read: the current Root cohort and
// actual physical data still require checking after topology or disk failure.
type LocalConfirmationRecord struct {
	Version              int                           `json:"version"`
	Claim                routedstate.LocalConfirmation `json:"claim"`
	ReferenceCommandHash string                        `json:"reference_command_hash"`
	RequestID            string                        `json:"request_id"`
	Index                uint64                        `json:"index"`
}

func localConfirmationKey(scope rootstate.Keyspace, key []byte) []byte {
	logical := capabilityKey(scope, key)
	return append(bytes.Clone(localConfirmationPrefix), logical[len(valuePrefix):]...)
}

func splitLocalConfirmationKey(key []byte) (rootstate.Keyspace, []byte, error) {
	if !bytes.HasPrefix(key, localConfirmationPrefix) {
		return rootstate.Keyspace{}, nil, errors.New("invalid local confirmation namespace")
	}
	logical := append(bytes.Clone(valuePrefix), key[len(localConfirmationPrefix):]...)
	scope, err := splitValueKey(logical)
	if err != nil {
		return rootstate.Keyspace{}, nil, err
	}
	parts := bytes.SplitN(key[len(localConfirmationPrefix):], []byte{'/'}, 4)
	return scope, parts[3], nil
}

func decodeLocalConfirmation(key, value []byte, group string, applied uint64) (LocalConfirmationRecord, error) {
	var record LocalConfirmationRecord
	scope, logical, err := splitLocalConfirmationKey(key)
	if err != nil || len(value) > maxLocalConfirmationBytes || decodePackJSON(value, &record) != nil ||
		record.Version != 1 || record.Claim.Validate() != nil || record.Claim.Target.Keyspace != scope ||
		!bytes.Equal(record.Claim.Key, logical) || record.Claim.Target.OwnerGroupID != group ||
		rootstate.ValidateRequestID(record.RequestID) != nil || record.Index == 0 || record.Index > applied ||
		len(record.ReferenceCommandHash) != 64 {
		return LocalConfirmationRecord{}, errors.New("invalid local confirmation record")
	}
	return record, nil
}

func validateLocalConfirmationLinks(record LocalConfirmationRecord, lookup func([]byte) ([]byte, bool, error), group string, applied uint64) error {
	claim := record.Claim
	refKey := recordReferenceKey(claim.Target.Keyspace, claim.Key)
	encoded, found, err := lookup(refKey)
	if err != nil || !found {
		return errors.Join(err, errors.New("local confirmation lacks its current record reference"))
	}
	ref, err := decodeRecordReference(refKey, encoded, group, applied)
	if err != nil {
		return err
	}
	if err := validateRecordReferenceLinks(ref, lookup); err != nil {
		return err
	}
	id, err := ref.Descriptor.ID()
	if err != nil || id != claim.RecordID || ref.RequestID != claim.ReferenceRequestID ||
		ref.Index != claim.ReferenceIndex || ref.CommandHash != record.ReferenceCommandHash ||
		ref.RangeID != claim.Target.RangeID || ref.RangeEpoch != claim.Target.Epoch ||
		!bytes.Equal(ref.RoutingKey, claim.Target.RoutingKey) {
		return errors.New("local confirmation differs from current reference publication")
	}
	encoded, found, err = lookup(resultKey(record.RequestID))
	if err != nil || !found {
		return errors.Join(err, errors.New("local confirmation lacks its successful result"))
	}
	var result Result
	if len(encoded) > MaxResultBytes || json.Unmarshal(encoded, &result) != nil ||
		!result.Applied || result.Error != "" || result.Index != record.Index || result.RequestID != record.RequestID ||
		result.Hash != commandHash("confirm_local", record.RequestID, claim, nil) {
		return errors.New("local confirmation result does not match its claim")
	}
	return nil
}

func (s *State) ApplyLocalConfirmation(index uint64, requestID string, claim routedstate.LocalConfirmation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkNext(index); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if _, found, err := s.result(requestID); err != nil {
		return err
	} else if found {
		return s.commitIndex(batch, index)
	}
	result := Result{RequestID: requestID, Hash: commandHash("confirm_local", requestID, claim, nil), Index: index}
	if rootstate.ValidateRequestID(requestID) != nil || claim.Validate() != nil {
		result.Error = "invalid local confirmation"
	} else {
		result.Error = s.localConfirmationRejection(claim)
	}
	if result.Error == "" {
		ref, _, err := s.recordReference(claim.Target.Keyspace, claim.Key)
		if err != nil {
			return err
		}
		record := LocalConfirmationRecord{Version: 1, Claim: claim,
			ReferenceCommandHash: ref.CommandHash, RequestID: requestID, Index: index}
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if len(encoded) > maxLocalConfirmationBytes {
			result.Error = "local confirmation record exceeds limit"
		} else {
			if err := batch.Set(localConfirmationKey(claim.Target.Keyspace, claim.Key), encoded, nil); err != nil {
				return err
			}
			result.Applied = true
		}
	}
	if err := setJSON(batch, resultKey(requestID), result); err != nil {
		return err
	}
	return s.commitIndex(batch, index)
}

func (s *State) localConfirmationRejection(claim routedstate.LocalConfirmation) string {
	fence, found, err := s.fence(claim.Target.RangeID)
	if err != nil || !found || router.CheckOwner(s.groupID, fence, claim.Target) != nil {
		return "local confirmation Range fence is unavailable or changed"
	}
	regional, found, membership, err := s.validateRegionMetadata(s.applied, s.conf, s.get)
	if err != nil || !found || membership != regional.Fence.MembershipIndex || !regional.Fence.Matches(s.conf) {
		return "local confirmation Region fence is unavailable or changed"
	}
	current := regional.Fence
	if current.RegionID != claim.RegionID || current.GroupID != claim.Target.OwnerGroupID ||
		current.Generation != claim.Generation || current.GroupEpoch != claim.GroupEpoch ||
		regional.Index != claim.FenceIndex || !slices.Equal(current.Nodes(), claim.Nodes) {
		return "local confirmation differs from current Region fence"
	}
	for _, member := range current.Members {
		if member.Role != rootstate.GroupMemberVoter {
			return "local confirmation cohort includes a non-voter"
		}
	}
	ref, found, err := s.recordReference(claim.Target.Keyspace, claim.Key)
	if err != nil || !found {
		return "local confirmation has no current authoritative reference"
	}
	id, err := ref.Descriptor.ID()
	if err != nil || id != claim.RecordID || ref.RequestID != claim.ReferenceRequestID ||
		ref.Index != claim.ReferenceIndex || ref.RangeID != claim.Target.RangeID || ref.RangeEpoch != claim.Target.Epoch ||
		!bytes.Equal(ref.RoutingKey, claim.Target.RoutingKey) {
		return "local confirmation differs from current record publication"
	}
	return ""
}

func (s *State) localConfirmation(scope rootstate.Keyspace, key []byte) (LocalConfirmationRecord, bool, error) {
	dbKey := localConfirmationKey(scope, key)
	encoded, found, err := s.getBounded(dbKey, maxLocalConfirmationBytes)
	if err != nil || !found {
		return LocalConfirmationRecord{}, false, err
	}
	record, err := decodeLocalConfirmation(dbKey, encoded, s.groupID, s.applied)
	if err != nil {
		return LocalConfirmationRecord{}, false, err
	}
	if err := validateLocalConfirmationLinks(record, func(key []byte) ([]byte, bool, error) {
		limit := maxRecordReferenceBytes
		if bytes.HasPrefix(key, []byte("result/")) {
			limit = MaxResultBytes
		}
		return s.getBounded(key, limit)
	}, s.groupID, s.applied); err != nil {
		return LocalConfirmationRecord{}, false, err
	}
	return record, true, nil
}

// LocalConfirmation returns a persisted claim for this exact current
// reference. Callers must compare it with a fresh Root plan and physical
// health before presenting a live durability level.
func (view ReadView) LocalConfirmation(key []byte) (LocalConfirmationRecord, bool, error) {
	if view.state == nil || validateCapabilityKey(key) != nil {
		return LocalConfirmationRecord{}, false, errors.New("invalid local confirmation read key")
	}
	return view.state.localConfirmation(view.keyspace, key)
}

func (s *State) ResultForLocalConfirmation(requestID string, claim routedstate.LocalConfirmation) (Result, bool, error) {
	result, found, err := s.Result(requestID)
	if err != nil || !found {
		return result, found, err
	}
	if result.Hash != commandHash("confirm_local", requestID, claim, nil) {
		return Result{}, true, ErrRequestIDReused
	}
	return result, true, nil
}
