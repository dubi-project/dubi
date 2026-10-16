package enginestate

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

const maxGeoCoverageBytes = routedstate.MaxGeoConfirmationBytes

var geoCoveragePrefix = []byte("data/geo-coverage/")

// GeoCoverageRecord is an Owner Group's durable account of a cross-Region
// physical observation. It is deliberately not a live GeoReady lease: callers
// must re-read Root placement and verify current holdings before serving.
type GeoCoverageRecord struct {
	Version              int                         `json:"version"`
	Claim                routedstate.GeoConfirmation `json:"claim"`
	ReferenceCommandHash string                      `json:"reference_command_hash"`
	RequestID            string                      `json:"request_id"`
	Index                uint64                      `json:"index"`
}

func geoCoverageKey(scope rootstate.Keyspace, key []byte) []byte {
	logical := engineKey(scope, key)
	return append(bytes.Clone(geoCoveragePrefix), logical[len(valuePrefix):]...)
}

func splitGeoCoverageKey(key []byte) (rootstate.Keyspace, []byte, error) {
	if !bytes.HasPrefix(key, geoCoveragePrefix) {
		return rootstate.Keyspace{}, nil, errors.New("invalid geo coverage namespace")
	}
	logical := append(bytes.Clone(valuePrefix), key[len(geoCoveragePrefix):]...)
	scope, err := splitValueKey(logical)
	if err != nil {
		return rootstate.Keyspace{}, nil, err
	}
	parts := bytes.SplitN(key[len(geoCoveragePrefix):], []byte{'/'}, 4)
	return scope, parts[3], nil
}

func decodeGeoCoverage(key, value []byte, group string, applied uint64) (GeoCoverageRecord, error) {
	var record GeoCoverageRecord
	scope, logical, err := splitGeoCoverageKey(key)
	if err != nil || len(value) > maxGeoCoverageBytes || decodePackJSON(value, &record) != nil ||
		record.Version != 1 || record.Claim.Validate() != nil || record.Claim.Target.Keyspace != scope ||
		!bytes.Equal(record.Claim.Key, logical) || record.Claim.Target.OwnerGroupID != group ||
		rootstate.ValidateRequestID(record.RequestID) != nil || record.Index == 0 || record.Index > applied ||
		len(record.ReferenceCommandHash) != 64 {
		return GeoCoverageRecord{}, errors.New("invalid geo coverage record")
	}
	return record, nil
}

func validateGeoCoverageLinks(record GeoCoverageRecord, lookup func([]byte) ([]byte, bool, error), group string, applied uint64) error {
	claim := record.Claim
	refKey := recordReferenceKey(claim.Target.Keyspace, claim.Key)
	encoded, found, err := lookup(refKey)
	if err != nil || !found {
		return errors.Join(err, errors.New("geo coverage lacks its current record reference"))
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
		ref.CommandHash != claim.ReferenceCommandHash || ref.RangeID != claim.Target.RangeID ||
		ref.RangeEpoch != claim.Target.Epoch || !bytes.Equal(ref.RoutingKey, claim.Target.RoutingKey) {
		return errors.New("geo coverage differs from current reference publication")
	}
	encoded, found, err = lookup(resultKey(record.RequestID))
	if err != nil || !found {
		return errors.Join(err, errors.New("geo coverage lacks its successful result"))
	}
	var result Result
	if len(encoded) > MaxResultBytes || json.Unmarshal(encoded, &result) != nil || !result.Applied || result.Error != "" ||
		result.Index != record.Index || result.RequestID != record.RequestID ||
		result.Hash != commandHash("confirm_geo", record.RequestID, claim, nil) {
		return errors.New("geo coverage result does not match its claim")
	}
	return nil
}

func (s *State) ApplyGeoConfirmation(index uint64, requestID string, claim routedstate.GeoConfirmation) error {
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
	result := Result{RequestID: requestID, Hash: commandHash("confirm_geo", requestID, claim, nil), Index: index}
	if rootstate.ValidateRequestID(requestID) != nil || claim.Validate() != nil {
		result.Error = "invalid geo confirmation"
	} else {
		result.Error = s.geoCoverageRejection(claim)
	}
	if result.Error == "" {
		ref, _, err := s.recordReference(claim.Target.Keyspace, claim.Key)
		if err != nil {
			return err
		}
		record := GeoCoverageRecord{Version: 1, Claim: claim, ReferenceCommandHash: ref.CommandHash, RequestID: requestID, Index: index}
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if len(encoded) > maxGeoCoverageBytes {
			result.Error = "geo coverage record exceeds limit"
		} else if err := batch.Set(geoCoverageKey(claim.Target.Keyspace, claim.Key), encoded, nil); err != nil {
			return err
		} else {
			result.Applied = true
		}
	}
	if err := setJSON(batch, resultKey(requestID), result); err != nil {
		return err
	}
	return s.commitIndex(batch, index)
}

func (s *State) geoCoverageRejection(claim routedstate.GeoConfirmation) string {
	fence, found, err := s.fence(claim.Target.RangeID)
	if err != nil || !found || router.CheckOwner(s.groupID, fence, claim.Target) != nil {
		return "geo coverage Range fence is unavailable or changed"
	}
	regional, found, membership, err := s.validateRegionMetadata(s.applied, s.conf, s.get)
	if err != nil || !found || membership != regional.Fence.MembershipIndex || !regional.Fence.Matches(s.conf) {
		return "geo coverage local Region fence is unavailable or changed"
	}
	local := regional.Fence
	localFound := false
	for _, region := range claim.Regions {
		if region.RegionID != local.RegionID {
			continue
		}
		localFound = true
		if region.Generation != local.Generation || region.GroupEpoch != local.GroupEpoch || region.FenceIndex != regional.Index || !slices.Equal(region.Nodes, local.Nodes()) {
			return "geo coverage differs from local Region fence"
		}
	}
	if !localFound {
		return "geo coverage has no local Region evidence"
	}
	ref, found, err := s.recordReference(claim.Target.Keyspace, claim.Key)
	if err != nil || !found {
		return "geo coverage has no current authoritative reference"
	}
	id, err := ref.Descriptor.ID()
	if err != nil || id != claim.RecordID || ref.RequestID != claim.ReferenceRequestID || ref.Index != claim.ReferenceIndex ||
		ref.CommandHash != claim.ReferenceCommandHash || ref.RangeID != claim.Target.RangeID || ref.RangeEpoch != claim.Target.Epoch ||
		!bytes.Equal(ref.RoutingKey, claim.Target.RoutingKey) {
		return "geo coverage differs from current record publication"
	}
	return ""
}

func (s *State) geoCoverage(scope rootstate.Keyspace, key []byte) (GeoCoverageRecord, bool, error) {
	dbKey := geoCoverageKey(scope, key)
	encoded, found, err := s.getBounded(dbKey, maxGeoCoverageBytes)
	if err != nil || !found {
		return GeoCoverageRecord{}, false, err
	}
	record, err := decodeGeoCoverage(dbKey, encoded, s.groupID, s.applied)
	if err != nil {
		return GeoCoverageRecord{}, false, err
	}
	if err := validateGeoCoverageLinks(record, func(key []byte) ([]byte, bool, error) {
		limit := maxRecordReferenceBytes
		if bytes.HasPrefix(key, []byte("result/")) {
			limit = MaxResultBytes
		}
		return s.getBounded(key, limit)
	}, s.groupID, s.applied); err != nil {
		return GeoCoverageRecord{}, false, err
	}
	return record, true, nil
}

// GeoCoverage returns a persisted physical coverage account for this exact
// current reference. It is not a live GeoReady assertion.
func (view ReadView) GeoCoverage(key []byte) (GeoCoverageRecord, bool, error) {
	if view.state == nil || validateEngineKey(key) != nil {
		return GeoCoverageRecord{}, false, errors.New("invalid geo coverage read key")
	}
	return view.state.geoCoverage(view.keyspace, key)
}

func (s *State) ResultForGeoConfirmation(requestID string, claim routedstate.GeoConfirmation) (Result, bool, error) {
	result, found, err := s.Result(requestID)
	if err != nil || !found {
		return result, found, err
	}
	if result.Hash != commandHash("confirm_geo", requestID, claim, nil) {
		return Result{}, true, ErrRequestIDReused
	}
	return result, true, nil
}
