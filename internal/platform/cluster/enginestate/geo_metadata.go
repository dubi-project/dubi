package enginestate

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
)

const maxGeoMetadataMirrorBytes = routedstate.MaxGeoMetadataMirrorBytes

var geoMetadataPrefix = []byte("data/geo-metadata/")

type GeoMetadataMirrorRecord struct {
	Version   int                           `json:"version"`
	Mirror    routedstate.GeoMetadataMirror `json:"mirror"`
	RequestID string                        `json:"request_id"`
	Index     uint64                        `json:"index"`
}

func geoMetadataKey(scope rootstate.Keyspace, key []byte) []byte {
	logical := engineKey(scope, key)
	return append(bytes.Clone(geoMetadataPrefix), logical[len(valuePrefix):]...)
}

func splitGeoMetadataKey(key []byte) (rootstate.Keyspace, []byte, error) {
	if !bytes.HasPrefix(key, geoMetadataPrefix) {
		return rootstate.Keyspace{}, nil, errors.New("invalid geo metadata namespace")
	}
	logical := append(bytes.Clone(valuePrefix), key[len(geoMetadataPrefix):]...)
	scope, err := splitValueKey(logical)
	if err != nil {
		return rootstate.Keyspace{}, nil, err
	}
	parts := bytes.SplitN(key[len(geoMetadataPrefix):], []byte{'/'}, 4)
	return scope, parts[3], nil
}

func decodeGeoMetadata(key, value []byte, group string, applied uint64) (GeoMetadataMirrorRecord, error) {
	var record GeoMetadataMirrorRecord
	scope, logical, err := splitGeoMetadataKey(key)
	if err != nil || len(value) > maxGeoMetadataMirrorBytes || decodePackJSON(value, &record) != nil ||
		record.Version != 1 || record.Mirror.Validate() != nil || record.Mirror.Target.Keyspace != scope ||
		!bytes.Equal(record.Mirror.Key, logical) || rootstate.ValidateRequestID(record.RequestID) != nil ||
		record.Index == 0 || record.Index > applied || group == "" {
		return GeoMetadataMirrorRecord{}, errors.New("invalid geo metadata mirror record")
	}
	return record, nil
}

func (s *State) ApplyGeoMetadataMirror(index uint64, requestID string, mirror routedstate.GeoMetadataMirror) error {
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
	result := Result{RequestID: requestID, Hash: commandHash("mirror_geo", requestID, mirror, nil), Index: index}
	if rootstate.ValidateRequestID(requestID) != nil || mirror.Validate() != nil {
		result.Error = "invalid geo metadata mirror"
	} else {
		result.Error = s.geoMetadataRejection(mirror)
	}
	if result.Error == "" {
		record := GeoMetadataMirrorRecord{Version: 1, Mirror: mirror, RequestID: requestID, Index: index}
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if len(encoded) > maxGeoMetadataMirrorBytes {
			result.Error = "geo metadata mirror exceeds limit"
		} else if err := batch.Set(geoMetadataKey(mirror.Target.Keyspace, mirror.Key), encoded, nil); err != nil {
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

func (s *State) geoMetadataRejection(mirror routedstate.GeoMetadataMirror) string {
	regional, found, membership, err := s.validateRegionMetadata(s.applied, s.conf, s.get)
	if err != nil || !found || membership != regional.Fence.MembershipIndex || !regional.Fence.Matches(s.conf) {
		return "geo metadata local Region fence is unavailable or changed"
	}
	if regional.Fence.RegionID != mirror.RegionID || regional.Fence.Generation != mirror.Generation ||
		regional.Fence.GroupEpoch != mirror.GroupEpoch || regional.Index != mirror.FenceIndex || regional.Fence.GroupID != s.groupID {
		return "geo metadata mirror differs from local Region fence"
	}
	return ""
}

func (view ReadView) GeoMetadata(key []byte) (GeoMetadataMirrorRecord, bool, error) {
	if view.state == nil || validateEngineKey(key) != nil {
		return GeoMetadataMirrorRecord{}, false, errors.New("invalid geo metadata read key")
	}
	dbKey := geoMetadataKey(view.keyspace, key)
	encoded, found, err := view.state.getBounded(dbKey, maxGeoMetadataMirrorBytes)
	if err != nil || !found {
		return GeoMetadataMirrorRecord{}, false, err
	}
	record, err := decodeGeoMetadata(dbKey, encoded, view.state.groupID, view.state.applied)
	if err != nil {
		return GeoMetadataMirrorRecord{}, false, err
	}
	return record, true, nil
}

func (s *State) ResultForGeoMetadataMirror(requestID string, mirror routedstate.GeoMetadataMirror) (Result, bool, error) {
	result, found, err := s.Result(requestID)
	if err != nil || !found {
		return result, found, err
	}
	if result.Hash != commandHash("mirror_geo", requestID, mirror, nil) {
		return Result{}, true, ErrRequestIDReused
	}
	return result, true, nil
}
