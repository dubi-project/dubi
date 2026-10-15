package routedstate

import (
	"encoding/hex"
	"errors"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// GeoMetadataMirror is a destination Region's durable copy of the current
// immutable reference metadata. It is not a routing Owner and cannot authorize
// writes; it exists so recovery can distinguish a current metadata cut from
// stale bytes after the source Region is lost.
type GeoMetadataMirror struct {
	Target               router.Target      `json:"target"`
	Key                  []byte             `json:"key"`
	DestinationGroupID   string             `json:"destination_group_id"`
	RegionID             string             `json:"region_id"`
	Generation           uint64             `json:"generation"`
	GroupEpoch           uint64             `json:"group_epoch"`
	FenceIndex           uint64             `json:"fence_index"`
	RecordID             string             `json:"record_id"`
	ReferenceRequestID   string             `json:"reference_request_id"`
	ReferenceIndex       uint64             `json:"reference_index"`
	ReferenceCommandHash string             `json:"reference_command_hash"`
	Descriptor           records.Descriptor `json:"descriptor"`
}

const MaxGeoMetadataMirrorBytes = 512 << 10

func (m GeoMetadataMirror) Validate() error {
	if m.Target.Validate() != nil || raft.ValidateGroupID(m.DestinationGroupID) != nil || len(m.Key) == 0 || len(m.Key) > 4096 ||
		m.Target.Keyspace != m.Descriptor.Keyspace || string(m.Key) != string(m.Descriptor.Key) ||
		m.Descriptor.Validate() != nil || rootstate.ValidateRangeID(m.RegionID) != nil ||
		m.Generation == 0 || m.GroupEpoch < 2 || m.FenceIndex == 0 ||
		rootstate.ValidateRequestID(m.ReferenceRequestID) != nil || m.ReferenceIndex == 0 ||
		!validMetadataRecordID(m.RecordID) {
		return errors.New("invalid Geo metadata mirror")
	}
	id, err := m.Descriptor.ID()
	if err != nil || id != m.RecordID {
		return errors.New("Geo metadata mirror RecordID differs from descriptor")
	}
	if m.Target.RangeID == "" || m.Target.Epoch == 0 || m.Target.OwnerGroupID == "" {
		return errors.New("Geo metadata mirror lacks current route")
	}
	if len(m.ReferenceCommandHash) != 64 {
		return errors.New("invalid Geo metadata reference hash")
	}
	digest, err := hex.DecodeString(m.ReferenceCommandHash)
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != m.ReferenceCommandHash {
		return errors.New("invalid Geo metadata reference hash")
	}
	return nil
}

func validMetadataRecordID(value string) bool {
	const prefix = "record_"
	if len(value) != len(prefix)+64 || !strings.HasPrefix(value, prefix) {
		return false
	}
	digest, err := hex.DecodeString(value[len(prefix):])
	return err == nil && len(digest) == 32 && hex.EncodeToString(digest) == value[len(prefix):]
}
