// Package record is the first small, built-in data capability. Its wire
// payload is deliberately versioned independently of the cluster envelope.
package record

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

const CapabilityID = "record"

const (
	putVersion    = byte(1) // legacy inline value, retained for old state reads
	refPutVersion = byte(2)
	readVersion   = byte(1)
)

const NotFound = "record not found"

type Handler struct{}

func PutPayload(value []byte) ([]byte, error) {
	if len(value) > capstate.MaxValueBytes-1 {
		return nil, errors.New("record value exceeds the value limit")
	}
	result := make([]byte, len(value)+1)
	result[0] = putVersion
	copy(result[1:], value)
	return result, nil
}

// ReferencePutPayload publishes an immutable Pack descriptor. The physical
// bytes are staged and verified by recordwrite.Writer before this payload is
// submitted to Raft; the capability state machine only publishes the
// authoritative reference.
func ReferencePutPayload(descriptor records.Descriptor) ([]byte, error) {
	if err := descriptor.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(descriptor)
	if err != nil {
		return nil, err
	}
	if len(encoded) > capstate.MaxValueBytes-1 {
		return nil, errors.New("record descriptor exceeds the value limit")
	}
	return append([]byte{refPutVersion}, encoded...), nil
}

func ReadPayload() []byte { return []byte{readVersion} }

func (Handler) Apply(tx *capstate.Transaction, target router.Target, payload []byte) (capstate.Outcome, error) {
	if len(payload) == 0 || len(payload) > capstate.MaxValueBytes {
		return capstate.Outcome{Rejection: "invalid record mutation"}, nil
	}
	if payload[0] == refPutVersion {
		var descriptor records.Descriptor
		if err := json.Unmarshal(payload[1:], &descriptor); err != nil || descriptor.Validate() != nil ||
			descriptor.Keyspace != target.Keyspace || descriptor.GroupID != target.OwnerGroupID ||
			!bytes.Equal(descriptor.Key, target.RoutingKey) {
			return capstate.Outcome{Rejection: "invalid immutable record descriptor"}, nil
		}
		if err := tx.SetRecordReference(target.RoutingKey, descriptor); err != nil {
			return capstate.Outcome{}, err
		}
		return capstate.Outcome{}, nil
	}
	if payload[0] != putVersion {
		return capstate.Outcome{Rejection: "invalid record mutation"}, nil
	}
	return capstate.Outcome{}, tx.Set(target.RoutingKey, payload[1:])
}

func (Handler) Read(view capstate.ReadView, target router.Target, payload []byte) (capstate.Outcome, error) {
	if len(payload) != 1 || payload[0] != readVersion {
		return capstate.Outcome{Rejection: "invalid record read"}, nil
	}
	value, found, err := view.Get(target.RoutingKey)
	if err != nil {
		return capstate.Outcome{}, err
	}
	if !found {
		return capstate.Outcome{Rejection: NotFound}, nil
	}
	return capstate.Outcome{Value: value}, nil
}
