// Package record is the first small, built-in data capability. Its wire
// payload is deliberately versioned independently of the cluster envelope.
package record

import (
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

const CapabilityID = "record"

const (
	putVersion  = byte(1)
	readVersion = byte(1)
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

func ReadPayload() []byte { return []byte{readVersion} }

func (Handler) Apply(tx *capstate.Transaction, target router.Target, payload []byte) (capstate.Outcome, error) {
	if len(payload) == 0 || payload[0] != putVersion || len(payload) > capstate.MaxValueBytes {
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
