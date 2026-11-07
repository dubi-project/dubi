package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

var ErrCancelled = errors.New("state operation was cancelled before publication; use a new request ID")

type abortOperation struct {
	RequestID string `json:"request_id"`
	Version   string `json:"version"`
	Key       []byte `json:"key"`
}

func applyAbort(tx *enginestate.Transaction, target router.Target, a abortOperation) (enginestate.Outcome, error) {
	if rootstate.ValidateRequestID(a.RequestID) != nil || !validVersion(a.Version) || len(a.Key) == 0 || len(a.Key) > MaxKeyBytes || target.Keyspace.EngineID != EngineID || !bytes.Equal(target.RoutingKey, dataKey(a.Key)) {
		return enginestate.Outcome{Rejection: ErrInvalid.Error()}, nil
	}
	b, ok, err := tx.Get(receiptKey(a.RequestID))
	if err != nil {
		return enginestate.Outcome{}, err
	}
	if ok {
		var r receipt
		if decode(b, &r) != nil {
			return enginestate.Outcome{}, errors.New("invalid State receipt")
		}
		if r.Version != a.Version {
			return enginestate.Outcome{Rejection: ErrRequestReused.Error()}, nil
		}
		return enginestate.Outcome{Value: b}, nil
	}
	b, _ = json.Marshal(receipt{Version: a.Version, Code: "cancelled"})
	if err := tx.Set(receiptKey(a.RequestID), b); err != nil {
		return enginestate.Outcome{}, err
	}
	return enginestate.Outcome{Value: b}, nil
}
