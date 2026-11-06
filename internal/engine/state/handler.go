package state

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// Handler is the narrow deterministic adapter to the Runtime state machine.
type Handler struct{}
type publication struct {
	Operation  operation          `json:"operation"`
	Descriptor records.Descriptor `json:"descriptor"`
}

func (Handler) Apply(tx *enginestate.Transaction, target router.Target, payload []byte) (enginestate.Outcome, error) {
	var p publication
	if len(payload) > 128<<10 || decode(payload, &p) != nil || p.Operation.validate() != nil || target.Keyspace.EngineID != EngineID || !bytes.Equal(target.RoutingKey, dataKey(p.Operation.Key)) {
		return enginestate.Outcome{Rejection: ErrInvalid.Error()}, nil
	}
	o := p.Operation
	expected, err := records.Describe(target.Keyspace, target.OwnerGroupID, target.RoutingKey, o.body())
	if err != nil {
		return enginestate.Outcome{Rejection: ErrInvalid.Error()}, nil
	}
	want, _ := expected.ID()
	actual, err := p.Descriptor.ID()
	if err != nil || actual != want {
		return enginestate.Outcome{Rejection: ErrInvalid.Error()}, nil
	}
	prior, found, err := tx.Get(receiptKey(o.RequestID))
	if err != nil {
		return enginestate.Outcome{}, err
	}
	if found {
		var r receipt
		if decode(prior, &r) != nil {
			return enginestate.Outcome{}, errors.New("invalid State operation receipt")
		}
		if r.Version != o.version() {
			return enginestate.Outcome{Rejection: ErrRequestReused.Error()}, nil
		}
		return enginestate.Outcome{Value: prior}, nil
	}
	b, found, err := tx.Get(target.RoutingKey)
	if err != nil {
		return enginestate.Outcome{}, err
	}
	var old head
	if found && decode(b, &old) != nil {
		return enginestate.Outcome{}, errors.New("invalid State head")
	}
	live := found && !old.Deleted
	r := receipt{Version: o.version(), Deleted: o.Delete}
	if (o.Condition.IfAbsent && live) || (o.Condition.Version != "" && (!live || old.Version != o.Condition.Version)) {
		r.Code = "conflict"
	} else {
		if found {
			// Preserve an earlier completed operation before replacing the
			// current reference and its receipt. This is historical replay only.
			confirmed, err := tx.PublicationConfirmed(target.RoutingKey)
			if err != nil {
				return enginestate.Outcome{}, err
			}
			if confirmed {
				b, ok, err := tx.Get(receiptKey(old.RequestID))
				if err != nil {
					return enginestate.Outcome{}, err
				}
				var previous receipt
				if !ok || decode(b, &previous) != nil || previous.Version != old.Version || previous.Code != "" {
					return enginestate.Outcome{}, errors.New("State head lacks matching receipt")
				}
				previous.Confirmed = true
				b, _ = json.Marshal(previous)
				if err := tx.Set(receiptKey(old.RequestID), b); err != nil {
					return enginestate.Outcome{}, err
				}
			}
		}
		b, _ = json.Marshal(head{Version: r.Version, Deleted: o.Delete, RequestID: o.RequestID})
		if err := tx.Set(target.RoutingKey, b); err != nil {
			return enginestate.Outcome{}, err
		}
		if err := tx.SetRecordReference(target.RoutingKey, p.Descriptor); err != nil {
			return enginestate.Outcome{}, err
		}
	}
	// Business conflicts are durable outcomes too. No reference or head is
	// changed for them; the physical writer cannot acknowledge a publication.
	b, _ = json.Marshal(r)
	if err := tx.Set(receiptKey(o.RequestID), b); err != nil {
		return enginestate.Outcome{}, err
	}
	return enginestate.Outcome{Value: b}, nil
}

func (Handler) Read(view enginestate.ReadView, target router.Target, payload []byte) (enginestate.Outcome, error) {
	var q query
	if len(payload) > 4096 || decode(payload, &q) != nil || q.Format != 1 || target.Keyspace.EngineID != EngineID || !bytes.HasPrefix(target.RoutingKey, []byte("v/")) {
		return enginestate.Outcome{Rejection: ErrInvalid.Error()}, nil
	}
	var result queryResult
	b, found, err := view.Get(target.RoutingKey)
	if err != nil {
		return enginestate.Outcome{}, err
	}
	if found {
		result.Head = new(head)
		if decode(b, result.Head) != nil {
			return enginestate.Outcome{}, errors.New("invalid State head")
		}
	}
	if q.RequestID != "" {
		b, found, err = view.Get(receiptKey(q.RequestID))
		if err != nil {
			return enginestate.Outcome{}, err
		}
		if found {
			result.Receipt = new(receipt)
			if decode(b, result.Receipt) != nil {
				return enginestate.Outcome{}, errors.New("invalid State operation receipt")
			}
			if result.Receipt.Version != q.Version {
				return enginestate.Outcome{Rejection: ErrRequestReused.Error()}, nil
			}
			if result.Head != nil && result.Head.RequestID == q.RequestID && result.Head.Version == q.Version && result.Receipt.Code == "" {
				result.Receipt.Confirmed, err = view.PublicationConfirmed(target.RoutingKey)
				if err != nil {
					return enginestate.Outcome{}, err
				}
			}
		}
	}
	b, _ = json.Marshal(result)
	return enginestate.Outcome{Value: b}, nil
}
