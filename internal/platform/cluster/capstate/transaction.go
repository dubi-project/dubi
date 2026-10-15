package capstate

import (
	"bytes"
	"errors"
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

const maxCapabilityKeyBytes = 4096

type transactionValue struct {
	value   []byte
	deleted bool
}

// Transaction is valid only during Handler.Apply. Keys are Keyspace-local;
// platform metadata and results are not addressable through this API.
type Transaction struct {
	state                  *State
	batch                  *pebble.Batch
	keyspace               rootstate.Keyspace
	staged                 map[string]transactionValue
	target                 router.Target
	requestID, commandHash string
	index                  uint64
	recordReferences       map[string]stagedRecordReference
}

func newTransaction(state *State, batch *pebble.Batch, target router.Target, requestID, hash string, index uint64) *Transaction {
	return &Transaction{state: state, batch: batch, keyspace: target.Keyspace, target: target, requestID: requestID, commandHash: hash, index: index,
		staged: make(map[string]transactionValue), recordReferences: make(map[string]stagedRecordReference)}
}

func (transaction *Transaction) Get(key []byte) ([]byte, bool, error) {
	if err := validateCapabilityKey(key); err != nil {
		return nil, false, err
	}
	if value, found := transaction.staged[string(key)]; found {
		return bytes.Clone(value.value), !value.deleted, nil
	}
	return transaction.state.getValue(capabilityKey(transaction.keyspace, key))
}

func (transaction *Transaction) Set(key, value []byte) error {
	if err := validateCapabilityKey(key); err != nil {
		return err
	}
	if value == nil || len(value) > MaxValueBytes {
		return errors.New("capability value is nil or exceeds limit")
	}
	copy := bytes.Clone(value)
	transaction.staged[string(key)] = transactionValue{value: copy}
	return nil
}

func (transaction *Transaction) Delete(key []byte) error {
	if err := validateCapabilityKey(key); err != nil {
		return err
	}
	transaction.staged[string(key)] = transactionValue{deleted: true}
	return nil
}

// Only final mutations from an accepted handler reach physical storage. A
// rejection, overwritten value or Set-then-Delete must not append a Pack record.
func (transaction *Transaction) flush() error {
	keys := make([]string, 0, len(transaction.staged))
	for key := range transaction.staged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := transaction.staged[key]
		dbKey := capabilityKey(transaction.keyspace, []byte(key))
		if value.deleted {
			if err := transaction.state.deleteValue(transaction.batch, dbKey); err != nil {
				return err
			}
		} else if err := transaction.state.stageValue(transaction.batch, dbKey, value.value); err != nil {
			return err
		}
	}
	keys = keys[:0]
	for key := range transaction.recordReferences {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		staged := transaction.recordReferences[key]
		dbKey := recordReferenceKey(transaction.keyspace, []byte(key))
		// A replacement or deletion invalidates any confirmation for the old
		// publication in the same atomic capability transaction.
		if err := transaction.batch.Delete(localConfirmationKey(transaction.keyspace, []byte(key)), nil); err != nil {
			return err
		}
		if err := transaction.batch.Delete(geoCoverageKey(transaction.keyspace, []byte(key)), nil); err != nil {
			return err
		}
		if staged.deleted {
			if err := transaction.batch.Delete(dbKey, nil); err != nil {
				return err
			}
		} else if err := setJSON(transaction.batch, dbKey, staged.reference); err != nil {
			return err
		}
	}
	return nil
}

func validateCapabilityKey(key []byte) error {
	if len(key) == 0 || len(key) > maxCapabilityKeyBytes {
		return errors.New("invalid capability key")
	}
	return nil
}
