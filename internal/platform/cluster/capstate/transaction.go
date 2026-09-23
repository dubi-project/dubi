package capstate

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/pebble"
)

const maxCapabilityKeyBytes = 4096

type transactionValue struct {
	value   []byte
	deleted bool
}

// Transaction is valid only during Handler.Apply. Keys are capability-local;
// platform metadata and results are not addressable through this API.
type Transaction struct {
	state        *State
	batch        *pebble.Batch
	capabilityID string
	staged       map[string]transactionValue
}

func newTransaction(state *State, batch *pebble.Batch, capabilityID string) *Transaction {
	return &Transaction{state: state, batch: batch, capabilityID: capabilityID,
		staged: make(map[string]transactionValue)}
}

func (transaction *Transaction) Get(key []byte) ([]byte, bool, error) {
	if err := validateCapabilityKey(key); err != nil {
		return nil, false, err
	}
	if value, found := transaction.staged[string(key)]; found {
		return bytes.Clone(value.value), !value.deleted, nil
	}
	return transaction.state.get(capabilityKey(transaction.capabilityID, key))
}

func (transaction *Transaction) Set(key, value []byte) error {
	if err := validateCapabilityKey(key); err != nil {
		return err
	}
	if value == nil {
		return errors.New("capability value is nil")
	}
	copy := bytes.Clone(value)
	transaction.staged[string(key)] = transactionValue{value: copy}
	return transaction.batch.Set(capabilityKey(transaction.capabilityID, key), copy, nil)
}

func (transaction *Transaction) Delete(key []byte) error {
	if err := validateCapabilityKey(key); err != nil {
		return err
	}
	transaction.staged[string(key)] = transactionValue{deleted: true}
	return transaction.batch.Delete(capabilityKey(transaction.capabilityID, key), nil)
}

func validateCapabilityKey(key []byte) error {
	if len(key) == 0 || len(key) > maxCapabilityKeyBytes {
		return errors.New("invalid capability key")
	}
	return nil
}
