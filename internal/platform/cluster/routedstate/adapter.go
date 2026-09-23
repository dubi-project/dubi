// Package routedstate binds capability data commands to an immutable DUBI
// Route Target before they enter a Raft StateMachine.
package routedstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"go.etcd.io/raft/v3/raftpb"
)

const FormatVersion = 1

const (
	Mutation      = "mutation"
	ActivateRange = "activate_range"
)

// Command is the platform envelope for a capability mutation. Payload remains
// capability-owned and opaque to the Raft and routing layers.
type Command struct {
	Version   int              `json:"version"`
	RequestID string           `json:"request_id"`
	Type      string           `json:"type"`
	Target    *router.Target   `json:"target,omitempty"`
	Range     *rootstate.Range `json:"range,omitempty"`
	Payload   []byte           `json:"payload,omitempty"`
}

// Machine owns one capability Group's durable state. ApplyRouted must compare
// target with the Range fence stored in that same state and commit that check,
// the capability mutation, its result, and AppliedIndex atomically. A stale
// target is a persisted command rejection and must not be returned as a fatal
// storage error.
type Machine interface {
	AppliedIndex() uint64
	ConfState() *raftpb.ConfState
	ApplyRouted(index uint64, requestID string, target router.Target, payload []byte) error
	ApplyRange(index uint64, requestID string, descriptor rootstate.Range) error
	Advance(index uint64) error
	ApplyMembership(index uint64, conf *raftpb.ConfState) error
	Restore(index uint64, snapshot []byte, conf *raftpb.ConfState) error
	Snapshot() (data []byte, index uint64, conf *raftpb.ConfState, err error)
	Close() error
}

// Adapter prevents capability Groups from accepting an unscoped raw command.
// It deliberately has no public method that applies a bare capability payload.
type Adapter struct{ machine Machine }

var _ raft.StateMachine = (*Adapter)(nil)
var _ raft.Snapshotter = (*Adapter)(nil)
var _ raft.CommandValidator = (*Adapter)(nil)

func New(machine Machine) (*Adapter, error) {
	if machine == nil {
		return nil, errors.New("Routed StateMachine is nil")
	}
	return &Adapter{machine: machine}, nil
}

func EncodeMutation(requestID string, target router.Target, payload []byte) ([]byte, error) {
	if rootstate.ValidateRequestID(requestID) != nil || target.Validate() != nil {
		return nil, errors.New("invalid routed mutation identity")
	}
	if len(payload) == 0 {
		return nil, errors.New("routed capability payload is empty")
	}
	return json.Marshal(Command{Version: FormatVersion, RequestID: requestID, Type: Mutation,
		Target: &target, Payload: payload})
}

func EncodeActivateRange(requestID string, descriptor rootstate.Range) ([]byte, error) {
	if rootstate.ValidateRequestID(requestID) != nil || descriptor.Validate() != nil ||
		len(descriptor.Start) != 0 || len(descriptor.End) != 0 || descriptor.Epoch != 1 ||
		descriptor.State != rootstate.RangeStateActive {
		return nil, errors.New("invalid initial Range activation")
	}
	return json.Marshal(Command{Version: FormatVersion, RequestID: requestID, Type: ActivateRange,
		Range: &descriptor})
}

func DecodeCommand(encoded []byte) (Command, error) {
	var command Command
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&command); err != nil {
		return Command{}, fmt.Errorf("decode routed command: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Command{}, errors.New("trailing routed command data")
	}
	if command.Version != FormatVersion || rootstate.ValidateRequestID(command.RequestID) != nil {
		return Command{}, errors.New("invalid routed command")
	}
	switch command.Type {
	case Mutation:
		if command.Target == nil || command.Range != nil || command.Target.Validate() != nil || len(command.Payload) == 0 {
			return Command{}, errors.New("invalid routed mutation")
		}
	case ActivateRange:
		if command.Target != nil || command.Range == nil || len(command.Payload) != 0 ||
			command.Range.Validate() != nil || len(command.Range.Start) != 0 || len(command.Range.End) != 0 ||
			command.Range.Epoch != 1 || command.Range.State != rootstate.RangeStateActive {
			return Command{}, errors.New("invalid initial Range activation")
		}
	default:
		return Command{}, errors.New("unsupported routed command type")
	}
	return command, nil
}

func (adapter *Adapter) ValidateCommand(encoded []byte) error {
	_, err := DecodeCommand(encoded)
	return err
}

func (adapter *Adapter) AppliedIndex() uint64         { return adapter.machine.AppliedIndex() }
func (adapter *Adapter) ConfState() *raftpb.ConfState { return adapter.machine.ConfState() }
func (adapter *Adapter) Advance(index uint64) error   { return adapter.machine.Advance(index) }
func (adapter *Adapter) Close() error                 { return adapter.machine.Close() }
func (adapter *Adapter) Apply(index uint64, encoded []byte) error {
	command, err := DecodeCommand(encoded)
	if err != nil {
		return err
	}
	if command.Type == ActivateRange {
		return adapter.machine.ApplyRange(index, command.RequestID, *command.Range)
	}
	return adapter.machine.ApplyRouted(index, command.RequestID, *command.Target, command.Payload)
}
func (adapter *Adapter) ApplyMembership(index uint64, conf *raftpb.ConfState) error {
	return adapter.machine.ApplyMembership(index, conf)
}
func (adapter *Adapter) Restore(index uint64, snapshot []byte, conf *raftpb.ConfState) error {
	return adapter.machine.Restore(index, snapshot, conf)
}
func (adapter *Adapter) Snapshot() ([]byte, uint64, *raftpb.ConfState, error) {
	return adapter.machine.Snapshot()
}
