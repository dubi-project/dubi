// Package routedstate binds engine data commands to an immutable DUBI
// Route Target before they enter a Raft StateMachine.
package routedstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"go.etcd.io/raft/v3/raftpb"
)

const FormatVersion = 1

const (
	Mutation      = "mutation"
	ActivateRange = "activate_range"
	RegionFence   = "region_fence"
	RegionHandoff = "region_handoff"
	ConfirmLocal  = "confirm_local"
	ConfirmGeo    = "confirm_geo"
	MirrorGeo     = "mirror_geo"
)

// Command is the platform envelope for a engine mutation. Payload remains
// engine-owned and opaque to the Raft and routing layers.
type Command struct {
	Version     int                         `json:"version"`
	RequestID   string                      `json:"request_id"`
	Type        string                      `json:"type"`
	Target      *router.Target              `json:"target,omitempty"`
	Range       *rootstate.Range            `json:"range,omitempty"`
	Payload     []byte                      `json:"payload,omitempty"`
	Fence       *regionfence.Descriptor     `json:"fence,omitempty"`
	Handoff     *regionfence.HandoffCommand `json:"handoff,omitempty"`
	Local       *LocalConfirmation          `json:"local,omitempty"`
	Geo         *GeoConfirmation            `json:"geo,omitempty"`
	GeoMetadata *GeoMetadataMirror          `json:"geo_metadata,omitempty"`
}

// Machine owns one engine Group's durable state. ApplyRouted must compare
// target with the Range fence stored in that same state and commit that check,
// the engine mutation, its result, and AppliedIndex atomically. A stale
// target is a persisted command rejection and must not be returned as a fatal
// storage error.
type Machine interface {
	AppliedIndex() uint64
	ConfState() *raftpb.ConfState
	ApplyRouted(index uint64, requestID string, target router.Target, payload []byte) error
	ApplyRange(index uint64, requestID string, descriptor rootstate.Range) error
	ApplyRegionFence(index uint64, requestID string, descriptor regionfence.Descriptor) error
	Advance(index uint64) error
	ApplyMembership(index uint64, conf *raftpb.ConfState) error
	Restore(index uint64, snapshot []byte, conf *raftpb.ConfState) error
	Snapshot() (data []byte, index uint64, conf *raftpb.ConfState, err error)
	Close() error
}

// Adapter prevents engine Groups from accepting an unscoped raw command.
// It deliberately has no public method that applies a bare engine payload.
type Adapter struct{ machine Machine }

type handoffMachine interface {
	ApplyRegionHandoff(uint64, string, regionfence.HandoffCommand) error
}

type localConfirmationMachine interface {
	ApplyLocalConfirmation(uint64, string, LocalConfirmation) error
}

type geoConfirmationMachine interface {
	ApplyGeoConfirmation(uint64, string, GeoConfirmation) error
}

type geoMetadataMachine interface {
	ApplyGeoMetadataMirror(uint64, string, GeoMetadataMirror) error
}

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
		return nil, errors.New("routed engine payload is empty")
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

func EncodeRegionFence(requestID string, fence regionfence.Descriptor) ([]byte, error) {
	if rootstate.ValidateRequestID(requestID) != nil || fence.Validate() != nil {
		return nil, errors.New("invalid regional fence command")
	}
	return json.Marshal(Command{Version: FormatVersion, RequestID: requestID, Type: RegionFence, Fence: &fence})
}

func EncodeRegionHandoff(requestID string, handoff regionfence.HandoffCommand) ([]byte, error) {
	if rootstate.ValidateRequestID(requestID) != nil || handoff.Validate() != nil {
		return nil, errors.New("invalid regional handoff command")
	}
	encoded, err := json.Marshal(Command{Version: FormatVersion, RequestID: requestID, Type: RegionHandoff, Handoff: &handoff})
	if len(encoded) > regionfence.MaxRecordBytes {
		return nil, errors.New("regional handoff exceeds command limit")
	}
	return encoded, err
}

func EncodeLocalConfirmation(requestID string, claim LocalConfirmation) ([]byte, error) {
	if rootstate.ValidateRequestID(requestID) != nil || claim.Validate() != nil {
		return nil, errors.New("invalid local confirmation command")
	}
	encoded, err := json.Marshal(Command{Version: FormatVersion, RequestID: requestID, Type: ConfirmLocal, Local: &claim})
	if len(encoded) > MaxLocalConfirmationBytes {
		return nil, errors.New("local confirmation exceeds command limit")
	}
	return encoded, err
}

func EncodeGeoConfirmation(requestID string, claim GeoConfirmation) ([]byte, error) {
	if rootstate.ValidateRequestID(requestID) != nil || claim.Validate() != nil {
		return nil, errors.New("invalid geo confirmation command")
	}
	encoded, err := json.Marshal(Command{Version: FormatVersion, RequestID: requestID, Type: ConfirmGeo, Geo: &claim})
	if len(encoded) > MaxGeoConfirmationBytes {
		return nil, errors.New("geo confirmation exceeds command limit")
	}
	return encoded, err
}

func EncodeGeoMetadataMirror(requestID string, mirror GeoMetadataMirror) ([]byte, error) {
	if rootstate.ValidateRequestID(requestID) != nil || mirror.Validate() != nil {
		return nil, errors.New("invalid geo metadata mirror command")
	}
	encoded, err := json.Marshal(Command{Version: FormatVersion, RequestID: requestID, Type: MirrorGeo, GeoMetadata: &mirror})
	if len(encoded) > MaxGeoMetadataMirrorBytes {
		return nil, errors.New("geo metadata mirror exceeds command limit")
	}
	return encoded, err
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
		if command.Target == nil || command.Range != nil || command.Fence != nil || command.Handoff != nil || command.Local != nil || command.Geo != nil || command.GeoMetadata != nil || command.Target.Validate() != nil || len(command.Payload) == 0 {
			return Command{}, errors.New("invalid routed mutation")
		}
	case ActivateRange:
		if command.Target != nil || command.Range == nil || command.Fence != nil || command.Handoff != nil || command.Local != nil || command.Geo != nil || command.GeoMetadata != nil || len(command.Payload) != 0 ||
			command.Range.Validate() != nil || len(command.Range.Start) != 0 || len(command.Range.End) != 0 ||
			command.Range.Epoch != 1 || command.Range.State != rootstate.RangeStateActive {
			return Command{}, errors.New("invalid initial Range activation")
		}
	case RegionFence:
		if command.Fence == nil || command.Fence.Validate() != nil || command.Handoff != nil || command.Local != nil || command.Geo != nil || command.GeoMetadata != nil || command.Target != nil || command.Range != nil || len(command.Payload) != 0 {
			return Command{}, errors.New("invalid regional fence command")
		}
	case RegionHandoff:
		if len(encoded) > regionfence.MaxRecordBytes || command.Handoff == nil || command.Handoff.Validate() != nil || command.Fence != nil || command.Local != nil || command.Geo != nil || command.GeoMetadata != nil || command.Target != nil || command.Range != nil || len(command.Payload) != 0 {
			return Command{}, errors.New("invalid regional handoff envelope")
		}
	case ConfirmLocal:
		if len(encoded) > MaxLocalConfirmationBytes || command.Local == nil || command.Local.Validate() != nil || command.Fence != nil || command.Handoff != nil || command.Geo != nil || command.GeoMetadata != nil || command.Target != nil || command.Range != nil || len(command.Payload) != 0 {
			return Command{}, errors.New("invalid local confirmation envelope")
		}
	case ConfirmGeo:
		if len(encoded) > MaxGeoConfirmationBytes || command.Geo == nil || command.Geo.Validate() != nil || command.Fence != nil || command.Handoff != nil || command.Target != nil || command.Range != nil || command.Local != nil || command.GeoMetadata != nil || len(command.Payload) != 0 {
			return Command{}, errors.New("invalid geo confirmation envelope")
		}
	case MirrorGeo:
		if len(encoded) > MaxGeoMetadataMirrorBytes || command.GeoMetadata == nil || command.GeoMetadata.Validate() != nil || command.Fence != nil || command.Handoff != nil || command.Local != nil || command.Geo != nil || command.Target != nil || command.Range != nil || len(command.Payload) != 0 {
			return Command{}, errors.New("invalid geo metadata mirror envelope")
		}
	default:
		return Command{}, errors.New("unsupported routed command type")
	}
	return command, nil
}

func (adapter *Adapter) ValidateCommand(encoded []byte) error {
	command, err := DecodeCommand(encoded)
	if err == nil && command.Type == RegionHandoff {
		if _, ok := adapter.machine.(handoffMachine); !ok {
			return errors.New("state machine does not support regional handoff")
		}
	}
	if err == nil && command.Type == ConfirmLocal {
		if _, ok := adapter.machine.(localConfirmationMachine); !ok {
			return errors.New("state machine does not support local confirmations")
		}
	}
	if err == nil && command.Type == ConfirmGeo {
		if _, ok := adapter.machine.(geoConfirmationMachine); !ok {
			return errors.New("state machine does not support geo confirmations")
		}
	}
	if err == nil && command.Type == MirrorGeo {
		if _, ok := adapter.machine.(geoMetadataMachine); !ok {
			return errors.New("state machine does not support geo metadata mirrors")
		}
	}
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
	if command.Type == RegionFence {
		return adapter.machine.ApplyRegionFence(index, command.RequestID, *command.Fence)
	}
	if command.Type == RegionHandoff {
		machine, ok := adapter.machine.(handoffMachine)
		if !ok {
			return errors.New("state machine does not support regional handoff")
		}
		return machine.ApplyRegionHandoff(index, command.RequestID, *command.Handoff)
	}
	if command.Type == ConfirmLocal {
		machine, ok := adapter.machine.(localConfirmationMachine)
		if !ok {
			return errors.New("state machine does not support local confirmations")
		}
		return machine.ApplyLocalConfirmation(index, command.RequestID, *command.Local)
	}
	if command.Type == ConfirmGeo {
		machine, ok := adapter.machine.(geoConfirmationMachine)
		if !ok {
			return errors.New("state machine does not support geo confirmations")
		}
		return machine.ApplyGeoConfirmation(index, command.RequestID, *command.Geo)
	}
	if command.Type == MirrorGeo {
		machine, ok := adapter.machine.(geoMetadataMachine)
		if !ok {
			return errors.New("state machine does not support geo metadata mirrors")
		}
		return machine.ApplyGeoMetadataMirror(index, command.RequestID, *command.GeoMetadata)
	}
	return adapter.machine.ApplyRouted(index, command.RequestID, *command.Target, command.Payload)
}
func (adapter *Adapter) ApplyMembership(index uint64, conf *raftpb.ConfState) error {
	return adapter.machine.ApplyMembership(index, conf)
}

func (adapter *Adapter) MembershipCut() (raft.MembershipCut, error) {
	reader, ok := adapter.machine.(raft.MembershipReader)
	if !ok {
		return raft.MembershipCut{}, errors.New("state machine does not support membership cuts")
	}
	return reader.MembershipCut()
}
func (adapter *Adapter) Restore(index uint64, snapshot []byte, conf *raftpb.ConfState) error {
	return adapter.machine.Restore(index, snapshot, conf)
}
func (adapter *Adapter) Snapshot() ([]byte, uint64, *raftpb.ConfState, error) {
	return adapter.machine.Snapshot()
}
