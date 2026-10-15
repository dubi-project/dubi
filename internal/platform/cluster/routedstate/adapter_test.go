package routedstate

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type testMachine struct {
	applied    uint64
	conf       *raftpb.ConfState
	target     router.Target
	rangeValue rootstate.Range
	requestID  string
	payload    []byte
	calls      int
	closed     bool
}

type testHandoffMachine struct{ *testMachine }

func (m *testHandoffMachine) ApplyRegionHandoff(index uint64, id string, _ regionfence.HandoffCommand) error {
	m.applied, m.requestID = index, id
	m.calls++
	return nil
}

func TestAdapterRegionHandoffIsExclusiveAndRequiresSupport(t *testing.T) {
	i := regionfence.HandoffIntent{Previous: regionfence.Record{Fence: regionfence.Descriptor{RegionID: "west", GroupID: "data-1", Generation: 1, PlannedIndex: 100, GroupEpoch: 2, MembershipIndex: 1, Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}, RequestID: "fence", Index: 2}, Next: rootstate.RegionCohort{RegionID: "west", GroupID: "data-1", Generation: 2, PlannedIndex: 101, Nodes: []uint64{1, 2, 3, 4}, State: rootstate.RegionCohortPlanned}}
	encoded, err := EncodeRegionHandoff("seal", regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &i})
	if err != nil {
		t.Fatal(err)
	}
	unsupported, _ := New(&testMachine{})
	if err := unsupported.ValidateCommand(encoded); err == nil {
		t.Fatal("unsupported handoff passed proposal validation")
	}
	if err := unsupported.Apply(3, encoded); err == nil {
		t.Fatal("unsupported handoff applied")
	}
	m := &testHandoffMachine{testMachine: &testMachine{}}
	a, _ := New(m)
	if err := a.ValidateCommand(encoded); err != nil {
		t.Fatal(err)
	}
	if err := a.Apply(3, encoded); err != nil || m.applied != 3 || m.calls != 1 {
		t.Fatal(err, m)
	}
	command, _ := DecodeCommand(encoded)
	for _, mutate := range []func(*Command){
		func(c *Command) { c.Payload = []byte("payload") },
		func(c *Command) { target := validTarget(); c.Target = &target },
		func(c *Command) { c.Range = &rootstate.Range{} },
		func(c *Command) { c.Fence = &i.Previous.Fence },
		func(c *Command) { c.Handoff = nil },
		func(c *Command) { c.Type = RegionFence; c.Fence = &i.Previous.Fence },
		func(c *Command) {
			c.Type = Mutation
			target := validTarget()
			c.Target = &target
			c.Payload = []byte("payload")
		},
	} {
		bad := command
		mutate(&bad)
		data, _ := json.Marshal(bad)
		if err := a.Apply(4, data); err == nil || m.calls != 1 {
			t.Fatal("mixed handoff reached state", err)
		}
	}
}

func TestAdapterRegionFenceCannotCarryCapabilityPayload(t *testing.T) {
	d := regionfence.Descriptor{RegionID: "west", GroupID: "data-1", Generation: 1, PlannedIndex: 100, GroupEpoch: 2, MembershipIndex: 3,
		Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}
	encoded, err := EncodeRegionFence("barrier", d)
	if err != nil {
		t.Fatal(err)
	}
	m := &testMachine{}
	a, _ := New(m)
	if err := a.Apply(9, encoded); err != nil || m.calls != 1 || m.applied != 9 || m.requestID != "barrier" {
		t.Fatal(m, err)
	}
	c, err := DecodeCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Command){
		func(c *Command) { c.Payload = []byte("business-data") },
		func(c *Command) { target := validTarget(); c.Target = &target },
		func(c *Command) { c.Range = &rootstate.Range{} },
		func(c *Command) {
			c.Type = Mutation
			target := validTarget()
			c.Target = &target
			c.Payload = []byte("payload")
		},
		func(c *Command) { c.Fence = nil },
	} {
		bad := c
		mutate(&bad)
		data, _ := json.Marshal(bad)
		if err := a.Apply(10, data); err == nil || m.calls != 1 {
			t.Fatal("mixed envelope accepted", string(data), err)
		}
	}
}

func (machine *testMachine) ApplyRegionFence(index uint64, requestID string, _ regionfence.Descriptor) error {
	machine.applied, machine.requestID = index, requestID
	machine.calls++
	return nil
}

func (machine *testMachine) AppliedIndex() uint64 { return machine.applied }
func (machine *testMachine) ConfState() *raftpb.ConfState {
	return proto.Clone(machine.conf).(*raftpb.ConfState)
}
func (machine *testMachine) ApplyRouted(index uint64, requestID string, target router.Target, payload []byte) error {
	machine.applied = index
	machine.requestID = requestID
	machine.target = target
	machine.payload = bytes.Clone(payload)
	machine.calls++
	return nil
}
func (machine *testMachine) ApplyRange(index uint64, requestID string, descriptor rootstate.Range) error {
	machine.applied = index
	machine.requestID = requestID
	machine.rangeValue = descriptor
	machine.calls++
	return nil
}
func (machine *testMachine) Advance(index uint64) error {
	machine.applied = index
	return nil
}
func (machine *testMachine) ApplyMembership(index uint64, conf *raftpb.ConfState) error {
	machine.applied = index
	machine.conf = proto.Clone(conf).(*raftpb.ConfState)
	return nil
}
func (machine *testMachine) Restore(index uint64, _ []byte, conf *raftpb.ConfState) error {
	machine.applied = index
	machine.conf = proto.Clone(conf).(*raftpb.ConfState)
	return nil
}
func (machine *testMachine) Snapshot() ([]byte, uint64, *raftpb.ConfState, error) {
	return []byte("snapshot"), machine.applied, machine.ConfState(), nil
}
func (machine *testMachine) Close() error {
	machine.closed = true
	return nil
}

func validTarget() router.Target {
	return router.Target{
		Keyspace: rootstate.Keyspace{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items"},
		RangeID:  "range-a", OwnerGroupID: "data-1", Epoch: 3, RoutingKey: []byte("key"),
	}
}

func TestAdapterAppliesOnlyRoutedCapabilityCommands(t *testing.T) {
	machine := &testMachine{conf: &raftpb.ConfState{Voters: []uint64{1}}}
	adapter, err := New(machine)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeMutation("request-1", validTarget(), []byte("mutation"))
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Apply(7, encoded); err != nil {
		t.Fatal(err)
	}
	if machine.calls != 1 || machine.applied != 7 || machine.requestID != "request-1" || machine.target.RangeID != "range-a" ||
		string(machine.payload) != "mutation" {
		t.Fatalf("routed Apply = calls %d, index %d, target %+v, payload %q",
			machine.calls, machine.applied, machine.target, machine.payload)
	}
	if err := adapter.Apply(8, []byte(`{"version":1,"payload":"bXV0YXRpb24="}`)); err == nil || machine.calls != 1 {
		t.Fatalf("unscoped Apply error = %v, calls = %d", err, machine.calls)
	}
	if err := adapter.Apply(8, append(encoded, []byte(` {}`)...)); err == nil || machine.calls != 1 {
		t.Fatalf("trailing Apply error = %v, calls = %d", err, machine.calls)
	}
}

func TestRoutedCommandRejectsMalformedTargetAndUnknownFields(t *testing.T) {
	target := validTarget()
	target.RangeID = "not/a/range"
	if _, err := EncodeMutation("request-1", target, []byte("mutation")); err == nil {
		t.Fatalf("invalid RangeID error = %v", err)
	}
	encoded, err := EncodeMutation("request-1", validTarget(), []byte("mutation"))
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded[:len(encoded)-1], []byte(`,"unexpected":true}`)...)
	if _, err := DecodeCommand(encoded); err == nil {
		t.Fatal("unknown routed command field was accepted")
	}
}

func TestAdapterAppliesInitialRangeActivation(t *testing.T) {
	machine := &testMachine{conf: &raftpb.ConfState{Voters: []uint64{1}}}
	adapter, _ := New(machine)
	descriptor := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", CapabilityID: "record",
		DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	encoded, err := EncodeActivateRange("activate-1", descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Apply(3, encoded); err != nil {
		t.Fatal(err)
	}
	if machine.calls != 1 || machine.requestID != "activate-1" || !reflect.DeepEqual(machine.rangeValue, descriptor) {
		t.Fatalf("Range activation = calls %d, request %q, descriptor %+v",
			machine.calls, machine.requestID, machine.rangeValue)
	}
}

func TestAdapterDelegatesLifecycleAndSnapshot(t *testing.T) {
	machine := &testMachine{conf: &raftpb.ConfState{Voters: []uint64{1}}}
	adapter, _ := New(machine)
	if err := adapter.Advance(1); err != nil {
		t.Fatal(err)
	}
	conf := &raftpb.ConfState{Voters: []uint64{1, 2}}
	if err := adapter.ApplyMembership(2, conf); err != nil {
		t.Fatal(err)
	}
	data, index, gotConf, err := adapter.Snapshot()
	if err != nil || string(data) != "snapshot" || index != 2 || !proto.Equal(conf, gotConf) {
		t.Fatalf("snapshot = %q, %d, %+v, %v", data, index, gotConf, err)
	}
	if err := adapter.Close(); err != nil || !machine.closed {
		t.Fatalf("close = %v, closed = %v", err, machine.closed)
	}
}
