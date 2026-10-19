package enginestate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"go.etcd.io/raft/v3/raftpb"
)

type testMutation struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Reject    string `json:"reject,omitempty"`
	Increment bool   `json:"increment,omitempty"`
}

type testHandler struct{ calls int }

func (handler *testHandler) Apply(transaction *Transaction, _ router.Target, payload []byte) (Outcome, error) {
	handler.calls++
	var command testMutation
	if err := json.Unmarshal(payload, &command); err != nil {
		return Outcome{}, err
	}
	if command.Key == "" {
		return Outcome{}, errors.New("test engine key is empty")
	}
	if command.Increment {
		previous, found, err := transaction.Get([]byte(command.Key))
		if err != nil {
			return Outcome{}, err
		}
		if !found {
			previous = []byte("0")
		}
		command.Value = string(previous) + "+1"
	}
	if err := transaction.Set([]byte(command.Key), []byte(command.Value)); err != nil {
		return Outcome{}, err
	}
	if command.Reject != "" {
		return Outcome{Rejection: command.Reject}, nil
	}
	return Outcome{Value: []byte(command.Value)}, nil
}

func encodeTestMutation(t *testing.T, value testMutation) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestBootstrapMembershipPersistsWithoutAdvancingEngine(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cap-state")
	state, err := Open(root, "data-1", &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	conf := &raftpb.ConfState{Voters: []uint64{11}}
	if err := state.BootstrapMembership(conf); err != nil {
		t.Fatal(err)
	}
	if state.AppliedIndex() != 0 || len(state.ConfState().GetVoters()) != 1 || state.ConfState().GetVoters()[0] != 11 {
		t.Fatalf("bootstrap Engine membership = index %d, conf %v", state.AppliedIndex(), state.ConfState())
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(root, "data-1", &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.AppliedIndex() != 0 || len(reopened.ConfState().GetVoters()) != 1 || reopened.ConfState().GetVoters()[0] != 11 {
		t.Fatalf("reopened Engine bootstrap membership = index %d, conf %v", reopened.AppliedIndex(), reopened.ConfState())
	}
}

func TestEngineStateAtomicallyFencesAndAppliesMutation(t *testing.T) {
	root := t.TempDir()
	id, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	handler := &testHandler{}
	state, err := Open(filepath.Join(root, "cap-state"), "data-1", handler)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := routedstate.New(state)
	if err != nil {
		t.Fatal(err)
	}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: root, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if err := host.AddGroup("data-1", []uint64{id.RaftID}, false, adapter); err != nil {
		t.Fatal(err)
	}
	if err := host.Campaign("data-1"); err != nil {
		t.Fatal(err)
	}
	descriptor := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", EngineID: "record",
		DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	activation, err := routedstate.EncodeActivateRange("activate-a", descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Submit("data-1", activation); err != nil {
		t.Fatal(err)
	}
	if result, found, err := state.ResultForRange("activate-a", descriptor); err != nil || !found || !result.Applied {
		t.Fatalf("Range activation result = %+v, %v, %v", result, found, err)
	}
	target := router.Target{Keyspace: descriptor.Keyspace(), RangeID: descriptor.RangeID,
		OwnerGroupID: descriptor.OwnerGroupID, Epoch: descriptor.Epoch, RoutingKey: []byte("key")}
	payload := encodeTestMutation(t, testMutation{Key: "item/key", Value: "one"})
	mutation, err := routedstate.EncodeMutation("write-1", target, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Submit("data-1", mutation); err != nil {
		t.Fatal(err)
	}
	result, found, err := state.ResultForMutation("write-1", target, payload)
	if err != nil || !found || !result.Applied || string(result.Value) != "one" {
		t.Fatalf("mutation result = %+v, %v, %v", result, found, err)
	}
	if value, found, err := state.Value(target.Keyspace, []byte("item/key")); err != nil || !found || string(value) != "one" {
		t.Fatalf("engine value = %q, %v, %v", value, found, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	readIndex, err := state.ReadRouted(ctx, host, target, func(view ReadView) error {
		value, found, err := view.Get([]byte("item/key"))
		if err != nil || !found || string(value) != "one" {
			return errors.New("linearizable routed value is missing")
		}
		return nil
	})
	if err != nil || readIndex == 0 || readIndex > state.AppliedIndex() {
		t.Fatalf("linearizable routed read = index %d, applied %d, err %v", readIndex, state.AppliedIndex(), err)
	}

	stale := target
	stale.Epoch = 2
	stalePayload := encodeTestMutation(t, testMutation{Key: "item/stale", Value: "lost"})
	staleCommand, _ := routedstate.EncodeMutation("write-stale", stale, stalePayload)
	if err := host.Submit("data-1", staleCommand); err != nil {
		t.Fatal(err)
	}
	if rejected, found, err := state.Result("write-stale"); err != nil || !found ||
		rejected.Applied || rejected.Error != router.ErrRouteFenced.Error() {
		t.Fatalf("stale result = %+v, %v, %v", rejected, found, err)
	}
	if _, found, err := state.Value(target.Keyspace, []byte("item/stale")); err != nil || found {
		t.Fatalf("stale write escaped Fence: found=%v err=%v", found, err)
	}
	if handler.calls != 1 {
		t.Fatalf("handler calls after fenced write = %d", handler.calls)
	}

	before, err := host.Status("data-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Submit("data-1", []byte(`{"bare":true}`)); err == nil {
		t.Fatal("bare engine command entered Raft")
	}
	after, _ := host.Status("data-1")
	if after.CommitIndex != before.CommitIndex {
		t.Fatalf("invalid command changed CommitIndex: %d -> %d", before.CommitIndex, after.CommitIndex)
	}
}

func TestEngineStateDiscardsRejectedWritesAndDeduplicates(t *testing.T) {
	handler := &testHandler{}
	state, err := Open(t.TempDir(), "data-1", handler)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	descriptor := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", EngineID: "record",
		DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	if err := state.ApplyRange(1, "activate-a", descriptor); err != nil {
		t.Fatal(err)
	}
	target := router.Target{Keyspace: descriptor.Keyspace(), RangeID: descriptor.RangeID,
		OwnerGroupID: descriptor.OwnerGroupID, Epoch: 1, RoutingKey: []byte("key")}
	rejectedPayload := encodeTestMutation(t, testMutation{Key: "item/rejected", Value: "lost", Reject: "quota exceeded"})
	if err := state.ApplyRouted(2, "rejected", target, rejectedPayload); err != nil {
		t.Fatal(err)
	}
	if result, found, err := state.Result("rejected"); err != nil || !found || result.Applied || result.Error != "quota exceeded" {
		t.Fatalf("rejected result = %+v, %v, %v", result, found, err)
	}
	if _, found, err := state.Value(target.Keyspace, []byte("item/rejected")); err != nil || found {
		t.Fatalf("rejected staged write persisted: found=%v err=%v", found, err)
	}

	payload := encodeTestMutation(t, testMutation{Key: "counter", Increment: true})
	if err := state.ApplyRouted(3, "increment", target, payload); err != nil {
		t.Fatal(err)
	}
	if err := state.ApplyRouted(4, "increment", target, payload); err != nil {
		t.Fatal(err)
	}
	if value, found, err := state.Value(target.Keyspace, []byte("counter")); err != nil || !found || string(value) != "0+1" {
		t.Fatalf("deduplicated counter = %q, %v, %v", value, found, err)
	}
	changed := encodeTestMutation(t, testMutation{Key: "counter", Value: "reused"})
	if err := state.ApplyRouted(5, "increment", target, changed); err != nil {
		t.Fatal(err)
	}
	if _, found, err := state.ResultForMutation("increment", target, changed); !found || !errors.Is(err, ErrRequestIDReused) {
		t.Fatalf("RequestID reuse = found %v, err %v", found, err)
	}
	if handler.calls != 2 { // one rejected call and one successful increment
		t.Fatalf("deduplicated handler calls = %d", handler.calls)
	}
}

func TestEngineStateSnapshotRestoresFenceDataAndResults(t *testing.T) {
	handler := &testHandler{}
	source, err := Open(t.TempDir(), "data-1", handler)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	conf := &raftpb.ConfState{Voters: []uint64{1}}
	if err := source.ApplyMembership(1, conf); err != nil {
		t.Fatal(err)
	}
	descriptor := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", EngineID: "record",
		DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	if err := source.ApplyRange(2, "activate-a", descriptor); err != nil {
		t.Fatal(err)
	}
	target := router.Target{Keyspace: descriptor.Keyspace(), RangeID: descriptor.RangeID,
		OwnerGroupID: "data-1", Epoch: 1, RoutingKey: []byte("key")}
	payload := encodeTestMutation(t, testMutation{Key: "item", Value: "restored"})
	if err := source.ApplyRouted(3, "write-a", target, payload); err != nil {
		t.Fatal(err)
	}
	image, index, snapshotConf, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	targetState, err := Open(t.TempDir(), "data-1", &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer targetState.Close()
	if err := targetState.Restore(index, image, snapshotConf); err != nil {
		t.Fatal(err)
	}
	if value, found, err := targetState.Value(target.Keyspace, []byte("item")); err != nil || !found || !bytes.Equal(value, []byte("restored")) {
		t.Fatalf("restored value = %q, %v, %v", value, found, err)
	}
	if fence, found, err := targetState.Range("range-a"); err != nil || !found || fence.Epoch != 1 {
		t.Fatalf("restored Fence = %+v, %v, %v", fence, found, err)
	}
	if result, found, err := targetState.ResultForMutation("write-a", target, payload); err != nil || !found || !result.Applied {
		t.Fatalf("restored result = %+v, %v, %v", result, found, err)
	}
}

func TestRewriteSnapshotForRestoreChangesOnlyMembership(t *testing.T) {
	source, err := Open(t.TempDir(), "data-restore", &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err := source.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{11}}); err != nil {
		t.Fatal(err)
	}
	image, index, _, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	rewritten, err := RewriteSnapshotForRestore(image, &raftpb.ConfState{Voters: []uint64{21}})
	if err != nil {
		t.Fatal(err)
	}
	destination, err := Open(t.TempDir(), "data-restore", &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if err := destination.Restore(index, rewritten, &raftpb.ConfState{Voters: []uint64{21}}); err != nil {
		t.Fatal(err)
	}
	if got := destination.ConfState().GetVoters(); len(got) != 1 || got[0] != 21 {
		t.Fatalf("restored membership = %v", got)
	}
}

func TestRewriteSnapshotForRestoreRewritesRegionalFenceIdentity(t *testing.T) {
	image, err := json.Marshal(snapshotImage{Version: FormatVersion, GroupID: "data-restore", Applied: 4, Conf: []byte("old"), Entries: []snapshotEntry{{Key: regionFenceKey, Value: []byte(`{"fence":{"members":[{"raft_id":11}],"nodes":[11]}}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	rewritten, err := RewriteSnapshotForRestoreWithIdentity(image, &raftpb.ConfState{Voters: []uint64{21}}, rootstate.RestoreIdentityMap{RaftIDs: map[uint64]uint64{11: 21}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded snapshotImage
	if err := json.Unmarshal(rewritten, &decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(decoded.Entries[0].Value, []byte(`"raft_id":21`)) || !bytes.Contains(decoded.Entries[0].Value, []byte(`"nodes":[21]`)) {
		t.Fatalf("regional identity was not rewritten: %s", decoded.Entries[0].Value)
	}
}

func TestEngineStateDirectoryIsBoundToGroup(t *testing.T) {
	root := t.TempDir()
	state, err := Open(root, "data-1", &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, "data-2", &testHandler{}); err == nil {
		t.Fatal("engine state reopened under a different GroupID")
	}
	reopened, err := Open(root, "data-1", &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
