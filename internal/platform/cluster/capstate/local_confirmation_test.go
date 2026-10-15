package capstate

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"go.etcd.io/raft/v3/raftpb"
)

func confirmationFixture(t *testing.T) (*State, router.Target, routedstate.LocalConfirmation, *packedHandler) {
	t.Helper()
	handler := referenceHandler()
	state, err := Open(t.TempDir(), "data-1", handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	if err := state.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	regionalApply(t, state, "fence", regionalDescriptor(), true)
	target := packedTarget(t, state, "record", "tenant")
	target.RoutingKey = []byte("route-shard") // Routing and logical data keys are independent.
	if err := state.ApplyRouted(state.AppliedIndex()+1, "publish", target, []byte("body")); err != nil {
		t.Fatal(err)
	}
	ref, found, err := readRecordReference(t, state, target)
	if err != nil || !found {
		t.Fatal(ref, found, err)
	}
	id, err := ref.Descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	claim := routedstate.LocalConfirmation{Target: target, Key: []byte("item"), ReferenceRequestID: "publish",
		ReferenceIndex: ref.Index, RecordID: id, RegionID: "west", Generation: 1,
		GroupEpoch: 2, FenceIndex: 2, Nodes: []uint64{1, 2, 3},
		Holdings: []placement.Holding{
			{RaftID: 1, NodeID: "node_00000000000000000000000000000001", StoreID: "store_00000000000000000000000000000001", RecordID: id},
			{RaftID: 2, NodeID: "node_00000000000000000000000000000002", StoreID: "store_00000000000000000000000000000002", RecordID: id},
		}}
	if claim.Validate() != nil {
		t.Fatal("fixture claim invalid")
	}
	return state, target, claim, handler
}

func readLocalConfirmation(t *testing.T, state *State, target router.Target) (LocalConfirmationRecord, bool, error) {
	t.Helper()
	var record LocalConfirmationRecord
	var found bool
	_, err := state.ReadRouted(context.Background(), &fakeReadBarrier{index: state.AppliedIndex()}, target,
		func(view ReadView) error {
			var readErr error
			record, found, readErr = view.LocalConfirmation([]byte("item"))
			return readErr
		})
	return record, found, err
}

func TestLocalConfirmationRaftResultReplaySnapshotAndReferenceABA(t *testing.T) {
	state, target, claim, handler := confirmationFixture(t)
	wrong := claim
	wrong.Generation++
	if err := state.ApplyLocalConfirmation(state.AppliedIndex()+1, "wrong-generation", wrong); err != nil {
		t.Fatal(err)
	}
	if result, found, err := state.ResultForLocalConfirmation("wrong-generation", wrong); err != nil || !found || result.Applied {
		t.Fatal("stale Region generation was accepted", result, err)
	}
	if _, found, err := readLocalConfirmation(t, state, target); err != nil || found {
		t.Fatal("rejected confirmation published a receipt", found, err)
	}
	encoded, err := routedstate.EncodeLocalConfirmation("confirm", claim)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := routedstate.New(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Apply(state.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	receipt, found, err := readLocalConfirmation(t, state, target)
	if err != nil || !found || receipt.Claim.ReferenceIndex != claim.ReferenceIndex || receipt.Index != state.AppliedIndex() {
		t.Fatal("confirmed majority was not persisted with its reference", receipt, found, err)
	}
	if err := adapter.Apply(state.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	replayed, found, err := readLocalConfirmation(t, state, target)
	if err != nil || !found || replayed.Index != receipt.Index {
		t.Fatal("RequestID replay replaced its durable receipt", replayed, err)
	}
	changed := claim
	changed.Generation++
	if _, found, err := state.ResultForLocalConfirmation("confirm", changed); !found || !errors.Is(err, ErrRequestIDReused) {
		t.Fatal("changed confirmation reused RequestID", found, err)
	}
	image, index, conf, err := state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := Open(t.TempDir(), "data-1", referenceHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := restored.Restore(index, image, conf); err != nil {
		t.Fatal("restore local confirmation", err)
	}
	if got, found, err := readLocalConfirmation(t, restored, target); err != nil || !found || got.Index != receipt.Index {
		t.Fatal("snapshot lost durable confirmation", got, found, err)
	}
	// Re-publishing the same immutable bytes is still a new logical version.
	handler.action = func(tx *Transaction, _ []byte) (Outcome, error) {
		d, err := records.Describe(target.Keyspace, "origin-group", []byte("item"), []byte("body"))
		if err != nil {
			return Outcome{}, err
		}
		return Outcome{}, tx.SetRecordReference([]byte("item"), d)
	}
	if err := state.ApplyRouted(state.AppliedIndex()+1, "republish", target, []byte("body")); err != nil {
		t.Fatal(err)
	}
	if _, found, err := readLocalConfirmation(t, state, target); err != nil || found {
		t.Fatal("old receipt survived same-content reference ABA", found, err)
	}
	if err := state.ApplyLocalConfirmation(state.AppliedIndex()+1, "stale-confirm", claim); err != nil {
		t.Fatal(err)
	}
	if result, found, err := state.ResultForLocalConfirmation("stale-confirm", claim); err != nil || !found || result.Applied {
		t.Fatal("old publication was reconfirmed after ABA", result, err)
	}
}

func TestLocalConfirmationCorruptBacklinkFailsReadAndSnapshot(t *testing.T) {
	state, target, claim, _ := confirmationFixture(t)
	if err := state.ApplyLocalConfirmation(state.AppliedIndex()+1, "confirm", claim); err != nil {
		t.Fatal(err)
	}
	receipt, found, err := readLocalConfirmation(t, state, target)
	if err != nil || !found {
		t.Fatal(receipt, err)
	}
	receipt.Claim.ReferenceIndex++
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.db.Set(localConfirmationKey(target.Keyspace, []byte("item")), encoded, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, found, err := readLocalConfirmation(t, state, target); err == nil || found {
		t.Fatal("forged confirmation passed Owner read", found, err)
	}
	if _, _, _, err := state.Snapshot(); err == nil {
		t.Fatal("forged confirmation passed inline snapshot validation")
	}
}

func TestLocalConfirmationStreamSnapshotRoundTrip(t *testing.T) {
	state, target, claim, _ := confirmationFixture(t)
	if err := state.ApplyLocalConfirmation(state.AppliedIndex()+1, "confirm", claim); err != nil {
		t.Fatal(err)
	}
	artifacts, _ := fileArtifacts(t)
	prepared, err := state.PrepareSnapshotTo(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	encoded, err := prepared.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	dest, err := Open(t.TempDir(), "data-1", referenceHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer dest.Close()
	if err := dest.RestoreFrom(context.Background(), artifacts, prepared.Index, encoded, prepared.Conf); err != nil {
		t.Fatal("stream snapshot lost local confirmation", err)
	}
	if receipt, found, err := readLocalConfirmation(t, dest, target); err != nil || !found || receipt.Claim.ReferenceIndex != claim.ReferenceIndex {
		t.Fatal("restored Owner cut lacks local confirmation", receipt, found, err)
	}
}

func TestLocalConfirmationRemovedWithReferenceDeletion(t *testing.T) {
	state, target, claim, _ := confirmationFixture(t)
	if err := state.ApplyLocalConfirmation(state.AppliedIndex()+1, "confirm", claim); err != nil {
		t.Fatal(err)
	}
	if _, found, err := readLocalConfirmation(t, state, target); err != nil || !found {
		t.Fatal("fixture confirmation absent", err)
	}
	if err := state.ApplyRouted(state.AppliedIndex()+1, "unlink", target, []byte("unlink")); err != nil {
		t.Fatal(err)
	}
	if _, found, err := readLocalConfirmation(t, state, target); err != nil || found {
		t.Fatal("deleted reference retained old local confirmation", found, err)
	}
	if _, found, err := readRecordReference(t, state, target); err != nil || found {
		t.Fatal("reference deletion did not commit with confirmation deletion", found, err)
	}
}
