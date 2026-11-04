package records

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

const retentionSourceA = "node_00000000000000000000000000000001"
const retentionSourceB = "node_00000000000000000000000000000002"

func balanceKeepers() []rootstate.Node {
	return []rootstate.Node{{RaftID: 2, NodeID: retentionSourceB, RegionID: "west", Address: "two:7443"},
		{RaftID: 3, NodeID: "node_00000000000000000000000000000003", RegionID: "west", Address: "three:7443"}}
}

func TestRetentionRejectsLatePinAndPreservesNewerPin(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t)
	d := descriptor(t, "retained", "payload")
	put(t, s, d, "payload")
	if err := s.ReleaseRetention(ctx, retentionSourceA, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Retain(ctx, retentionSourceA, 4, d); !errors.Is(err, ErrRetentionReleased) {
		t.Fatal("delayed pin recreated an acknowledged release", err)
	}
	for range 2 {
		if _, err := s.Retain(ctx, retentionSourceA, 5, d); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ReleaseRetention(ctx, retentionSourceA, 6); err == nil {
		t.Fatal("future release erased an unfinished older pin")
	}
	if err := s.ReleaseRetention(ctx, retentionSourceA, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginNodeBalance(ctx, d, strings.Repeat("a", 64), balanceKeepers()); !errors.Is(err, ErrRetained) {
		t.Fatal("late release erased a newer holding promise", err)
	}
}

func TestCapacityHandoffRestartsWithDurableOffloadHint(t *testing.T) {
	ctx := context.Background()
	s, packs := testStore(t)
	d := descriptor(t, "move", "payload")
	put(t, s, d, "payload")
	task, err := s.BeginNodeBalance(ctx, d, strings.Repeat("a", 64), balanceKeepers())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FinishNodeBalance(ctx, task.Sequence); err == nil {
		t.Fatal("unfinished task forgot its keeper releases")
	}
	if err := s.DropBalanceHolding(ctx, task.Sequence); err != nil {
		t.Fatal(err)
	}
	s = reopen(t, s, packs)
	recovered, err := s.NodeBalanceTask()
	if err != nil || recovered == nil || recovered.Phase != BalanceDropped || recovered.Sequence != task.Sequence {
		t.Fatal("lost durable source drop/release progress", recovered, err)
	}
	if _, err := s.Verify(ctx, d); !errors.Is(err, ErrNotHeld) {
		t.Fatal("offloaded local index still advertises a holding", err)
	}
	if scope, found, err := s.CapacityOffload(d); err != nil || !found || scope != task.Scope {
		t.Fatal("restart lost the maintenance offload hint", scope, found, err)
	}
	if err := s.FinishNodeBalance(ctx, task.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := s.DropBalanceHolding(ctx, task.Sequence); err == nil {
		t.Fatal("old task could delete a later holding")
	}
	put(t, s, d, "payload")
	if _, found, err := s.CapacityOffload(d); err != nil || found {
		t.Fatal("foreground cache did not clear the old offload hint", found, err)
	}
	next, err := s.BeginNodeBalance(ctx, d, strings.Repeat("b", 64), balanceKeepers())
	if err != nil || next.Sequence <= task.Sequence {
		t.Fatal("handoff sequence reused after restart", next, err)
	}
}

// Both sources may observe the same three-node majority. Durable keeper pins
// must prevent them from simultaneously deleting each other's observations.
func TestConcurrentCapacityHandoffsCannotDeleteEachOthersMajority(t *testing.T) {
	ctx := context.Background()
	a, _ := testStore(t)
	b, _ := testStore(t)
	c, _ := testStore(t)
	d := descriptor(t, "same-record", "payload")
	for _, store := range []*Store{a, b, c} {
		put(t, store, d, "payload")
	}
	ta, err := a.BeginNodeBalance(ctx, d, strings.Repeat("a", 64), balanceKeepers())
	if err != nil {
		t.Fatal(err)
	}
	tb, err := b.BeginNodeBalance(ctx, d, strings.Repeat("a", 64), balanceKeepers())
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range []struct {
		store  *Store
		source string
		seq    uint64
	}{{b, retentionSourceA, ta.Sequence}, {c, retentionSourceA, ta.Sequence}, {a, retentionSourceB, tb.Sequence}, {c, retentionSourceB, tb.Sequence}} {
		if _, err := pin.store.Retain(ctx, pin.source, pin.seq, d); err != nil {
			t.Fatal(err)
		}
	}
	for _, store := range []*Store{a, b} {
		if err := store.DropBalanceHolding(ctx, 1); !errors.Is(err, ErrRetained) {
			t.Fatal("concurrent source deleted a promised keeper holding", err)
		}
	}
	if err := a.CancelNodeBalance(ctx, ta.Sequence); err != nil {
		t.Fatal(err)
	}
	for _, keeper := range []*Store{b, c} {
		if err := keeper.ReleaseRetention(ctx, retentionSourceA, ta.Sequence); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.DropBalanceHolding(ctx, tb.Sequence); err != nil {
		t.Fatal("remaining handoff failed to make progress", err)
	}
	if err := a.DropBalanceHolding(ctx, ta.Sequence); err == nil {
		t.Fatal("canceled source resumed destructive work")
	}
	for _, keeper := range []*Store{a, c} {
		if _, err := keeper.Verify(ctx, d); err != nil {
			t.Fatal("surviving two-node majority was lost", err)
		}
	}
}

func TestRetentionSyncUncertaintyStopsStoreAndRecoversDurablePin(t *testing.T) {
	ctx := context.Background()
	s, packs := testStore(t)
	d := descriptor(t, "uncertain-pin", "payload")
	put(t, s, d, "payload")
	s.commit = func(batch *pebble.Batch) error {
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
		return errors.New("lost Sync acknowledgement")
	}
	if _, err := s.Retain(ctx, retentionSourceA, 1, d); err == nil {
		t.Fatal("uncertain retention returned success")
	}
	if _, err := s.Verify(ctx, d); err == nil {
		t.Fatal("uncertain Store remained available")
	}
	s = reopen(t, s, packs)
	if _, err := s.BeginNodeBalance(ctx, d, strings.Repeat("a", 64), balanceKeepers()); !errors.Is(err, ErrRetained) {
		t.Fatal("restart lost a durable keeper promise", err)
	}
}

func TestCapacityReclaimAcknowledgementCannotEraseLaterDrops(t *testing.T) {
	ctx := context.Background()
	s, packs := testStore(t)
	d := descriptor(t, "reclaim", "payload")
	put(t, s, d, "payload")
	first, err := s.BeginNodeBalance(ctx, d, strings.Repeat("a", 64), balanceKeepers())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DropBalanceHolding(ctx, first.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishNodeBalance(ctx, first.Sequence); err != nil {
		t.Fatal(err)
	}
	s = reopen(t, s, packs)
	through, needed, err := s.NodeBalanceReclaim()
	if err != nil || !needed || through != first.Sequence {
		t.Fatal("restart lost pending Pack reclaim", through, needed, err)
	}
	put(t, s, d, "payload")
	second, err := s.BeginNodeBalance(ctx, d, strings.Repeat("b", 64), balanceKeepers())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DropBalanceHolding(ctx, second.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteNodeBalanceReclaim(ctx, first.Sequence); err != nil {
		t.Fatal(err)
	}
	through, needed, err = s.NodeBalanceReclaim()
	if err != nil || !needed || through != second.Sequence {
		t.Fatal("stale GC acknowledgement erased a later drop", through, needed, err)
	}
	if err := s.CompleteNodeBalanceReclaim(ctx, second.Sequence); err != nil {
		t.Fatal(err)
	}
	_, needed, err = s.NodeBalanceReclaim()
	if err != nil || needed {
		t.Fatal("completed GC retained old work", needed, err)
	}
}
