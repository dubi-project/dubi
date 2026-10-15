package capstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

type memoryPacks struct {
	id      string
	records map[pack.Location]pack.Record
	calls   int
	failAt  int
}

func (p *memoryPacks) StoreID() string { return p.id }
func (p *memoryPacks) Append(_ context.Context, record pack.Record) (pack.Location, error) {
	p.calls++
	if p.calls == p.failAt {
		return pack.Location{}, errors.New("injected Pack IO error")
	}
	loc := pack.Location{Scope: pack.Scope{StoreID: p.id, VolumeID: "v", PackID: "p"}, Offset: int64(p.calls)}
	record.Key, record.Payload = bytes.Clone(record.Key), bytes.Clone(record.Payload)
	if p.records == nil {
		p.records = make(map[pack.Location]pack.Record)
	}
	p.records[loc] = record
	return loc, nil
}
func (p *memoryPacks) Read(loc pack.Location) (pack.Record, error) {
	record, found := p.records[loc]
	if !found {
		return pack.Record{}, errors.New("missing Pack record")
	}
	record.Key, record.Payload = bytes.Clone(record.Key), bytes.Clone(record.Payload)
	return record, nil
}

type packedHandler struct {
	calls  int
	action func(*Transaction, []byte) (Outcome, error)
}

func (h *packedHandler) Apply(tx *Transaction, _ router.Target, payload []byte) (Outcome, error) {
	h.calls++
	if h.action != nil {
		return h.action(tx, payload)
	}
	return Outcome{Value: []byte("ok")}, tx.Set([]byte("item"), payload)
}

func (h *packedHandler) Read(view ReadView, _ router.Target, payload []byte) (Outcome, error) {
	value, found, err := view.Get(payload)
	if err != nil {
		return Outcome{}, err
	}
	if !found {
		return Outcome{Rejection: "not found"}, nil
	}
	return Outcome{Value: value}, nil
}

func packedTarget(t *testing.T, s *State, capability, tenant string) router.Target {
	t.Helper()
	i := s.AppliedIndex() + 1
	descriptor := rootstate.Range{RangeID: fmt.Sprintf("range-%d", i), TenantID: tenant, CapabilityID: capability,
		DatasetID: "items", OwnerGroupID: s.groupID, Epoch: 1, State: rootstate.RangeStateActive}
	if err := s.ApplyRange(i, fmt.Sprintf("activate-%d", i), descriptor); err != nil {
		t.Fatal(err)
	}
	return router.Target{Keyspace: descriptor.Keyspace(), RangeID: descriptor.RangeID,
		OwnerGroupID: s.groupID, Epoch: 1, RoutingKey: []byte("item")}
}

func readRef(t *testing.T, s *State, capability, key string) packReference {
	t.Helper()
	encoded, found, err := s.get(referenceKey(capabilityKey(soleKeyspace(t, s, capability), []byte(key))))
	if err != nil || !found {
		t.Fatalf("missing local reference: %v, %v", found, err)
	}
	var ref packReference
	if err := decodePackJSON(encoded, &ref); err != nil {
		t.Fatal(err)
	}
	return ref
}

func requireValue(t *testing.T, s *State, capability, key string, expected []byte) {
	t.Helper()
	value, found, err := s.Value(soleKeyspace(t, s, capability), []byte(key))
	if err != nil || !found || !bytes.Equal(value, expected) {
		t.Fatalf("value %s/%s: found=%v bytes=%d want=%d err=%v", capability, key, found, len(value), len(expected), err)
	}
}

// This helper is only for the pre-existing single-Keyspace-per-capability
// fixtures. Isolation tests use explicit Keyspaces and never infer ownership.
func soleKeyspace(t *testing.T, s *State, capability string) rootstate.Keyspace {
	t.Helper()
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/fence/range/"), UpperBound: []byte("data/fence/range0")})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	var keyspace rootstate.Keyspace
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		var descriptor rootstate.Range
		if err := json.Unmarshal(iter.Value(), &descriptor); err != nil {
			t.Fatal(err)
		}
		if descriptor.CapabilityID == capability {
			count++
			keyspace = descriptor.Keyspace()
		}
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("fixture requires one Keyspace for %s, found %d", capability, count)
	}
	return keyspace
}

func TestPackedTransactionTransitionsAndDeduplication(t *testing.T) {
	p := &memoryPacks{id: "store-a"}
	h := &packedHandler{}
	s, err := open(t.TempDir(), "data-1", h, p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	target := packedTarget(t, s, "record", "tenant-a")
	large := bytes.Repeat([]byte("a"), PackValueThreshold)
	if err := s.ApplyRouted(2, "large", target, large); err != nil {
		t.Fatal(err)
	}
	if p.calls != 1 {
		t.Fatal("expected one Pack append", p.calls)
	}
	ref := readRef(t, s, "record", "item")
	if _, found, err := s.get(capabilityKey(target.Keyspace, []byte("item"))); found || err != nil {
		t.Fatal("duplicate inline body", err)
	}
	if p.records[ref.Location].TenantID != "tenant-a" {
		t.Fatal("lost tenant attribution")
	}
	requireValue(t, s, "record", "item", large)
	outcome, index, err := s.QueryRouted(context.Background(), &fakeReadBarrier{index: 2}, target, []byte("item"))
	if err != nil || index != 2 || !bytes.Equal(outcome.Value, large) {
		t.Fatal("routed Pack read", err)
	}
	if err := s.ApplyRouted(3, "large", target, large); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyRouted(4, "large", target, []byte("different")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ResultForMutation("large", target, []byte("different")); !errors.Is(err, ErrRequestIDReused) {
		t.Fatal(err)
	}
	if p.calls != 1 || h.calls != 1 {
		t.Fatal("duplicate mutation reached Pack")
	}
	if err := s.ApplyRouted(5, "small", target, []byte("small")); err != nil {
		t.Fatal(err)
	}
	requireValue(t, s, "record", "item", []byte("small"))
	if _, found, _ := s.get(referenceKey(capabilityKey(target.Keyspace, []byte("item")))); found {
		t.Fatal("small replacement kept stale reference")
	}
	if err := s.ApplyRouted(6, "large-again", target, large); err != nil {
		t.Fatal(err)
	}
	h.action = func(tx *Transaction, _ []byte) (Outcome, error) {
		previous, found, err := tx.Get([]byte("item"))
		if err != nil || !found || !bytes.Equal(previous, large) {
			return Outcome{}, errors.New("transaction could not read Pack")
		}
		if err := tx.Set([]byte("item"), large); err != nil {
			return Outcome{}, err
		}
		if err := tx.Delete([]byte("item")); err != nil {
			return Outcome{}, err
		}
		if _, found, err := tx.Get([]byte("item")); found || err != nil {
			return Outcome{}, errors.New("staged delete invisible")
		}
		return Outcome{}, nil
	}
	if err := s.ApplyRouted(7, "delete", target, []byte("delete")); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.Value(target.Keyspace, []byte("item")); found || err != nil {
		t.Fatal("deleted value visible", err)
	}
	if p.calls != 2 || len(p.records) != 2 {
		t.Fatal("Set-then-Delete appended, or deleted physical history")
	}
}

func TestPackedRejectedAndSupersededWritesDoNotAppend(t *testing.T) {
	p := &memoryPacks{id: "store-a"}
	large := bytes.Repeat([]byte("r"), PackValueThreshold)
	h := &packedHandler{action: func(tx *Transaction, _ []byte) (Outcome, error) {
		return Outcome{Rejection: "rejected"}, tx.Set([]byte("item"), large)
	}}
	s, err := open(t.TempDir(), "data-1", h, p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	target := packedTarget(t, s, "record", "t")
	if err := s.ApplyRouted(2, "reject", target, large); err != nil {
		t.Fatal(err)
	}
	h.action = func(tx *Transaction, _ []byte) (Outcome, error) {
		if err := tx.Set([]byte("item"), large); err != nil {
			return Outcome{}, err
		}
		value, found, err := tx.Get([]byte("item"))
		if err != nil || !found || !bytes.Equal(value, large) {
			return Outcome{}, errors.New("read-your-write failed")
		}
		return Outcome{}, tx.Set([]byte("item"), []byte{})
	}
	if err := s.ApplyRouted(3, "replace-staged", target, large); err != nil {
		t.Fatal(err)
	}
	if p.calls != 0 {
		t.Fatal("rejected/intermediate body reached disk")
	}
	requireValue(t, s, "record", "item", []byte{})
}

func TestPackFailureDoesNotPublishPartialTransaction(t *testing.T) {
	p := &memoryPacks{id: "store-a", failAt: 2}
	h := &packedHandler{action: func(tx *Transaction, payload []byte) (Outcome, error) {
		if err := tx.Set([]byte("a"), payload); err != nil {
			return Outcome{}, err
		}
		return Outcome{}, tx.Set([]byte("b"), payload)
	}}
	s, err := open(t.TempDir(), "data-1", h, p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	target := packedTarget(t, s, "record", "t")
	large := bytes.Repeat([]byte("x"), PackValueThreshold)
	if err := s.ApplyRouted(2, "write", target, large); err == nil {
		t.Fatal("injected failure ignored")
	}
	if s.AppliedIndex() != 1 || len(p.records) != 1 {
		t.Fatal("failure publication boundary")
	}
	for _, key := range []string{"a", "b"} {
		if _, found, err := s.Value(target.Keyspace, []byte(key)); found || err != nil {
			t.Fatal("partial transaction visible", err)
		}
	}
	if _, found, err := s.Result("write"); found || err != nil {
		t.Fatal("failed request published", err)
	}
	if err := s.ApplyRouted(2, "write", target, large); err != nil {
		t.Fatal(err)
	}
	if len(p.records) != 3 {
		t.Fatal("unreferenced data must be retained for future GC")
	}
	requireValue(t, s, "record", "a", large)
	requireValue(t, s, "record", "b", large)
}

func TestUncertainIndexCommitRequiresReopen(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		t.Run(fmt.Sprint(persisted), func(t *testing.T) {
			root := t.TempDir()
			p := &memoryPacks{id: "store-a"}
			h := &packedHandler{}
			s, err := open(root, "data-1", h, p)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			target := packedTarget(t, s, "record", "t")
			s.commit = func(batch *pebble.Batch) error {
				if persisted {
					if err := batch.Commit(pebble.Sync); err != nil {
						return err
					}
				}
				return errors.New("injected uncertainty")
			}
			large := bytes.Repeat([]byte("z"), PackValueThreshold)
			if err := s.ApplyRouted(2, "write", target, large); err == nil {
				t.Fatal("commit error ignored")
			}
			if _, _, err := s.Value(target.Keyspace, []byte("item")); err == nil {
				t.Fatal("read after ambiguous commit")
			}
			if _, _, err := s.Result("write"); err == nil {
				t.Fatal("result after ambiguous commit")
			}
			if _, _, _, err := s.Snapshot(); err == nil {
				t.Fatal("snapshot after ambiguous commit")
			}
			if err := s.Advance(2); err == nil {
				t.Fatal("apply after ambiguous commit")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = open(root, "data-1", h, p)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			wantIndex, wantCalls := uint64(1), 2
			if persisted {
				wantIndex, wantCalls = 2, 1
			}
			if s.AppliedIndex() != wantIndex {
				t.Fatal("wrong recovered applied index")
			}
			if err := s.ApplyRouted(wantIndex+1, "write", target, large); err != nil {
				t.Fatal(err)
			}
			if p.calls != wantCalls {
				t.Fatal("replay deduplication", p.calls, wantCalls)
			}
			requireValue(t, s, "record", "item", large)
		})
	}
}

func TestPackedReferencesFailClosedOnMismatch(t *testing.T) {
	for _, damage := range []string{"tenant", "capability", "group-key", "payload", "missing", "inline-conflict"} {
		t.Run(damage, func(t *testing.T) {
			p := &memoryPacks{id: "store-a"}
			s, err := open(t.TempDir(), "data-1", &packedHandler{}, p)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			target := packedTarget(t, s, "record", "t")
			if err := s.ApplyRouted(2, "write", target, bytes.Repeat([]byte("x"), PackValueThreshold)); err != nil {
				t.Fatal(err)
			}
			ref := readRef(t, s, "record", "item")
			r := p.records[ref.Location]
			switch damage {
			case "tenant":
				r.TenantID = "other"
			case "capability":
				r.CapabilityID = "search"
			case "group-key":
				r.Key = []byte("other-group")
			case "payload":
				r.Payload = []byte("wrong-version")
			case "inline-conflict":
				if err := s.db.Set(capabilityKey(target.Keyspace, []byte("item")), []byte("inline"), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			p.records[ref.Location] = r
			if damage == "missing" {
				delete(p.records, ref.Location)
			}
			if _, _, err := s.Value(target.Keyspace, []byte("item")); err == nil {
				t.Fatal("damaged reference returned data/miss")
			}
			if _, _, _, err := s.Snapshot(); err == nil {
				t.Fatal("snapshot hid missing/damaged Pack")
			}
		})
	}
}

func TestPackStoreBindingOnReopen(t *testing.T) {
	root := t.TempDir()
	p := &memoryPacks{id: "store-a"}
	s, err := open(root, "data-1", &packedHandler{}, p)
	if err != nil {
		t.Fatal(err)
	}
	target := packedTarget(t, s, "record", "t")
	if err := s.ApplyRouted(2, "write", target, bytes.Repeat([]byte("x"), PackValueThreshold)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, backend := range []packStorage{nil, &memoryPacks{id: "other-store"}} {
		if state, err := open(root, "data-1", &packedHandler{}, backend); err == nil {
			state.Close()
			t.Fatal("foreign/missing Store accepted")
		}
	}
	s, err = open(root, "data-1", &packedHandler{}, p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	requireValue(t, s, "record", "item", bytes.Repeat([]byte("x"), PackValueThreshold))
}

func realPackPool(t *testing.T) *pack.Pool {
	t.Helper()
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	volumes, err := volume.Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := volumes.Close(); err != nil {
			t.Error(err)
		}
	})
	lease, err := volumes.Reserve(8<<20, nil)
	if errors.Is(err, volume.ErrInsufficientSpace) {
		t.Skip("requires disk headroom above safety floor")
	}
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	p, err := pack.OpenPool(volumes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	return p
}

func TestPackedSnapshotRebuildsLocalLocationsAndKeepsMixedPacks(t *testing.T) {
	p, q := realPackPool(t), realPackPool(t)
	s, err := OpenWithPacks(t.TempDir(), "data-1", &packedHandler{}, p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	record := packedTarget(t, s, "record", "tenant-a")
	search := packedTarget(t, s, "search", "tenant-b")
	large := bytes.Repeat([]byte("a"), PackValueThreshold)
	other := bytes.Repeat([]byte("b"), PackValueThreshold+1)
	if err := s.ApplyRouted(3, "record-write", record, large); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyRouted(4, "search-write", search, other); err != nil {
		t.Fatal(err)
	}
	a, b := readRef(t, s, "record", "item"), readRef(t, s, "search", "item")
	if a.Location.Scope != b.Location.Scope {
		t.Fatal("capabilities did not share physical Pack")
	}
	image, index, conf, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{p.StoreID(), a.Location.VolumeID, a.Location.PackID, "local/pack/", "location"} {
		if bytes.Contains(image, []byte(forbidden)) {
			t.Fatal("snapshot leaked physical reference", forbidden)
		}
	}
	root := filepath.Join(t.TempDir(), "target")
	dest, err := OpenWithPacks(root, "data-1", &packedHandler{}, q)
	if err != nil {
		t.Fatal(err)
	}
	defer dest.Close()
	oldTarget := packedTarget(t, dest, "record", "tenant-a")
	if err := dest.ApplyRouted(2, "old", oldTarget, other); err != nil {
		t.Fatal(err)
	}
	if err := dest.Restore(index, image, conf); err != nil {
		t.Fatal(err)
	}
	local := readRef(t, dest, "record", "item")
	if local.Location.StoreID != q.StoreID() || local.Location.Scope == a.Location.Scope {
		t.Fatal("foreign Location retained")
	}
	requireValue(t, dest, "record", "item", large)
	requireValue(t, dest, "search", "item", other)
	if _, found, err := dest.Result("old"); found || err != nil {
		t.Fatal("restore retained obsolete result")
	}
	result, found, err := dest.ResultForMutation("record-write", record, large)
	if err != nil || !found || !result.Applied {
		t.Fatal("restored result", err)
	}
	roundTrip, _, _, err := dest.Snapshot()
	if err != nil || !bytes.Equal(image, roundTrip) {
		t.Fatal("snapshot depends on node-local placement", err)
	}
	if err := dest.Close(); err != nil {
		t.Fatal(err)
	}
	dest, err = OpenWithPacks(root, "data-1", &packedHandler{}, q)
	if err != nil {
		t.Fatal(err)
	}
	defer dest.Close()
	requireValue(t, dest, "search", "item", other)
}

func TestPackedRestorePrevalidatesAndPublishesAtomically(t *testing.T) {
	p := &memoryPacks{id: "source"}
	source, err := open(t.TempDir(), "data-1", &packedHandler{}, p)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	a := packedTarget(t, source, "record", "t")
	b := packedTarget(t, source, "search", "t")
	large := bytes.Repeat([]byte("x"), PackValueThreshold)
	if err := source.ApplyRouted(3, "a", a, large); err != nil {
		t.Fatal(err)
	}
	if err := source.ApplyRouted(4, "b", b, large); err != nil {
		t.Fatal(err)
	}
	encoded, index, conf, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	q := &memoryPacks{id: "target", failAt: 2}
	dest, err := open(t.TempDir(), "data-1", &packedHandler{}, q)
	if err != nil {
		t.Fatal(err)
	}
	defer dest.Close()
	old := packedTarget(t, dest, "record", "t")
	if err := dest.ApplyRouted(2, "old", old, []byte("old")); err != nil {
		t.Fatal(err)
	}
	var image snapshotImage
	if err := json.Unmarshal(encoded, &image); err != nil {
		t.Fatal(err)
	}
	image.Entries = append(image.Entries, snapshotEntry{Key: []byte("local/pack/injected"), Value: []byte("physical")})
	invalid, _ := json.Marshal(image)
	if err := dest.Restore(index, invalid, conf); err == nil || q.calls != 0 {
		t.Fatal("invalid snapshot had storage side effects")
	}
	if err := dest.Restore(index, encoded, conf); err == nil {
		t.Fatal("Pack failure ignored during Restore")
	}
	if dest.AppliedIndex() != 2 || len(q.records) != 1 {
		t.Fatal("partial snapshot published")
	}
	requireValue(t, dest, "record", "item", []byte("old"))
	if _, found, err := dest.Value(b.Keyspace, []byte("item")); found || err != nil {
		t.Fatal("partial snapshot visible")
	}
	if err := dest.Restore(index, encoded, conf); err != nil {
		t.Fatal(err)
	}
	requireValue(t, dest, "search", "item", large)
	// Legacy capability-local snapshots lack an unambiguous Keyspace. Refuse
	// rather than silently guessing ownership, even if the image otherwise parses.
	if err := json.Unmarshal(encoded, &image); err != nil {
		t.Fatal(err)
	}
	image.Version = 1
	for i := range image.Entries {
		image.Entries[i].TenantID = ""
	}
	legacy, _ := json.Marshal(image)
	inline, err := Open(t.TempDir(), "data-1", &packedHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer inline.Close()
	if err := inline.Restore(index, legacy, conf); err == nil {
		t.Fatal("legacy snapshot accepted")
	}
	if inline.AppliedIndex() != 0 {
		t.Fatal("legacy rejection mutated state")
	}
}
