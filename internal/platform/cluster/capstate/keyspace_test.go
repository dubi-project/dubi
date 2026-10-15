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
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
)

func activateKeyspace(t *testing.T, s *State, keyspace rootstate.Keyspace) router.Target {
	t.Helper()
	index := s.AppliedIndex() + 1
	descriptor := rootstate.Range{RangeID: fmt.Sprintf("range-%d", index), TenantID: keyspace.TenantID,
		CapabilityID: keyspace.CapabilityID, DatasetID: keyspace.DatasetID, OwnerGroupID: s.groupID, Epoch: 1, State: rootstate.RangeStateActive}
	request := fmt.Sprintf("activate-%d", index)
	if err := s.ApplyRange(index, request, descriptor); err != nil {
		t.Fatal(err)
	}
	if result, found, err := s.ResultForRange(request, descriptor); err != nil || !found || !result.Applied {
		t.Fatal(result, err)
	}
	return router.Target{Keyspace: keyspace, RangeID: descriptor.RangeID, OwnerGroupID: s.groupID, Epoch: 1, RoutingKey: []byte("item")}
}

func TestKeyspaceIsolationWithMixedPackReopenAndRestore(t *testing.T) {
	pool := realPackPool(t)
	root := t.TempDir()
	handler := &packedHandler{action: func(tx *Transaction, payload []byte) (Outcome, error) {
		if _, found, err := tx.Get([]byte("item")); err != nil || found {
			return Outcome{}, errors.New("transaction read another Keyspace")
		}
		return Outcome{}, tx.Set([]byte("item"), payload)
	}}
	s, err := OpenWithPacks(root, "data-1", handler, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	scopes := []rootstate.Keyspace{
		{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items"},
		{TenantID: "tenant-b", CapabilityID: "record", DatasetID: "items"},
		{TenantID: "tenant-a", CapabilityID: "record", DatasetID: "other"},
		{TenantID: "tenant-a", CapabilityID: "search", DatasetID: "items"},
	}
	var targets []router.Target
	var values [][]byte
	var first packReference
	for i, scope := range scopes {
		target := activateKeyspace(t, s, scope)
		value := bytes.Repeat([]byte{byte('a' + i)}, PackValueThreshold+i)
		if err := s.ApplyRouted(s.AppliedIndex()+1, fmt.Sprintf("write-%d", i), target, value); err != nil {
			t.Fatal(err)
		}
		encoded, found, err := s.get(referenceKey(capabilityKey(scope, []byte("item"))))
		if err != nil || !found {
			t.Fatal("missing isolated reference", err)
		}
		var ref packReference
		if err := decodePackJSON(encoded, &ref); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = ref
		} else if ref.Location.Scope != first.Location.Scope {
			t.Fatal("logical isolation split physical Pack")
		}
		targets, values = append(targets, target), append(values, value)
	}
	check := func(state *State, deleted bool) {
		t.Helper()
		for i, target := range targets {
			value, found, err := state.Value(target.Keyspace, []byte("item"))
			if i == 0 && deleted {
				if found || err != nil {
					t.Fatal("delete escaped its Keyspace", err)
				}
				continue
			}
			if err != nil || !found || !bytes.Equal(value, values[i]) {
				t.Fatal("Keyspace value collision", target.Keyspace, err)
			}
			result, _, err := state.QueryRouted(context.Background(), &fakeReadBarrier{index: state.AppliedIndex()}, target, []byte("item"))
			if err != nil || result.Rejection != "" || !bytes.Equal(result.Value, values[i]) {
				t.Fatal("routed view escaped Keyspace", err)
			}
		}
	}
	check(s, false)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenWithPacks(root, "data-1", handler, pool)
	if err != nil {
		t.Fatal(err)
	}
	check(s, false)
	image, index, conf, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// The same scoped image works on a foreign Store and on an inline-only
	// test state; physical representation cannot change logical attribution.
	otherPool := realPackPool(t)
	packed, err := OpenWithPacks(t.TempDir(), "data-1", &packedHandler{}, otherPool)
	if err != nil {
		t.Fatal(err)
	}
	defer packed.Close()
	inline, err := Open(t.TempDir(), "data-1", &packedHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer inline.Close()
	for _, destination := range []*State{packed, inline} {
		if err := destination.Restore(index, image, conf); err != nil {
			t.Fatal(err)
		}
		check(destination, false)
		copy, _, _, err := destination.Snapshot()
		if err != nil || !bytes.Equal(copy, image) {
			t.Fatal("snapshot depends on physical layout", err)
		}
	}
	handler.action = func(tx *Transaction, _ []byte) (Outcome, error) {
		value, found, err := tx.Get([]byte("item"))
		if err != nil || !found || !bytes.Equal(value, values[0]) {
			return Outcome{}, errors.New("delete read another Keyspace")
		}
		return Outcome{}, tx.Delete([]byte("item"))
	}
	if err := s.ApplyRouted(s.AppliedIndex()+1, "delete", targets[0], []byte("delete")); err != nil {
		t.Fatal(err)
	}
	check(s, true)
}

func TestKeyspaceEncodingCannotEscapeIntoAnotherScope(t *testing.T) {
	a := rootstate.Keyspace{TenantID: "a", CapabilityID: "record", DatasetID: "items"}
	b := rootstate.Keyspace{TenantID: "b", CapabilityID: "record", DatasetID: "items"}
	for _, key := range [][]byte{[]byte("../b/record/items/item"), []byte("data/keyspace/b/record/items/item"), {0, '/', 255}} {
		encoded := capabilityKey(a, key)
		decoded, err := splitValueKey(encoded)
		if err != nil || decoded != a || bytes.Equal(encoded, capabilityKey(b, key)) {
			t.Fatal("key escaped scope", decoded, err)
		}
	}
	for _, key := range []string{"data/capability/record/item", "data/keyspace/a/record/items/", "data/keyspace/a/record//item", "data/keyspace/a/record"} {
		if _, err := splitValueKey([]byte(key)); err == nil {
			t.Fatal("invalid scope accepted", key)
		}
	}
}

func FuzzKeyspaceValueKey(f *testing.F) {
	f.Add("tenant-a", "record", "items", []byte("../other/item"))
	f.Add("tenant-b", "search", "index", []byte{0, '/', 255})
	f.Fuzz(func(t *testing.T, tenant, capability, dataset string, key []byte) {
		scope := rootstate.Keyspace{TenantID: tenant, CapabilityID: capability, DatasetID: dataset}
		if scope.Validate() != nil || validateCapabilityKey(key) != nil {
			return
		}
		encoded := capabilityKey(scope, key)
		decoded, err := splitValueKey(encoded)
		if err != nil || decoded != scope {
			t.Fatal("Keyspace round trip", decoded, err)
		}
		if !bytes.HasSuffix(encoded, key) {
			t.Fatal("opaque key changed")
		}
	})
}

func TestLegacyAndUnknownStateFormatsFailWithoutMigration(t *testing.T) {
	for _, version := range []string{"missing", "2", "3", "4", "5", "6", "99"} {
		t.Run(version, func(t *testing.T) {
			root := t.TempDir()
			s, err := Open(root, "data-1", &packedHandler{})
			if err != nil {
				t.Fatal(err)
			}
			legacyKey := []byte("data/capability/record/item")
			if err := s.db.Set(legacyKey, []byte("unattributed"), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			if version == "missing" {
				err = s.db.Delete(stateFormatKey, pebble.Sync)
			} else {
				err = s.db.Set(stateFormatKey, []byte(version), pebble.Sync)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if opened, err := Open(root, "data-1", &packedHandler{}); !errors.Is(err, ErrStateFormat) {
				if opened != nil {
					opened.Close()
				}
				t.Fatal("unsupported format opened", err)
			}
			db, err := pebbleutil.Open(filepath.Join(root, "dubi-capability-state"), &pebble.Options{Logger: pebbleutil.QuietLogger{}})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			value, closer, err := db.Get(legacyKey)
			if err != nil {
				t.Fatal(err)
			}
			if string(value) != "unattributed" {
				t.Fatal("old value modified")
			}
			closer.Close()
			value, closer, err = db.Get(stateFormatKey)
			if version == "missing" {
				if !errors.Is(err, pebble.ErrNotFound) {
					if closer != nil {
						closer.Close()
					}
					t.Fatal("legacy format silently marked current", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer closer.Close()
				if string(value) != version {
					t.Fatal("version rewritten")
				}
			}
		})
	}
}

func TestSnapshotRejectsCrossKeyspaceAttributionBeforePackWrites(t *testing.T) {
	source, err := open(t.TempDir(), "data-1", &packedHandler{}, &memoryPacks{id: "source"})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	target := activateKeyspace(t, source, rootstate.Keyspace{TenantID: "a", CapabilityID: "record", DatasetID: "items"})
	if err := source.ApplyRouted(2, "write", target, bytes.Repeat([]byte("a"), PackValueThreshold)); err != nil {
		t.Fatal(err)
	}
	encoded, index, conf, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, damage := range []string{"tenant", "keyspace", "range", "missing-fence", "old-namespace", "unknown-metadata", "result", "version"} {
		t.Run(damage, func(t *testing.T) {
			var image snapshotImage
			if err := json.Unmarshal(encoded, &image); err != nil {
				t.Fatal(err)
			}
			for i := range image.Entries {
				entry := &image.Entries[i]
				if bytes.HasPrefix(entry.Key, valuePrefix) {
					switch damage {
					case "tenant":
						entry.TenantID = "b"
					case "keyspace":
						entry.Key = capabilityKey(rootstate.Keyspace{TenantID: "b", CapabilityID: "record", DatasetID: "items"}, []byte("item"))
						entry.TenantID = "b"
					case "old-namespace":
						entry.Key = []byte("data/capability/record/item")
						entry.TenantID = ""
					}
				}
				if damage == "range" && bytes.HasPrefix(entry.Key, []byte("data/fence/keyspace/")) {
					entry.Value = []byte("other-range")
				}
				if damage == "missing-fence" && bytes.HasPrefix(entry.Key, []byte("data/fence/range/")) {
					image.Entries = append(image.Entries[:i], image.Entries[i+1:]...)
					break
				}
				if damage == "result" && bytes.HasPrefix(entry.Key, []byte("result/")) {
					entry.Value = []byte("{}")
				}
			}
			if damage == "unknown-metadata" {
				image.Entries = append(image.Entries, snapshotEntry{Key: []byte("data/fence/unknown"), Value: []byte("x")})
			}
			if damage == "version" {
				image.Version = 2
			}
			invalid, err := json.Marshal(image)
			if err != nil {
				t.Fatal(err)
			}
			packs := &memoryPacks{id: "destination"}
			destination, err := open(t.TempDir(), "data-1", &packedHandler{}, packs)
			if err != nil {
				t.Fatal(err)
			}
			defer destination.Close()
			if err := destination.Restore(index, invalid, conf); err == nil {
				t.Fatal("invalid image accepted")
			}
			if packs.calls != 0 || destination.AppliedIndex() != 0 {
				t.Fatal("validation had storage side effects")
			}
		})
	}
}
