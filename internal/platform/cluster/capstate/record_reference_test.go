package capstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

func referenceHandler() *packedHandler {
	return &packedHandler{action: func(tx *Transaction, payload []byte) (Outcome, error) {
		key := []byte("item")
		if string(payload) == "unlink" {
			return Outcome{}, tx.DeleteRecordReference(key)
		}
		d, err := records.Describe(tx.keyspace, "origin-group", key, payload)
		if err != nil {
			return Outcome{}, err
		}
		return Outcome{}, tx.SetRecordReference(key, d)
	}}
}

func readRecordReference(t *testing.T, s *State, target router.Target) (RecordReference, bool, error) {
	t.Helper()
	var ref RecordReference
	var found bool
	_, err := s.ReadRouted(context.Background(), &fakeReadBarrier{index: s.AppliedIndex()}, target, func(view ReadView) error {
		var err error
		ref, found, err = view.RecordReference([]byte("item"))
		return err
	})
	return ref, found, err
}

func TestRecordReferencePublicationReplayABAAndUnlink(t *testing.T) {
	root := t.TempDir()
	h := referenceHandler()
	s, err := Open(root, "data-1", h)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	target := packedTarget(t, s, "search", "tenant")
	apply := func(id, body string) {
		t.Helper()
		if err := s.ApplyRouted(s.AppliedIndex()+1, id, target, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	apply("first", "a")
	first, found, err := readRecordReference(t, s, target)
	if err != nil || !found || first.Version != 2 || first.Index != 2 || first.OwnerGroupID != "data-1" ||
		first.Descriptor.GroupID != "origin-group" || !bytes.Equal(first.RoutingKey, target.RoutingKey) {
		t.Fatal(first, found, err)
	}
	apply("first", "a")
	replay, _, err := readRecordReference(t, s, target)
	if err != nil || replay.Index != first.Index || h.calls != 1 {
		t.Fatal(replay, h.calls, err)
	}
	apply("second", "b")
	apply("third", "a")
	latest, _, err := readRecordReference(t, s, target)
	latestID, latestErr := latest.Descriptor.ID()
	firstID, firstErr := first.Descriptor.ID()
	if err != nil || latestErr != nil || firstErr != nil || latestID != firstID || latest.Index == first.Index || latest.RequestID == first.RequestID {
		t.Fatal(latest, err)
	}
	latest.Descriptor.Key[0] = 'X'
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root, "data-1", h)
	if err != nil {
		t.Fatal(err)
	}
	latest, _, err = readRecordReference(t, s, target)
	if err != nil || string(latest.Descriptor.Key) != "item" || latest.RequestID != "third" {
		t.Fatal(latest, err)
	}
	h.action = func(tx *Transaction, _ []byte) (Outcome, error) { return Outcome{}, tx.Delete([]byte("item")) }
	apply("kv-delete", "delete")
	if _, found, err := readRecordReference(t, s, target); err != nil || !found {
		t.Fatal("KV delete unlinked a record", found, err)
	}
	h.action = referenceHandler().action
	apply("unlink", "unlink")
	if _, found, err := readRecordReference(t, s, target); err != nil || found {
		t.Fatal(found, err)
	}
	if result, found, err := s.Result("first"); err != nil || !found || !result.Applied {
		t.Fatal("historical result lost", result, err)
	}
}

func TestRecordReferenceStagingScopeAndRollback(t *testing.T) {
	for _, mode := range []string{"accept", "reject", "error", "limit"} {
		t.Run(mode, func(t *testing.T) {
			h := &packedHandler{action: func(tx *Transaction, _ []byte) (Outcome, error) {
				key := []byte("item")
				d, err := records.Describe(tx.keyspace, "origin", key, []byte("body"))
				if err != nil {
					return Outcome{}, err
				}
				for _, bad := range []string{"tenant", "capability", "dataset", "key", "digest"} {
					v := d
					switch bad {
					case "tenant":
						v.Keyspace.TenantID = "other"
					case "capability":
						v.Keyspace.CapabilityID = "other"
					case "dataset":
						v.Keyspace.DatasetID = "other"
					case "key":
						v.Key = []byte("other")
					case "digest":
						v.SHA256 = "invalid"
					}
					if err := tx.SetRecordReference(key, v); err == nil {
						t.Fatal("accepted", bad)
					}
				}
				if err := tx.SetRecordReference(key, d); err != nil {
					return Outcome{}, err
				}
				d.Key[0] = 'X'
				got, found, err := tx.RecordReference(key)
				if err != nil || !found || string(got.Descriptor.Key) != "item" {
					t.Fatal(got, found, err)
				}
				got.Descriptor.Key[0] = 'Y'
				if err := tx.DeleteRecordReference(key); err != nil {
					return Outcome{}, err
				}
				if _, found, err := tx.RecordReference(key); err != nil || found {
					t.Fatal(found, err)
				}
				d.Key = key
				if err := tx.SetRecordReference(key, d); err != nil {
					return Outcome{}, err
				}
				if err := tx.Set(key, []byte("kv")); err != nil {
					return Outcome{}, err
				}
				switch mode {
				case "reject":
					return Outcome{Rejection: "rejected"}, nil
				case "error":
					return Outcome{}, errors.New("handler failure")
				case "limit":
					for i := 1; i < MaxRecordReferencesPerTransaction; i++ {
						k := []byte(fmt.Sprintf("k/%d", i))
						d.Key = k
						if err := tx.SetRecordReference(k, d); err != nil {
							t.Fatal(err)
						}
					}
					d.Key = key
					if err := tx.SetRecordReference(key, d); err != nil {
						t.Fatal("overwrite consumed a slot", err)
					}
					d.Key = []byte("overflow")
					err := tx.SetRecordReference(d.Key, d)
					if err == nil {
						t.Fatal("unbounded references")
					}
					return Outcome{}, err
				}
				return Outcome{}, nil
			}}
			s, err := Open(t.TempDir(), "data-1", h)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			target := packedTarget(t, s, "record", "t")
			err = s.ApplyRouted(2, "publish", target, []byte("command"))
			if (err != nil) != (mode == "error" || mode == "limit") {
				t.Fatal(err)
			}
			_, found, err := readRecordReference(t, s, target)
			if err != nil || found != (mode == "accept") {
				t.Fatal(found, err)
			}
			_, found, err = s.Value(target.Keyspace, []byte("item"))
			if err != nil || found != (mode == "accept") {
				t.Fatal("partial KV publication", found, err)
			}
		})
	}
}

func TestRecordReferenceUncertainSync(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		t.Run(fmt.Sprint(persisted), func(t *testing.T) {
			root := t.TempDir()
			h := referenceHandler()
			s, err := Open(root, "data-1", h)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { s.Close() }()
			target := packedTarget(t, s, "record", "t")
			s.commit = func(b *pebble.Batch) error {
				if persisted {
					if err := b.Commit(pebble.Sync); err != nil {
						return err
					}
				}
				return errors.New("uncertain")
			}
			if err := s.ApplyRouted(2, "publish", target, []byte("body")); err == nil {
				t.Fatal("ignored commit error")
			}
			if _, _, err := readRecordReference(t, s, target); err == nil {
				t.Fatal("served uncertain reference")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(root, "data-1", h)
			if err != nil {
				t.Fatal(err)
			}
			ref, found, err := readRecordReference(t, s, target)
			if err != nil || found != persisted {
				t.Fatal(ref, found, err)
			}
			_, resultFound, err := s.Result("publish")
			if err != nil || resultFound != found || (s.AppliedIndex() == 2) != found {
				t.Fatal("non-atomic publication", err)
			}
		})
	}
}

func TestRecordReferenceSnapshotFixedCutWithoutPhysicalData(t *testing.T) {
	artifacts, _ := fileArtifacts(t)
	h := referenceHandler()
	h.action = func(tx *Transaction, body []byte) (Outcome, error) {
		_, err := referenceHandler().action(tx, body)
		return Outcome{Value: bytes.Repeat([]byte("r"), 128<<10)}, err
	}
	s, err := Open(t.TempDir(), "data-1", h)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	target := packedTarget(t, s, "record", "t")
	if err := s.ApplyRouted(2, "publish", target, []byte("old-body")); err != nil {
		t.Fatal(err)
	}
	inline, index, conf, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := s.PrepareSnapshotTo(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if err := s.ApplyRouted(3, "unlink", target, []byte("unlink")); err != nil {
		t.Fatal(err)
	}
	encoded, err := prepared.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := snapshot.DecodeReference(encoded)
	if err != nil || ref.Index != index {
		t.Fatal(err)
	}
	for _, file := range []bool{false, true} {
		t.Run(fmt.Sprint(file), func(t *testing.T) {
			packs := &memoryPacks{id: "other-store"}
			dest, err := open(t.TempDir(), "data-1", referenceHandler(), packs)
			if err != nil {
				t.Fatal(err)
			}
			defer dest.Close()
			if file {
				err = dest.RestoreFrom(context.Background(), artifacts, index, encoded, conf)
			} else {
				err = dest.Restore(index, inline, conf)
			}
			if err != nil {
				t.Fatal(err)
			}
			got, found, err := readRecordReference(t, dest, target)
			if err != nil || !found || got.RequestID != "publish" || got.Index != 2 || packs.calls != 0 {
				t.Fatal(got, found, packs.calls, err)
			}
			if _, found, err := dest.Result("unlink"); err != nil || found {
				t.Fatal("post-cut result leaked", err)
			}
		})
	}
}

func TestRecordReferenceCorruptBacklinksFailClosed(t *testing.T) {
	for _, damage := range []string{"result-missing", "result-rejected", "result-hash", "range-missing", "keyspace-missing", "future-index", "foreign-owner", "foreign-key", "unknown-field"} {
		t.Run(damage, func(t *testing.T) {
			s, err := Open(t.TempDir(), "data-1", referenceHandler())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			target := packedTarget(t, s, "record", "t")
			if err := s.ApplyRouted(2, "publish", target, []byte("body")); err != nil {
				t.Fatal(err)
			}
			ref, _, err := readRecordReference(t, s, target)
			if err != nil {
				t.Fatal(err)
			}
			key := recordReferenceKey(target.Keyspace, []byte("item"))
			set := func(k []byte, v any) {
				t.Helper()
				data, err := json.Marshal(v)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.db.Set(k, data, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			del := func(k []byte) {
				t.Helper()
				if err := s.db.Delete(k, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			switch damage {
			case "result-missing":
				del(resultKey("publish"))
			case "result-rejected", "result-hash":
				r, _, _ := s.Result("publish")
				if damage == "result-rejected" {
					r.Applied = false
				} else {
					r.Hash = string(bytes.Repeat([]byte("0"), 64))
				}
				set(resultKey("publish"), r)
			case "range-missing":
				del(fenceRangeKey(target.RangeID))
			case "keyspace-missing":
				del(fenceKeyspaceKey(target.Keyspace))
			case "future-index":
				ref.Index = 3
				set(key, ref)
			case "foreign-owner":
				ref.OwnerGroupID = "other"
				set(key, ref)
			case "foreign-key":
				ref.Descriptor.Key = []byte("other")
				set(key, ref)
			case "unknown-field":
				data, _ := json.Marshal(ref)
				var m map[string]any
				if err := json.Unmarshal(data, &m); err != nil {
					t.Fatal(err)
				}
				m["store_id"] = "foreign"
				set(key, m)
			}
			if _, _, err := readRecordReference(t, s, target); err == nil {
				t.Fatal("served corrupt reference")
			}
			if _, _, _, err := s.Snapshot(); err == nil {
				t.Fatal("snapshotted corrupt reference")
			}
			artifacts, _ := fileArtifacts(t)
			prepared, err := s.PrepareSnapshotTo(artifacts)
			if err == nil {
				defer prepared.Close()
				if _, err := prepared.Build(context.Background()); err == nil {
					t.Fatal("file snapshot accepted corruption")
				}
			}
		})
	}
}

func TestRecordReferenceForgedSnapshotPreservesDestination(t *testing.T) {
	source, err := Open(t.TempDir(), "data-1", referenceHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	target := packedTarget(t, source, "record", "t")
	if err := source.ApplyRouted(2, "publish", target, []byte("body")); err != nil {
		t.Fatal(err)
	}
	// The incoming image must be newer than the populated destination. A
	// stale-image rejection would not exercise semantic validation at all.
	if err := source.Advance(3); err != nil {
		t.Fatal(err)
	}
	valid, index, conf, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	artifacts, _ := fileArtifacts(t)
	for _, damage := range []string{"missing-result", "rejected-result", "foreign-scope", "future-index", "range-epoch", "physical-locator", "old-format", "tenant-tag"} {
		t.Run(damage, func(t *testing.T) {
			var image snapshotImage
			if err := json.Unmarshal(valid, &image); err != nil {
				t.Fatal(err)
			}
			if damage == "old-format" {
				image.Version--
			}
			for i := 0; i < len(image.Entries); i++ {
				e := &image.Entries[i]
				if bytes.Equal(e.Key, resultKey("publish")) {
					if damage == "missing-result" {
						image.Entries = append(image.Entries[:i], image.Entries[i+1:]...)
						i--
						continue
					}
					if damage == "rejected-result" {
						var r Result
						json.Unmarshal(e.Value, &r)
						r.Applied = false
						r.Error = "rejected"
						e.Value, _ = json.Marshal(r)
					}
				}
				if bytes.HasPrefix(e.Key, recordReferencePrefix) {
					var r RecordReference
					if err := json.Unmarshal(e.Value, &r); err != nil {
						t.Fatal(err)
					}
					switch damage {
					case "foreign-scope":
						r.Descriptor.Keyspace.TenantID = "other"
					case "future-index":
						r.Index = index + 1
					case "range-epoch":
						r.RangeEpoch++
					case "tenant-tag":
						e.TenantID = "other"
					}
					e.Value, _ = json.Marshal(r)
					if damage == "physical-locator" {
						var m map[string]any
						json.Unmarshal(e.Value, &m)
						m["location"] = "foreign-offset"
						e.Value, _ = json.Marshal(m)
					}
				}
			}
			bad, err := json.Marshal(image)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := artifacts.Create(context.Background(), snapshot.Header{Version: snapshot.StreamVersion, Format: fmt.Sprintf("capstate-v%d", image.Version), GroupID: "data-1", Index: index, Conf: image.Conf}, snapshot.MaxStreamBytes, func(e *snapshot.Encoder) error {
				for _, row := range image.Entries {
					if err := e.Add(row.Key, row.Value); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			badFile, err := ref.Encode()
			if err != nil {
				t.Fatal(err)
			}
			formats := []string{"inline", "file"}
			// File streams derive attribution from the logical key; they have no
			// separate mutable Tenant tag to forge.
			if damage == "tenant-tag" {
				formats = []string{"inline"}
			}
			for _, format := range formats {
				packs := &memoryPacks{id: "destination"}
				dest, err := open(t.TempDir(), "data-1", &packedHandler{}, packs)
				if err != nil {
					t.Fatal(err)
				}
				defer dest.Close()
				old := packedTarget(t, dest, "search", "old")
				if err := dest.ApplyRouted(2, "existing", old, []byte("keep")); err != nil {
					t.Fatal(err)
				}
				if format == "inline" {
					err = dest.Restore(index, bad, conf)
				} else {
					err = dest.RestoreFrom(context.Background(), artifacts, index, badFile, conf)
				}
				if err == nil || dest.AppliedIndex() != 2 || packs.calls != 0 {
					t.Fatal("damaged snapshot mutated destination", format, err)
				}
				if bytes.Contains([]byte(err.Error()), []byte("stale")) {
					t.Fatal("fixture did not reach semantic validation", format, err)
				}
				value, found, err := dest.Value(old.Keyspace, []byte("item"))
				if err != nil || !found || string(value) != "keep" {
					t.Fatal("existing value lost", format, err)
				}
				if err := dest.Restore(index, valid, conf); err != nil {
					t.Fatal("valid image cannot replace the same destination", format, err)
				}
				if _, found, err := readRecordReference(t, dest, target); err != nil || !found {
					t.Fatal("valid control image lost its reference", format, found, err)
				}
			}
		})
	}
}

func TestRecordReferenceRealRaftReplayAndOwnerFence(t *testing.T) {
	root := t.TempDir()
	h := referenceHandler()
	openGroup := func() (*State, *raft.Group) {
		s, err := Open(filepath.Join(root, "state"), "data-1", h)
		if err != nil {
			t.Fatal(err)
		}
		adapter, err := routedstate.New(s)
		if err != nil {
			t.Fatal(err)
		}
		g, err := raft.OpenGroup(raft.GroupOptions{StoreRoot: root, GroupID: "data-1", RaftID: 71, BootstrapPeers: []uint64{71}, State: adapter, Transport: offlineMembershipTransport{}})
		if err != nil {
			s.Close()
			t.Fatal(err)
		}
		if err := g.Campaign(); err != nil {
			g.Close()
			t.Fatal(err)
		}
		return s, g
	}
	s, g := openGroup()
	defer func() { g.Close() }()
	r := rootstate.Range{RangeID: "r", TenantID: "t", CapabilityID: "record", DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	command, err := routedstate.EncodeActivateRange("activate", r)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Submit(command); err != nil {
		t.Fatal(err)
	}
	target := router.Target{Keyspace: r.Keyspace(), RangeID: r.RangeID, OwnerGroupID: r.OwnerGroupID, Epoch: 1, RoutingKey: []byte("item")}
	command, err = routedstate.EncodeMutation("publish", target, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Submit(command); err != nil {
		t.Fatal(err)
	}
	first, found, err := readRecordReference(t, s, target)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	s, g = openGroup()
	if err := g.Submit(command); err != nil {
		t.Fatal(err)
	}
	got, found, err := readRecordReference(t, s, target)
	if err != nil || !found || got.Index != first.Index || h.calls != 1 {
		t.Fatal(got, h.calls, err)
	}
	for _, wrongOwner := range []bool{false, true} {
		stale := target
		if wrongOwner {
			stale.OwnerGroupID = "other"
		} else {
			stale.Epoch++
		}
		command, err := routedstate.EncodeMutation(fmt.Sprintf("stale-%v", wrongOwner), stale, []byte("other"))
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Submit(command); err != nil {
			t.Fatal(err)
		}
		if h.calls != 1 {
			t.Fatal("stale owner reached handler")
		}
		got, found, err = readRecordReference(t, s, target)
		if err != nil || !found || got.Index != first.Index {
			t.Fatal(got, err)
		}
	}
}

func TestRecordReferencePhysicalHoldingIsIndependent(t *testing.T) {
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	v, err := volume.Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	p, err := pack.OpenPool(v)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	store, err := records.OpenNodeStore(p, v)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s, err := OpenWithPacks(filepath.Join(root, "state"), "data-1", referenceHandler(), p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	target := packedTarget(t, s, "search", "t")
	d, err := records.Describe(target.Keyspace, "origin-group", []byte("item"), []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), d, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyRouted(2, "old", target, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyRouted(3, "new", target, []byte("new")); err != nil {
		t.Fatal(err)
	}
	current, found, err := readRecordReference(t, s, target)
	if err != nil || !found || current.RequestID != "new" {
		t.Fatal(current, err)
	}
	if _, err := store.Verify(context.Background(), current.Descriptor); !errors.Is(err, records.ErrNotHeld) {
		t.Fatal("metadata fabricated a holding", err)
	}
	if _, err := store.Verify(context.Background(), d); err != nil {
		t.Fatal("overwrite removed old physical data", err)
	}
	if err := s.ApplyRouted(4, "unlink", target, []byte("unlink")); err != nil {
		t.Fatal(err)
	}
	if _, found, err := readRecordReference(t, s, target); err != nil || found {
		t.Fatal(found, err)
	}
	if _, err := store.Verify(context.Background(), d); err != nil {
		t.Fatal("unlink deleted physical data", err)
	}
}

func TestRecordReferenceCrashHelper(t *testing.T) {
	root := os.Getenv("DUBI_RECORD_REFERENCE_CRASH_ROOT")
	if root == "" {
		return
	}
	s, err := Open(root, "data-1", referenceHandler())
	if err != nil {
		t.Fatal(err)
	}
	target := packedTarget(t, s, "record", "t")
	s.commit = func(b *pebble.Batch) error {
		if os.Getenv("DUBI_RECORD_REFERENCE_CRASH_POINT") == "after-sync" {
			if err := b.Commit(pebble.Sync); err != nil {
				return err
			}
		}
		os.Exit(63)
		return nil
	}
	if err := s.ApplyRouted(2, "publish", target, []byte("body")); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash point not reached")
}

func TestRecordReferenceProcessCrashAtomicPublication(t *testing.T) {
	for _, point := range []string{"before-sync", "after-sync"} {
		t.Run(point, func(t *testing.T) {
			root := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecordReferenceCrashHelper$")
			cmd.Env = append(os.Environ(), "DUBI_RECORD_REFERENCE_CRASH_ROOT="+root, "DUBI_RECORD_REFERENCE_CRASH_POINT="+point)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 63 {
				t.Fatalf("crash helper: %v\n%s", err, out)
			}
			s, err := Open(root, "data-1", referenceHandler())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			target := router.Target{Keyspace: rootstate.Keyspace{TenantID: "t", CapabilityID: "record", DatasetID: "items"}, OwnerGroupID: "data-1", RangeID: "range-1", Epoch: 1, RoutingKey: []byte("item")}
			ref, found, err := readRecordReference(t, s, target)
			want := point == "after-sync"
			if err != nil || found != want {
				t.Fatal(ref, found, err)
			}
			_, resultFound, err := s.Result("publish")
			if err != nil || resultFound != want || (s.AppliedIndex() == 2) != want {
				t.Fatal("partial publication survived crash", err)
			}
			if err := s.ApplyRouted(s.AppliedIndex()+1, "publish", target, []byte("body")); err != nil {
				t.Fatal(err)
			}
			if _, found, err := readRecordReference(t, s, target); err != nil || !found {
				t.Fatal(found, err)
			}
		})
	}
}
