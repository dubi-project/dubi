package capstate

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
)

func regionalState(t *testing.T, root string) *State {
	t.Helper()
	s, err := Open(root, "data-1", &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func regionalDescriptor() regionfence.Descriptor {
	return regionfence.Descriptor{RegionID: "west", GroupID: "data-1", Generation: 1, PlannedIndex: 10000, GroupEpoch: 2, MembershipIndex: 1,
		Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}
}
func regionalApply(t *testing.T, s *State, request string, d regionfence.Descriptor, accepted bool) {
	t.Helper()
	encoded, err := routedstate.EncodeRegionFence(request, d)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := routedstate.New(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Apply(s.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	r, found, err := s.ResultForRegionFence(request, d)
	if err != nil || !found || r.Applied != accepted || (r.Error == "") != accepted {
		t.Fatal(r, found, err)
	}
}

func TestRegionalFenceDurableApplyRetryAndMembershipABA(t *testing.T) {
	root := t.TempDir()
	s := regionalState(t, root)
	conf := &raftpb.ConfState{Voters: []uint64{3, 1, 2}}
	if err := s.ApplyMembership(1, conf); err != nil {
		t.Fatal(err)
	}
	d := regionalDescriptor()
	regionalApply(t, s, "first", d, true)
	first, err := s.RegionFence()
	if err != nil || !first.Found || !first.Active || first.Record.Index != 2 || first.MembershipIndex != 1 {
		t.Fatal(first, err)
	}
	regionalApply(t, s, "first", d, true)
	view, _ := s.RegionFence()
	if !reflect.DeepEqual(first.Record, view.Record) || view.AppliedIndex != 3 {
		t.Fatal("retry changed barrier", view)
	}
	changed := d
	changed.GroupEpoch++
	if _, _, err := s.ResultForRegionFence("first", changed); !errors.Is(err, ErrRequestIDReused) {
		t.Fatal(err)
	}
	regionalApply(t, s, "duplicate-new-request", d, false)
	if err := s.ApplyMembership(5, &raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyMembership(6, conf); err != nil {
		t.Fatal(err)
	}
	view, err = s.RegionFence()
	if err != nil || view.Active || !view.Found || view.MembershipIndex != 6 {
		t.Fatal("old barrier revived after ABA", view, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = regionalState(t, root)
	view, err = s.RegionFence()
	if err != nil || view.Active || view.MembershipIndex != 6 {
		t.Fatal(view, err)
	}
	regionalApply(t, s, "stale-membership", d, false)
	changed.MembershipIndex = 6
	regionalApply(t, s, "refence", changed, true)
	view, err = s.RegionFence()
	if err != nil || !view.Active || view.Record.Fence.GroupEpoch != 3 {
		t.Fatal(view, err)
	}
	intent := regionfence.HandoffIntent{Previous: view.Record, Next: rootstate.RegionCohort{RegionID: "west", GroupID: "data-1", Generation: 2, PlannedIndex: changed.PlannedIndex + 1, Nodes: []uint64{1, 2, 3, 4}, State: rootstate.RegionCohortPlanned}}
	handoffApply(t, s, "seal-expansion", regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &intent}, true)
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}); err != nil {
		t.Fatal(err)
	}
	next := changed
	next.Generation++
	next.PlannedIndex++
	next.GroupEpoch++
	next.MembershipIndex = s.AppliedIndex()
	next.Members = append(next.Members[:len(next.Members):len(next.Members)], rootstate.GroupMember{RaftID: 4, Role: rootstate.GroupMemberLearner})
	regionalApply(t, s, "expanded", next, true)
	view, err = s.RegionFence()
	if err != nil || !view.Active || len(view.Record.Fence.Members) != 4 {
		t.Fatal(view, err)
	}
}

func TestRegionalFenceFreshSameNameRegionHasNoOldHandoff(t *testing.T) {
	root := t.TempDir()
	s := regionalState(t, root)
	if err := s.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	d := regionalDescriptor()
	d.Generation = 2
	regionalApply(t, s, "unmarked-generation", d, false)
	d.Reformation = true
	regionalApply(t, s, "fresh-generation", d, true)
	check := func(state *State) {
		t.Helper()
		view, err := state.RegionFence()
		if err != nil || !view.Active || !view.Found || view.Handoff != nil || !reflect.DeepEqual(view.Record.Fence, d) {
			t.Fatal("fresh Group did not retain its reformation fence", view, err)
		}
	}
	check(s)
	inline, index, conf, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	dest := regionalState(t, t.TempDir())
	if err := dest.Restore(index, inline, conf); err != nil {
		t.Fatal(err)
	}
	check(dest)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	check(regionalState(t, root))
}

func TestRegionalFenceUncertainCommitDoesNotExposeHalfPublication(t *testing.T) {
	for _, change := range []bool{false, true} {
		for _, persisted := range []bool{false, true} {
			t.Run(fmt.Sprint(change, persisted), func(t *testing.T) {
				root := t.TempDir()
				s := regionalState(t, root)
				conf := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
				if err := s.ApplyMembership(1, conf); err != nil {
					t.Fatal(err)
				}
				if change {
					regionalApply(t, s, "first", regionalDescriptor(), true)
				}
				injected := errors.New("uncertain regional publication")
				s.commit = func(b *pebble.Batch) error {
					if persisted {
						if err := b.Commit(pebble.Sync); err != nil {
							t.Fatal(err)
						}
					}
					return injected
				}
				var err error
				if change {
					err = s.ApplyMembership(3, conf)
				} else {
					err = s.ApplyRegionFence(2, "first", regionalDescriptor())
				}
				if !errors.Is(err, injected) {
					t.Fatal(err)
				}
				if _, err := s.RegionFence(); !errors.Is(err, injected) {
					t.Fatal("failed state served proof", err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s = regionalState(t, root)
				view, err := s.RegionFence()
				if err != nil {
					t.Fatal(err)
				}
				if change {
					if !view.Found || view.Active == persisted || (view.MembershipIndex == 3) != persisted {
						t.Fatal(view)
					}
				} else if view.Found != persisted || view.Active != persisted || (view.AppliedIndex == 2) != persisted {
					t.Fatal(view)
				}
			})
		}
	}
}

func TestRegionalFenceRefreshAfterDescriptorOnlyEpochAdvance(t *testing.T) {
	s := regionalState(t, t.TempDir())
	if err := s.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	d := regionalDescriptor()
	regionalApply(t, s, "original", d, true)
	// A completed no-op Root member task advances descriptor epoch without
	// producing another Group ConfChange. It must not strand confirmation.
	d.GroupEpoch++
	regionalApply(t, s, "refreshed", d, true)
	view, err := s.RegionFence()
	if err != nil || !view.Active || view.MembershipIndex != 1 || view.Record.Fence.GroupEpoch != 3 || view.Record.Index != 3 {
		t.Fatal(view, err)
	}
	old := d
	old.GroupEpoch--
	regionalApply(t, s, "late-original", old, false)
}

func TestRegionalFenceSnapshotRoundtripAndSemanticDamage(t *testing.T) {
	for _, active := range []bool{true, false} {
		s := regionalState(t, t.TempDir())
		conf := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
		if err := s.ApplyMembership(1, conf); err != nil {
			t.Fatal(err)
		}
		regionalApply(t, s, "fence", regionalDescriptor(), true)
		if !active {
			if err := s.ApplyMembership(3, conf); err != nil {
				t.Fatal(err)
			}
		}
		inline, index, conf, err := s.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		var original snapshotImage
		if err := json.Unmarshal(inline, &original); err != nil {
			t.Fatal(err)
		}
		artifacts, _ := fileArtifacts(t)
		prepared, err := s.PrepareSnapshotTo(artifacts)
		if err != nil {
			t.Fatal(err)
		}
		// Change live membership after capture; neither file nor inline image
		// may accidentally use that later cut to validate the captured fence.
		if err := s.ApplyMembership(index+1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}); err != nil {
			t.Fatal(err)
		}
		file, err := prepared.Build(context.Background())
		prepared.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, format := range []string{"inline", "file"} {
			dest := regionalState(t, t.TempDir())
			if format == "inline" {
				err = dest.Restore(index, inline, conf)
			} else {
				err = dest.RestoreFrom(context.Background(), artifacts, index, file, conf)
			}
			if err != nil {
				t.Fatal(active, format, err)
			}
			view, err := dest.RegionFence()
			if err != nil || !view.Found || view.Active != active {
				t.Fatal(active, format, view, err)
			}
		}
		for _, damage := range []string{"missing-position", "future-position", "missing-result", "bad-digest", "wrong-group", "wrong-members", "future-index", "old-format"} {
			var image snapshotImage
			json.Unmarshal(inline, &image)
			if damage == "old-format" {
				image.Version = 3
			}
			for i := 0; i < len(image.Entries); i++ {
				e := &image.Entries[i]
				if bytes.Equal(e.Key, membershipIndexKey) {
					if damage == "missing-position" {
						image.Entries = append(image.Entries[:i], image.Entries[i+1:]...)
						i--
						continue
					}
					if damage == "future-position" {
						binary.BigEndian.PutUint64(e.Value, index+1)
					}
				}
				if bytes.Equal(e.Key, resultKey("fence")) {
					if damage == "missing-result" {
						image.Entries = append(image.Entries[:i], image.Entries[i+1:]...)
						i--
						continue
					}
					if damage == "bad-digest" {
						var r Result
						json.Unmarshal(e.Value, &r)
						r.Hash = fmt.Sprintf("%064x", 1)
						e.Value, _ = json.Marshal(r)
					}
				}
				if bytes.Equal(e.Key, regionFenceKey) {
					var r regionfence.Record
					json.Unmarshal(e.Value, &r)
					switch damage {
					case "wrong-group":
						r.Fence.GroupID = "foreign"
					case "wrong-members":
						r.Fence.Members[2].RaftID = 4
					case "future-index":
						r.Index = index + 1
					}
					e.Value, _ = json.Marshal(r)
				}
			}
			bad, _ := json.Marshal(image)
			ref, err := artifacts.Create(context.Background(), snapshot.Header{Version: snapshot.StreamVersion, Format: fmt.Sprintf("capstate-v%d", image.Version), GroupID: "data-1", Index: index, Conf: original.Conf}, snapshot.MaxStreamBytes, func(e *snapshot.Encoder) error {
				for _, entry := range image.Entries {
					if err := e.Add(entry.Key, entry.Value); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			badFile, _ := ref.Encode()
			for _, format := range []string{"inline", "file"} {
				dest := regionalState(t, t.TempDir())
				if format == "inline" {
					err = dest.Restore(index, bad, conf)
				} else {
					err = dest.RestoreFrom(context.Background(), artifacts, index, badFile, conf)
				}
				if err == nil || dest.AppliedIndex() != 0 {
					t.Fatal("damaged snapshot published", active, damage, format, err)
				}
			}
		}
	}
}
