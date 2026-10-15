package capstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
)

func handoffApply(t *testing.T, s *State, id string, command regionfence.HandoffCommand, accepted bool) {
	t.Helper()
	encoded, err := routedstate.EncodeRegionHandoff(id, command)
	if err != nil {
		t.Fatal(err)
	}
	adapter, _ := routedstate.New(s)
	if err := adapter.Apply(s.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	r, found, err := s.ResultForRegionHandoff(id, command)
	if err != nil || !found || r.Applied != accepted || (r.Error == "") != accepted {
		t.Fatal(r, found, err)
	}
}

func handoffFixture(t *testing.T, root string) (*State, regionfence.HandoffIntent, router.Target) {
	t.Helper()
	s := regionalState(t, root)
	if err := s.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	r := rootstate.Range{RangeID: "range-a", TenantID: "tenant-a", CapabilityID: "record", DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	if err := s.ApplyRange(2, "range", r); err != nil {
		t.Fatal(err)
	}
	regionalApply(t, s, "initial", regionalDescriptor(), true)
	v, err := s.RegionFence()
	if err != nil {
		t.Fatal(err)
	}
	i := regionfence.HandoffIntent{Previous: v.Record, Next: rootstate.RegionCohort{RegionID: "west", GroupID: "data-1", Generation: 2, PlannedIndex: 11000, Nodes: []uint64{1, 2, 3, 4}, State: rootstate.RegionCohortPlanned}}
	return s, i, router.Target{Keyspace: r.Keyspace(), RangeID: r.RangeID, OwnerGroupID: r.OwnerGroupID, Epoch: 1, RoutingKey: []byte("key")}
}

func handoffCandidate(t *testing.T, s *State, i regionfence.HandoffIntent) regionfence.Activation {
	t.Helper()
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 2, 3, 4}}); err != nil {
		t.Fatal(err)
	}
	d := i.Previous.Fence
	d.Generation, d.PlannedIndex, d.GroupEpoch, d.MembershipIndex = i.Next.Generation, i.Next.PlannedIndex, d.GroupEpoch+1, s.AppliedIndex()
	d.Members = append(d.Members[:len(d.Members):len(d.Members)], rootstate.GroupMember{RaftID: 4, Role: rootstate.GroupMemberVoter})
	regionalApply(t, s, "candidate", d, true)
	v, err := s.RegionFence()
	if err != nil {
		t.Fatal(err)
	}
	c := i.Next
	c.State, c.ConfirmedIndex, c.GroupEpoch, c.GroupFenceRequestID, c.GroupFenceIndex = rootstate.RegionCohortConfirmed, c.PlannedIndex+1, d.GroupEpoch, v.Record.RequestID, v.Record.Index
	return regionfence.Activation{Candidate: v.Record, Confirmed: c}
}

func handoffWrite(t *testing.T, s *State, target router.Target, id string, accepted bool) {
	t.Helper()
	h := s.handler.(*testHandler)
	before := h.calls
	payload := encodeTestMutation(t, testMutation{Key: "item", Value: id})
	if err := s.ApplyRouted(s.AppliedIndex()+1, id, target, payload); err != nil {
		t.Fatal(err)
	}
	r, found, err := s.Result(id)
	if err != nil || !found || r.Applied != accepted || (h.calls == before+1) != accepted {
		t.Fatal(r, found, err, h.calls, before)
	}
}

func TestRegionalHandoffSealsWritesUntilExactActivationAndSurvivesRestart(t *testing.T) {
	root := t.TempDir()
	s, intent, target := handoffFixture(t, root)
	handoffWrite(t, s, target, "before", true)
	seal := regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &intent}
	handoffApply(t, s, "seal", seal, true)
	h, found, err := s.RegionHandoff()
	if err != nil || !found || h.Activation != nil {
		t.Fatal(h, found, err)
	}
	handoffApply(t, s, "seal", seal, true)
	handoffApply(t, s, "duplicate-seal", seal, false)
	again, _, _ := s.RegionHandoff()
	if !reflect.DeepEqual(h, again) {
		t.Fatal("replay changed seal")
	}
	v, err := s.RegionFence()
	if err != nil || v.Handoff == nil || !reflect.DeepEqual(h, *v.Handoff) {
		t.Fatal("combined observation lost handoff", v, err)
	}
	v.Handoff.Intent.Next.Nodes[0] = 99
	again, _, _ = s.RegionHandoff()
	if !reflect.DeepEqual(h, again) {
		t.Fatal("observation aliased durable state")
	}
	handoffWrite(t, s, target, "sealed", false)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = regionalState(t, root)
	handoffWrite(t, s, target, "sealed-restart", false)
	activation := handoffCandidate(t, s, intent)
	handoffWrite(t, s, target, "candidate-only", false)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = regionalState(t, root)
	handoffWrite(t, s, target, "candidate-restart", false)
	activate := regionfence.HandoffCommand{Action: regionfence.Activate, Activation: &activation}
	handoffApply(t, s, "activate", activate, true)
	handoffApply(t, s, "activate", activate, true)
	handoffApply(t, s, "duplicate-activation", activate, false)
	handoffWrite(t, s, target, "after", true)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = regionalState(t, root)
	handoffWrite(t, s, target, "after-restart", true)
	changed := activation
	changed.Confirmed.ConfirmedIndex++
	if _, _, err := s.ResultForRegionHandoff("activate", regionfence.HandoffCommand{Action: regionfence.Activate, Activation: &changed}); !errors.Is(err, ErrRequestIDReused) {
		t.Fatal(err)
	}
	// A new generation can seal the activated generation without recovering
	// any coordinator memory. It cannot be undone by replaying old activation.
	next := regionfence.HandoffIntent{Previous: activation.Candidate, Next: rootstate.RegionCohort{RegionID: "west", GroupID: "data-1", Generation: 3, PlannedIndex: 12000, Nodes: []uint64{1, 2, 3, 4, 5}, State: rootstate.RegionCohortPlanned}}
	handoffApply(t, s, "next-seal", regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &next}, true)
	handoffApply(t, s, "activate", activate, true) // Original success, NOT reactivation.
	handoffWrite(t, s, target, "old-activation-replay", false)
}

func TestRegionalHandoffRejectsUnsealedOrWrongCandidateAndMembershipABA(t *testing.T) {
	for _, mode := range []string{"unsealed", "wrong-plan", "wrong-candidate", "aba", "range"} {
		t.Run(mode, func(t *testing.T) {
			s, intent, target := handoffFixture(t, t.TempDir())
			if mode == "unsealed" {
				if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 2, 3, 4}}); err != nil {
					t.Fatal(err)
				}
				d := intent.Previous.Fence
				d.Generation++
				d.PlannedIndex = intent.Next.PlannedIndex
				d.GroupEpoch++
				d.MembershipIndex = s.AppliedIndex()
				d.Members = append(d.Members, rootstate.GroupMember{RaftID: 4, Role: rootstate.GroupMemberVoter})
				regionalApply(t, s, "unsafe-growth", d, false)
				handoffWrite(t, s, target, "changed-membership", false)
				return
			}
			handoffApply(t, s, "seal", regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &intent}, true)
			if mode == "range" {
				r := rootstate.Range{RangeID: "new-range", TenantID: "another", CapabilityID: "record", DatasetID: "items", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
				if err := s.ApplyRange(s.AppliedIndex()+1, "new-range", r); err != nil {
					t.Fatal(err)
				}
				result, _, _ := s.Result("new-range")
				if !result.Applied || result.Error != "" {
					t.Fatal(result)
				}
				// Empty Range creation is control metadata with its own fixed-ID
				// protocol. It must not be permanently poisoned by a temporary
				// handoff; writes into that Range are still fenced.
				target := router.Target{Keyspace: r.Keyspace(), RangeID: r.RangeID, OwnerGroupID: r.OwnerGroupID, Epoch: r.Epoch, RoutingKey: []byte("key")}
				handoffWrite(t, s, target, "new-range-write", false)
				return
			}
			a := handoffCandidate(t, s, intent)
			if mode == "wrong-plan" {
				a.Confirmed.PlannedIndex++
			}
			if mode == "wrong-candidate" {
				a.Candidate.RequestID = "other"
				a.Confirmed.GroupFenceRequestID = "other"
			}
			if mode == "aba" {
				if err := s.ApplyMembership(s.AppliedIndex()+1, s.ConfState()); err != nil {
					t.Fatal(err)
				}
			}
			command := regionfence.HandoffCommand{Action: regionfence.Activate, Activation: &a}
			// Direct Apply also rejects invalid commands, independently of wire validation.
			if err := s.ApplyRegionHandoff(s.AppliedIndex()+1, "bad-activate", command); err != nil {
				t.Fatal(err)
			}
			r, _, _ := s.Result("bad-activate")
			if r.Applied || r.Error == "" {
				t.Fatal(r)
			}
			handoffWrite(t, s, target, "still-fenced", false)
		})
	}
}

func TestRegionalHandoffUncertainCommitRemainsFailClosed(t *testing.T) {
	for _, action := range []string{regionfence.Seal, regionfence.Activate} {
		for _, persisted := range []bool{false, true} {
			t.Run(fmt.Sprint(action, persisted), func(t *testing.T) {
				root := t.TempDir()
				s, intent, target := handoffFixture(t, root)
				command := regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &intent}
				if action == regionfence.Activate {
					handoffApply(t, s, "seal", command, true)
					a := handoffCandidate(t, s, intent)
					command = regionfence.HandoffCommand{Action: regionfence.Activate, Activation: &a}
				}
				injected := errors.New("uncertain handoff sync")
				s.commit = func(b *pebble.Batch) error {
					if persisted {
						if err := b.Commit(pebble.Sync); err != nil {
							t.Fatal(err)
						}
					}
					return injected
				}
				if err := s.ApplyRegionHandoff(s.AppliedIndex()+1, "uncertain", command); !errors.Is(err, injected) {
					t.Fatal(err)
				}
				if _, _, err := s.RegionHandoff(); !errors.Is(err, injected) {
					t.Fatal("failed state exposed handoff", err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s = regionalState(t, root)
				writable := action == regionfence.Seal && !persisted || action == regionfence.Activate && persisted
				handoffWrite(t, s, target, "recovered", writable)
			})
		}
	}
}

func TestRegionalHandoffBothSnapshotsPreserveSealedAndActivatedCuts(t *testing.T) {
	for _, phase := range []string{"sealed", "candidate", "activated", "invalidated"} {
		t.Run(phase, func(t *testing.T) {
			s, intent, target := handoffFixture(t, t.TempDir())
			handoffApply(t, s, "seal", regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &intent}, true)
			if phase != "sealed" {
				a := handoffCandidate(t, s, intent)
				if phase != "candidate" {
					handoffApply(t, s, "activate", regionfence.HandoffCommand{Action: regionfence.Activate, Activation: &a}, true)
				}
			}
			if phase == "invalidated" {
				if err := s.ApplyMembership(s.AppliedIndex()+1, s.ConfState()); err != nil {
					t.Fatal(err)
				}
			}
			h, _, _ := s.RegionHandoff()
			inline, index, conf, err := s.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			artifacts, _ := fileArtifacts(t)
			prepared, err := s.PrepareSnapshotTo(artifacts)
			if err != nil {
				t.Fatal(err)
			}
			// A later apply must not contaminate the captured cut.
			if err := s.Advance(s.AppliedIndex() + 1); err != nil {
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
					t.Fatal(format, err)
				}
				got, found, err := dest.RegionHandoff()
				if err != nil || !found || !reflect.DeepEqual(h, got) {
					t.Fatal(format, got, found, err)
				}
				handoffWrite(t, dest, target, "after-restore", phase == "activated")
			}
			for _, damage := range []string{"missing-seal-result", "future-seal", "wrong-plan", "wrong-digest", "old-format"} {
				var image snapshotImage
				if err := json.Unmarshal(inline, &image); err != nil {
					t.Fatal(err)
				}
				if damage == "old-format" {
					image.Version = 4
				}
				for j := 0; j < len(image.Entries); j++ {
					e := &image.Entries[j]
					if bytes.Equal(e.Key, resultKey("seal")) {
						if damage == "missing-seal-result" {
							image.Entries = append(image.Entries[:j], image.Entries[j+1:]...)
							j--
							continue
						}
						if damage == "wrong-digest" {
							var r Result
							json.Unmarshal(e.Value, &r)
							r.Hash = fmt.Sprintf("%064x", 1)
							e.Value, _ = json.Marshal(r)
						}
					}
					if bytes.Equal(e.Key, regionHandoffKey) {
						var bad regionfence.Handoff
						json.Unmarshal(e.Value, &bad)
						if damage == "future-seal" {
							bad.SealIndex = index + 1
						}
						if damage == "wrong-plan" {
							bad.Intent.Next.PlannedIndex++
						}
						e.Value, _ = json.Marshal(bad)
					}
				}
				bad, _ := json.Marshal(image)
				ref, err := artifacts.Create(context.Background(), snapshot.Header{Version: snapshot.StreamVersion, Format: fmt.Sprintf("capstate-v%d", image.Version), GroupID: "data-1", Index: index, Conf: image.Conf}, snapshot.MaxStreamBytes, func(e *snapshot.Encoder) error {
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
						t.Fatal("damaged handoff installed", phase, damage, format, err)
					}
				}
			}
		})
	}
}
