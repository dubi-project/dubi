package rootstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
)

func growthFixture(t *testing.T) (*State, string, RegionGrowthSeal) {
	t.Helper()
	s, dir := cohortFixture(t)
	applyRootFileCommand(t, s, cohortPlan(0, 1, 2, 3))
	applyRootFileCommand(t, s, cohortConfirm(1, 2, 100000))
	applyRootFileCommand(t, s, cohortPlan(1, 1, 2, 3, 4))
	next, _, _ := s.RegionCohort("west", 2)
	return s, dir, RegionGrowthSeal{RegionID: "west", GroupID: "regional-west", Generation: 2, PlannedIndex: next.PlannedIndex, ExpectedEpoch: 2,
		PreviousFenceRequestID: "fence-1", PreviousFenceIndex: 100000, PreviousMembershipIndex: 1,
		SealRequestID: "seal-2", SealIndex: 100001, ChangeID: "grow-2",
		Members: []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 2, Role: GroupMemberVoter}, {RaftID: 3, Role: GroupMemberVoter}, {RaftID: 4, Role: GroupMemberLearner}}}
}

func TestRegionGrowthAtomicTaskRestartAndHistoricalRetry(t *testing.T) {
	s, dir, seal := growthFixture(t)
	command := Command{Type: AuthorizeRegionGrowth, RegionGrowth: &seal}
	applyRootFileCommand(t, s, command)
	original, found, err := s.RegionGrowth("west", 2)
	if err != nil || !found || original.Validate() != nil || !reflect.DeepEqual(original.Seal, seal) {
		t.Fatal(original, found, err)
	}
	check := func(s *State, pending bool) {
		t.Helper()
		g, found, err := s.RegionGrowth("west", 2)
		if err != nil || !found || !reflect.DeepEqual(g, original) {
			t.Fatal("lost immutable authorization", g, found, err)
		}
		task, found, err := s.GroupChange(seal.ChangeID)
		if err != nil || !found || !growthMatchesTask(g, task) || (task.State == GroupChangePending) != pending {
			t.Fatal(task, found, err)
		}
		active, found, err := s.ActiveGroupChange(seal.GroupID)
		if err != nil || found != pending || found && active.ChangeID != seal.ChangeID {
			t.Fatal(active, found, err)
		}
	}
	check(s, true)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = rootFileState(t, dir)
	check(s, true)
	ref := GroupTaskReference{GroupID: seal.GroupID, ChangeID: seal.ChangeID, ExpectedEpoch: 2}
	if err := s.AuthorizeGroupPreparation(1, 4, ref, 4); err != nil {
		t.Fatal(err)
	}
	if err := s.AuthorizeGroupPreparation(1, 5, ref, 5); !errors.Is(err, ErrGroupMutationUnauthorized) {
		t.Fatal(err)
	}
	if _, ready, err := s.PendingRegionConfirmation(seal.GroupID); err != nil || ready {
		t.Fatal(ready, err)
	}
	cohortReject(t, s, cohortConfirm(2, 3, 100003))
	applyRootFileCommand(t, s, command)
	check(s, true)
	other := seal
	other.SealIndex++
	cohortReject(t, s, Command{Type: AuthorizeRegionGrowth, RegionGrowth: &other})
	applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{GroupID: seal.GroupID, ChangeID: seal.ChangeID}})
	check(s, false)
	if _, ready, err := s.PendingRegionConfirmation(seal.GroupID); err != nil || !ready {
		t.Fatal(ready, err)
	}
	cohortReject(t, s, cohortConfirm(2, 3, seal.SealIndex))
	badConfirm := cohortConfirm(2, 3, 100003)
	badConfirm.RegionConfirmation.GroupFenceRequestID = seal.SealRequestID
	cohortReject(t, s, badConfirm)
	applyRootFileCommand(t, s, cohortConfirm(2, 3, 100003))
	applyRootFileCommand(t, s, cohortPlan(2, 1, 2, 3, 4, 5))
	applyRootFileCommand(t, s, command) // Historical assertion cannot re-create its active task.
	check(s, false)
	if err := s.AuthorizeGroupPreparation(1, 4, ref, 4); !errors.Is(err, ErrGroupMutationUnauthorized) {
		t.Fatal(err)
	}
}

func TestRegionGrowthRejectsUnboundSealAndMembershipBypasses(t *testing.T) {
	for _, mode := range []string{"plan", "generation", "region", "group", "epoch", "old-fence-id", "old-fence-index", "node-set", "old-role", "joint", "removed", "task-collision", "direct-publish", "generic-plan"} {
		t.Run(mode, func(t *testing.T) {
			s, _, p := growthFixture(t)
			command := Command{Type: AuthorizeRegionGrowth, RegionGrowth: &p}
			switch mode {
			case "plan":
				p.PlannedIndex++
			case "generation":
				p.Generation++
			case "region":
				p.RegionID = "east"
			case "group":
				p.GroupID = "other"
			case "epoch":
				p.ExpectedEpoch++
			case "old-fence-id":
				p.PreviousFenceRequestID = "wrong"
			case "old-fence-index":
				p.PreviousFenceIndex--
			case "node-set":
				p.Members[3].RaftID = 5
			case "old-role":
				p.Members[0].Role = GroupMemberLearner
			case "joint", "removed":
				conf := s.ConfState()
				if mode == "joint" {
					conf.VotersOutgoing = []uint64{1}
				} else {
					conf.Learners = []uint64{2, 3, 5, 6, 7, 8}
				}
				if err := s.ApplyMembership(s.AppliedIndex()+1, conf); err != nil {
					t.Fatal(err)
				}
			case "task-collision":
				applyRootFileCommand(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: p.ChangeID, GroupID: "other", ExpectedEpoch: 1, Members: p.Members}})
			case "direct-publish":
				command = Command{Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: p.GroupID, ExpectedEpoch: 2, Members: p.Members}}
			case "generic-plan":
				command = Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: p.ChangeID, GroupID: p.GroupID, ExpectedEpoch: 2, Members: p.Members}}
			}
			cohortReject(t, s, command)
			if _, found, err := s.RegionGrowth("west", 2); err != nil || found {
				t.Fatal("rejected growth wrote authorization", found, err)
			}
			if _, found, err := s.ActiveGroupChange("regional-west"); err != nil || found {
				t.Fatal("rejected growth wrote task", found, err)
			}
		})
	}
}

func TestRegionGrowthCompletionRequiresLiveAuthorization(t *testing.T) {
	s, _, p := growthFixture(t)
	applyRootFileCommand(t, s, Command{Type: AuthorizeRegionGrowth, RegionGrowth: &p})
	stable := s.ConfState()
	joint := s.ConfState()
	joint.VotersOutgoing = []uint64{1}
	if err := s.ApplyMembership(s.AppliedIndex()+1, joint); err != nil {
		t.Fatal(err)
	}
	ref := GroupTaskReference{GroupID: p.GroupID, ChangeID: p.ChangeID, ExpectedEpoch: p.ExpectedEpoch}
	if err := s.AuthorizeGroupPreparation(1, 4, ref, 4); !errors.Is(err, ErrGroupMutationUnauthorized) {
		t.Fatal(err)
	}
	cohortReject(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{GroupID: p.GroupID, ChangeID: p.ChangeID}})
	if err := s.ApplyMembership(s.AppliedIndex()+1, stable); err != nil {
		t.Fatal(err)
	}
	if err := s.AuthorizeGroupPreparation(1, 4, ref, 4); err != nil {
		t.Fatal(err)
	}
	applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{GroupID: p.GroupID, ChangeID: p.ChangeID}})
}

func TestRegionGrowthUncertainCommitIsAtomic(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		t.Run(fmt.Sprint(persisted), func(t *testing.T) {
			s, dir, p := growthFixture(t)
			command, err := EncodeCommand(Command{Version: FormatVersion, RequestID: "uncertain-growth", Type: AuthorizeRegionGrowth, RegionGrowth: &p})
			if err != nil {
				t.Fatal(err)
			}
			before := s.AppliedIndex()
			injected := errors.New("uncertain growth Sync")
			s.commit = func(b *pebble.Batch) error {
				if persisted {
					if err := b.Commit(pebble.Sync); err != nil {
						return err
					}
				}
				return injected
			}
			if err := s.Apply(before+1, command); !errors.Is(err, injected) {
				t.Fatal(err)
			}
			if _, _, err := s.RegionGrowth("west", 2); !errors.Is(err, injected) {
				t.Fatal("uncertain read escaped", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = rootFileState(t, dir)
			if _, found, err := s.RegionGrowth("west", 2); err != nil || found != persisted {
				t.Fatal(found, err)
			}
			if _, found, err := s.GroupChange(p.ChangeID); err != nil || found != persisted {
				t.Fatal(found, err)
			}
			if _, found, err := s.ActiveGroupChange(p.GroupID); err != nil || found != persisted {
				t.Fatal(found, err)
			}
			if _, found, err := s.ResultForCommand(command); err != nil || found != persisted {
				t.Fatal(found, err)
			}
			if !persisted {
				if err := s.Apply(before+1, command); err != nil {
					t.Fatal(err)
				}
			}
			if _, found, err := s.ActiveGroupChange(p.GroupID); err != nil || !found {
				t.Fatal(found, err)
			}
		})
	}
}

func TestRegionGrowthSnapshotAllPhases(t *testing.T) {
	for _, phase := range []string{"pending", "completed", "confirmed", "historical"} {
		t.Run(phase, func(t *testing.T) {
			s, _, p := growthFixture(t)
			applyRootFileCommand(t, s, Command{Type: AuthorizeRegionGrowth, RegionGrowth: &p})
			if phase != "pending" {
				applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{GroupID: p.GroupID, ChangeID: p.ChangeID}})
			}
			if phase == "confirmed" || phase == "historical" {
				applyRootFileCommand(t, s, cohortConfirm(2, 3, 100003))
			}
			if phase == "historical" {
				applyRootFileCommand(t, s, cohortPlan(2, 1, 2, 3, 4, 5))
				cohortPublish(t, s, 3, []uint64{1, 2, 3, 4, 5})
				applyRootFileCommand(t, s, cohortConfirm(3, 4, 100006))
			}
			inline, index, conf, err := s.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			store := rootArtifacts(t, t.TempDir())
			file, _, _ := buildRootFile(t, s, store)
			for _, format := range []string{"inline", "file"} {
				dest := rootFileState(t, t.TempDir())
				if format == "inline" {
					err = dest.Restore(index, inline, conf)
				} else {
					err = dest.RestoreFrom(context.Background(), store, index, file, conf)
				}
				if err != nil {
					t.Fatal(format, err)
				}
				want, _, _ := s.RegionGrowth("west", 2)
				got, found, err := dest.RegionGrowth("west", 2)
				if err != nil || !found || !reflect.DeepEqual(want, got) {
					t.Fatal(format, got, found, err)
				}
			}
		})
	}
}

func TestRegionGrowthSealValidation(t *testing.T) {
	for _, mode := range []string{"unsorted", "duplicate", "seal-before-fence", "membership-after-fence", "same-request", "unknown-role", "initial-generation"} {
		t.Run(mode, func(t *testing.T) {
			_, _, p := growthFixture(t)
			switch mode {
			case "unsorted":
				p.Members[0], p.Members[1] = p.Members[1], p.Members[0]
			case "duplicate":
				p.Members[1].RaftID = 1
			case "seal-before-fence":
				p.SealIndex = p.PreviousFenceIndex
			case "membership-after-fence":
				p.PreviousMembershipIndex = p.PreviousFenceIndex
			case "same-request":
				p.SealRequestID = p.PreviousFenceRequestID
			case "unknown-role":
				p.Members[3].Role = "owner"
			case "initial-generation":
				p.Generation = 1
			}
			if p.Validate() == nil {
				t.Fatal("invalid seal accepted")
			}
		})
	}
}

func TestRegionGrowthSnapshotCannotStripAuthorizationIntoGenericTask(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(fmt.Sprint(completed), func(t *testing.T) {
			s, _, p := growthFixture(t)
			applyRootFileCommand(t, s, Command{Type: AuthorizeRegionGrowth, RegionGrowth: &p})
			if completed {
				applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{GroupID: p.GroupID, ChangeID: p.ChangeID}})
			}
			inline, index, conf, err := s.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			store := rootArtifacts(t, t.TempDir())
			file, _, _ := buildRootFile(t, s, store)
			ref, _, _ := snapshot.DecodeReference(file)
			r, err := store.Open(ref)
			if err != nil {
				t.Fatal(err)
			}
			d, err := snapshot.NewDecoder(r, ref.Size)
			if err != nil {
				t.Fatal(err)
			}
			header := d.Header()
			r.Close()
			var original snapshotImage
			if err := json.Unmarshal(inline, &original); err != nil {
				t.Fatal(err)
			}
			entries := make([]snapshotEntry, 0, len(original.Entries))
			for _, entry := range original.Entries {
				if string(entry.Key) == string(regionGrowthKey("west", 2)) {
					continue
				}
				if string(entry.Key) == string(groupChangeKey(p.ChangeID)) {
					var task GroupChange
					if err := json.Unmarshal(entry.Value, &task); err != nil {
						t.Fatal(err)
					}
					task.RegionID, task.GrowthGeneration = "", 0
					entry.Value, err = json.Marshal(task)
					if err != nil {
						t.Fatal(err)
					}
				}
				entries = append(entries, entry)
			}
			original.Entries = entries
			badInline, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			badRef, err := store.Create(context.Background(), header, snapshot.MaxStreamBytes, func(e *snapshot.Encoder) error {
				for _, entry := range entries {
					if err := e.Add(entry.Key, entry.Value); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			badFile, err := badRef.Encode()
			if err != nil {
				t.Fatal(err)
			}
			for _, format := range []string{"inline", "file"} {
				dest := rootFileState(t, t.TempDir())
				applyRootFileCommand(t, dest, Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: "preserve"}})
				if format == "inline" {
					err = dest.Restore(index, badInline, conf)
				} else {
					err = dest.RestoreFrom(context.Background(), store, index, badFile, conf)
				}
				if err == nil || dest.AppliedIndex() != 1 {
					t.Fatal(format, "accepted stripped authorization", err)
				}
				if _, found, err := dest.Tenant("preserve"); err != nil || !found {
					t.Fatal(format, "damaged destination", found, err)
				}
			}
		})
	}
}
