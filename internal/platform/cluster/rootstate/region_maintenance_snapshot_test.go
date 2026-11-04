package rootstate

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"testing"
)

// Exercise real committed commands at every cut of ordinary Remove + Join.
// The old immutable cohort remains the historical data denominator throughout.
func maintenanceSnapshotFixture(t *testing.T, generation uint64) (*State, RegionCohort) {
	t.Helper()
	s, _ := cohortFixture(t)
	applyRootFileCommand(t, s, cohortPlan(0, 1, 2, 3))
	applyRootFileCommand(t, s, cohortConfirm(1, 2, 100000))
	if generation == 2 {
		applyRootFileCommand(t, s, cohortPlan(1, 1, 2, 3, 4))
		cohortPublish(t, s, 2, []uint64{1, 2, 3, 4})
		applyRootFileCommand(t, s, cohortConfirm(2, 3, 100003))
	}
	old, _, _ := s.RegionCohort("west", generation)
	applyRootFileCommand(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "maintenance-remove", GroupID: old.GroupID, ExpectedEpoch: old.GroupEpoch, Maintenance: true,
		Members: []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 2, Role: GroupMemberVoter}}}})
	applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{GroupID: old.GroupID, ChangeID: "maintenance-remove"}})
	return s, old
}

func TestRegionMaintenanceSnapshotAllPhases(t *testing.T) {
	for _, generation := range []uint64{1, 2} {
		for _, phase := range []string{"removed", "planned", "authorized", "completed", "confirmed"} {
			t.Run(string(rune('0'+generation))+"/"+phase, func(t *testing.T) {
				s, old := maintenanceSnapshotFixture(t, generation)
				if phase != "removed" {
					applyRootFileCommand(t, s, cohortPlan(generation, 1, 2, 5))
				}
				if phase == "authorized" || phase == "completed" || phase == "confirmed" {
					next, _, _ := s.RegionCohort("west", generation+1)
					seal := RegionGrowthSeal{RegionID: "west", GroupID: old.GroupID, Generation: generation + 1, PlannedIndex: next.PlannedIndex, ExpectedEpoch: old.GroupEpoch + 1,
						PreviousFenceRequestID: old.GroupFenceRequestID, PreviousFenceIndex: old.GroupFenceIndex, PreviousMembershipIndex: old.GroupFenceIndex + 1, Recovery: true,
						SealRequestID: "recovery-seal", SealIndex: old.GroupFenceIndex + 2, ChangeID: "recovery-grow", Members: []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 2, Role: GroupMemberVoter}, {RaftID: 5, Role: GroupMemberVoter}}}
					applyRootFileCommand(t, s, Command{Type: AuthorizeRegionGrowth, RegionGrowth: &seal})
					if phase != "authorized" {
						applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{GroupID: old.GroupID, ChangeID: seal.ChangeID}})
					}
					if phase == "confirmed" {
						applyRootFileCommand(t, s, cohortConfirm(generation+1, old.GroupEpoch+2, old.GroupFenceIndex+3))
					}
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
					got, found, err := dest.RegionCohort("west", generation)
					if err != nil || !found || !reflect.DeepEqual(got, old) {
						t.Fatal("changed historical cohort", got, old, err)
					}
					want, _, _ := s.Group(old.GroupID)
					restored, _, _ := dest.Group(old.GroupID)
					if !reflect.DeepEqual(want, restored) {
						t.Fatal("changed live Group", restored, want)
					}
					head, _, _ := s.RegionCohortHead("west")
					restoredHead, _, _ := dest.RegionCohortHead("west")
					if !reflect.DeepEqual(head, restoredHead) {
						t.Fatal("changed cohort head", restoredHead, head)
					}
				}
			})
		}
	}
}

func TestRegionMaintenanceSnapshotRejectsMissingOrForgedHistory(t *testing.T) {
	for _, mode := range []string{"missing", "pending", "not-maintenance", "wrong-epoch", "wrong-completion", "role-change", "wrong-member", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			s, old := maintenanceSnapshotFixture(t, 1)
			inline, index, conf, err := s.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			var image snapshotImage
			if err = json.Unmarshal(inline, &image); err != nil {
				t.Fatal(err)
			}
			view := inlineRootView{}
			for _, e := range image.Entries {
				view[string(e.Key)] = slices.Clone(e.Value)
			}
			key := string(groupChangeKey("maintenance-remove"))
			var task GroupChange
			if err = json.Unmarshal(view[key], &task); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "missing":
				delete(view, key)
			case "pending":
				task.State = GroupChangePending
			case "not-maintenance":
				task.Maintenance = false
			case "wrong-epoch":
				task.ExpectedEpoch++
			case "wrong-completion":
				task.CompletedEpoch++
			case "role-change":
				task.Members[0].Role = GroupMemberLearner
			case "wrong-member":
				task.Members[1].RaftID = 3
			case "duplicate":
				task.ChangeID = "duplicate"
				view[string(groupChangeKey(task.ChangeID))], _ = json.Marshal(task)
			}
			if mode != "missing" && mode != "duplicate" {
				view[key], _ = json.Marshal(task)
			}
			cohortKey := regionCohortKey("west", old.Generation)
			if err := validateRegionCohortSnapshot(view, index, cohortKey, view[string(cohortKey)]); err == nil {
				t.Fatal("accepted invalid maintenance history")
			}
			image.Entries = nil
			for key, value := range view {
				image.Entries = append(image.Entries, snapshotEntry{Key: []byte(key), Value: value})
			}
			slices.SortFunc(image.Entries, func(a, b snapshotEntry) int {
				if string(a.Key) < string(b.Key) {
					return -1
				}
				if string(a.Key) > string(b.Key) {
					return 1
				}
				return 0
			})
			bad, err := json.Marshal(image)
			if err != nil {
				t.Fatal(err)
			}
			dest := rootFileState(t, t.TempDir())
			if err = dest.Restore(index, bad, conf); err == nil {
				t.Fatal("restored invalid maintenance history")
			}
		})
	}
}
