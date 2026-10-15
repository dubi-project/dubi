package rootstate

import (
	"context"
	"reflect"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestRegionFormationPermitSurvivesBothSnapshotFormats(t *testing.T) {
	for _, published := range []bool{false, true} {
		s, _ := cohortFixture(t)
		if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2, 3}}); err != nil {
			t.Fatal(err)
		}
		plan := cohortPlan(0, 1, 2, 3)
		plan.RegionPlan.GroupID = "other"
		applyRootFileCommand(t, s, plan)
		if published {
			applyRootFileCommand(t, s, Command{Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "other", ExpectedEpoch: 1, Members: []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 2, Role: GroupMemberVoter}, {RaftID: 3, Role: GroupMemberVoter}}}})
		}
		original, _, _ := s.GroupInitialCohort("other")
		inline, index, conf, err := s.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		artifacts := rootArtifacts(t, t.TempDir())
		file, _, _ := buildRootFile(t, s, artifacts)
		for _, format := range []string{"inline", "file"} {
			dest := rootFileState(t, t.TempDir())
			if format == "inline" {
				err = dest.Restore(index, inline, conf)
			} else {
				err = dest.RestoreFrom(context.Background(), artifacts, index, file, conf)
			}
			if err != nil {
				t.Fatal(published, format, err)
			}
			restored, found, err := dest.GroupInitialCohort("other")
			if err != nil || !found || !restored.Formation || !reflect.DeepEqual(original, restored) {
				t.Fatal(published, format, restored, found, err)
			}
			head, found, err := dest.RegionCohortHead("west")
			if err != nil || !found || head.CurrentGeneration != 0 || head.PendingGeneration != 1 {
				t.Fatal("snapshot changed durability authorization", published, format, head, found, err)
			}
		}
	}
}

func TestRegionFormationPermitRequiresRootMembersAndExclusiveInitialMembership(t *testing.T) {
	s, _ := cohortFixture(t)
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	plan := cohortPlan(0, 1, 2, 3)
	plan.RegionPlan.GroupID = "other"
	cohortReject(t, s, plan) // Registered but not yet Root-admitted.
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2, 3}}); err != nil {
		t.Fatal(err)
	}
	applyRootFileCommand(t, s, plan)
	c, found, err := s.GroupInitialCohort("other")
	if err != nil || !found || !c.Formation {
		t.Fatal(c, found, err)
	}
	cohortReject(t, s, Command{Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "other", ExpectedEpoch: 1, Members: []GroupMember{{RaftID: 1, Role: GroupMemberVoter}}}})
	members := []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 2, Role: GroupMemberVoter}, {RaftID: 3, Role: GroupMemberVoter}}
	cohortReject(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "too-early", GroupID: "other", ExpectedEpoch: 1, Members: members}})
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}); err != nil {
		t.Fatal(err)
	}
	cohortReject(t, s, Command{Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "other", ExpectedEpoch: 1, Members: members}})
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2, 3}}); err != nil {
		t.Fatal(err)
	}
	applyRootFileCommand(t, s, Command{Type: PublishGroupMembers, Membership: &GroupMembership{GroupID: "other", ExpectedEpoch: 1, Members: members}})
	applyRootFileCommand(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "after-formation", GroupID: "other", ExpectedEpoch: 2, Members: members}})
}

func TestRegionFormationCannotTakeOverAnExistingMembershipTask(t *testing.T) {
	s, _ := cohortFixture(t)
	applyRootFileCommand(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "existing", GroupID: "other", ExpectedEpoch: 1, Members: []GroupMember{{RaftID: 1, Role: GroupMemberVoter}}}})
	plan := cohortPlan(0, 1, 2, 3)
	plan.RegionPlan.GroupID = "other"
	cohortReject(t, s, plan)
	if _, found, err := s.GroupInitialCohort("other"); err != nil || found {
		t.Fatal(found, err)
	}
	// Cohorts for already published Groups never become bootstrap permits.
	applyRootFileCommand(t, s, cohortPlan(0, 1, 2, 3))
	c, found, err := s.GroupInitialCohort("regional-west")
	if err != nil || !found || c.Formation {
		t.Fatal(c, found, err)
	}
}
