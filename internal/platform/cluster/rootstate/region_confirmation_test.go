package rootstate

import (
	"go.etcd.io/raft/v3/raftpb"
	"testing"
)

func TestRegionCohortConfirmationRequiresCurrentMembersAtApply(t *testing.T) {
	for _, change := range []string{"removed", "joint", "task", "epoch"} {
		t.Run(change, func(t *testing.T) {
			s, _ := cohortFixture(t)
			applyRootFileCommand(t, s, cohortPlan(0, 1, 2, 3))
			before, found, err := s.PendingRegionConfirmation("regional-west")
			if err != nil || !found || before.Group.Epoch != 2 {
				t.Fatal(before, found, err)
			}
			switch change {
			case "removed":
				if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}); err != nil {
					t.Fatal(err)
				}
			case "joint":
				if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1}, Learners: []uint64{3}}); err != nil {
					t.Fatal(err)
				}
			case "task":
				applyRootFileCommand(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "busy", GroupID: "regional-west", ExpectedEpoch: 2, Members: before.Group.Members}})
			case "epoch":
				cohortPublish(t, s, 2, []uint64{1, 2, 3})
			}
			after, ready, err := s.PendingRegionConfirmation("regional-west")
			if err != nil || ready != (change == "epoch") || ready && after.Group.Epoch != 3 {
				t.Fatal(after, ready, err)
			}
			cohortReject(t, s, cohortConfirm(1, 2, 50))
			head, _, _ := s.RegionCohortHead("west")
			if head.CurrentGeneration != 0 || head.PendingGeneration != 1 {
				t.Fatal(head)
			}
		})
	}
}

func TestConfirmedRegionAllowsOnlyExplicitMajorityMemberRemoval(t *testing.T) {
	s, _ := cohortFixture(t)
	applyRootFileCommand(t, s, cohortPlan(0, 1, 2, 3))
	applyRootFileCommand(t, s, cohortConfirm(1, 2, 50))

	withoutMaintenance := Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "remove-without-maintenance", GroupID: "regional-west", ExpectedEpoch: 2,
		Members: []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 2, Role: GroupMemberVoter}}}}
	cohortReject(t, s, withoutMaintenance)
	cohortReject(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "remove-too-many", GroupID: "regional-west", ExpectedEpoch: 2, Maintenance: true,
		Members: []GroupMember{{RaftID: 1, Role: GroupMemberVoter}}}})

	plan := Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "remove-maintenance", GroupID: "regional-west", ExpectedEpoch: 2, Maintenance: true,
		Members: []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 2, Role: GroupMemberVoter}}}}
	applyRootFileCommand(t, s, plan)
	applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{GroupID: "regional-west", ChangeID: "remove-maintenance"}})
	group, found, err := s.Group("regional-west")
	if err != nil || !found || group.Epoch != 3 || len(group.Members) != 2 {
		t.Fatal("maintenance member removal did not complete", group, found, err)
	}
}

func TestRegionCohortConfirmationViewIsOwnedAndHistoricalRetryDoesNotReauthorize(t *testing.T) {
	s, _ := cohortFixture(t)
	applyRootFileCommand(t, s, cohortPlan(0, 1, 2, 3))
	v, found, err := s.PendingRegionConfirmation("regional-west")
	if err != nil || !found {
		t.Fatal(v, found, err)
	}
	v.Cohort.Nodes[0], v.Group.Members[0].RaftID = 99, 99
	v, found, err = s.PendingRegionConfirmation("regional-west")
	if err != nil || !found || v.Cohort.Nodes[0] != 1 || v.Group.Members[0].RaftID != 1 {
		t.Fatal(v, found, err)
	}
	applyRootFileCommand(t, s, cohortConfirm(1, 2, 50))
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	applyRootFileCommand(t, s, cohortConfirm(1, 2, 50)) // Historical assertion stays immutable, not a live write permit.
	if _, ready, err := s.PendingRegionConfirmation("regional-west"); err != nil || ready {
		t.Fatal(ready, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.PendingRegionConfirmation("regional-west"); err == nil {
		t.Fatal("closed state admitted observation")
	}
}
