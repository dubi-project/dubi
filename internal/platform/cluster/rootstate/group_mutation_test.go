package rootstate

import (
	"errors"
	"go.etcd.io/raft/v3/raftpb"
	"testing"
)

func TestGroupMutationPreparationAndTransferBindOriginalTask(t *testing.T) {
	for _, action := range []string{"prepare", "transfer"} {
		for _, mode := range []string{"normal", "unknown-task", "old-epoch", "completed", "replaced", "wrong-target", "caller-removed", "server-removed", "target-removed", "source-retained", "target-learner"} {
			t.Run(action+"/"+mode, func(t *testing.T) {
				s, _ := cohortFixture(t)
				members := []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 3, Role: GroupMemberVoter}, {RaftID: 4, Role: GroupMemberVoter}, {RaftID: 5, Role: GroupMemberLearner}}
				if mode == "source-retained" {
					members = append(members, GroupMember{RaftID: 2, Role: GroupMemberVoter})
				}
				applyRootFileCommand(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "replace", GroupID: "regional-west", ExpectedEpoch: 2, Members: members}})
				task := GroupTaskReference{GroupID: "regional-west", ChangeID: "replace", ExpectedEpoch: 2}
				caller, server, target := uint64(6), uint64(4), uint64(4)
				if action == "transfer" {
					server, target = 2, 1
				}
				switch mode {
				case "unknown-task":
					task.ChangeID = "old"
				case "old-epoch":
					task.ExpectedEpoch++
				case "wrong-target":
					target = server + 1
					if action == "transfer" {
						target = server
					}
				case "target-learner":
					target = 5
					if action == "prepare" {
						server = 5
					}
				case "completed", "replaced":
					applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{ChangeID: "replace", GroupID: task.GroupID}})
					if mode == "replaced" {
						applyRootFileCommand(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "next", GroupID: task.GroupID, ExpectedEpoch: 3, Members: members}})
					}
				case "caller-removed", "server-removed", "target-removed":
					missing := map[string]uint64{"caller-removed": caller, "server-removed": server, "target-removed": target}[mode]
					conf := &raftpb.ConfState{}
					for id := uint64(1); id <= 8; id++ {
						if id != missing {
							conf.Voters = append(conf.Voters, id)
						}
					}
					if err := s.ApplyMembership(s.AppliedIndex()+1, conf); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				if action == "prepare" {
					err = s.AuthorizeGroupPreparation(caller, server, task, target)
				} else {
					err = s.AuthorizeGroupTransfer(caller, server, task, target)
				}
				allowed := mode == "normal" || action == "prepare" && (mode == "source-retained" || mode == "target-learner")
				if (err == nil) != allowed {
					t.Fatal(mode, err)
				}
			})
		}
	}
}

func TestGroupMutationAuthorizationBindsActiveTaskAndCurrentMembers(t *testing.T) {
	for _, mode := range []string{"normal", "desired-server", "unrelated-server", "unknown-task", "old-epoch", "root-group", "wrong-operation", "not-desired", "remove-desired", "promote-learner-only", "caller-removed", "server-removed", "target-removed", "completed", "closed"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := cohortFixture(t)
			members := []GroupMember{{RaftID: 1, Role: GroupMemberVoter}, {RaftID: 3, Role: GroupMemberVoter}, {RaftID: 4, Role: GroupMemberVoter}, {RaftID: 5, Role: GroupMemberLearner}}
			applyRootFileCommand(t, s, Command{Type: PlanGroupMembers, GroupPlan: &GroupChangePlan{ChangeID: "replace", GroupID: "regional-west", ExpectedEpoch: 2, Members: members}})
			m := GroupMutation{GroupID: "regional-west", ChangeID: "replace", ExpectedEpoch: 2, Member: 4, Operation: raftpb.ConfChangeType_ConfChangeAddLearnerNode}
			caller, server := uint64(6), uint64(1)
			switch mode {
			case "desired-server":
				server = 4
			case "unrelated-server":
				server = 6
			case "unknown-task":
				m.ChangeID = "other"
			case "old-epoch":
				m.ExpectedEpoch = 1
			case "root-group":
				m.GroupID = "root"
			case "wrong-operation":
				m.Operation = raftpb.ConfChangeType(999)
			case "not-desired":
				m.Member = 8
			case "remove-desired":
				m.Operation = raftpb.ConfChangeType_ConfChangeRemoveNode
			case "promote-learner-only":
				m.Member = 5
				m.Operation = raftpb.ConfChangeType_ConfChangeAddNode
			case "caller-removed", "server-removed", "target-removed":
				missing := map[string]uint64{"caller-removed": 6, "server-removed": 1, "target-removed": 4}[mode]
				conf := &raftpb.ConfState{Voters: []uint64{3}}
				for _, id := range []uint64{1, 2, 4, 5, 6, 7, 8} {
					if id != missing {
						conf.Learners = append(conf.Learners, id)
					}
				}
				if err := s.ApplyMembership(s.AppliedIndex()+1, conf); err != nil {
					t.Fatal(err)
				}
			case "completed":
				applyRootFileCommand(t, s, Command{Type: CompleteGroupMembers, GroupCompletion: &GroupChangeCompletion{ChangeID: "replace", GroupID: "regional-west"}})
			case "closed":
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			err := s.AuthorizeGroupMutation(caller, server, m)
			allowed := mode == "normal" || mode == "desired-server"
			if (err == nil) != allowed {
				t.Fatal(mode, err)
			}
			if mode == "normal" {
				m.Member = 2
				m.Operation = raftpb.ConfChangeType_ConfChangeRemoveNode
				if err := s.AuthorizeGroupMutation(caller, server, m); err != nil {
					t.Fatal("authorized removal refused", err)
				}
				m.ExpectedEpoch++
				if err := s.AuthorizeGroupMutation(caller, server, m); !errors.Is(err, ErrGroupMutationUnauthorized) {
					t.Fatal(err)
				}
			}
		})
	}
}
