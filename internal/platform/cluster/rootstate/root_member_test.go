package rootstate

import (
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRootMemberRequiresCurrentMembershipAndUsableDescriptor(t *testing.T) {
	s := rootFileState(t, t.TempDir())
	for id := uint64(1); id <= 5; id++ {
		applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: &Node{NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: "local", Address: fmt.Sprintf("n%d:7443", id)}})
	}
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1}, VotersOutgoing: []uint64{2}, Learners: []uint64{3}, LearnersNext: []uint64{4}}); err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 5; id++ {
		node, found, err := s.RootMember(id)
		if err != nil || found != (id < 5) || found && node.RaftID != id {
			t.Fatal(id, node, found, err)
		}
	}
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.RootMember(2); err != nil || found {
		t.Fatal("removed member", found, err)
	}
	if _, found, err := s.Node(2); err != nil || !found {
		t.Fatal("historical descriptor should remain", found, err)
	}
	if err := s.db.Delete(nodeKey(1), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.RootMember(1); err == nil {
		t.Fatal("member with missing descriptor accepted")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.RootMember(2); err == nil {
		t.Fatal("closed state returned nonmembership")
	}
}

func TestRootMembersUsesOneAuthorizationCut(t *testing.T) {
	s := rootFileState(t, t.TempDir())
	for id := uint64(1); id <= 2; id++ {
		applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: &Node{NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: "local", Address: fmt.Sprintf("n%d:7443", id)}})
	}
	if err := s.ApplyMembership(3, &raftpb.ConfState{Voters: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				if _, all, err := s.RootMembers(1, 2); err != nil || all {
					t.Error("mixed authorization cuts", all, err)
				}
			}
		})
	}
	for index := uint64(4); index < 50; index++ {
		if err := s.ApplyMembership(index, &raftpb.ConfState{Voters: []uint64{1 + index%2}}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if err := s.ApplyMembership(50, &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}); err != nil {
		t.Fatal(err)
	}
	if nodes, all, err := s.RootMembers(2, 1); err != nil || !all || len(nodes) != 2 || nodes[0].RaftID != 2 || nodes[1].RaftID != 1 {
		t.Fatal(nodes, all, err)
	}
	if _, _, err := s.RootMembers(); err == nil {
		t.Fatal("empty authorization set accepted")
	}
	cohort := make([]uint64, 17)
	for i := range cohort {
		cohort[i] = uint64(i + 1)
	}
	if _, all, err := s.RootMembers(cohort...); err != nil || all {
		t.Fatal("bounded regional authorization set was rejected or accepted missing members", all, err)
	}
	if _, _, err := s.RootMembers(make([]uint64, MaxRegionCohortNodes+1)...); err == nil {
		t.Fatal("unbounded authorization set accepted")
	}
}
