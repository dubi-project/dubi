package rootstate

import (
	"strings"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestEnrollmentAuthorityRequiresRootMajorityAndPersistsEpoch(t *testing.T) {
	s := rootFileState(t, t.TempDir())
	for id := uint64(1); id <= 3; id++ {
		applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: fenceTestNode(id)})
	}
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	applyRootFileCommand(t, s, Command{Type: PublishEnrollmentAuthority,
		EnrollmentAuthority: &EnrollmentAuthorityPlan{Epoch: 1, Members: []uint64{1, 2}, ReadyMembers: []uint64{1, 2}, State: AuthorityStateActive}})
	authority, found, err := s.EnrollmentAuthority()
	if err != nil || !found || authority.Epoch != 1 || authority.ConfirmedIndex == 0 {
		t.Fatalf("initial authority = %+v, %v, %v", authority, found, err)
	}
	applyRootFileCommand(t, s, Command{Type: PublishEnrollmentAuthority,
		EnrollmentAuthority: &EnrollmentAuthorityPlan{ExpectedEpoch: 1, Epoch: 2, Members: []uint64{2, 3}, ReadyMembers: []uint64{2, 3}, State: AuthorityStateActive}})
	authority, found, err = s.EnrollmentAuthority()
	if err != nil || !found || authority.Epoch != 2 || len(authority.Members) != 2 || authority.Members[0] != 2 || authority.Members[1] != 3 {
		t.Fatalf("updated authority = %+v, %v, %v", authority, found, err)
	}
	command := Command{Version: FormatVersion, RequestID: "authority-minority", Type: PublishEnrollmentAuthority,
		EnrollmentAuthority: &EnrollmentAuthorityPlan{ExpectedEpoch: 2, Epoch: 3, Members: []uint64{2}, ReadyMembers: []uint64{2}, State: AuthorityStateActive}}
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(s.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := s.Result(command.RequestID)
	if err != nil || !found || !strings.Contains(result.Error, "majority") {
		t.Fatalf("minority authority result = %+v, %v, %v", result, found, err)
	}
	if _, _, _, err := s.Snapshot(); err != nil {
		t.Fatal("authority record did not survive snapshot validation", err)
	}
}
