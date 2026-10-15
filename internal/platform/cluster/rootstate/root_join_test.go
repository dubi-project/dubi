package rootstate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"go.etcd.io/raft/v3/raftpb"
)

func rootJoinFixture(t *testing.T, root string, register, consume bool) (*State, RootJoinStart) {
	t.Helper()
	s := rootFileState(t, root)
	applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: &Node{NodeID: "node_00000000000000000000000000000001", RaftID: 11, RegionID: "r1", Address: "issuer:7443"}})
	grant := testJoinGrant()
	applyRootFileCommand(t, s, Command{Type: CreateJoinGrant, JoinGrant: &grant})
	_, request, err := identityenrollment.Prepare(t.TempDir(), nodepki.DefaultServerName, "r1", "joining:7443")
	if err != nil {
		t.Fatal(err)
	}
	if register {
		command, err := NodeEnrollmentCommand(request)
		if err != nil {
			t.Fatal(err)
		}
		index := s.AppliedIndex()
		if result := applyDirect(t, s, &index, command); result.Error != "" {
			t.Fatal(result.Error)
		}
	}
	if consume {
		encoded, err := identityenrollment.EncodeRequest(request)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(encoded)
		applyRootFileCommand(t, s, Command{Type: ConsumeJoinGrant, JoinConsumption: &JoinGrantConsumption{GrantID: grant.GrantID, RequestSHA256: hex.EncodeToString(digest[:]), AuthorizedUnix: 200}})
	}
	return s, RootJoinStart{GrantID: grant.GrantID, Request: request}
}

func rejectRootJoinCommand(t *testing.T, s *State, command Command) {
	t.Helper()
	command.Version = 1
	command.RequestID = fmt.Sprintf("rejected-%d", s.AppliedIndex()+1)
	index := s.AppliedIndex()
	if result := applyDirect(t, s, &index, command); result.Error == "" {
		t.Fatal("invalid Root join transition accepted", command.Type)
	}
}

func TestRootJoinIntentPersistsAndRequiresAppliedMembership(t *testing.T) {
	root := t.TempDir()
	s, start := rootJoinFixture(t, root, true, true)
	finish := &RootJoinFinish{GrantID: start.GrantID}
	rejectRootJoinCommand(t, s, Command{Type: CompleteRootJoin, RootJoinFinish: finish})
	applyRootFileCommand(t, s, Command{Type: BeginRootJoin, RootJoinStart: &start})
	applyRootFileCommand(t, s, Command{Type: BeginRootJoin, RootJoinStart: &start})
	task, found, err := s.RootJoin(start.GrantID)
	if err != nil || !found || task.State != RootJoinPending || task.RaftID != start.Request.RaftID {
		t.Fatal(task, found, err)
	}
	rejectRootJoinCommand(t, s, Command{Type: CompleteRootJoin, RootJoinFinish: finish})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = rootFileState(t, root)
	pending, err := s.PendingRootJoins()
	if err != nil || len(pending) != 1 || pending[0] != task {
		t.Fatal("lost pending task", pending, err)
	}
	artifacts := rootArtifacts(t, t.TempDir())
	encoded, index, conf := buildRootFile(t, s, artifacts)
	restored := rootFileState(t, t.TempDir())
	if err := restored.RestoreFrom(context.Background(), artifacts, index, encoded, conf); err != nil {
		t.Fatal(err)
	}
	if got, found, err := restored.RootJoin(start.GrantID); err != nil || !found || got != task {
		t.Fatal("snapshot lost pending task", got, err)
	}
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{11}, Learners: []uint64{start.Request.RaftID}}); err != nil {
		t.Fatal(err)
	}
	applyRootFileCommand(t, s, Command{Type: CompleteRootJoin, RootJoinFinish: finish})
	task, found, err = s.RootJoin(start.GrantID)
	if err != nil || !found || task.State != RootJoinAdmitted || task.AdmittedIndex != s.AppliedIndex() {
		t.Fatal("completion not bound to applied Root", task, err)
	}
	if err := s.ApplyMembership(s.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{11}}); err != nil {
		t.Fatal(err)
	}
	applyRootFileCommand(t, s, Command{Type: BeginRootJoin, RootJoinStart: &start})
	applyRootFileCommand(t, s, Command{Type: CompleteRootJoin, RootJoinFinish: finish})
	got, found, err := s.RootJoin(start.GrantID)
	if err != nil || !found || got != task {
		t.Fatal("completed task was reactivated after member removal", got, err)
	}
	if pending, err := s.PendingRootJoins(); err != nil || len(pending) != 0 {
		t.Fatal("admitted task still pending", pending, err)
	}
	encoded, index, conf = buildRootFile(t, s, artifacts)
	if err := restored.RestoreFrom(context.Background(), artifacts, index, encoded, conf); err != nil {
		t.Fatal(err)
	}
	if got, found, err := restored.RootJoin(start.GrantID); err != nil || !found || got != task {
		t.Fatal("snapshot changed historical admitted task", got, err)
	}
}

func TestRootJoinIntentRejectsUnauthorizedOrDifferentRequest(t *testing.T) {
	for _, mode := range []string{"unconsumed", "unregistered", "different-request", "different-grant", "metadata-differs", "no-csr-registration"} {
		t.Run(mode, func(t *testing.T) {
			s, start := rootJoinFixture(t, t.TempDir(), mode != "unregistered" && mode != "metadata-differs" && mode != "no-csr-registration", mode != "unconsumed")
			switch mode {
			case "different-request":
				start.Request.Address = "other:7443"
			case "different-grant":
				start.GrantID = "join_00000000000000000000000000000002"
			case "metadata-differs":
				applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: &Node{NodeID: start.Request.NodeID, RaftID: start.Request.RaftID, RegionID: start.Request.RegionID, Address: "other:7443"}})
			case "no-csr-registration":
				applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: &Node{NodeID: start.Request.NodeID, RaftID: start.Request.RaftID, RegionID: start.Request.RegionID, Address: start.Request.Address}})
			}
			rejectRootJoinCommand(t, s, Command{Type: BeginRootJoin, RootJoinStart: &start})
			if pending, err := s.PendingRootJoins(); err != nil || len(pending) != 0 {
				t.Fatal("rejected admission created task", pending, err)
			}
		})
	}
	s, start := rootJoinFixture(t, t.TempDir(), true, true)
	for _, command := range []Command{
		{Type: BeginRootJoin}, {Type: CompleteRootJoin},
		{Type: CompleteRootJoin, RootJoinFinish: &RootJoinFinish{GrantID: "invalid"}},
		{Type: BeginRootJoin, RootJoinStart: &start, RootJoinFinish: &RootJoinFinish{GrantID: start.GrantID}},
	} {
		command.Version, command.RequestID = 1, "invalid"
		if _, err := EncodeCommand(command); err == nil {
			t.Fatal("invalid payload accepted")
		}
	}
	if _, found, err := s.RootJoin(start.GrantID); err != nil || found {
		t.Fatal("validation mutated state", found, err)
	}
}

func TestRootJoinSnapshotRejectsDanglingOrImpossibleTask(t *testing.T) {
	for _, damage := range []string{"missing-grant", "other-node", "wrong-request", "future-index", "invalid-state"} {
		t.Run(damage, func(t *testing.T) {
			s, start := rootJoinFixture(t, t.TempDir(), true, true)
			applyRootFileCommand(t, s, Command{Type: BeginRootJoin, RootJoinStart: &start})
			task, _, err := s.RootJoin(start.GrantID)
			if err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "missing-grant":
				task.GrantID = "join_00000000000000000000000000000002"
			case "other-node":
				task.NodeID = "node_00000000000000000000000000000001"
			case "wrong-request":
				task.RequestSHA256 = testJoinGrant().SecretSHA256
			case "future-index":
				task.State, task.AdmittedIndex = RootJoinAdmitted, s.AppliedIndex()+100
			case "invalid-state":
				task.State = "ready"
			}
			batch := s.db.NewBatch()
			defer batch.Close()
			if err := setJSON(batch, rootJoinKey(task.GrantID), task); err != nil {
				t.Fatal(err)
			}
			if err := s.commitBatch(batch); err != nil {
				t.Fatal(err)
			}
			prepared, err := s.PrepareSnapshotTo(rootArtifacts(t, t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			if _, err := prepared.Build(context.Background()); err == nil {
				t.Fatal("invalid Root join task published")
			}
		})
	}
}
