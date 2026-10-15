package rootstate

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

func testJoinGrant() JoinGrant {
	return JoinGrant{GrantID: "join_00000000000000000000000000000001", IssuerRaftID: 11,
		SecretSHA256: strings.Repeat("a", 64), EnrollmentKeySHA256: strings.Repeat("b", 64),
		CreatedUnix: 100, ExpiresUnix: 1000, State: JoinGrantActive}
}

func TestJoinGrantTransitionPersistenceAndSnapshot(t *testing.T) {
	root := t.TempDir()
	s := rootFileState(t, root)
	applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: &Node{NodeID: "node_00000000000000000000000000000001",
		RaftID: 11, RegionID: "r1", Address: "node:7443"}})
	grant := testJoinGrant()
	applyRootFileCommand(t, s, Command{Type: CreateJoinGrant, JoinGrant: &grant})
	use := JoinGrantConsumption{GrantID: grant.GrantID, RequestSHA256: strings.Repeat("c", 64), AuthorizedUnix: 200}
	applyRootFileCommand(t, s, Command{Type: ConsumeJoinGrant, JoinConsumption: &use})
	use.AuthorizedUnix = 300
	applyRootFileCommand(t, s, Command{Type: ConsumeJoinGrant, JoinConsumption: &use})
	persisted, found, err := s.JoinGrant(grant.GrantID)
	if err != nil || !found || persisted.State != JoinGrantConsumed || persisted.ConsumedUnix != 200 {
		t.Fatal(persisted, found, err)
	}
	for i, command := range []Command{
		{Type: CreateJoinGrant, JoinGrant: &grant},
		{Type: ConsumeJoinGrant, JoinConsumption: &JoinGrantConsumption{GrantID: grant.GrantID, RequestSHA256: strings.Repeat("d", 64), AuthorizedUnix: 300}},
		{Type: RevokeJoinGrant, JoinRevocation: &JoinGrantRevocation{GrantID: grant.GrantID, RevokedUnix: 400}},
	} {
		command.Version = FormatVersion
		command.RequestID = []string{"duplicate", "steal", "revoke-consumed"}[i]
		index := s.AppliedIndex()
		if result := applyDirect(t, s, &index, command); result.Error == "" {
			t.Fatal("invalid transition succeeded", command.Type)
		}
	}
	revoked := grant
	revoked.GrantID = "join_00000000000000000000000000000002"
	applyRootFileCommand(t, s, Command{Type: CreateJoinGrant, JoinGrant: &revoked})
	applyRootFileCommand(t, s, Command{Type: RevokeJoinGrant, JoinRevocation: &JoinGrantRevocation{GrantID: revoked.GrantID, RevokedUnix: 1200}})
	active := grant
	active.GrantID = "join_00000000000000000000000000000003"
	applyRootFileCommand(t, s, Command{Type: CreateJoinGrant, JoinGrant: &active})
	store := rootArtifacts(t, t.TempDir())
	encoded, index, conf := buildRootFile(t, s, store)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = rootFileState(t, root)
	destination := rootFileState(t, t.TempDir())
	if err := destination.RestoreFrom(context.Background(), store, index, encoded, conf); err != nil {
		t.Fatal(err)
	}
	for _, state := range []*State{s, destination} {
		for id, want := range map[string]string{grant.GrantID: JoinGrantConsumed, revoked.GrantID: JoinGrantRevoked, active.GrantID: JoinGrantActive} {
			got, found, err := state.JoinGrant(id)
			if err != nil || !found || got.State != want {
				t.Fatal(got, found, err)
			}
		}
	}
}

func TestJoinGrantRejectsInvalidWindowsAndMixedPayloads(t *testing.T) {
	grant := testJoinGrant()
	variants := []JoinGrant{grant, grant, grant, grant, grant, grant}
	variants[0].SecretSHA256 = strings.Repeat("A", 64)
	variants[1].ExpiresUnix = grant.CreatedUnix
	variants[2].ExpiresUnix = grant.CreatedUnix + 86401
	variants[3].State = JoinGrantConsumed
	variants[4].State = JoinGrantRevoked
	variants[5].ConsumedUnix = 200
	for _, invalid := range variants {
		if invalid.Validate() == nil {
			t.Fatal("invalid Grant accepted", invalid.State)
		}
	}
	node := &Node{NodeID: "node_00000000000000000000000000000001", RaftID: 11, RegionID: "r1", Address: "node:7443"}
	for _, kind := range []string{RegisterNode, CreateJoinGrant} {
		_, err := EncodeCommand(Command{Version: FormatVersion, RequestID: "mixed", Type: kind, Node: node, JoinGrant: &grant})
		if err == nil {
			t.Fatal("mixed legacy/new payload accepted")
		}
	}
	s := rootFileState(t, t.TempDir())
	var index uint64
	result := applyDirect(t, s, &index, Command{Version: FormatVersion, RequestID: "no-issuer", Type: CreateJoinGrant, JoinGrant: &grant})
	if result.Error == "" {
		t.Fatal("unknown issuer accepted")
	}
	applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: node})
	applyRootFileCommand(t, s, Command{Type: CreateJoinGrant, JoinGrant: &grant})
	for _, at := range []int64{99, 1000, 1001} {
		index = s.AppliedIndex()
		command := Command{Version: FormatVersion, RequestID: "time-" + strconv.FormatInt(at, 10), Type: ConsumeJoinGrant,
			JoinConsumption: &JoinGrantConsumption{GrantID: grant.GrantID, RequestSHA256: strings.Repeat("c", 64), AuthorizedUnix: at}}
		result := applyDirect(t, s, &index, command)
		if result.Error == "" {
			t.Fatal("out-of-window consumption accepted", at)
		}
	}
	current, _, err := s.JoinGrant(grant.GrantID)
	if err != nil || current.State != JoinGrantActive {
		t.Fatal("rejection changed Grant", current, err)
	}
	// Invalid committed commands still produce deterministic rejection results,
	// but cannot mutate the ledger or smuggle another payload into it.
	mixed, _ := json.Marshal(Command{Version: FormatVersion, RequestID: "bad-committed", Type: CreateJoinGrant, JoinGrant: &grant, Node: node})
	if err := s.Apply(s.AppliedIndex()+1, mixed); err != nil {
		t.Fatal(err)
	}
	if result, _, err := s.Result("bad-committed"); err != nil || result.Error == "" {
		t.Fatal(result, err)
	}
}

func TestJoinGrantSnapshotRejectsDanglingIssuer(t *testing.T) {
	s := rootFileState(t, t.TempDir())
	if err := s.Advance(1); err != nil {
		t.Fatal(err)
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	grant := testJoinGrant()
	if err := setJSON(batch, joinGrantKey(grant.GrantID), grant); err != nil {
		t.Fatal(err)
	}
	if err := s.commitBatch(batch); err != nil {
		t.Fatal(err)
	}
	store := rootArtifacts(t, t.TempDir())
	prepared, err := s.PrepareSnapshotTo(store)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if _, err := prepared.Build(context.Background()); err == nil {
		t.Fatal("dangling Grant issuer published")
	}
}
