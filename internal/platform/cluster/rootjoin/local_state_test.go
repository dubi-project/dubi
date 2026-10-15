package rootjoin

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"go.etcd.io/raft/v3/raftpb"
)

const localTestStore = "store_00000000000000000000000000000001"

// Tests only the recovery transition model, not authentication or publication.
// The receipt metadata and confirmed cut are constructed; real mTLS tests
// exercise the preceding authority boundary in enrollmentrpc/server.
func localStateFixture(t *testing.T) (LocalState, identity.Identity, identityenrollment.Receipt, rootstate.RootJoin) {
	t.Helper()
	id, request, err := identityenrollment.Prepare(t.TempDir(), nodepki.DefaultServerName, "r2", "joiner:7443")
	if err != nil {
		t.Fatal(err)
	}
	id.ClusterID = "cluster_00000000000000000000000000000001"
	encoded, err := identityenrollment.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	receipt := identityenrollment.Receipt{Version: 1, GrantID: "join_00000000000000000000000000000001", Request: request,
		RequestSHA256: hex.EncodeToString(digest[:]), Bundle: identityenrollment.Bundle{ClusterID: id.ClusterID, NodeID: id.NodeID, RaftID: id.RaftID}}
	view := View{ClusterID: id.ClusterID, AppliedIndex: 120, Voters: []uint64{1}, Learners: []uint64{id.RaftID}, LeaderID: 1,
		Peers: []rootstate.Node{{NodeID: "node_00000000000000000000000000000001", RaftID: 1, RegionID: "r1", Address: "leader:7443"},
			{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: request.RegionID, Address: request.Address}}}
	state, err := NewLocalState(id, localTestStore, receipt, view, 100)
	if err != nil {
		t.Fatal(err)
	}
	task := rootstate.RootJoin{GrantID: receipt.GrantID, RequestSHA256: receipt.RequestSHA256, RegistrationSHA256: strings.Repeat("a", 64),
		NodeID: id.NodeID, RaftID: id.RaftID, State: rootstate.RootJoinAdmitted, AdmittedIndex: 100}
	return state, id, receipt, task
}

func TestRootJoinLocalStateNeverConfusesRemoteAdmissionAndLocalActivation(t *testing.T) {
	state, id, _, task := localStateFixture(t)
	if state.Phase != LocalCatchingUp || state.ActivatedIndex != 0 || state.View.LeaderID != 0 {
		t.Fatal("remote admission became active", state)
	}
	if err := state.Validate(id, localTestStore); err != nil {
		t.Fatal(err)
	}
	newer := state.View.CloneCanonical()
	newer.AppliedIndex = 130
	newer.LeaderID = 1
	observed, err := state.Observe(newer)
	if err != nil || observed.Phase != LocalCatchingUp || observed.ActivatedIndex != 0 {
		t.Fatal("remote view activated Node", observed, err)
	}
	active, err := observed.Activate(newer, task)
	if err != nil || active.Phase != LocalActive || active.ActivatedIndex != 130 || active.InitialViewIndex != 120 || active.AdmittedIndex != 100 {
		t.Fatal("wrong activation fence", active, err)
	}
	if err := active.Validate(id, localTestStore); err != nil {
		t.Fatal(err)
	}
	newer.AppliedIndex = 150
	retried, err := active.Activate(newer, task)
	if err != nil || retried.ActivatedIndex != 130 || retried.View.AppliedIndex != 150 {
		t.Fatal("activation retry replaced first fence", retried, err)
	}
	// Returned records must own their views even if the caller reuses input.
	newer.Learners[0] = 99
	newer.Peers[0].Address = "changed:7443"
	if err := retried.Validate(id, localTestStore); err != nil || retried.View.Peers[0].Address != "leader:7443" {
		t.Fatal("mutated returned state", err)
	}
	if state.View.AppliedIndex != 120 || state.Phase != LocalCatchingUp {
		t.Fatal("transition mutated previous record")
	}
}

func TestRootJoinLocalStateRecoveryFencesReplayAndRemoval(t *testing.T) {
	state, id, _, task := localStateFixture(t)
	member := &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{id.RaftID}}
	absent := &raftpb.ConfState{Voters: []uint64{1}}
	for _, index := range []uint64{0, 99} {
		if err := state.CheckRecovered(index, absent); err != nil {
			t.Fatal("authorized pre-admission catch-up blocked", err)
		}
	}
	for _, index := range []uint64{100, 120, 999} {
		if err := state.CheckRecovered(index, absent); !errors.Is(err, ErrMemberRemoved) {
			t.Fatal("catching-up marker bypassed removal", index, err)
		}
	}
	if err := state.CheckRecovered(120, member); err != nil {
		t.Fatal(err)
	}
	active, err := state.Activate(state.View, task)
	if err != nil {
		t.Fatal(err)
	}
	if err := active.CheckRecovered(119, member); !errors.Is(err, ErrRecoveryBehind) {
		t.Fatal("lost consensus resource re-entered joining", err)
	}
	if err := active.CheckRecovered(120, absent); !errors.Is(err, ErrMemberRemoved) {
		t.Fatal("active removed member accepted", err)
	}
	if err := active.CheckRecovered(120, member); err != nil {
		t.Fatal(err)
	}
}

func TestRootJoinLocalStateRoutingCannotRegressOrConflict(t *testing.T) {
	state, _, _, _ := localStateFixture(t)
	view := state.View.CloneCanonical()
	view.AppliedIndex--
	if _, err := state.Observe(view); !errors.Is(err, ErrOlderView) {
		t.Fatal("routing cut rolled back", err)
	}
	view = state.View.CloneCanonical()
	view.Peers[1].Address = "new-joiner:7443"
	if _, err := state.Observe(view); !errors.Is(err, ErrConflictingView) {
		t.Fatal("same cut changed address", err)
	}
	view.AppliedIndex++
	updated, err := state.Observe(view)
	if err != nil || updated.View.Peers[1].Address != "new-joiner:7443" {
		t.Fatal("new confirmed address not accepted", err)
	}
	view.Peers[1].NodeID = "node_00000000000000000000000000000003"
	view.AppliedIndex++
	if _, err := updated.Observe(view); err == nil {
		t.Fatal("local Node identity changed")
	}
	view = state.View.CloneCanonical()
	view.Learners = nil
	view.Peers = view.Peers[:1]
	view.AppliedIndex++
	if _, err := state.Observe(view); !errors.Is(err, ErrMemberRemoved) {
		t.Fatal("removed Node view accepted", err)
	}
}

func TestRootJoinLocalStateRejectsMismatchedIdentityReceiptAndTask(t *testing.T) {
	state, id, receipt, task := localStateFixture(t)
	if _, err := NewLocalState(id, localTestStore, receipt, state.View, 0); err == nil {
		t.Fatal("zero admission index accepted")
	}
	if _, err := NewLocalState(id, localTestStore, receipt, state.View, 121); err == nil {
		t.Fatal("future admission accepted")
	}
	other := id
	other.Origin = identity.OriginBootstrap
	if err := state.Validate(other, localTestStore); err == nil {
		t.Fatal("bootstrap Node became joiner")
	}
	if err := state.Validate(id, "store_00000000000000000000000000000002"); err == nil {
		t.Fatal("foreign Store accepted")
	}
	receipt.RequestSHA256 = strings.Repeat("b", 64)
	if _, err := NewLocalState(id, localTestStore, receipt, state.View, 100); err == nil {
		t.Fatal("wrong receipt hash accepted")
	}
	for _, mutate := range []func(*rootstate.RootJoin){
		func(t *rootstate.RootJoin) { t.RequestSHA256 = strings.Repeat("b", 64) },
		func(t *rootstate.RootJoin) { t.GrantID = "join_00000000000000000000000000000002" },
		func(t *rootstate.RootJoin) { t.NodeID = "node_00000000000000000000000000000002" },
		func(t *rootstate.RootJoin) { t.AdmittedIndex++ },
		func(t *rootstate.RootJoin) { t.State = rootstate.RootJoinPending; t.AdmittedIndex = 0 },
	} {
		wrong := task
		mutate(&wrong)
		if _, err := state.Activate(state.View, wrong); err == nil {
			t.Fatal("wrong task activated Node", wrong)
		}
	}
	for _, mutate := range []func(*LocalState){
		func(s *LocalState) { s.Version++ }, func(s *LocalState) { s.Phase = "unknown" },
		func(s *LocalState) { s.ActivatedIndex = 121 }, func(s *LocalState) { s.InitialViewIndex = 99 },
		func(s *LocalState) { s.View.LeaderID = 1 }, func(s *LocalState) { s.GrantID = "manual-grant" },
	} {
		wrong := state
		mutate(&wrong)
		if err := wrong.Validate(id, localTestStore); err == nil {
			t.Fatal("invalid local state accepted", wrong)
		}
		if _, err := wrong.Observe(state.View); err == nil {
			t.Fatal("invalid state mutated")
		}
	}
}
