package raft

import (
	"errors"
	"testing"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestFencedMembershipAlreadyAppliedIsIdempotentButStaleCutIsRejected(t *testing.T) {
	transport := &queuedTransport{groups: make(map[uint64]*Group), blocked: make(map[uint64]bool)}
	for _, id := range []uint64{71, 72} {
		root := t.TempDir()
		options := GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: id,
			State: &testTransferState{testState: openTestState(t, root)}, Transport: transport, Joining: id == 72}
		if id == 71 {
			options.BootstrapPeers = []uint64{71}
		}
		group, err := OpenGroup(options)
		if err != nil {
			t.Fatal(err)
		}
		transport.groups[id] = group
		t.Cleanup(func() { _ = group.Close() })
	}
	leader := transport.groups[71]
	if err := leader.Campaign(); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	apply := func(operation raftpb.ConfChangeType) {
		t.Helper()
		cut, err := leader.MembershipCut()
		if err != nil {
			t.Fatal(err)
		}
		if err := leader.ChangeMembership(cut, 72, operation); err != nil {
			t.Fatal(err)
		}
		transport.deliver(t)
	}
	repeat := func(operation raftpb.ConfChangeType) {
		t.Helper()
		before, err := leader.storage.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		apply(operation)
		if after, err := leader.storage.LastIndex(); err != nil || after != before {
			t.Fatal("already-applied membership appended another command", before, after, err)
		}
	}
	apply(raftpb.ConfChangeType_ConfChangeAddLearnerNode)
	repeat(raftpb.ConfChangeType_ConfChangeAddLearnerNode)
	stale, err := leader.MembershipCut()
	if err != nil {
		t.Fatal(err)
	}
	apply(raftpb.ConfChangeType_ConfChangeAddNode)
	repeat(raftpb.ConfChangeType_ConfChangeAddNode)
	apply(raftpb.ConfChangeType_ConfChangeRemoveNode)
	repeat(raftpb.ConfChangeType_ConfChangeRemoveNode)
	if err := leader.ChangeMembership(stale, 72, raftpb.ConfChangeType_ConfChangeRemoveNode); !errors.Is(err, ErrMembershipChanged) {
		t.Fatal("idempotence bypassed the membership CAS", err)
	}
	if err := leader.RemoveMember(72); err == nil {
		t.Fatal("unfenced removal unexpectedly became idempotent")
	}
}

// A matched log is historical evidence, not evidence that the learner is
// still reachable. Promoting this disconnected second node would change a
// working one-voter quorum into a two-voter quorum that cannot commit.
func TestGroupPromotionRejectsCaughtUpButInactiveLearner(t *testing.T) {
	transport := &queuedTransport{groups: make(map[uint64]*Group), blocked: make(map[uint64]bool)}
	for _, id := range []uint64{71, 72} {
		root := t.TempDir()
		options := GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: id,
			State: openTestState(t, root), Transport: transport, Joining: id == 72}
		if id == 71 {
			options.BootstrapPeers = []uint64{71}
		}
		group, err := OpenGroup(options)
		if err != nil {
			t.Fatal(err)
		}
		transport.groups[id] = group
		t.Cleanup(func() { _ = group.Close() })
	}
	leader := transport.groups[71]
	if err := leader.Campaign(); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	if err := leader.AddLearner(72); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	if err := leader.Submit([]byte("before-disconnect")); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	before := leader.raw.Status()
	if progress, ok := before.Progress[72]; !ok || !progress.RecentActive || progress.Match < before.GetCommit() {
		t.Fatalf("learner did not initially catch up: %+v", before)
	}
	transport.blocked[72] = true
	// No new proposals: Match remains equal to Commit even after check-quorum
	// has forgotten the disconnected learner's activity.
	for tick := 0; tick < 20; tick++ {
		if err := leader.Tick(); err != nil {
			t.Fatal(err)
		}
		transport.deliver(t)
	}
	stale := leader.raw.Status()
	progress, found := stale.Progress[72]
	if stale.RaftState != etcdraft.StateLeader || !found || progress.RecentActive || progress.Match != stale.GetCommit() {
		t.Fatalf("expected still-matched inactive learner: %+v", stale)
	}
	last, err := leader.storage.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.PromoteLearner(72); err == nil {
		t.Fatal("disconnected learner promoted solely on historical matched index")
	}
	if after, err := leader.storage.LastIndex(); err != nil || after != last {
		t.Fatalf("rejected promotion changed durable log: %d -> %d, %v", last, after, err)
	}
	if conf, err := leader.Membership(); err != nil || !containsLearner(conf, 72) || containsVoter(conf, 72) {
		t.Fatalf("rejected promotion changed membership: %+v, %v", conf, err)
	}
	if err := leader.Submit([]byte("still-writable")); err != nil {
		t.Fatal(err)
	}
	if status, err := leader.Status(); err != nil || status.AppliedIndex <= stale.GetCommit() {
		t.Fatalf("rejection lost original quorum availability: %+v, %v", status, err)
	}
	delete(transport.blocked, 72)
	for tick := 0; tick < 20; tick++ {
		if err := leader.Tick(); err != nil {
			t.Fatal(err)
		}
		transport.deliver(t)
		s := leader.raw.Status()
		p := s.Progress[72]
		if p.RecentActive && p.Match >= s.GetCommit() {
			break
		}
	}
	if err := leader.PromoteLearner(72); err != nil {
		t.Fatalf("reconnected and caught-up learner was not promotable: %v", err)
	}
	transport.deliver(t)
	for id, group := range transport.groups {
		if conf, err := group.Membership(); err != nil || !containsVoter(conf, 72) || containsLearner(conf, 72) {
			t.Fatalf("node %d promotion was not committed: %+v, %v", id, conf, err)
		}
	}
}
