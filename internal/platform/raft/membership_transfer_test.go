package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// testTransferState tracks applied ConfChanges under the owning Group lock.
// Persistent cut/reopen coverage lives in capstate/membership_raft_test.go.
type testTransferState struct {
	*testState
	memberIndex uint64
	reads       int
	failRetry   bool
}

func (s *testTransferState) ApplyMembership(index uint64, conf *raftpb.ConfState) error {
	if err := s.testState.ApplyMembership(index, conf); err != nil {
		return err
	}
	s.memberIndex = index
	return nil
}
func (s *testTransferState) MembershipCut() (MembershipCut, error) {
	s.reads++
	if s.failRetry && s.reads > 1 {
		return MembershipCut{}, ErrMembershipChanged
	}
	return MembershipCut{Index: s.memberIndex, Conf: s.ConfState()}, nil
}

func TestMembershipFenceRaftTransferChecksEveryAttempt(t *testing.T) {
	for _, mode := range []string{"stale-index", "stale-term", "normal", "retry-reader-error"} {
		t.Run(mode, func(t *testing.T) {
			transport := &queuedTransport{groups: make(map[uint64]*Group), blocked: make(map[uint64]bool)}
			states := make(map[uint64]*testTransferState)
			for _, id := range []uint64{51, 52, 53} {
				root := t.TempDir()
				state := &testTransferState{testState: openTestState(t, root)}
				g, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: id, BootstrapPeers: []uint64{51, 52, 53}, State: state, Transport: transport})
				if err != nil {
					t.Fatal(err)
				}
				transport.groups[id], states[id] = g, state
				t.Cleanup(func() { _ = g.Close() })
			}
			transport.deliver(t)
			leader := transport.groups[51]
			if err := leader.Campaign(); err != nil {
				t.Fatal(err)
			}
			transport.deliver(t)
			cut, err := leader.MembershipCut()
			if err != nil {
				t.Fatal(err)
			}
			current, err := leader.Status()
			if err != nil {
				t.Fatal(err)
			}
			term := current.Term
			if mode == "stale-index" {
				cut.Index++
			}
			if mode == "stale-term" {
				term++
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if mode == "stale-index" || mode == "stale-term" {
				if _, err := leader.TransferLeadershipFenced(ctx, 52, cut, term); !errors.Is(err, ErrMembershipChanged) {
					t.Fatal(err)
				}
				current, _ := leader.Status()
				if current.LeadTransferee != 0 || current.LeaderID != 51 {
					t.Fatal("rejected transfer changed leadership", current)
				}
				return
			}
			if mode == "retry-reader-error" {
				transport.blocked[52] = true
				leader.mu.Lock()
				states[51].reads = 0
				states[51].failRetry = true
				leader.mu.Unlock()
			}
			done := make(chan error, 1)
			go func() { _, err := leader.TransferLeadershipFenced(ctx, 52, cut, term); done <- err }()
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				transport.deliver(t)
				select {
				case err := <-done:
					if mode == "normal" {
						if err != nil {
							t.Fatal(err)
						}
						current, _ := leader.Status()
						if current.LeaderID != 52 {
							t.Fatal(current)
						}
					} else if !errors.Is(err, ErrMembershipChanged) {
						t.Fatal("retry did not recheck membership", err)
					}
					return
				case <-ctx.Done():
					t.Fatal("transfer did not finish", ctx.Err())
				case <-ticker.C:
					for _, id := range []uint64{51, 53} {
						if err := transport.groups[id].Tick(); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
		})
	}
}
