package raftrpc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type actionJoiner func(context.Context, rootstate.GroupTaskReference, uint64) error

func (f actionJoiner) PrepareJoining(ctx context.Context, task rootstate.GroupTaskReference, target uint64) error {
	return f(ctx, task, target)
}

func completeActionTask(t *testing.T, b *guardedMembershipBackend, id string) {
	t.Helper()
	index := b.root.AppliedIndex()
	r := applyRootCommand(t, b.root, &index, rootstate.Command{Version: 1, RequestID: "complete-" + id, Type: rootstate.CompleteGroupMembers, GroupCompletion: &rootstate.GroupChangeCompletion{GroupID: "data-1", ChangeID: id}})
	if r.Error != "" {
		t.Fatal(r)
	}
}

func transferActionFixture(t *testing.T) (*MembershipServer, *guardedMembershipBackend, context.Context, *raftwire.GroupLeadershipTransferRequest) {
	t.Helper()
	s, b, ctx, mutation := membershipFenceFixture(t)
	completeActionTask(t, b, "add-node")
	index := b.root.AppliedIndex()
	r := applyRootCommand(t, b.root, &index, rootstate.Command{Version: 1, RequestID: "retire", Type: rootstate.PlanGroupMembers, GroupPlan: &rootstate.GroupChangePlan{ChangeID: "retire", GroupID: "data-1", ExpectedEpoch: 3, Members: []rootstate.GroupMember{{RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}})
	if r.Error != "" {
		t.Fatal(r)
	}
	return s, b, ctx, &raftwire.GroupLeadershipTransferRequest{ClusterId: "cluster", GroupId: "data-1", FromRaftId: 991, ToRaftId: 1, TargetRaftId: 2, ChangeId: "retire", ExpectedEpoch: 3, ExpectedMembershipIndex: 1, ExpectedConfState: mutation.ExpectedConfState, ExpectedLeaderTerm: 1}
}

func TestMembershipFencePreparationRechecksOriginalTask(t *testing.T) {
	for _, mode := range []string{"normal", "old-request", "wrong-task", "wrong-epoch", "completed", "completed-in-factory", "removed-after-wait", "removed-in-factory", "expired-in-factory", "oversized", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			s, b, ctx, _ := membershipFenceFixture(t)
			r := &raftwire.PrepareJoiningGroupRequest{ClusterId: "cluster", GroupId: "data-1", FromRaftId: 991, ToRaftId: 1, ChangeId: "add-node", ExpectedEpoch: 2}
			calls := 0
			expired := false
			s.joiner = actionJoiner(func(_ context.Context, task rootstate.GroupTaskReference, target uint64) error {
				calls++
				if task != (rootstate.GroupTaskReference{GroupID: "data-1", ChangeID: "add-node", ExpectedEpoch: 2}) || target != 1 {
					t.Error("original task/target lost")
				}
				if mode == "completed-in-factory" {
					completeActionTask(t, b, "add-node")
				}
				if mode == "removed-in-factory" {
					removeMutationCaller(t, b.root)
				}
				if mode == "expired-in-factory" {
					expired = true
				}
				return nil
			})
			want := codes.PermissionDenied
			switch mode {
			case "normal":
				want = codes.OK
			case "old-request":
				r.ChangeId = ""
				want = codes.InvalidArgument
			case "wrong-task":
				r.ChangeId = "old"
			case "wrong-epoch":
				r.ExpectedEpoch++
			case "completed":
				completeActionTask(t, b, "add-node")
			case "removed-after-wait":
				b.onRead = func(context.Context, string) error { removeMutationCaller(t, b.root); return nil }
			case "expired-in-factory":
				s.authorize = func(context.Context, uint64) error {
					if expired {
						return errors.New("expired")
					}
					return nil
				}
			case "oversized":
				r.ChangeId = strings.Repeat("x", 4096)
				want = codes.ResourceExhausted
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = codes.Canceled
			}
			response, err := s.PrepareJoining(ctx, r)
			if status.Code(err) != want || (response != nil) != (want == codes.OK) {
				t.Fatal(response, err, want)
			}
			if response != nil && (response.ClusterId != r.ClusterId || response.GroupId != r.GroupId || response.RaftId != r.ToRaftId || response.ChangeId != r.ChangeId || response.ExpectedEpoch != r.ExpectedEpoch) {
				t.Fatal("misbound response", response)
			}
			invoked := mode == "normal" || strings.HasSuffix(mode, "-in-factory")
			if (calls == 1) != invoked || len(s.changes) != 0 {
				t.Fatal("unexpected factory/admission", calls, len(s.changes))
			}
		})
	}
}

func TestMembershipFenceTransferRequiresTaskCutAndTerm(t *testing.T) {
	for _, mode := range []string{"normal", "old-request", "wrong-task", "wrong-epoch", "completed", "removed-after-wait", "stale-index", "stale-term", "completed-after-transfer", "removed-after-transfer", "oversized", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			s, b, ctx, r := transferActionFixture(t)
			want := codes.PermissionDenied
			switch mode {
			case "normal":
				want = codes.OK
			case "old-request":
				r.ExpectedLeaderTerm = 0
				want = codes.InvalidArgument
			case "wrong-task":
				r.ChangeId = "add-node"
			case "wrong-epoch":
				r.ExpectedEpoch--
			case "completed":
				completeActionTask(t, b, "retire")
			case "removed-after-wait":
				b.onRead = func(context.Context, string) error { removeMutationCaller(t, b.root); return nil }
			case "stale-index":
				r.ExpectedMembershipIndex++
				want = codes.FailedPrecondition
			case "stale-term":
				r.ExpectedLeaderTerm++
				want = codes.FailedPrecondition
			case "completed-after-transfer", "removed-after-transfer":
				b.onRead = func(context.Context, string) error {
					b.shared.mu.Lock()
					transferred := b.shared.leader == 2
					b.shared.mu.Unlock()
					if transferred {
						if mode == "completed-after-transfer" {
							completeActionTask(t, b, "retire")
						} else {
							removeMutationCaller(t, b.root)
						}
					}
					return nil
				}
			case "oversized":
				r.ChangeId = strings.Repeat("x", membershipMessageBytes)
				want = codes.ResourceExhausted
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = codes.Canceled
			}
			response, err := s.TransferLeadership(ctx, r)
			if status.Code(err) != want || (response != nil) != (want == codes.OK) {
				t.Fatal(response, err, want)
			}
			if response != nil && (response.ClusterId != r.ClusterId || response.GroupId != r.GroupId || response.RaftId != r.ToRaftId || response.ChangeId != r.ChangeId || response.ExpectedEpoch != r.ExpectedEpoch || response.LeaderRaftId != r.TargetRaftId) {
				t.Fatal("misbound response", response)
			}
			b.shared.mu.Lock()
			transferred := b.shared.leader == 2
			b.shared.mu.Unlock()
			if transferred != (mode == "normal" || strings.HasSuffix(mode, "-after-transfer")) || len(s.changes) != 0 {
				t.Fatal("unexpected transfer/admission", transferred, len(s.changes))
			}
		})
	}
}

func TestMembershipFenceActionsShareAdmissionAndCancel(t *testing.T) {
	s, b, ctx, transfer := transferActionFixture(t)
	prepare := &raftwire.PrepareJoiningGroupRequest{ClusterId: "cluster", GroupId: "data-1", FromRaftId: 991, ToRaftId: 1, ChangeId: "retire", ExpectedEpoch: 3}
	entered := make(chan struct{}, 8)
	b.onRead = func(ctx context.Context, _ string) error { entered <- struct{}{}; <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 8)
	for i := range 8 {
		go func() {
			var err error
			if i%2 == 0 {
				_, err = s.PrepareJoining(ctx, prepare)
			} else {
				_, err = s.TransferLeadership(ctx, transfer)
			}
			done <- err
		}()
	}
	for range 8 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("action never entered")
		}
	}
	if _, err := s.Change(ctx, &raftwire.GroupMembershipChangeRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	cancel()
	for range 8 {
		select {
		case err := <-done:
			if status.Code(err) != codes.Canceled {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("action failed to cancel")
		}
	}
	if len(s.changes) != 0 {
		t.Fatal("admission slots leaked")
	}
}

func TestMembershipFenceLocalActionsRecheckAfterWait(t *testing.T) {
	for _, action := range []string{"prepare", "transfer"} {
		for _, mode := range []string{"normal", "completed", "removed"} {
			t.Run(action+"/"+mode, func(t *testing.T) {
				s, b, _, _ := membershipFenceFixture(t)
				task := rootstate.GroupTaskReference{GroupID: "data-1", ChangeID: "add-node", ExpectedEpoch: 2}
				invalidate := func() {
					if mode == "completed" {
						completeActionTask(t, b, task.ChangeID)
					}
					if mode == "removed" {
						if err := b.root.ApplyMembership(b.root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{2, 3}, Learners: []uint64{991}}); err != nil {
							t.Fatal(err)
						}
					}
				}
				if action == "transfer" {
					s, b, _, _ = transferActionFixture(t)
					task.ChangeID, task.ExpectedEpoch = "retire", 3
					b.onRead = func(_ context.Context, group string) error {
						if group == "data-1" {
							invalidate()
						}
						return nil
					}
				} else {
					s.joiner = actionJoiner(func(context.Context, rootstate.GroupTaskReference, uint64) error { invalidate(); return nil })
				}
				control, err := groupmembership.NewLocalControl(b, s.joiner, b.root, 1)
				if err != nil {
					t.Fatal(err)
				}
				if action == "prepare" {
					err = control.PrepareJoining(context.Background(), task, 1)
				} else {
					_, err = control.TransferLeadership(context.Background(), task, 2)
				}
				if (err == nil) != (mode == "normal") {
					t.Fatal(err)
				}
				if action == "transfer" {
					current, _ := b.Status("data-1")
					if (current.LeaderID == 2) != (mode == "normal") {
						t.Fatal("invalidated local action transferred leadership")
					}
				}
			})
		}
	}
}

var _ groupmembership.JoiningControl = actionJoiner(nil)

func TestMembershipFenceActionResponsesRejectOldOrMisboundAcknowledgments(t *testing.T) {
	task := rootstate.GroupTaskReference{GroupID: "data-1", ChangeID: "change", ExpectedEpoch: 2}
	for _, mode := range []string{"normal", "nil", "old", "cluster", "group", "server", "task", "epoch", "leader"} {
		t.Run(mode, func(t *testing.T) {
			p := &raftwire.PrepareJoiningGroupResponse{ClusterId: "cluster", GroupId: task.GroupID, RaftId: 1, ChangeId: task.ChangeID, ExpectedEpoch: task.ExpectedEpoch}
			r := &raftwire.GroupLeadershipTransferResponse{ClusterId: "cluster", GroupId: task.GroupID, RaftId: 1, ChangeId: task.ChangeID, ExpectedEpoch: task.ExpectedEpoch, LeaderRaftId: 2}
			switch mode {
			case "nil":
				p, r = nil, nil
			case "old":
				p = &raftwire.PrepareJoiningGroupResponse{}
				r = &raftwire.GroupLeadershipTransferResponse{LeaderRaftId: 2}
			case "cluster":
				p.ClusterId, r.ClusterId = "other", "other"
			case "group":
				p.GroupId, r.GroupId = "other", "other"
			case "server":
				p.RaftId, r.RaftId = 3, 3
			case "task":
				p.ChangeId, r.ChangeId = "other", "other"
			case "epoch":
				p.ExpectedEpoch++
				r.ExpectedEpoch++
			case "leader":
				r.LeaderRaftId = 3
			}
			if err := validatePreparationResponse("cluster", 1, task, p); (err == nil) != (mode == "normal" || mode == "leader") {
				t.Fatal(err)
			}
			if err := validateTransferResponse("cluster", 1, task, 2, r); (err == nil) != (mode == "normal") {
				t.Fatal(err)
			}
		})
	}
}
