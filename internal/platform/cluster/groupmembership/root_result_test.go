package groupmembership

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

type delayedRootCommand struct {
	host        *fakeHost
	command     []byte
	submissions int
	reads       int
	applyAfter  int
}

func (d *delayedRootCommand) Submit(group string, command []byte) error {
	if group != raft.RootGroupID {
		return errors.New("unexpected Group")
	}
	d.submissions++
	d.command = append([]byte(nil), command...)
	return nil
}
func (d *delayedRootCommand) ReadIndex(ctx context.Context, group string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	d.reads++
	if d.applyAfter > 0 && d.reads == d.applyAfter {
		if err := d.host.Submit(group, d.command); err != nil {
			return 0, err
		}
	}
	return d.host.rootIndex, nil
}
func delayedPlan(t *testing.T) (*Reconciler, *delayedRootCommand, rootstate.GroupChangePlan) {
	t.Helper()
	s, h := openRootForReconcile(t, []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}})
	d := &delayedRootCommand{host: h}
	r, err := New(d, h, s)
	if err != nil {
		t.Fatal(err)
	}
	p := rootstate.GroupChangePlan{ChangeID: "delayed-removal", GroupID: "data-1", ExpectedEpoch: 2, Maintenance: true, Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}}}
	return r, d, p
}
func TestRootMembershipCommandWaitsForItsDurableResult(t *testing.T) {
	for _, phase := range []string{"plan", "complete", "rejected"} {
		t.Run(phase, func(t *testing.T) {
			r, d, p := delayedPlan(t)
			command := rootstate.Command{Version: rootstate.FormatVersion, RequestID: "delayed-command", Type: rootstate.PlanGroupMembers, GroupPlan: &p}
			if phase == "complete" {
				submitFakeRoot(t, d.host, command)
				command = rootstate.Command{Version: rootstate.FormatVersion, RequestID: "delayed-completion", Type: rootstate.CompleteGroupMembers, GroupCompletion: &rootstate.GroupChangeCompletion{GroupID: p.GroupID, ChangeID: p.ChangeID}}
			}
			if phase == "rejected" {
				p.ExpectedEpoch = 99
			}
			d.applyAfter = 3
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := r.submitRoot(ctx, command)
			if (err != nil) != (phase == "rejected") {
				t.Fatalf("durable %s result: %v", phase, err)
			}
			if d.submissions != 1 || d.reads != 3 {
				t.Fatal("mutation replayed or first read cut treated as completion", d.submissions, d.reads)
			}
			encoded, _ := rootstate.EncodeCommand(command)
			result, found, e := d.host.root.ResultForCommand(encoded)
			if e != nil || !found || (result.Error != "") != (phase == "rejected") {
				t.Fatal(result, found, e)
			}
		})
	}
}
func TestRootMembershipCommandWaitHonorsCancellationWithoutReplay(t *testing.T) {
	for _, mode := range []string{"before", "after-submit"} {
		t.Run(mode, func(t *testing.T) {
			r, d, p := delayedPlan(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if mode == "before" {
				cancel()
			}
			err := r.Plan(ctx, p)
			if !errors.Is(err, ctx.Err()) || ctx.Err() == nil {
				t.Fatal("lost caller cancellation", err)
			}
			expected := 1
			if mode == "before" {
				expected = 0
			}
			if d.submissions != expected {
				t.Fatal("unexpected proposal count", d.submissions)
			}
			if mode == "after-submit" {
				// The timed-out proposal can commit later. A fresh observer accepts only
				// that exact durable result; the timeout was not a rollback.
				if err := d.host.Submit(raft.RootGroupID, d.command); err != nil {
					t.Fatal(err)
				}
				if _, found, err := d.host.root.GroupChange(p.ChangeID); err != nil || !found {
					t.Fatal(found, err)
				}
			}
		})
	}
}
