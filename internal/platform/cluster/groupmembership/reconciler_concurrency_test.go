package groupmembership

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

// Model two callers observing the same old task while the first membership
// operation is in flight. A second proposal must retain the real CAS failure;
// the reconciler prevents it by reading the task after the first pass finishes.
type heldMembershipControl struct {
	*fakeHost
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (h *heldMembershipControl) ChangeMembership(ctx context.Context, mutation rootstate.GroupMutation) error {
	if h.calls.Add(1) != 1 {
		return raft.ErrMembershipChanged
	}
	close(h.entered)
	select {
	case <-h.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return h.fakeHost.ChangeMembership(ctx, mutation)
}
func heldRemoval(t *testing.T) (*Reconciler, *heldMembershipControl, string) {
	t.Helper()
	s, h := openRootForReconcile(t, []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}})
	held := &heldMembershipControl{fakeHost: h, entered: make(chan struct{}), release: make(chan struct{})}
	r, err := New(h, held, s)
	if err != nil {
		t.Fatal(err)
	}
	p := rootstate.GroupChangePlan{ChangeID: "concurrent-maintenance", GroupID: "data-1", ExpectedEpoch: 2, Maintenance: true, Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}}}
	if err := r.Plan(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	return r, held, p.ChangeID
}
func awaitReconcile(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("reconciler did not exit")
		return nil
	}
}
func TestReconcileSerializesMaintenanceWithScheduler(t *testing.T) {
	r, h, id := heldRemoval(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- r.Reconcile(ctx, id) }()
	select {
	case <-h.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	go func() { second <- r.Reconcile(ctx, id) }()
	// Holding the first mutation makes an overlapping pass observable, instead
	// of relying on the relative speed of two otherwise tiny fake operations.
	select {
	case err := <-second:
		close(h.release)
		awaitReconcile(t, first)
		t.Fatalf("second pass entered an in-flight task: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(h.release)
	if err := awaitReconcile(t, first); err != nil {
		t.Fatal(err)
	}
	if err := awaitReconcile(t, second); err != nil {
		t.Fatal("waiter did not reread completed task", err)
	}
	if h.calls.Load() != 1 || !slices.Equal(h.operations, []string{"remove:3"}) {
		t.Fatal("duplicate membership proposal", h.calls.Load(), h.operations)
	}
	task, found, err := h.root.GroupChange(id)
	if err != nil || !found || task.State != rootstate.GroupChangeCompleted || task.CompletedEpoch != 3 {
		t.Fatal(task, found, err)
	}
}
func TestReconcileWaitingCallerCanCancelWithoutReleasingActivePass(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			r, h, id := heldRemoval(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			first := make(chan error, 1)
			go func() { first <- r.Reconcile(ctx, id) }()
			select {
			case <-h.entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var waiting context.Context
			var done context.CancelFunc
			if mode == "cancel" {
				waiting, done = context.WithCancel(ctx)
			} else {
				waiting, done = context.WithTimeout(ctx, 50*time.Millisecond)
			}
			defer done()
			second := make(chan error, 1)
			go func() { second <- r.Reconcile(waiting, id) }()
			if mode == "cancel" {
				done()
			}
			err := awaitReconcile(t, second)
			if !errors.Is(err, waiting.Err()) || waiting.Err() == nil {
				t.Fatal("waiting call did not preserve cancellation", err)
			}
			if h.calls.Load() != 1 {
				t.Fatal("cancelled waiter submitted a mutation", h.calls.Load())
			}
			select {
			case err := <-first:
				t.Fatal("waiter interrupted active pass", err)
			default:
			}
			close(h.release)
			if err := awaitReconcile(t, first); err != nil {
				t.Fatal(err)
			}
			if err := r.Reconcile(ctx, id); err != nil {
				t.Fatal("cancelled waiter leaked admission", err)
			}
			if h.calls.Load() != 1 {
				t.Fatal("completed task repeated a proposal")
			}
		})
	}
}
