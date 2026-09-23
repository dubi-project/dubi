package groupmembership

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
)

type schedulerLeadership struct {
	status raft.GroupStatus
	err    error
}

func (leadership *schedulerLeadership) Status(string) (raft.GroupStatus, error) {
	return leadership.status, leadership.err
}

type schedulerReconciler struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (reconciler *schedulerReconciler) ReconcilePending(context.Context) error {
	reconciler.mu.Lock()
	defer reconciler.mu.Unlock()
	reconciler.calls++
	return reconciler.err
}

func (reconciler *schedulerReconciler) count() int {
	reconciler.mu.Lock()
	defer reconciler.mu.Unlock()
	return reconciler.calls
}

func TestSchedulerOnlyReconcilesOnLocalRootLeader(t *testing.T) {
	leadership := &schedulerLeadership{status: raft.GroupStatus{RaftID: 1, LeaderID: 2,
		Role: etcdraft.StateFollower}}
	reconciler := &schedulerReconciler{}
	scheduler, err := NewScheduler(leadership, reconciler, time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if active, err := scheduler.ReconcileOnce(context.Background()); err != nil || active || reconciler.count() != 0 {
		t.Fatalf("Follower reconciliation = active %v, calls %d, err %v", active, reconciler.count(), err)
	}
	leadership.status = raft.GroupStatus{RaftID: 1, LeaderID: 1, Role: etcdraft.StateLeader}
	if active, err := scheduler.ReconcileOnce(context.Background()); err != nil || !active || reconciler.count() != 1 {
		t.Fatalf("Leader reconciliation = active %v, calls %d, err %v", active, reconciler.count(), err)
	}
}

func TestSchedulerReportsTransientErrorsAndKeepsRunning(t *testing.T) {
	leadership := &schedulerLeadership{status: raft.GroupStatus{RaftID: 1, LeaderID: 1,
		Role: etcdraft.StateLeader}}
	reconciler := &schedulerReconciler{err: errors.New("transient")}
	reported := make(chan error, 1)
	scheduler, _ := NewScheduler(leadership, reconciler, time.Millisecond,
		func(err error) {
			select {
			case reported <- err:
			default:
			}
		})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	select {
	case err := <-reported:
		if err == nil {
			t.Fatal("scheduler reported nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not report reconciliation error")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("scheduler shutdown = %v", err)
	}
}
