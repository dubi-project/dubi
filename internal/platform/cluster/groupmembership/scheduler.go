package groupmembership

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
)

type RootLeadership interface {
	Status(groupID string) (raft.GroupStatus, error)
}

type PendingReconciler interface {
	ReconcilePending(ctx context.Context) error
}

type ErrorReporter func(error)

type Scheduler struct {
	leadership RootLeadership
	reconciler PendingReconciler
	interval   time.Duration
	report     ErrorReporter
}

// Membership reconciliation is maintenance work, not a heartbeat. Retrying
// every second across a WAN can fill the Root control queue while an existing
// election or snapshot is still converging. Durable tasks remain available for
// the next pass, so a modest default cadence gives recovery traffic priority.
const DefaultInterval = 5 * time.Second

func NewScheduler(leadership RootLeadership, reconciler PendingReconciler,
	interval time.Duration, report ErrorReporter,
) (*Scheduler, error) {
	if leadership == nil || reconciler == nil || interval < 0 {
		return nil, errors.New("membership scheduler requires leadership, reconciler, and non-negative interval")
	}
	if interval == 0 {
		interval = DefaultInterval
	}
	if report == nil {
		report = func(error) {}
	}
	return &Scheduler{leadership: leadership, reconciler: reconciler,
		interval: interval, report: report}, nil
}

// ReconcileOnce is deliberately gated by local Root leadership. Root commands
// still use CAS and every data-Group operation rechecks its actual ConfState,
// so a leadership change resumes from durable facts rather than scheduler RAM.
func (scheduler *Scheduler) ReconcileOnce(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	status, err := scheduler.leadership.Status(raft.RootGroupID)
	if err != nil {
		return false, fmt.Errorf("read Root leadership: %w", err)
	}
	if status.Role != etcdraft.StateLeader || status.LeaderID == 0 || status.LeaderID != status.RaftID {
		return false, nil
	}
	if err := scheduler.reconciler.ReconcilePending(ctx); err != nil {
		return true, err
	}
	return true, nil
}

func (scheduler *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(scheduler.interval)
	defer ticker.Stop()
	for {
		if _, err := scheduler.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
			scheduler.report(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
