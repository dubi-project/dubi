package server

import (
	"context"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
)

type rootJoinReconciler interface{ Reconcile(context.Context) error }

// Root admission and role selection share a serial lane. Raft's ConfState CAS
// still fences concurrent RPC admission; data Group work remains independent.
type rootControlTasks struct {
	join      rootJoinReconciler
	topology  rootJoinReconciler
	authority rootJoinReconciler
}

func (tasks rootControlTasks) Reconcile(ctx context.Context) error {
	joinErr := tasks.join.Reconcile(ctx)
	// Rotate the enrollment signer before changing Root voters.  Topology may
	// add a voter in a staging cut, allowing this pass to prepare its signer;
	// a later pass can then safely remove the old authority member.
	result := joinErr
	if tasks.authority != nil {
		result = errors.Join(result, tasks.authority.Reconcile(ctx))
	}
	if tasks.topology != nil {
		result = errors.Join(result, tasks.topology.Reconcile(ctx))
	}
	return result
}

// membershipTasks runs independent Root join and data-Group control work in
// parallel, so a waiting data-Group does not prevent Root admission recovery.
// The scheduler still gates each round on actual local Root leadership, and
// each reconciler rechecks its own durable authority. Deadlines are cooperative;
// they cannot interrupt an unresponsive filesystem syscall.
type membershipTasks struct {
	root    rootJoinReconciler
	data    groupmembership.PendingReconciler
	timeout time.Duration
}

func (tasks membershipTasks) ReconcilePending(ctx context.Context) error {
	limit := tasks.timeout
	if limit == 0 {
		limit = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	results := make(chan error, 2)
	go func() { results <- tasks.root.Reconcile(ctx) }()
	go func() { results <- tasks.data.ReconcilePending(ctx) }()
	// Always join both workers; no detached work survives a scheduler round.
	return errors.Join(<-results, <-results)
}
