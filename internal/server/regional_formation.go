package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
)

type regionalFormation struct {
	coordinator rootJoinReconciler
	confirmer   rootJoinReconciler
	grower      rootJoinReconciler
	placement   rootJoinReconciler
	report      groupmembership.ErrorReporter
}

// Formation is a control-plane reconciler. A one-second retry interval causes
// every Node to continuously enqueue Root/Group barriers while a WAN election
// or snapshot is still converging, starving the very control traffic needed to
// recover. Keep the normal retry cadence deliberately slower; an in-flight
// round is still bounded and resumes from durable intent on the next tick.
const regionalFormationInterval = 5 * time.Second

// All nodes observe formation intents independently of Root leadership. One
// in-flight round per node, bounded lifetime, joined before Store resources close.
func (r *regionalFormation) run(ctx context.Context) {
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := r.coordinator.Reconcile(attempt)
		cancel()
		if r.confirmer != nil && ctx.Err() == nil {
			attempt, cancel = context.WithTimeout(ctx, 30*time.Second)
			err = errors.Join(err, r.confirmer.Reconcile(attempt))
			cancel()
		}
		if r.grower != nil && ctx.Err() == nil {
			// Only growth can run a physical backfill. Keep formation and initial
			// confirmation on their shorter control-plane deadlines.
			attempt, cancel = context.WithTimeout(ctx, 2*time.Minute)
			err = errors.Join(err, r.grower.Reconcile(attempt))
			cancel()
		}
		if r.placement != nil && ctx.Err() == nil {
			attempt, cancel = context.WithTimeout(ctx, 30*time.Second)
			err = errors.Join(err, r.placement.Reconcile(attempt))
			cancel()
		}
		if err != nil && ctx.Err() == nil && r.report != nil {
			r.report(fmt.Errorf("regional Group reconciliation: %w", err))
		}
		timer := time.NewTimer(regionalFormationInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Local requests do not loop through network/self-target RPC. They still cross
// real Root and data Group read barriers and use the same persisted state cut.
type regionalParticipants struct {
	runtime *node.Runtime
	client  *raftrpc.GroupClient
}

func (p regionalParticipants) Observe(ctx context.Context, target uint64, group string) (regiongroups.Observation, error) {
	if target != p.runtime.Identity.RaftID {
		o, err := p.client.ObserveRegionFence(ctx, target, group)
		return regiongroups.Observation{RootConfirmedIndex: o.RootConfirmedIndex, GroupConfirmedIndex: o.GroupConfirmedIndex, View: o.View}, err
	}
	if _, err := p.runtime.ReadRootIndex(ctx); err != nil {
		return regiongroups.Observation{}, err
	}
	state, found := p.runtime.Engines.Resolve(group)
	if !found {
		return regiongroups.Observation{}, errors.New("local regional Group is not open")
	}
	index, err := p.runtime.Host.ReadIndex(ctx, group)
	if err != nil {
		return regiongroups.Observation{}, err
	}
	view, err := state.RegionFence()
	if err != nil {
		return regiongroups.Observation{}, err
	}
	rootIndex, err := p.runtime.ReadRootIndex(ctx)
	if err != nil {
		return regiongroups.Observation{}, err
	}
	if _, member, err := p.runtime.Root.RootMember(target); err != nil || !member {
		return regiongroups.Observation{}, errors.Join(err, errors.New("local observer is not a current Root member"))
	}
	return regiongroups.Observation{RootConfirmedIndex: rootIndex, GroupConfirmedIndex: index, View: view}, ctx.Err()
}

func (p regionalParticipants) Propose(ctx context.Context, target uint64, group string, command []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if target != p.runtime.Identity.RaftID {
		_, err := p.client.Submit(ctx, target, group, command)
		return err
	}
	if err := p.runtime.Host.Submit(group, command); err != nil {
		return err
	}
	_, err := p.runtime.Host.ReadIndex(ctx, group)
	return err
}
