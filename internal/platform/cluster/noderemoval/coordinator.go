package noderemoval

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

var (
	ErrGroupStillPresent      = errors.New("Node still belongs to a data Group after reconciliation")
	ErrRootMemberStillPresent = errors.New("Node is still a Root member after removal")
	ErrGroupUnavailable       = errors.New("Node removal target Group has no available quorum")
)

// Executor adapts the existing Group membership reconciler and the local Root
// maintenance control. Implementations must return only after the requested
// durable operation is observed in their local Root state.
type Executor interface {
	ConfirmRoot(context.Context) error
	CheckGroup(context.Context, string) error
	PlanGroup(context.Context, rootstate.GroupChangePlan) error
	ReconcileGroup(context.Context, string) error
	RetireRegionGroup(context.Context, string) error
	RemoveRootMember(context.Context, uint64) error
	FenceNode(context.Context, rootstate.NodeFenceRequest) error
}

type Coordinator struct {
	root     *rootstate.State
	executor Executor
}

func NewCoordinator(root *rootstate.State, executor Executor) (*Coordinator, error) {
	if root == nil || executor == nil {
		return nil, errors.New("Node removal coordinator requires Root state and an executor")
	}
	return &Coordinator{root: root, executor: executor}, nil
}

// Reconcile performs one restart-safe pass. GroupChange records are the only
// durable progress: the coordinator itself has no replacement or workflow
// state. Calling it again after a crash rebuilds the same plans and resumes at
// the first unfinished membership change.
func (c *Coordinator) Reconcile(ctx context.Context, raftID uint64) (Plan, error) {
	if ctx == nil {
		return Plan{}, errors.New("Node removal context is required")
	}
	if err := ctx.Err(); err != nil {
		return Plan{}, err
	}
	if err := c.executor.ConfirmRoot(ctx); err != nil {
		return Plan{}, err
	}
	plan, err := Build(c.root, raftID)
	if err != nil {
		return Plan{}, err
	}
	for _, group := range plan.Groups {
		if err := c.executor.CheckGroup(ctx, group.GroupID); err != nil {
			if !groupUnavailable(err) {
				return plan, err
			}
			// This is still an explicit operator request. A Group that cannot
			// answer a membership read is retired in Root first; no timeout or
			// health observer may take this branch on its own.
			if err := c.executor.RetireRegionGroup(ctx, group.GroupID); err != nil {
				return plan, err
			}
			continue
		}
		if err := c.executor.PlanGroup(ctx, group.Plan); err != nil {
			return plan, err
		}
		if err := c.executor.ReconcileGroup(ctx, group.Plan.ChangeID); err != nil {
			return plan, err
		}
	}
	if len(plan.Groups) != 0 {
		if err := c.executor.ConfirmRoot(ctx); err != nil {
			return plan, err
		}
		plan, err = Build(c.root, raftID)
		if err != nil {
			return plan, err
		}
		if len(plan.Groups) != 0 {
			return plan, ErrGroupStillPresent
		}
	}
	if plan.RootMember {
		if err := c.executor.RemoveRootMember(ctx, raftID); err != nil {
			return plan, err
		}
		if err := c.executor.ConfirmRoot(ctx); err != nil {
			return plan, err
		}
		plan, err = Build(c.root, raftID)
		if err != nil {
			return plan, err
		}
		if plan.RootMember {
			return plan, ErrRootMemberStillPresent
		}
	}
	if !plan.AlreadyFenced {
		if err := c.executor.FenceNode(ctx, rootstate.NodeFenceRequest{NodeID: plan.Node.NodeID, RaftID: plan.Node.RaftID}); err != nil {
			return plan, err
		}
		if err := c.executor.ConfirmRoot(ctx); err != nil {
			return plan, err
		}
		plan, err = Build(c.root, raftID)
		if err != nil {
			return plan, err
		}
		if !plan.AlreadyFenced || plan.RootMember || len(plan.Groups) != 0 {
			return plan, errors.New("Node removal Fence was not durably confirmed")
		}
	}
	return plan, nil
}

func groupUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return errors.Is(err, ErrGroupUnavailable)
}
