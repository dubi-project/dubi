package server

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

// One bounded local repair page shares an admission cut across physical work
// and metadata reconciliation. Owner reads remain fresh. finish must succeed
// before the worker advances its durable cursor or clears any debt.
type repairPageCut struct {
	base  placement.ReadBarrier
	root  placement.RootReader
	index uint64
	plan  placement.LocalPlan
}

func beginRepairPage(ctx context.Context, base placement.ReadBarrier, root placement.RootReader, group string) (repairPageCut, error) {
	index, err := base.ReadIndex(ctx, raft.RootGroupID)
	if err != nil {
		return repairPageCut{}, err
	}
	plan, err := placement.ReadLocalPlanAtCut(index, root, group)
	if err != nil {
		return repairPageCut{}, err
	}
	return repairPageCut{base: base, root: root, index: index, plan: plan}, nil
}
func (p repairPageCut) ReadIndex(ctx context.Context, group string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if group == raft.RootGroupID {
		return p.index, nil
	}
	return p.base.ReadIndex(ctx, group)
}
func (p repairPageCut) finish(ctx context.Context) error {
	latest, err := placement.ReadLocalPlan(ctx, p.base, p.root, p.plan.GroupID)
	if err != nil {
		return err
	}
	if !p.plan.Matches(latest) {
		return errors.New("Region cohort changed across physical and confirmation repair page")
	}
	return ctx.Err()
}
