package placement

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

type ReadBarrier interface {
	ReadIndex(context.Context, string) (uint64, error)
}

type RootReader interface {
	RegionProgress(string) (rootstate.RegionProgress, bool, error)
	RootMembers(...uint64) ([]rootstate.Node, bool, error)
}

// ReadLocalPlan fixes one Root quorum cut before deriving the exact local
// physical denominator. It never filters the cohort by online health.
func ReadLocalPlan(ctx context.Context, barrier ReadBarrier, root RootReader, group string) (LocalPlan, error) {
	if ctx == nil || barrier == nil || root == nil {
		return LocalPlan{}, errors.New("local placement requires Root, barrier and context")
	}
	index, err := barrier.ReadIndex(ctx, raft.RootGroupID)
	if err != nil {
		return LocalPlan{}, err
	}
	if index == 0 {
		return LocalPlan{}, errors.New("Root read barrier returned no confirmed index")
	}
	view, found, err := root.RegionProgress(group)
	if err != nil || !found || view.AppliedIndex < index {
		return LocalPlan{}, errors.Join(err, errors.New("confirmed Region progress is not available at Root read cut"))
	}
	members, all, err := root.RootMembers(view.Current.Nodes...)
	if err != nil || !all {
		return LocalPlan{}, errors.Join(err, errors.New("confirmed Region members are not available at Root read cut"))
	}
	return PlanLocal(view, members)
}
