package placement

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

type RangePlacementReader interface {
	RangeRegionPlacementView(string, int, int) (rootstate.RangePlacement, rootstate.RegionPlacementView, error)
}

// ReadRangeGeoPlan derives a GeoPlan from the durable Range placement mapping
// after one Root read barrier. It is the bridge between routing metadata and
// the explicit WriteGeo path; callers must still re-read/refresh the plan when
// a write spans a long physical staging window.
func ReadRangeGeoPlan(ctx context.Context, barrier ReadBarrier, root RangePlacementReader,
	rangeID string, maxRegions, maxNodes int) (rootstate.RangePlacement, GeoPlan, error) {
	if ctx == nil || barrier == nil || root == nil || rootstate.ValidateRangeID(rangeID) != nil {
		return rootstate.RangePlacement{}, GeoPlan{}, errors.New("Range geo placement requires Root, barrier and valid Range")
	}
	index, err := barrier.ReadIndex(ctx, raft.RootGroupID)
	if err != nil {
		return rootstate.RangePlacement{}, GeoPlan{}, err
	}
	if index == 0 {
		return rootstate.RangePlacement{}, GeoPlan{}, errors.New("Root read barrier returned no confirmed index")
	}
	return ReadRangeGeoPlanAtCut(index, root, rangeID, maxRegions, maxNodes)
}

// ReadRangeGeoPlanAtCut derives a plan from Root state known to have applied
// at least one previously confirmed Root read cut. It lets bounded scans reuse
// one linearizable barrier across several records without weakening the
// applied-state check.
func ReadRangeGeoPlanAtCut(index uint64, root RangePlacementReader,
	rangeID string, maxRegions, maxNodes int,
) (rootstate.RangePlacement, GeoPlan, error) {
	if index == 0 || root == nil || rootstate.ValidateRangeID(rangeID) != nil {
		return rootstate.RangePlacement{}, GeoPlan{}, errors.New("Range geo placement requires a confirmed Root cut and valid Range")
	}
	placement, view, err := root.RangeRegionPlacementView(rangeID, maxRegions, maxNodes)
	if err != nil {
		return rootstate.RangePlacement{}, GeoPlan{}, err
	}
	if view.AppliedIndex < index {
		return rootstate.RangePlacement{}, GeoPlan{}, errors.New("Range geo placement is not available at Root read cut")
	}
	plan, err := PlanGeo(view)
	if err != nil {
		return rootstate.RangePlacement{}, GeoPlan{}, err
	}
	return placement, plan, nil
}
