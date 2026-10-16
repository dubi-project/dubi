package recordwrite

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

type distinctGeoObserver struct{ root *testRoot }

func (o distinctGeoObserver) ObserveRecordHolding(_ context.Context, id uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	for _, node := range o.root.members {
		if node.RaftID != id {
			continue
		}
		holding := testHolding(d)
		holding.StoreID = fmt.Sprintf("store-remote-%d", id)
		return raftrpc.HoldingObservation{RaftID: id, NodeID: node.NodeID, RootConfirmedIndex: 5, Holding: holding}, nil
	}
	return raftrpc.HoldingObservation{}, fmt.Errorf("unknown Node %d", id)
}

func TestObserveGeoFindsLocalNodeBeforeStoppingAtRegionMajority(t *testing.T) {
	plan, _, descriptor := geoThreeRegionPlan(t)
	root, _, _ := stageFixture(t)
	reader := Reader{LocalID: 1, LocalNodeID: root.members[0].NodeID,
		Observer: distinctGeoObserver{root: root}}

	evidence, nodes, err := reader.observeGeo(context.Background(), plan, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 2 || len(nodes) != 3 {
		t.Fatalf("geo observation = evidence %d, nodes %d", len(evidence), len(nodes))
	}
	covered := []string{evidence[0].RegionID, evidence[1].RegionID}
	if !slices.Equal(covered, []string{"east", "north"}) {
		t.Fatalf("unexpected covered Regions = %v", covered)
	}
}
