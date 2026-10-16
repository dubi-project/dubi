package recordwrite

import (
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
)

func TestGeoConfirmationClaimBindsAndSortsCoveredEvidence(t *testing.T) {
	target := router.Target{Keyspace: rootstate.Keyspace{TenantID: "tenant-a", EngineID: "record", DatasetID: "items"}, RangeID: "range-a", OwnerGroupID: "data-1", Epoch: 3, RoutingKey: []byte("key")}
	recordID := "record_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	coverage := placement.GeoDataCoverage{RecordID: recordID, ReferenceRequestID: "publish-1", ReferenceIndex: 8,
		ReferenceCommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:              "range-a", RangeEpoch: 3, RoutingKey: []byte("key"), RequiredRegions: 2,
		CoveredRegions: []string{"east", "west"}}
	evidence := []placement.RegionHoldings{
		{RegionID: "west", Generation: 1, GroupEpoch: 2, FenceIndex: 4, Nodes: []uint64{1, 2, 3}, Holdings: []placement.Holding{
			{RaftID: 1, NodeID: "node_00000000000000000000000000000001", StoreID: "store_00000000000000000000000000000001", RecordID: recordID},
			{RaftID: 2, NodeID: "node_00000000000000000000000000000002", StoreID: "store_00000000000000000000000000000002", RecordID: recordID},
		}},
		{RegionID: "east", Generation: 1, GroupEpoch: 2, FenceIndex: 5, Nodes: []uint64{4, 5, 6}, Holdings: []placement.Holding{
			{RaftID: 4, NodeID: "node_00000000000000000000000000000004", StoreID: "store_00000000000000000000000000000004", RecordID: recordID},
			{RaftID: 5, NodeID: "node_00000000000000000000000000000005", StoreID: "store_00000000000000000000000000000005", RecordID: recordID},
		}},
	}
	claim, err := GeoConfirmationClaim(target, []byte("item"), coverage, evidence)
	if err != nil || len(claim.Regions) != 2 || claim.Regions[0].RegionID != "east" || claim.Regions[1].RegionID != "west" {
		t.Fatal(claim, err)
	}
	if claim.ReferenceCommandHash != coverage.ReferenceCommandHash || claim.RecordID != recordID || claim.Validate() != nil {
		t.Fatal("claim lost publication binding", claim)
	}
}
