package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

type geoObserverTestRoot struct {
	node    rootstate.Node
	applied uint64
	member  bool
}

func (r *geoObserverTestRoot) AppliedIndex() uint64 { return r.applied }
func (r *geoObserverTestRoot) RootMember(id uint64) (rootstate.Node, bool, error) {
	return r.node, r.member && id == r.node.RaftID, nil
}

type geoObserverTestStore struct {
	verify func(context.Context, records.Descriptor) (records.Holding, error)
}

func (s geoObserverTestStore) Verify(ctx context.Context, d records.Descriptor) (records.Holding, error) {
	return s.verify(ctx, d)
}

func TestGeoRepairLocalObservationRequiresPhysicalBytesAndCurrentIdentity(t *testing.T) {
	for _, mode := range []string{"valid", "no-cut", "unapplied-cut", "wrong-identity", "fenced", "missing", "corrupt", "fenced-during-verify", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			local := identity.Identity{RaftID: 1, NodeID: fmt.Sprintf("node_%032x", 1)}
			node := rootstate.Node{RaftID: 1, NodeID: local.NodeID, RegionID: "west"}
			root := &geoObserverTestRoot{node: node, applied: 9, member: true}
			ctx := router.WithConfirmedRootIndex(context.Background(), 9)
			calls := 0
			d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "record", DatasetID: "items"}, "region-west", []byte("key"), []byte("body"))
			if err != nil {
				t.Fatal(err)
			}
			id, _ := d.ID()
			store := geoObserverTestStore{verify: func(context.Context, records.Descriptor) (records.Holding, error) {
				calls++
				if mode == "missing" {
					return records.Holding{}, records.ErrNotHeld
				}
				if mode == "corrupt" {
					return records.Holding{}, records.ErrCorrupt
				}
				if mode == "fenced-during-verify" {
					root.member = false
				}
				return records.Holding{RecordID: id, Descriptor: d, StoreID: "store-local", VolumeID: "volume-local"}, nil
			}}
			switch mode {
			case "no-cut":
				ctx = context.Background()
			case "unapplied-cut":
				root.applied = 8
			case "wrong-identity":
				node.NodeID = "another-node"
			case "fenced":
				root.member = false
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			observe := geoRepairObserver(local, root, store, func(context.Context, rootstate.Node, records.Descriptor) (raftrpc.HoldingObservation, error) {
				t.Fatal("self observation was sent to remote RPC")
				return raftrpc.HoldingObservation{}, nil
			})
			observation, err := observe(ctx, node, d)
			if mode == "valid" {
				if err != nil || calls != 1 || observation.Holding.RecordID != id || observation.NodeID != local.NodeID || observation.RootConfirmedIndex != 9 {
					t.Fatal(observation, calls, err)
				}
			} else if err == nil || observation.RootConfirmedIndex != 0 {
				t.Fatal("unverified or fenced bytes counted", observation, err)
			}
		})
	}
}

type geoObserverRepairTransport struct {
	held   map[uint64]bool
	full   map[uint64]bool
	writes []uint64
}

func (t *geoObserverRepairTransport) RepairHasCapacity(_ context.Context, n rootstate.Node, _ uint64) (bool, error) {
	return !t.full[n.RaftID], nil
}
func (t *geoObserverRepairTransport) RepairRecordOnNode(_ context.Context, target, source rootstate.Node, _ string, d records.Descriptor) (records.Holding, error) {
	if t.full[target.RaftID] || !t.held[source.RaftID] {
		return records.Holding{}, errors.New("no capacity or no source bytes")
	}
	t.held[target.RaftID] = true
	t.writes = append(t.writes, target.RaftID)
	id, _ := d.ID()
	return records.Holding{RecordID: id, Descriptor: d, StoreID: fmt.Sprint("store-", target.RaftID), VolumeID: fmt.Sprint("volume-", target.RaftID)}, nil
}
func (t *geoObserverRepairTransport) FetchRecordOnNode(ctx context.Context, target, source rootstate.Node, group string, d records.Descriptor) (records.Holding, error) {
	return t.RepairRecordOnNode(ctx, target, source, group, d)
}

// The real RPC rejects self. With only local+one peer holding bytes and the
// third local member full, that omission used to repair the already-held local
// copy forever instead of restoring a second Region. The complete retry path
// must retain debt unless fresh physical majorities exist in two of three Regions.
func TestGeoRepairCountsLocalHolderWithFullThirdNode(t *testing.T) {
	for _, remoteFull := range []bool{false, true} {
		t.Run(fmt.Sprint(remoteFull), func(t *testing.T) {
			view := rootstate.RegionPlacementView{AppliedIndex: 9}
			var localNode rootstate.Node
			for regionIndex, region := range []string{"west", "east", "north"} {
				var nodes []rootstate.Node
				var ids []uint64
				var members []rootstate.GroupMember
				for i := 1; i <= 3; i++ {
					id := uint64(regionIndex*3 + i)
					n := rootstate.Node{RaftID: id, NodeID: fmt.Sprintf("node_%032x", id), RegionID: region, Address: fmt.Sprint("node-", id)}
					if id == 1 {
						localNode = n
					}
					nodes = append(nodes, n)
					ids = append(ids, id)
					members = append(members, rootstate.GroupMember{RaftID: id, Role: rootstate.GroupMemberVoter})
				}
				group := "region-" + region
				progress := rootstate.RegionProgress{AppliedIndex: 9, Eligible: true, Head: rootstate.RegionCohortHead{RegionID: region, CurrentGeneration: 1}, Current: rootstate.RegionCohort{RegionID: region, GroupID: group, Generation: 1, Nodes: ids, State: rootstate.RegionCohortConfirmed, PlannedIndex: 1, ConfirmedIndex: 5, GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 4}, Group: rootstate.Group{GroupID: group, Epoch: 2, Members: members}}
				view.Regions = append(view.Regions, rootstate.RegionPlacement{Progress: progress, Nodes: nodes})
			}
			plan, err := placement.PlanGeo(view)
			if err != nil {
				t.Fatal(err)
			}
			d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "record", DatasetID: "items"}, "region-west", []byte("key"), []byte("body"))
			if err != nil {
				t.Fatal(err)
			}
			id, _ := d.ID()
			ref := enginestate.RecordReference{Version: 2, Descriptor: d, OwnerGroupID: d.GroupID, RangeID: "range-test", RangeEpoch: 1, RoutingKey: d.Key, RequestID: "publication", Index: 7, CommandHash: strings.Repeat("0", 64)}
			ledger, err := recordrepair.OpenGeoRecordRepairStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer ledger.Close()
			if err = ledger.Save(ref); err != nil {
				t.Fatal(err)
			}
			transport := &geoObserverRepairTransport{held: map[uint64]bool{1: true, 2: true}, full: map[uint64]bool{3: true}}
			if remoteFull {
				for id := uint64(4); id <= 9; id++ {
					transport.full[id] = true
				}
			}
			root := &geoObserverTestRoot{node: localNode, applied: 9, member: true}
			local := identity.Identity{RaftID: 1, NodeID: localNode.NodeID}
			selfRPCs := 0
			remote := func(_ context.Context, n rootstate.Node, d records.Descriptor) (raftrpc.HoldingObservation, error) {
				if n.RaftID == 1 {
					selfRPCs++
					return raftrpc.HoldingObservation{}, errors.New("invalid record observation target")
				}
				if !transport.held[n.RaftID] {
					return raftrpc.HoldingObservation{}, records.ErrNotHeld
				}
				return raftrpc.HoldingObservation{RaftID: n.RaftID, NodeID: n.NodeID, RootConfirmedIndex: 9, Holding: records.Holding{RecordID: id, Descriptor: d, StoreID: fmt.Sprint("store-", n.RaftID), VolumeID: fmt.Sprint("volume-", n.RaftID)}}, nil
			}
			observer := geoRepairObserver(local, root, geoObserverTestStore{verify: func(context.Context, records.Descriptor) (records.Holding, error) {
				return records.Holding{RecordID: id, Descriptor: d, StoreID: "store-1", VolumeID: "volume-1"}, nil
			}}, remote)
			report, err := recordrepair.RetryPendingGeoRecordsWithLocal(context.Background(), ledger, func(context.Context, enginestate.RecordReference) (placement.GeoPlan, string, uint64, error) {
				return plan, "west", 9, nil
			}, observer, transport, 1, func(context.Context, rootstate.Node, string, records.Descriptor) (records.Holding, error) {
				t.Fatal("already-held local bytes selected for repair")
				return records.Holding{}, nil
			})
			count, _ := ledger.Count()
			if selfRPCs != 0 {
				t.Fatal("self was routed to RPC", selfRPCs)
			}
			if remoteFull {
				if err == nil || report.Completed != 0 || count != 1 || len(transport.writes) != 0 {
					t.Fatal("full destinations weakened durability", report, count, err)
				}
			} else if err != nil || report.Completed != 1 || report.Pending != 0 || count != 0 || !transport.held[4] || !transport.held[5] || transport.held[3] {
				t.Fatal("remote majority not restored", report, count, transport.writes, err)
			}
		})
	}
}
