package recordwrite

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type reconfirmRoot struct {
	*testRoot
	view rootstate.RegionPlacementView
}

func (r reconfirmRoot) RangeRegionPlacementView(string, int, int) (rootstate.RangePlacement, rootstate.RegionPlacementView, error) {
	return rootstate.RangePlacement{}, r.view, nil
}

type reconfirmCutObserver struct {
	t     *testing.T
	nodes map[uint64]rootstate.Node
	calls int
}

func (o *reconfirmCutObserver) ObserveRecordHolding(ctx context.Context, id uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	o.t.Helper()
	if cut, ok := router.ConfirmedRootIndex(ctx); !ok || cut != 5 {
		o.t.Fatal("reconfirmation discarded its fresh Root cut", cut, ok)
	}
	o.calls++
	node := o.nodes[id]
	holding := testHolding(d)
	holding.StoreID = fmt.Sprintf("store_%032x", id)
	return raftrpc.HoldingObservation{RaftID: id, NodeID: node.NodeID, RootConfirmedIndex: 5, Holding: holding}, nil
}

type stopReconfirmMirror struct{ called bool }

type reconfirmStore struct{ testStore }

func (*reconfirmStore) Verify(ctx context.Context, d records.Descriptor) (records.Holding, error) {
	return (validReadStore{held: true}).Verify(ctx, d)
}

func (m *stopReconfirmMirror) MirrorGeo(context.Context, uint64, string, []byte) (enginestate.Result, error) {
	m.called = true
	return enginestate.Result{}, status.Error(codes.FailedPrecondition, "stop after verified holdings")
}

func TestGeoReconfirmationCarriesFreshRootCutOnlyAfterAppliedPlacement(t *testing.T) {
	for _, applied := range []uint64{4, 5} {
		t.Run(fmt.Sprint(applied), func(t *testing.T) {
			base, target, _, owner, _ := publishedFixture(t)
			var ref enginestate.RecordReference
			_, err := owner.ReadRouted(context.Background(), publishedBarrier{ownerIndex: owner.AppliedIndex()}, target,
				func(view enginestate.ReadView) error {
					var found bool
					var err error
					ref, found, err = view.RecordReference(target.RoutingKey)
					if err == nil && !found {
						return errors.New("missing fixture reference")
					}
					return err
				})
			if err != nil {
				t.Fatal(err)
			}
			eastNode := rootstate.Node{RaftID: 4, NodeID: fmt.Sprintf("node_%032x", 4), RegionID: "east", Address: "e1:7443"}
			east := base.view
			east.Head = rootstate.RegionCohortHead{RegionID: "east", CurrentGeneration: 1}
			east.Current.RegionID, east.Current.GroupID, east.Current.Nodes = "east", "region-east", []uint64{4}
			east.Group = rootstate.Group{GroupID: "region-east", Epoch: 2,
				Members: []rootstate.GroupMember{{RaftID: 4, Role: rootstate.GroupMemberVoter}}}
			root := reconfirmRoot{testRoot: base, view: rootstate.RegionPlacementView{AppliedIndex: applied,
				Regions: []rootstate.RegionPlacement{{Progress: base.view, Nodes: base.members}, {Progress: east, Nodes: []rootstate.Node{eastNode}}}}}
			observer := &reconfirmCutObserver{t: t, nodes: map[uint64]rootstate.Node{4: eastNode}}
			for _, n := range base.members {
				observer.nodes[n.RaftID] = n
			}
			mirror := &stopReconfirmMirror{}
			writer := Writer{Barrier: publishedBarrier{ownerIndex: owner.AppliedIndex()}, Root: root, Owner: owner,
				LocalID: 1, LocalNodeID: base.members[0].NodeID, Store: &reconfirmStore{}, Observer: observer,
				MirrorGeo: mirror, Submit: func(context.Context, string, []byte) error {
					t.Fatal("mirror rejection was ignored")
					return nil
				}}
			confirmed, err := writer.EnsureCurrentGeoConfirmation(context.Background(), ref, 3, 3)
			if confirmed || err == nil {
				t.Fatal("unconfirmed repair became a success", confirmed, err)
			}
			if applied == 4 && (observer.calls != 0 || mirror.called) {
				t.Fatal("Root state behind the read cut was used", observer.calls, mirror.called)
			}
			if applied == 5 && (observer.calls != 2 || !mirror.called) {
				t.Fatal("fresh Root cut was not used for both Region majorities", observer.calls, mirror.called, err)
			}
		})
	}
}
