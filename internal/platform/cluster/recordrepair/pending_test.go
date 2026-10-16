package recordrepair

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pendingTestBarrier struct{ calls int }

func (b *pendingTestBarrier) ReadIndex(_ context.Context, group string) (uint64, error) {
	if group != raft.RootGroupID {
		return 0, errors.New("wrong barrier Group")
	}
	b.calls++
	return uint64(b.calls), nil
}

type pendingTestRoot struct {
	view    rootstate.RegionProgress
	sources []rootstate.Node
}

func (r *pendingTestRoot) RegionProgress(string) (rootstate.RegionProgress, bool, error) {
	return r.view, true, nil
}
func (r *pendingTestRoot) RootMembers(ids ...uint64) ([]rootstate.Node, bool, error) {
	if len(ids) != len(r.sources) {
		return nil, false, nil
	}
	for i, id := range ids {
		if id != r.sources[i].RaftID {
			return nil, false, nil
		}
	}
	return append([]rootstate.Node(nil), r.sources...), true, nil
}

func pendingFixture(t *testing.T) (regionfence.Handoff, *pendingTestRoot, capstate.RecordCut) {
	t.Helper()
	group := "region-west"
	members := []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}
	previous := regionfence.Record{Fence: regionfence.Descriptor{RegionID: "west", GroupID: group, Generation: 1,
		PlannedIndex: 10, GroupEpoch: 2, MembershipIndex: 2, Members: members}, RequestID: "old-fence", Index: 3}
	current := rootstate.RegionCohort{RegionID: "west", GroupID: group, Generation: 1, Nodes: []uint64{1, 2, 3},
		State: rootstate.RegionCohortConfirmed, PlannedIndex: 10, ConfirmedIndex: 11, GroupEpoch: 2,
		GroupFenceRequestID: previous.RequestID, GroupFenceIndex: previous.Index}
	next := rootstate.RegionCohort{RegionID: "west", GroupID: group, Generation: 2,
		Nodes: []uint64{1, 2, 3, 4}, State: rootstate.RegionCohortPlanned, PlannedIndex: 12}
	seal := regionfence.Handoff{Intent: regionfence.HandoffIntent{Previous: previous, Next: next}, SealRequestID: "seal", SealIndex: 4}
	if err := seal.Validate(); err != nil {
		t.Fatal(err)
	}
	root := &pendingTestRoot{view: rootstate.RegionProgress{AppliedIndex: 15, Eligible: true, Current: current, Pending: &next,
		PendingGrowth: &rootstate.RegionGrowth{Seal: rootstate.RegionGrowthSeal{SealRequestID: seal.SealRequestID, SealIndex: seal.SealIndex}}},
		sources: []rootstate.Node{
			{RaftID: 1, RegionID: "west", NodeID: "node-1", Address: "node-1:7443"},
			{RaftID: 2, RegionID: "west", NodeID: "node-2", Address: "node-2:7443"},
			{RaftID: 3, RegionID: "west", NodeID: "node-3", Address: "node-3:7443"},
		}}
	cut := capstate.RecordCut{Version: 1, Seal: seal, Artifact: snapshot.Reference{Version: snapshot.StreamVersion,
		GroupID: group, Index: 4, Size: 1, SHA256: zeros()}, RecordCutSummary: capstate.RecordCutSummary{SHA256: zeros()}}
	if err := cut.Validate(); err != nil {
		t.Fatal(err)
	}
	return seal, root, cut
}

func zeros() string { return "0000000000000000000000000000000000000000000000000000000000000000" }

func TestFillPendingLocalUsesFullOldCohortAndRechecksRoot(t *testing.T) {
	seal, root, cut := pendingFixture(t)
	barrier := &pendingTestBarrier{}
	var fetched []uint64
	filled := 0
	want := Report{References: 2, Fetched: 2, ReferencedBytes: 9}
	got, err := fillPendingLocal(context.Background(), barrier, root, 4, seal,
		func(_ context.Context, source rootstate.Node) (capstate.RecordCut, error) {
			fetched = append(fetched, source.RaftID)
			if source.RaftID == 1 {
				return capstate.RecordCut{}, status.Error(codes.Unavailable, "offline")
			}
			return cut, nil
		}, func(_ context.Context, received capstate.RecordCut, sources []rootstate.Node) (Report, error) {
			filled++
			if !reflect.DeepEqual(received, cut) || !reflect.DeepEqual(sources, root.sources) {
				t.Fatal("fill received a different cut or source set")
			}
			return want, nil
		})
	if err != nil || got != want || filled != 1 || !reflect.DeepEqual(fetched, []uint64{1, 2}) || barrier.calls != 3 {
		t.Fatal(got, err, filled, fetched, barrier.calls)
	}
}

func TestPendingCutAllowsSingleNodeInitialRegionGrowth(t *testing.T) {
	seal, root, _ := pendingFixture(t)
	seal.Intent.Previous.Fence.Members = []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}}
	seal.Intent.Next.Nodes = []uint64{1, 4}
	root.view.Current.Nodes = []uint64{1}
	root.view.Pending = &seal.Intent.Next
	root.sources = []rootstate.Node{{RaftID: 1, RegionID: "west", NodeID: "node-1", Address: "node-1:7443"}}
	if err := seal.Validate(); err != nil {
		t.Fatal(err)
	}

	view, sources, err := pendingCut(context.Background(), &pendingTestBarrier{}, root, 4, "region-west", seal)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(view, root.view) || !reflect.DeepEqual(sources, root.sources) {
		t.Fatalf("single-node old cohort was not retained: view=%+v sources=%+v", view, sources)
	}
}

func TestFillPendingLocalLeavesTimeForAnotherSealedCutSource(t *testing.T) {
	seal, root, cut := pendingFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	var fetched []uint64
	got, err := fillPendingLocal(ctx, &pendingTestBarrier{}, root, 4, seal,
		func(attempt context.Context, source rootstate.Node) (capstate.RecordCut, error) {
			fetched = append(fetched, source.RaftID)
			if source.RaftID == 1 {
				<-attempt.Done()
				return capstate.RecordCut{}, attempt.Err()
			}
			return cut, nil
		}, func(context.Context, capstate.RecordCut, []rootstate.Node) (Report, error) {
			return Report{References: 1}, nil
		})
	if err != nil || got.References != 1 || ctx.Err() != nil || !reflect.DeepEqual(fetched, []uint64{1, 2}) {
		t.Fatal("stalled cut source consumed the whole growth round", got, fetched, ctx.Err(), err)
	}
}

func TestFillPendingLocalRejectsPlanAndSourceChanges(t *testing.T) {
	for _, phase := range []string{"before-fill", "after-fill"} {
		t.Run(phase, func(t *testing.T) {
			seal, root, cut := pendingFixture(t)
			barrier := &pendingTestBarrier{}
			fills := 0
			_, err := fillPendingLocal(context.Background(), barrier, root, 4, seal,
				func(context.Context, rootstate.Node) (capstate.RecordCut, error) {
					if phase == "before-fill" {
						root.sources[0].Address = "replaced:7443"
					}
					return cut, nil
				}, func(context.Context, capstate.RecordCut, []rootstate.Node) (Report, error) {
					fills++
					root.view.Pending = nil
					return Report{References: 1}, nil
				})
			if err == nil || phase == "before-fill" && fills != 0 || phase == "after-fill" && fills != 1 {
				t.Fatal("stale plan/source accepted", err, fills)
			}
		})
	}
}

func TestFillPendingLocalRejectsInvalidSourceOrCut(t *testing.T) {
	seal, root, cut := pendingFixture(t)
	if _, err := fillPendingLocal(context.Background(), &pendingTestBarrier{}, root, 2, seal,
		func(context.Context, rootstate.Node) (capstate.RecordCut, error) {
			t.Fatal("fetched for old Node")
			return cut, nil
		},
		func(context.Context, capstate.RecordCut, []rootstate.Node) (Report, error) { return Report{}, nil }); err == nil {
		t.Fatal("old Node accepted as newly added destination")
	}
	cut.Seal.SealRequestID = "different"
	if _, err := fillPendingLocal(context.Background(), &pendingTestBarrier{}, root, 4, seal,
		func(context.Context, rootstate.Node) (capstate.RecordCut, error) { return cut, nil },
		func(context.Context, capstate.RecordCut, []rootstate.Node) (Report, error) {
			t.Fatal("filled wrong cut")
			return Report{}, nil
		}); err == nil {
		t.Fatal("different cut accepted")
	}
}
