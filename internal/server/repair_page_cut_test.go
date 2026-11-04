package server

import (
	"context"
	"errors"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

type pageCutTestRoot struct {
	view  rootstate.RegionProgress
	nodes []rootstate.Node
}

func (r *pageCutTestRoot) RegionProgress(string) (rootstate.RegionProgress, bool, error) {
	return r.view, true, nil
}
func (r *pageCutTestRoot) RootMembers(...uint64) ([]rootstate.Node, bool, error) {
	return r.nodes, true, nil
}

type pageCutTestBarrier struct {
	roots, owners int
	fail          bool
}

func (b *pageCutTestBarrier) ReadIndex(ctx context.Context, g string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if g == raft.RootGroupID {
		b.roots++
		if b.fail {
			return 0, errors.New("Root quorum lost")
		}
	} else {
		b.owners++
	}
	return 5, nil
}
func pageCutFixture() (*pageCutTestRoot, *pageCutTestBarrier) {
	nodes := []rootstate.Node{{RaftID: 1, NodeID: "node_00000000000000000000000000000001", RegionID: "west", Address: "n1:7443"}, {RaftID: 2, NodeID: "node_00000000000000000000000000000002", RegionID: "west", Address: "n2:7443"}, {RaftID: 3, NodeID: "node_00000000000000000000000000000003", RegionID: "west", Address: "n3:7443"}}
	cohort := rootstate.RegionCohort{RegionID: "west", GroupID: "region-west", Generation: 1, Nodes: []uint64{1, 2, 3}, State: rootstate.RegionCohortConfirmed, PlannedIndex: 1, ConfirmedIndex: 5, GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 4}
	view := rootstate.RegionProgress{AppliedIndex: 5, Eligible: true, Head: rootstate.RegionCohortHead{RegionID: "west", CurrentGeneration: 1}, Current: cohort, Group: rootstate.Group{GroupID: "region-west", Epoch: 2, Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}}
	return &pageCutTestRoot{view: view, nodes: nodes}, &pageCutTestBarrier{}
}
func TestRepairPageSharesOnlyRootAdmissionAndRequiresFreshFinish(t *testing.T) {
	for _, mode := range []string{"success", "quorum-loss", "changed-cohort", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			root, barrier := pageCutFixture()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cut, err := beginRepairPage(ctx, barrier, root, "region-west")
			if err != nil {
				t.Fatal(err)
			}
			// Physical and metadata stages can each ask for Root without more WAN cuts.
			for range 6 {
				if _, err := placement.ReadLocalPlan(ctx, cut, root, "region-west"); err != nil {
					t.Fatal(err)
				}
			}
			for range 3 {
				if _, err := cut.ReadIndex(ctx, "region-west"); err != nil {
					t.Fatal(err)
				}
			}
			if barrier.roots != 1 || barrier.owners != 3 {
				t.Fatal("cut leaked into Owner or repeated Root", barrier)
			}
			switch mode {
			case "quorum-loss":
				barrier.fail = true
			case "changed-cohort":
				root.view.Current.GroupEpoch++
				root.view.Group.Epoch++
			case "canceled":
				cancel()
			}
			err = cut.finish(ctx)
			if (err == nil) != (mode == "success") {
				t.Fatal("page finish accepted invalid proof", mode, err)
			}
			if mode != "canceled" && barrier.roots != 2 {
				t.Fatal("missing fresh final Root cut", barrier)
			}
		})
	}
}
