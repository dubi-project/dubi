package roottopology

import (
	"fmt"
	"slices"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

const testCluster = "cluster_00000000000000000000000000000001"

func planView(sizes ...int) rootjoin.View {
	view := rootjoin.View{ClusterID: testCluster, AppliedIndex: 10, Voters: []uint64{1}}
	var id uint64
	for region, count := range sizes {
		for n := 0; n < count; n++ {
			id++
			view.Peers = append(view.Peers, rootstate.Node{NodeID: fmt.Sprintf("node_%032x", id), RaftID: id,
				RegionID: fmt.Sprintf("r%02d", region), Address: fmt.Sprintf("node%d:7443", id)})
			if id != 1 {
				view.Learners = append(view.Learners, id)
			}
		}
	}
	return view
}

func TestRootTopologyPlanUsesDurableRegionsAndRetainsVoters(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sizes []int
		want  []uint64
	}{
		{"one", []int{1}, []uint64{1}},
		{"two", []int{2}, []uint64{1, 2}},
		{"three", []int{3}, []uint64{1, 2, 3}},
		{"four", []int{4}, []uint64{1, 2, 3}},
		{"five", []int{5}, []uint64{1, 2, 3, 4, 5}},
		{"two-regions", []int{2, 2}, []uint64{1, 2, 3}},
		{"3x3", []int{3, 3, 3}, []uint64{1, 4, 7}},
		{"3x5", []int{5, 5, 5}, []uint64{1, 6, 11}},
		{"4x3", []int{3, 3, 3, 3}, []uint64{1, 4, 7, 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := planView(tc.sizes...)
			got, err := DesiredVoters(view)
			if err != nil || !slices.Equal(got, tc.want) {
				t.Fatal(got, err)
			}
			slices.Reverse(view.Peers)
			slices.Reverse(view.Learners)
			again, err := DesiredVoters(view)
			if err != nil || !slices.Equal(again, got) {
				t.Fatal("input order changed plan", again, err)
			}
		})
	}
	view := planView(4)
	view.Voters, view.Learners = []uint64{4, 2, 3}, []uint64{1}
	got, err := DesiredVoters(view)
	if err != nil || !slices.Equal(got, []uint64{2, 3, 4}) {
		t.Fatal("lower-ID learner churned existing voters", got, err)
	}
	large, err := DesiredVoters(planView(64))
	if err != nil || len(large) != MaxRootVoters {
		t.Fatal("internal voter bound", large, err)
	}
}

func TestRootTopologyPlanRejectsIncompleteOrJointViews(t *testing.T) {
	view := planView(3)
	view.VotersOutgoing, view.AutoLeave = []uint64{1}, true
	if _, err := DesiredVoters(view); err != ErrJoint {
		t.Fatal(err)
	}
	view = planView(3)
	view.Peers = view.Peers[:2]
	if _, err := DesiredVoters(view); err == nil {
		t.Fatal("incomplete Node catalog accepted")
	}
	sizes := make([]int, MaxRootVoters+1)
	for i := range sizes {
		sizes[i] = 1
	}
	if _, err := DesiredVoters(planView(sizes...)); err == nil {
		t.Fatal("excess Regions silently omitted")
	}
}
