package regionfence

import (
	"math"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

func handoffIntent() HandoffIntent {
	return HandoffIntent{Previous: Record{Fence: Descriptor{RegionID: "west", GroupID: "data-1", Generation: 1, PlannedIndex: 100, GroupEpoch: 2, MembershipIndex: 1, Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}, RequestID: "first", Index: 2}, Next: rootstate.RegionCohort{RegionID: "west", GroupID: "data-1", Generation: 2, PlannedIndex: 101, State: rootstate.RegionCohortPlanned, Nodes: []uint64{1, 2, 3, 4}}}
}

func TestRegionalHandoffIntentNeverShrinksOrSkips(t *testing.T) {
	for _, mode := range []string{"normal", "replace", "shrink", "same", "skip", "overflow", "group", "region", "stale-plan", "confirmed", "formation", "duplicate", "unsorted"} {
		t.Run(mode, func(t *testing.T) {
			i := handoffIntent()
			switch mode {
			case "replace":
				i.Next.Nodes = []uint64{1, 2, 4, 5}
			case "shrink":
				i.Next.Nodes = []uint64{1, 2}
			case "same":
				i.Next.Nodes = []uint64{1, 2, 3}
			case "skip":
				i.Next.Generation++
			case "overflow":
				i.Previous.Fence.Generation = math.MaxUint64
				i.Next.Generation = 0
			case "group":
				i.Next.GroupID = "data-2"
			case "region":
				i.Next.RegionID = "east"
			case "stale-plan":
				i.Next.PlannedIndex--
			case "confirmed":
				i.Next.State = rootstate.RegionCohortConfirmed
			case "formation":
				i.Next.Formation = true
			case "duplicate":
				i.Next.Nodes = []uint64{1, 2, 3, 3}
			case "unsorted":
				i.Next.Nodes = []uint64{1, 3, 2, 4}
			}
			if err := i.Validate(); (err == nil) != (mode == "normal" || mode == "replace") {
				t.Fatal(mode, err)
			}
		})
	}
}

func TestRegionalHandoffCandidateBindsPostSealMembershipAndRootPlan(t *testing.T) {
	i := handoffIntent()
	for _, mode := range []string{"normal", "before-seal", "old-membership", "wrong-plan", "wrong-epoch", "wrong-nodes"} {
		t.Run(mode, func(t *testing.T) {
			r := i.Previous
			r.Index = 5
			r.RequestID = "candidate"
			r.Fence.Generation = 2
			r.Fence.PlannedIndex = 101
			r.Fence.GroupEpoch = 3
			r.Fence.MembershipIndex = 4
			r.Fence.Members = append(r.Fence.Members[:3:3], rootstate.GroupMember{RaftID: 4, Role: rootstate.GroupMemberVoter})
			switch mode {
			case "before-seal":
				r.Index = 3
			case "old-membership":
				r.Fence.MembershipIndex = 3
			case "wrong-plan":
				r.Fence.PlannedIndex++
			case "wrong-epoch":
				r.Fence.GroupEpoch--
			case "wrong-nodes":
				r.Fence.Members[3].RaftID = 5
			}
			if i.Accepts(r, 3) != (mode == "normal") {
				t.Fatal(mode, r)
			}
		})
	}
}
