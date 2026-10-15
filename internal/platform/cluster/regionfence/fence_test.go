package regionfence

import (
	"slices"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"go.etcd.io/raft/v3/raftpb"
)

func sample() Descriptor {
	return Descriptor{RegionID: "west", GroupID: "regional", Generation: 1, PlannedIndex: 900, GroupEpoch: 2, MembershipIndex: 3,
		Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}
}
func TestRegionFenceBoundsAndStableMembership(t *testing.T) {
	d := sample()
	if d.Validate() != nil || !d.Matches(&raftpb.ConfState{Voters: []uint64{3, 2, 1}}) {
		t.Fatal(d)
	}
	for name, mutate := range map[string]func(*Descriptor){
		"region":              func(d *Descriptor) { d.RegionID = "../bad" },
		"root":                func(d *Descriptor) { d.GroupID = "root" },
		"generation":          func(d *Descriptor) { d.Generation = 0 },
		"initial-reformation": func(d *Descriptor) { d.Reformation = true },
		"plan":                func(d *Descriptor) { d.PlannedIndex = 0 },
		"epoch":               func(d *Descriptor) { d.GroupEpoch = 1 },
		"membership-position": func(d *Descriptor) { d.MembershipIndex = 0 },
		"duplicate":           func(d *Descriptor) { d.Members[2].RaftID = 2 },
		"unsorted":            func(d *Descriptor) { slices.Reverse(d.Members) },
		"few-voters": func(d *Descriptor) {
			for i := range d.Members {
				d.Members[i].Role = rootstate.GroupMemberLearner
			}
		},
		"role": func(d *Descriptor) { d.Members[2].Role = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			d := sample()
			mutate(&d)
			if d.Validate() == nil {
				t.Fatal("invalid fence accepted")
			}
		})
	}
	autoLeave := true
	for _, conf := range []*raftpb.ConfState{nil, {}, {Voters: []uint64{1, 2, 3}, VotersOutgoing: []uint64{1}}, {Voters: []uint64{1, 2, 3}, AutoLeave: &autoLeave}, {Voters: []uint64{1, 2, 3}, LearnersNext: []uint64{4}}, {Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}, {Voters: []uint64{1, 1, 3}}} {
		if d.Matches(conf) {
			t.Fatal("unstable or different membership matched", conf)
		}
	}
	large := sample()
	large.Members = make([]rootstate.GroupMember, rootstate.MaxRegionCohortNodes)
	for i := range large.Members {
		large.Members[i] = rootstate.GroupMember{RaftID: uint64(i + 1), Role: rootstate.GroupMemberVoter}
	}
	if err := large.Validate(); err != nil {
		t.Fatal(err)
	}
	large.Members = append(large.Members, rootstate.GroupMember{RaftID: uint64(len(large.Members) + 1), Role: rootstate.GroupMemberVoter})
	if large.Validate() == nil {
		t.Fatal("unbounded members accepted")
	}
}

func TestRegionFenceAdvancementCannotShrinkOrSkipGeneration(t *testing.T) {
	old := sample()
	next := sample()
	next.GroupEpoch++
	if !next.Advances(old) {
		t.Fatal("descriptor-only epoch refresh rejected")
	}
	next.MembershipIndex++
	if !next.Advances(old) {
		t.Fatal("same cohort refence rejected")
	}
	next.Generation++
	next.PlannedIndex++
	next.Members = append(next.Members, rootstate.GroupMember{RaftID: 4, Role: rootstate.GroupMemberLearner})
	if !next.Advances(old) {
		t.Fatal("learner data participant addition rejected")
	}
	for _, mutate := range []func(*Descriptor){
		func(d *Descriptor) { d.GroupEpoch = old.GroupEpoch },
		func(d *Descriptor) { d.MembershipIndex = old.MembershipIndex },
		func(d *Descriptor) { d.PlannedIndex = old.PlannedIndex },
		func(d *Descriptor) { d.Generation++ },
		func(d *Descriptor) { d.RegionID = "east" },
		func(d *Descriptor) { d.GroupID = "another" },
		func(d *Descriptor) { d.Members = d.Members[2:] },
		func(d *Descriptor) { d.Members[2].RaftID = 5 },
	} {
		bad := next
		bad.Members = slices.Clone(next.Members)
		mutate(&bad)
		if bad.Advances(old) {
			t.Fatal("invalid advancement", bad)
		}
	}
}
