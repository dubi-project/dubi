// Package regionfence defines the capability-neutral, generation-bound Group
// barrier. A local applied barrier is not a data durability receipt.
package regionfence

import (
	"errors"
	"math"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"go.etcd.io/raft/v3/raftpb"
)

const MaxRecordBytes = 1 << 20

type Descriptor struct {
	RegionID   string `json:"region_id"`
	GroupID    string `json:"group_id"`
	Generation uint64 `json:"generation"`
	// Reformation marks a fresh same-name Region Group. It has no old local
	// fence to seal; normal later generations still require a Handoff record.
	Reformation     bool                    `json:"reformation,omitempty"`
	PlannedIndex    uint64                  `json:"planned_index"` // Root index, not comparable with Group indexes.
	GroupEpoch      uint64                  `json:"group_epoch"`
	MembershipIndex uint64                  `json:"membership_index"` // Last applied Group ConfChange.
	Members         []rootstate.GroupMember `json:"members"`
}

func (d Descriptor) Nodes() []uint64 {
	nodes := make([]uint64, len(d.Members))
	for i, m := range d.Members {
		nodes[i] = m.RaftID
	}
	return nodes
}

func (d Descriptor) Validate() error {
	if len(d.Members) > rootstate.MaxRegionCohortNodes || d.GroupEpoch < 2 || d.MembershipIndex == 0 {
		return errors.New("invalid regional fence bounds or membership position")
	}
	c := rootstate.RegionCohort{RegionID: d.RegionID, GroupID: d.GroupID, Generation: d.Generation,
		Reformation: d.Reformation, PlannedIndex: d.PlannedIndex, Nodes: d.Nodes(), State: rootstate.RegionCohortPlanned}
	if err := c.Validate(); err != nil {
		return err
	}
	voters := 0
	for _, m := range d.Members {
		switch m.Role {
		case rootstate.GroupMemberVoter:
			voters++
		case rootstate.GroupMemberLearner:
		default:
			return errors.New("invalid regional fence member role")
		}
	}
	if voters < min(3, len(d.Members)) {
		return errors.New("regional fence requires three voters or all small-Region members as voters")
	}
	return nil
}

func (d Descriptor) Matches(conf *raftpb.ConfState) bool {
	if d.Validate() != nil || conf == nil || len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() || len(conf.GetVoters())+len(conf.GetLearners()) != len(d.Members) {
		return false
	}
	roles := make(map[uint64]string, len(d.Members))
	for _, id := range conf.GetVoters() {
		if id == 0 || roles[id] != "" {
			return false
		}
		roles[id] = rootstate.GroupMemberVoter
	}
	for _, id := range conf.GetLearners() {
		if id == 0 || roles[id] != "" {
			return false
		}
		roles[id] = rootstate.GroupMemberLearner
	}
	for _, member := range d.Members {
		if roles[member.RaftID] != member.Role {
			return false
		}
	}
	return true
}

// Advances permits re-fencing the same cohort after a completed membership
// task, or a next cohort that retains an old voter majority. The latter also
// covers recovery after an explicit maintenance removal.
func (d Descriptor) Advances(old Descriptor) bool {
	if d.Validate() != nil || old.Validate() != nil || d.RegionID != old.RegionID || d.GroupID != old.GroupID || d.GroupEpoch <= old.GroupEpoch || d.MembershipIndex < old.MembershipIndex {
		return false
	}
	if d.Generation == old.Generation {
		return d.PlannedIndex == old.PlannedIndex && slices.Equal(d.Nodes(), old.Nodes()) &&
			(d.MembershipIndex != old.MembershipIndex || slices.Equal(d.Members, old.Members))
	}
	if old.Generation == math.MaxUint64 || d.Generation != old.Generation+1 || d.PlannedIndex <= old.PlannedIndex || d.MembershipIndex == old.MembershipIndex {
		return false
	}
	return overlapsOldVoterMajority(old, d.Nodes())
}

func overlapsOldVoterMajority(old Descriptor, next []uint64) bool {
	oldVoters := 0
	oldIDs := make(map[uint64]struct{}, len(old.Members))
	for _, member := range old.Members {
		oldIDs[member.RaftID] = struct{}{}
		if member.Role == rootstate.GroupMemberVoter {
			oldVoters++
		}
	}
	overlap := 0
	fresh := false
	for _, id := range next {
		if _, found := oldIDs[id]; found {
			overlap++
		} else {
			fresh = true
		}
	}
	return fresh && overlap >= oldVoters/2+1
}

type Record struct {
	Fence     Descriptor `json:"fence"`
	RequestID string     `json:"request_id"`
	Index     uint64     `json:"index"`
}

func (r Record) Validate() error {
	if err := r.Fence.Validate(); err != nil {
		return err
	}
	if rootstate.ValidateRequestID(r.RequestID) != nil || r.Index <= r.Fence.MembershipIndex {
		return errors.New("invalid regional fence application position")
	}
	return nil
}
