package raft

import (
	"errors"
	"slices"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// RemoveRootMember is an explicit maintenance primitive, not a liveness
// policy. Expected ConfState is checked in the same critical section as the
// proposal. The caller must separately authorize the operator and confirm
// that the Node has no remaining data responsibilities. Success is proposed,
// not committed; obtain ReadIndex and inspect durable membership afterwards.
func (g *Group) RemoveRootMember(expected *raftpb.ConfState, target uint64) error {
	if g.groupID != RootGroupID || expected == nil || target == 0 {
		return errors.New("Root removal requires Root Group, membership and target")
	}
	expected = proto.Clone(expected).(*raftpb.ConfState)
	return g.mutate(func() error {
		if err := g.admitProposal(); err != nil {
			return err
		}
		status := g.raw.Status()
		if status.RaftState != etcdraft.StateLeader {
			return errors.New("Root removal requires current Root leader")
		}
		conf := g.state.ConfState()
		if !proto.Equal(expected, conf) {
			return ErrTopologyChanged
		}
		if len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
			return ErrTopologyPending
		}
		if !containsMember(conf, target) {
			return nil
		}
		if target == g.id {
			return ErrTopologyTransfer
		}
		required := len(conf.GetVoters())/2 + 1
		if slices.Contains(conf.GetVoters(), target) && len(conf.GetVoters())-1 < required {
			return errors.New("Root removal would lose the current voter majority")
		}
		ready := 0
		for _, id := range conf.GetVoters() {
			if id == target {
				continue
			}
			progress, found := status.Progress[id]
			if found && progress.RecentActive && progress.Match >= status.GetCommit() {
				ready++
			}
		}
		if ready < required {
			return ErrTopologyPending
		}
		return g.raw.ProposeConfChange(&raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{
			Type: raftpb.ConfChangeType_ConfChangeRemoveNode.Enum(), NodeId: proto.Uint64(target),
		}}})
	})
}

func (h *Host) RemoveRootMember(expected *raftpb.ConfState, target uint64) error {
	group, err := h.group(RootGroupID)
	if err != nil {
		return err
	}
	return group.RemoveRootMember(expected, target)
}
