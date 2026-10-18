package raft

import (
	"errors"
	"fmt"
	"slices"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	ErrTopologyChanged  = errors.New("Raft topology no longer matches the confirmed plan")
	ErrTopologyPending  = errors.New("Raft topology is not ready for reconfiguration")
	ErrTopologyTransfer = errors.New("Raft topology requires leader transfer before local demotion")
)

// ReconfigureVoters changes roles of existing members only. The expected
// durable ConfState is a compare-and-swap fence, not a new bootstrap list.
// Every old/new voter must be recently active and caught through current
// Commit. This conservative gate never reduces quorum because peers disappear.
// A multi-role change uses automatic joint consensus, committing both quorums
// before leaving joint state; success means proposed, not confirmed.
func (g *Group) ReconfigureVoters(expected *raftpb.ConfState, voters []uint64) error {
	if expected == nil || len(voters) == 0 || len(voters) > 4096 {
		return errors.New("invalid Raft topology plan")
	}
	voters = slices.Clone(voters)
	slices.Sort(voters)
	for i, id := range voters {
		if id == 0 || i > 0 && voters[i-1] == id {
			return errors.New("invalid or duplicate Raft voter")
		}
	}
	return g.mutate(func() error {
		if err := g.admitProposal(); err != nil {
			return err
		}
		status := g.raw.Status()
		if status.RaftState != etcdraft.StateLeader {
			return fmt.Errorf("topology change requires Group leader; current leader is %d", status.Lead)
		}
		conf := g.state.ConfState()
		if !proto.Equal(conf, expected) {
			return ErrTopologyChanged
		}
		if len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
			return ErrTopologyPending
		}
		var changes []*raftpb.ConfChangeSingle
		for _, id := range voters {
			if !containsMember(conf, id) {
				return fmt.Errorf("RaftID %d must first join as an authorized learner", id)
			}
			if !containsVoter(conf, id) {
				changes = append(changes, &raftpb.ConfChangeSingle{Type: raftpb.ConfChangeType_ConfChangeAddNode.Enum(), NodeId: proto.Uint64(id)})
			}
		}
		for _, id := range conf.GetVoters() {
			if !slices.Contains(voters, id) {
				if id == g.id {
					return ErrTopologyTransfer
				}
				changes = append(changes, &raftpb.ConfChangeSingle{Type: raftpb.ConfChangeType_ConfChangeAddLearnerNode.Enum(), NodeId: proto.Uint64(id)})
			}
		}
		if len(changes) == 0 {
			return nil
		}
		for _, ids := range [][]uint64{conf.GetVoters(), voters} {
			for _, id := range ids {
				p, found := status.Progress[id]
				if !found || !p.RecentActive || !g.topologyProgressCaughtUp(p.Match, status.GetCommit()) {
					return fmt.Errorf("%w: voter %d is inactive or behind commit %d", ErrTopologyPending, id, status.GetCommit())
				}
			}
		}
		return g.raw.ProposeConfChange(&raftpb.ConfChangeV2{
			Transition: raftpb.ConfChangeTransition_ConfChangeTransitionJointImplicit.Enum(), Changes: changes,
		})
	})
}

// topologyProgressCaughtUp permits a learner that is one entry behind only
// when the trailing entry is the empty ReadIndex barrier emitted by the local
// leader. The topology coordinator obtains its confirmed Root cut immediately
// before this CAS; that barrier may not have reached a WAN learner yet, while
// admitting the learner at the preceding committed cut is safe. Any actual
// data or membership entry still requires an exact Match == commit.
func (g *Group) topologyProgressCaughtUp(match, commit uint64) bool {
	if match >= commit {
		return true
	}
	if commit == 0 || match+1 != commit {
		return false
	}
	entries, err := g.storage.Entries(commit, commit+1, 1<<20)
	if err != nil || len(entries) != 1 {
		return false
	}
	entry := entries[0]
	return entry.GetType() == raftpb.EntryNormal && len(entry.GetData()) == 0
}

func (h *Host) ReconfigureVoters(groupID string, expected *raftpb.ConfState, voters []uint64) error {
	group, err := h.group(groupID)
	if err != nil {
		return err
	}
	return group.ReconfigureVoters(expected, voters)
}
