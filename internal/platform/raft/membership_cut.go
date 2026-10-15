package raft

import (
	"context"
	"errors"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// MembershipCut identifies the last durably applied membership entry, not a
// business applied index. Equal lists after remove/re-add are different cuts.
type MembershipCut struct {
	Index uint64
	Conf  *raftpb.ConfState
}

type MembershipReader interface{ MembershipCut() (MembershipCut, error) }

var ErrMembershipChanged = errors.New("Group membership cut changed")

func (g *Group) MembershipCut() (MembershipCut, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkLocked(); err != nil {
		return MembershipCut{}, err
	}
	return g.membershipCutLocked()
}

func (g *Group) membershipCutLocked() (MembershipCut, error) {
	reader, ok := g.state.(MembershipReader)
	if !ok {
		return MembershipCut{}, errors.New("Group state does not support fenced membership changes")
	}
	cut, err := reader.MembershipCut()
	if err != nil {
		return MembershipCut{}, err
	}
	if cut.Index == 0 || cut.Index > g.state.AppliedIndex() || cut.Conf == nil || !proto.Equal(cut.Conf, g.state.ConfState()) {
		return MembershipCut{}, errors.New("invalid durable Group membership cut")
	}
	cut.Conf = proto.Clone(cut.Conf).(*raftpb.ConfState)
	return cut, nil
}

// ChangeMembership applies the same role, leader, catch-up and admission checks
// as trusted local primitives, with an additional position+configuration CAS in
// that very same Group critical section. It does not authorize a Root task.
func (g *Group) ChangeMembership(expected MembershipCut, id uint64, operation raftpb.ConfChangeType) error {
	if g.groupID == RootGroupID || expected.Index == 0 || expected.Conf == nil {
		return errors.New("fenced membership change requires a data Group and an expected cut")
	}
	expected.Conf = proto.Clone(expected.Conf).(*raftpb.ConfState)
	return g.proposeMembershipExpected(id, operation, &expected)
}

func (h *Host) MembershipCut(groupID string) (MembershipCut, error) {
	g, err := h.group(groupID)
	if err != nil {
		return MembershipCut{}, err
	}
	return g.MembershipCut()
}
func (h *Host) ChangeMembership(groupID string, expected MembershipCut, id uint64, operation raftpb.ConfChangeType) error {
	g, err := h.group(groupID)
	if err != nil {
		return err
	}
	return g.ChangeMembership(expected, id, operation)
}

// TransferLeadershipFenced pins retries to the observed membership AND leader
// term. A timeout/re-election cannot silently revive an old transfer attempt.
func (g *Group) TransferLeadershipFenced(ctx context.Context, target uint64, expected MembershipCut, term uint64) (GroupStatus, error) {
	if g.groupID == RootGroupID || expected.Index == 0 || expected.Conf == nil || term == 0 {
		return GroupStatus{}, errors.New("fenced transfer requires a data Group, membership cut and leader term")
	}
	expected.Conf = proto.Clone(expected.Conf).(*raftpb.ConfState)
	return g.transferLeadership(ctx, target, &expected, term)
}
func (h *Host) TransferLeadershipFenced(ctx context.Context, groupID string, target uint64, expected MembershipCut, term uint64) (GroupStatus, error) {
	g, err := h.group(groupID)
	if err != nil {
		return GroupStatus{}, err
	}
	return g.TransferLeadershipFenced(ctx, target, expected, term)
}
