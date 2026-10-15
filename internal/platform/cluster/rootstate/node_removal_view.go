package rootstate

import (
	"errors"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// NodeRemovalView is an owned, atomic applied Root cut, not a removal task.
// Obtain ReadIndex before using it. It includes pending tasks even when their
// desired membership excludes the Node being removed.
type NodeRemovalView struct {
	AppliedIndex     uint64
	Node             Node
	Membership       *raftpb.ConfState
	Fenced           bool
	Responsibilities NodeResponsibilities
	Groups           []Group
	PendingChanges   []GroupChange
}

func (s *State) NodeRemovalView(raftID uint64) (NodeRemovalView, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var view NodeRemovalView
	if err := s.usable(); err != nil {
		return view, false, err
	}
	if raftID == 0 {
		return view, false, errors.New("invalid Node removal identity")
	}
	encoded, found, err := s.get(nodeKey(raftID))
	if err != nil || !found {
		return view, found, err
	}
	if err := decodeStreamJSON(encoded, &view.Node); err != nil {
		return view, false, err
	}
	if view.Node.RaftID != raftID || !nodeIdentity.MatchString(view.Node.NodeID) {
		return view, false, errors.New("invalid Node removal descriptor")
	}
	fence, fenced, err := s.nodeFence(raftID)
	if err != nil {
		return view, false, err
	}
	if fenced && fence.NodeID != view.Node.NodeID {
		return view, false, errors.New("Node removal fence differs from descriptor")
	}
	view.AppliedIndex, view.Fenced = s.applied, fenced
	view.Membership = proto.Clone(s.conf).(*raftpb.ConfState)
	view.Responsibilities, err = s.nodeResponsibilities(raftID)
	if err != nil {
		return view, false, err
	}
	for _, groupID := range view.Responsibilities.CurrentGroups {
		encoded, found, err := s.get(groupKey(groupID))
		if err != nil {
			return view, false, err
		}
		var group Group
		if !found || decodeStreamJSON(encoded, &group) != nil || group.GroupID != groupID {
			return view, false, errors.New("invalid Node removal Group descriptor")
		}
		view.Groups = append(view.Groups, group)
	}
	for _, groupID := range view.Responsibilities.PendingGroups {
		active, found, err := s.get(activeGroupChangeKey(groupID))
		if err != nil {
			return view, false, err
		}
		if !found {
			return view, false, errors.New("Node removal pending Group has no active task")
		}
		encoded, found, err := s.get(groupChangeKey(string(active)))
		if err != nil {
			return view, false, err
		}
		var task GroupChange
		if !found || decodeStreamJSON(encoded, &task) != nil || task.GroupID != groupID || task.ChangeID != string(active) || task.State != GroupChangePending {
			return view, false, errors.New("invalid Node removal active Group task")
		}
		view.PendingChanges = append(view.PendingChanges, task)
	}
	return view, true, nil
}
