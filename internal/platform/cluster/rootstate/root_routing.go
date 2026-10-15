package rootstate

import (
	"errors"
	"fmt"
	"slices"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// RootRouting is one applied-state cut, not independently sampled membership
// and addresses. Callers needing linearizability must obtain ReadIndex first.
// All returned memory belongs to the caller.
type RootRouting struct {
	AppliedIndex uint64
	Membership   *raftpb.ConfState
	Nodes        []Node
}

var ErrRootRoutingLimit = errors.New("Root routing view exceeds peer limit")

// RootMember samples the current applied membership and its Node descriptor
// under one lock. Historical registration alone is not membership. As with
// RootRoutingView, authorization callers must first obtain a Root ReadIndex.
// The boolean is false for a nonmember; a member missing its descriptor fails
// closed, as do uncertainty, restore and closed-state fences.
func (s *State) RootMember(raftID uint64) (Node, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return Node{}, false, err
	}
	return s.rootMember(raftID)
}

// RootMembers reads a bounded authorization set from one applied-state cut.
// In particular, a signer and its caller must not be admitted by mixing two
// different memberships during concurrent changes. The bound covers one
// Region cohort and does not constrain Cluster size. Obtain ReadIndex first
// for authorization.
func (s *State) RootMembers(raftIDs ...uint64) ([]Node, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return nil, false, err
	}
	if len(raftIDs) == 0 || len(raftIDs) > MaxRegionCohortNodes {
		return nil, false, errors.New("invalid Root authorization set size")
	}
	nodes := make([]Node, 0, len(raftIDs))
	for _, id := range raftIDs {
		node, member, err := s.rootMember(id)
		if err != nil || !member {
			return nil, false, err
		}
		nodes = append(nodes, node)
	}
	return nodes, true, nil
}

// Caller holds s.mu and has checked the state fence.
func (s *State) rootMember(raftID uint64) (Node, bool, error) {
	if raftID == 0 {
		return Node{}, false, errors.New("Root member identity is zero")
	}
	member := false
	for _, ids := range [][]uint64{s.conf.GetVoters(), s.conf.GetVotersOutgoing(), s.conf.GetLearners(), s.conf.GetLearnersNext()} {
		member = member || slices.Contains(ids, raftID)
	}
	if !member {
		return Node{}, false, nil
	}
	node, err := s.rootMemberNode(raftID)
	return node, err == nil, err
}

// Caller holds s.mu and has checked the state fence and current membership.
func (s *State) rootMemberNode(id uint64) (Node, error) {
	if _, fenced, err := s.nodeFence(id); err != nil {
		return Node{}, err
	} else if fenced {
		return Node{}, errors.New("Root member identity is fenced")
	}
	encoded, found, err := s.get(nodeKey(id))
	if err != nil {
		return Node{}, err
	}
	if !found {
		return Node{}, fmt.Errorf("Root member %d has no registered address", id)
	}
	var node Node
	if err := decodeStreamJSON(encoded, &node); err != nil {
		return Node{}, err
	}
	command := Command{Version: FormatVersion, RequestID: "routing-validation", Type: RegisterNode, Node: &node}
	if err := command.validate(); err != nil || node.RaftID != id {
		return Node{}, errors.New("Root member descriptor is invalid")
	}
	return node, nil
}

// RootRoutingView fails closed on a missing/corrupt member descriptor. The
// bound is chosen by the consumer before cloning/allocating a network view;
// it does not impose a user-configured replica count or alter Root membership.
func (s *State) RootRoutingView(maxPeers int) (RootRouting, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return RootRouting{}, err
	}
	if maxPeers <= 0 {
		return RootRouting{}, ErrRootRoutingLimit
	}
	seen := make(map[uint64]bool)
	var ids []uint64
	for _, members := range [][]uint64{s.conf.GetVoters(), s.conf.GetVotersOutgoing(), s.conf.GetLearners(), s.conf.GetLearnersNext()} {
		if len(members) > maxPeers {
			return RootRouting{}, ErrRootRoutingLimit
		}
		for _, id := range members {
			if id == 0 {
				return RootRouting{}, errors.New("Root membership contains zero identity")
			}
			if !seen[id] {
				if len(ids) == maxPeers {
					return RootRouting{}, ErrRootRoutingLimit
				}
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	slices.Sort(ids)
	view := RootRouting{AppliedIndex: s.applied, Membership: proto.Clone(s.conf).(*raftpb.ConfState), Nodes: make([]Node, 0, len(ids))}
	seenNodes := make(map[string]bool)
	for _, id := range ids {
		node, err := s.rootMemberNode(id)
		if err != nil {
			return RootRouting{}, err
		}
		if seenNodes[node.NodeID] {
			return RootRouting{}, errors.New("Root member descriptor is invalid or duplicated")
		}
		seenNodes[node.NodeID] = true
		view.Nodes = append(view.Nodes, node)
	}
	return view, nil
}
