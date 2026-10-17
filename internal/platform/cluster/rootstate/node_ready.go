package rootstate

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

func nodeReadyKey(raftID uint64) []byte {
	return []byte(fmt.Sprintf("data/node-ready/%020d", raftID))
}

// NodeReady reports the last local activation fence published by a Node.
// The marker is intentionally separate from RootJoin: admission and local
// recovery happen at different times and may be interrupted independently.
func (s *State) NodeReady(raftID uint64) (NodeReadyRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return NodeReadyRecord{}, false, err
	}
	encoded, found, err := s.get(nodeReadyKey(raftID))
	if err != nil || !found {
		return NodeReadyRecord{}, found, err
	}
	var ready NodeReadyRecord
	if err := decodeStreamJSON(encoded, &ready); err != nil {
		return NodeReadyRecord{}, false, err
	}
	if ready.RaftID != raftID || ready.RaftID == 0 || ready.ActivatedIndex == 0 || !nodeIdentity.MatchString(ready.NodeID) {
		return NodeReadyRecord{}, false, errors.New("invalid Node readiness record")
	}
	return ready, true, nil
}

// RootMemberReady is used by topology before promoting a Root learner. Nodes
// created before readiness publication have no RootJoin task and remain
// eligible for legacy recovery; admitted joins must publish the marker.
func (s *State) RootMemberReady(raftID uint64) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return false, err
	}
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/root-join/"), UpperBound: []byte("data/root-join0")})
	if err != nil {
		return false, err
	}
	defer iterator.Close()
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var task RootJoin
		if err := decodeStreamJSON(iterator.Value(), &task); err != nil {
			return false, err
		}
		if task.RaftID != raftID {
			continue
		}
		if err := task.Validate(); err != nil {
			return false, err
		}
		if task.State != RootJoinAdmitted {
			return false, nil
		}
		encoded, found, err := s.get(nodeReadyKey(raftID))
		if err != nil || !found {
			return false, err
		}
		var ready NodeReadyRecord
		if err := decodeStreamJSON(encoded, &ready); err != nil {
			return false, err
		}
		return ready.RaftID == raftID && ready.NodeID == task.NodeID && ready.ActivatedIndex != 0, nil
	}
	// A learner without a durable RootJoin task belongs to an older/local
	// formation path. Preserve its historical topology behavior.
	return true, iterator.Error()
}

func (s *State) applyNodeReady(batch *pebble.Batch, value NodeReadyRecord) (string, error) {
	encoded, found, err := s.get(nodeKey(value.RaftID))
	if err != nil {
		return "", err
	}
	if !found {
		return "Node is not registered", nil
	}
	var node Node
	if err := decodeStreamJSON(encoded, &node); err != nil {
		return "", err
	}
	if node.NodeID != value.NodeID || node.RaftID != value.RaftID {
		return "Node identity does not match", nil
	}
	if _, fenced, err := s.nodeFence(value.RaftID); err != nil {
		return "", err
	} else if fenced {
		return "Node is fenced", nil
	}
	return "", setJSON(batch, nodeReadyKey(value.RaftID), value)
}
