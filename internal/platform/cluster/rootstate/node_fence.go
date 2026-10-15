package rootstate

import (
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble"
)

// NodeFenceRequest is an explicit control-plane decision, never inferred from
// heartbeat or Volume health. Fencing is allowed for an unused identity or
// after its current Root/Group responsibilities have been deliberately
// removed; historical placement records do not block the tombstone.
type NodeFenceRequest struct {
	NodeID string `json:"node_id"`
	RaftID uint64 `json:"raft_id"`
}

type NodeFenceRecord struct {
	NodeFenceRequest
	FencedIndex uint64 `json:"fenced_index"`
}

// NodeResponsibilities is a read-only Root cut used by maintenance tooling.
// Health is deliberately absent: these are durable current/pending ownership
// claims, not liveness observations. Historical Region cohorts are not listed.
type NodeResponsibilities struct {
	RootMember       bool
	CurrentGroups    []string
	PendingGroups    []string
	PendingRootJoins []string
}

func (r NodeResponsibilities) Empty() bool {
	return !r.RootMember && len(r.CurrentGroups) == 0 && len(r.PendingGroups) == 0 && len(r.PendingRootJoins) == 0
}

func nodeFenceKey(id uint64) []byte { return []byte(fmt.Sprintf("data/node-fence/%020d", id)) }

func (s *State) NodeFence(raftID uint64) (NodeFenceRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return NodeFenceRecord{}, false, err
	}
	return s.nodeFence(raftID)
}

// NodeResponsibilities returns one durable Root view of all responsibilities
// that must be removed before an identity can be fenced. Callers can use this
// to present a simple manual-removal preflight without creating a replacement
// task or changing state.
func (s *State) NodeResponsibilities(raftID uint64) (NodeResponsibilities, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return NodeResponsibilities{}, err
	}
	return s.nodeResponsibilities(raftID)
}

func (s *State) nodeResponsibilities(raftID uint64) (NodeResponsibilities, error) {
	if raftID == 0 {
		return NodeResponsibilities{}, errors.New("invalid Node responsibility identity")
	}
	responsibilities := NodeResponsibilities{}
	currentGroups := make(map[string]bool)
	for _, ids := range [][]uint64{s.conf.GetVoters(), s.conf.GetVotersOutgoing(), s.conf.GetLearners(), s.conf.GetLearnersNext()} {
		if slices.Contains(ids, raftID) {
			responsibilities.RootMember = true
			break
		}
	}
	for _, prefix := range []string{"data/group/", "data/group-change/", "data/root-join/"} {
		iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(prefix), UpperBound: []byte(prefix[:len(prefix)-1] + "0")})
		if err != nil {
			return NodeResponsibilities{}, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			switch prefix {
			case "data/group/":
				var group Group
				if err := decodeStreamJSON(iter.Value(), &group); err != nil {
					_ = iter.Close()
					return NodeResponsibilities{}, err
				}
				retired, err := s.regionGroupRetired(group.GroupID)
				if err != nil {
					_ = iter.Close()
					return NodeResponsibilities{}, err
				}
				if retired {
					continue
				}
				for _, member := range group.Members {
					if member.RaftID == raftID {
						responsibilities.CurrentGroups = append(responsibilities.CurrentGroups, group.GroupID)
						currentGroups[group.GroupID] = true
						break
					}
				}
			case "data/group-change/":
				var change GroupChange
				if err := decodeStreamJSON(iter.Value(), &change); err != nil {
					_ = iter.Close()
					return NodeResponsibilities{}, err
				}
				retired, err := s.regionGroupRetired(change.GroupID)
				if err != nil {
					_ = iter.Close()
					return NodeResponsibilities{}, err
				}
				if retired {
					continue
				}
				if change.State == GroupChangePending {
					pending := currentGroups[change.GroupID]
					for _, member := range change.Members {
						if member.RaftID == raftID {
							pending = true
							break
						}
					}
					if pending {
						responsibilities.PendingGroups = append(responsibilities.PendingGroups, change.GroupID)
					}
				}
			case "data/root-join/":
				var join RootJoin
				if err := decodeStreamJSON(iter.Value(), &join); err != nil {
					_ = iter.Close()
					return NodeResponsibilities{}, err
				}
				if join.State == RootJoinPending && join.RaftID == raftID {
					responsibilities.PendingRootJoins = append(responsibilities.PendingRootJoins, join.GrantID)
				}
			}
		}
		if err := errors.Join(iter.Error(), iter.Close()); err != nil {
			return NodeResponsibilities{}, err
		}
	}
	return responsibilities, nil
}

func (s *State) nodeFence(raftID uint64) (NodeFenceRecord, bool, error) {
	if raftID == 0 {
		return NodeFenceRecord{}, false, errors.New("invalid Node fence identity")
	}
	encoded, found, err := s.get(nodeFenceKey(raftID))
	if err != nil || !found {
		return NodeFenceRecord{}, found, err
	}
	var record NodeFenceRecord
	if err := decodeStreamJSON(encoded, &record); err != nil {
		return NodeFenceRecord{}, false, err
	}
	if !nodeIdentity.MatchString(record.NodeID) || record.RaftID != raftID || record.FencedIndex == 0 || record.FencedIndex > s.applied {
		return NodeFenceRecord{}, false, errors.New("invalid Node fence record")
	}
	return record, true, nil
}

func (s *State) applyNodeFence(batch *pebble.Batch, request NodeFenceRequest) (string, error) {
	if existing, found, err := s.nodeFence(request.RaftID); err != nil {
		return "", err
	} else if found {
		if existing.NodeID == request.NodeID {
			return "", nil
		}
		return "Node fence belongs to another identity", nil
	}
	encoded, found, err := s.get(nodeKey(request.RaftID))
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
	if node.NodeID != request.NodeID || node.RaftID != request.RaftID {
		return "Node fence differs from registered identity", nil
	}
	responsibilities, err := s.nodeResponsibilities(request.RaftID)
	if err != nil {
		return "", err
	}
	if responsibilities.RootMember {
		return "Node is still a Root member", nil
	}
	if len(responsibilities.CurrentGroups) != 0 || len(responsibilities.PendingGroups) != 0 || len(responsibilities.PendingRootJoins) != 0 {
		return "Node still has current or pending cluster responsibility", nil
	}
	return "", setJSON(batch, nodeFenceKey(request.RaftID), NodeFenceRecord{NodeFenceRequest: request, FencedIndex: s.applied + 1})
}
