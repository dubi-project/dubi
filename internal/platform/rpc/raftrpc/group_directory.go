package raftrpc

import (
	"errors"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/raft"
)

type GroupRoutes interface {
	Candidates(groupID string) []uint64
	ObserveLeader(groupID string, raftID uint64)
}

type groupRoute struct {
	members []uint64
	leader  uint64
}

// StaticGroupRoutes stores control-plane membership facts and runtime Leader
// hints. Leader hints never add a member that was not supplied by Update.
type StaticGroupRoutes struct {
	mu     sync.RWMutex
	routes map[string]groupRoute
}

func NewStaticGroupRoutes() *StaticGroupRoutes {
	return &StaticGroupRoutes{routes: make(map[string]groupRoute)}
}

func (routes *StaticGroupRoutes) Update(groupID string, members []uint64) error {
	if raft.ValidateGroupID(groupID) != nil || len(members) == 0 {
		return errors.New("Group route requires a valid GroupID and members")
	}
	seen := make(map[uint64]bool, len(members))
	copy := make([]uint64, 0, len(members))
	for _, member := range members {
		if member == 0 || seen[member] {
			return errors.New("Group route contains a zero or duplicate member")
		}
		seen[member] = true
		copy = append(copy, member)
	}
	routes.mu.Lock()
	previous := routes.routes[groupID]
	if !seen[previous.leader] {
		previous.leader = 0
	}
	previous.members = copy
	routes.routes[groupID] = previous
	routes.mu.Unlock()
	return nil
}

func (routes *StaticGroupRoutes) Candidates(groupID string) []uint64 {
	routes.mu.RLock()
	defer routes.mu.RUnlock()
	route := routes.routes[groupID]
	result := make([]uint64, 0, len(route.members))
	if route.leader != 0 {
		result = append(result, route.leader)
	}
	for _, member := range route.members {
		if member != route.leader {
			result = append(result, member)
		}
	}
	return result
}

func (routes *StaticGroupRoutes) ObserveLeader(groupID string, raftID uint64) {
	if raftID == 0 {
		return
	}
	routes.mu.Lock()
	defer routes.mu.Unlock()
	route, found := routes.routes[groupID]
	if !found {
		return
	}
	for _, member := range route.members {
		if member == raftID {
			route.leader = raftID
			routes.routes[groupID] = route
			return
		}
	}
}

func (routes *StaticGroupRoutes) Remove(groupID string) {
	routes.mu.Lock()
	delete(routes.routes, groupID)
	routes.mu.Unlock()
}
