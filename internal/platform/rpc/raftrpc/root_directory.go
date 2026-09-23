package raftrpc

import (
	"sync"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

// RootDirectory projects persistent Node addresses and Group membership from
// the Root state. Only the Leader preference is transient; it can never add a
// candidate that is absent from the current persistent Group descriptor.
type RootDirectory struct {
	root *rootstate.State

	mu      sync.RWMutex
	leaders map[string]uint64
}

var _ Directory = (*RootDirectory)(nil)
var _ GroupRoutes = (*RootDirectory)(nil)

func NewRootDirectory(root *rootstate.State) *RootDirectory {
	return &RootDirectory{root: root, leaders: make(map[string]uint64)}
}

func (directory *RootDirectory) Resolve(raftID uint64) (string, bool) {
	if directory == nil || directory.root == nil || raftID == 0 {
		return "", false
	}
	node, found, err := directory.root.Node(raftID)
	if err != nil || !found {
		return "", false
	}
	return node.Address, true
}

func (directory *RootDirectory) Candidates(groupID string) []uint64 {
	if directory == nil || directory.root == nil {
		return nil
	}
	group, found, err := directory.root.Group(groupID)
	if err != nil || !found || len(group.Members) == 0 {
		return nil
	}
	directory.mu.RLock()
	leader := directory.leaders[groupID]
	directory.mu.RUnlock()
	result := make([]uint64, 0, len(group.Members))
	if containsGroupMember(group.Members, leader) {
		result = append(result, leader)
	}
	for _, member := range group.Members {
		if member.RaftID != leader {
			result = append(result, member.RaftID)
		}
	}
	return result
}

func (directory *RootDirectory) ObserveLeader(groupID string, raftID uint64) {
	if directory == nil || directory.root == nil || raftID == 0 {
		return
	}
	group, found, err := directory.root.Group(groupID)
	if err != nil || !found || !containsGroupMember(group.Members, raftID) {
		return
	}
	directory.mu.Lock()
	directory.leaders[groupID] = raftID
	directory.mu.Unlock()
}

func containsGroupMember(members []rootstate.GroupMember, raftID uint64) bool {
	for _, member := range members {
		if member.RaftID == raftID {
			return true
		}
	}
	return false
}
