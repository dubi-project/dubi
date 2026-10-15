package raftrpc

import (
	"slices"
	"sync"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

// RootDirectory projects persistent Node addresses and Group membership from
// the Root state. Root routes use its applied Raft ConfState, not a second
// self-describing Group record. Only the Leader preference is transient; it
// can never introduce a candidate absent from persistent membership.
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
	if _, fenced, err := directory.root.NodeFence(raftID); err != nil || fenced {
		return "", false
	}
	return node.Address, true
}

func (directory *RootDirectory) Candidates(groupID string) []uint64 {
	if directory == nil || directory.root == nil {
		return nil
	}
	members := directory.members(groupID)
	if len(members) == 0 {
		return nil
	}
	directory.mu.RLock()
	leader := directory.leaders[groupID]
	directory.mu.RUnlock()
	result := make([]uint64, 0, len(members))
	if slices.Contains(members, leader) {
		result = append(result, leader)
	}
	for _, member := range members {
		if member != leader {
			result = append(result, member)
		}
	}
	return result
}

func (directory *RootDirectory) ObserveLeader(groupID string, raftID uint64) {
	if directory == nil || directory.root == nil || raftID == 0 {
		return
	}
	if !slices.Contains(directory.members(groupID), raftID) {
		return
	}
	directory.mu.Lock()
	directory.leaders[groupID] = raftID
	directory.mu.Unlock()
}

func (directory *RootDirectory) members(groupID string) []uint64 {
	if groupID == raft.RootGroupID {
		conf, err := directory.root.RootMembership()
		if err != nil {
			return nil
		}
		seen := make(map[uint64]bool)
		var members []uint64
		for _, ids := range [][]uint64{conf.GetVoters(), conf.GetVotersOutgoing(), conf.GetLearners(), conf.GetLearnersNext()} {
			for _, id := range ids {
				if id != 0 && !seen[id] {
					seen[id] = true
					members = append(members, id)
				}
			}
		}
		slices.Sort(members)
		return members
	}
	group, found, err := directory.root.Group(groupID)
	if err != nil || !found {
		return nil
	}
	// An explicitly retired Region Group no longer participates in routing.
	// Its historical cohort remains in Root for repair/audit, while a future
	// same-name Region forms with fresh identities.
	if retired, err := directory.root.RegionGroupIsRetired(groupID); err == nil && retired {
		// Until the fresh cut is published, the old Group is not routable. In
		// the empty formation window route only to the exact new target cut;
		// after publication the descriptor must contain only those targets.
		rebuild, planned, err := directory.root.RegionGroupRebuild(groupID)
		if err != nil || !planned {
			return nil
		}
		if len(group.Members) == 0 {
			return slices.Clone(rebuild.TargetNodes)
		}
		for _, member := range group.Members {
			if !slices.Contains(rebuild.TargetNodes, member.RaftID) {
				return nil
			}
		}
	}
	members := make([]uint64, 0, len(group.Members))
	for _, member := range group.Members {
		members = append(members, member.RaftID)
	}
	return members
}
