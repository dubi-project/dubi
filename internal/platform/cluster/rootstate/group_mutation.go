package rootstate

import (
	"errors"
	"fmt"
	"maps"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

// GroupMutation is internal intent, never a free-standing permission. The
// referenced task must still be active at the expected descriptor epoch.
type GroupMutation struct {
	GroupID       string
	ChangeID      string
	ExpectedEpoch uint64
	Member        uint64
	Operation     raftpb.ConfChangeType
}

var ErrGroupMutationUnauthorized = errors.New("Group mutation is not authorized by the current Root task")

// GroupTaskReference is carried unchanged from the reconciler to every action,
// including local state construction. Never infer a maintenance task by GroupID.
type GroupTaskReference struct {
	GroupID       string
	ChangeID      string
	ExpectedEpoch uint64
}

func (r GroupTaskReference) Validate() error {
	if raft.ValidateGroupID(r.GroupID) != nil || r.GroupID == raft.RootGroupID || !validID(r.ChangeID) || r.ExpectedEpoch == 0 {
		return ErrGroupMutationUnauthorized
	}
	return nil
}

func (r GroupTaskReference) mutation(member uint64, operation raftpb.ConfChangeType) GroupMutation {
	return GroupMutation{GroupID: r.GroupID, ChangeID: r.ChangeID, ExpectedEpoch: r.ExpectedEpoch, Member: member, Operation: operation}
}

func validateMaintenanceRemoval(current Group, desired []GroupMember) error {
	if len(desired) >= len(current.Members) {
		return errors.New("maintenance Group change must remove at least one member")
	}
	currentRoles := make(map[uint64]string, len(current.Members))
	currentVoters := 0
	for _, member := range current.Members {
		currentRoles[member.RaftID] = member.Role
		if member.Role == GroupMemberVoter {
			currentVoters++
		}
	}
	desiredRoles := make(map[uint64]string, len(desired))
	desiredVoters := 0
	for _, member := range desired {
		if currentRoles[member.RaftID] != member.Role {
			return fmt.Errorf("maintenance Group change may only remove existing members")
		}
		desiredRoles[member.RaftID] = member.Role
		if member.Role == GroupMemberVoter {
			desiredVoters++
		}
	}
	if desiredVoters < currentVoters/2+1 {
		return fmt.Errorf("maintenance Group change would remove the current voter majority")
	}
	if len(desiredRoles) == 0 || maps.Equal(currentRoles, desiredRoles) {
		return errors.New("maintenance Group change has no removable member")
	}
	return nil
}

func (s *State) AuthorizeGroupPreparation(caller, server uint64, task GroupTaskReference, target uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if target != server || task.Validate() != nil {
		return ErrGroupMutationUnauthorized
	}
	return s.authorizeGroupMutation(caller, server, task.mutation(target, raftpb.ConfChangeType_ConfChangeAddLearnerNode), false)
}

func (s *State) AuthorizeGroupTransfer(caller, server uint64, task GroupTaskReference, target uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if target == server || task.Validate() != nil {
		return ErrGroupMutationUnauthorized
	}
	return s.authorizeGroupMutation(caller, server, task.mutation(target, raftpb.ConfChangeType_ConfChangeAddNode), true)
}

func (m GroupMutation) Validate() error {
	if raft.ValidateGroupID(m.GroupID) != nil || m.GroupID == raft.RootGroupID || !validID(m.ChangeID) || m.ExpectedEpoch == 0 || m.Member == 0 {
		return ErrGroupMutationUnauthorized
	}
	switch m.Operation {
	case raftpb.ConfChangeType_ConfChangeAddLearnerNode, raftpb.ConfChangeType_ConfChangeAddNode, raftpb.ConfChangeType_ConfChangeRemoveNode:
		return nil
	default:
		return ErrGroupMutationUnauthorized
	}
}

// AuthorizeGroupMutation samples caller/server eligibility, task linkage and
// Group epoch under one Root lock. It is a point-in-time check, not a cross-
// Group transaction; callers must first obtain Root ReadIndex and then use a
// target Group membership-position CAS. It does not authorize Root mutations.
func (s *State) AuthorizeGroupMutation(caller, server uint64, m GroupMutation) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authorizeGroupMutation(caller, server, m, false)
}

func (s *State) authorizeGroupMutation(caller, server uint64, m GroupMutation, transfer bool) error {
	if err := s.usable(); err != nil {
		return err
	}
	if m.Validate() != nil || caller == 0 || server == 0 {
		return ErrGroupMutationUnauthorized
	}
	for _, id := range []uint64{caller, server} {
		if _, member, err := s.rootMember(id); err != nil {
			return err
		} else if !member {
			return ErrGroupMutationUnauthorized
		}
	}
	active, found, err := s.get(activeGroupChangeKey(m.GroupID))
	if err != nil {
		return err
	}
	if !found || string(active) != m.ChangeID {
		return ErrGroupMutationUnauthorized
	}
	encoded, found, err := s.get(groupChangeKey(m.ChangeID))
	if err != nil {
		return err
	}
	if !found {
		return errors.New("active Group task is missing")
	}
	var task GroupChange
	if err := decodeStreamJSON(encoded, &task); err != nil {
		return err
	}
	if task.ChangeID != m.ChangeID || task.GroupID != m.GroupID || task.State != GroupChangePending || task.ExpectedEpoch != m.ExpectedEpoch || task.CompletedEpoch != 0 || validateGroupMembers(task.Members) != nil {
		return ErrGroupMutationUnauthorized
	}
	var taskErr error
	if task.Maintenance {
		taskErr = s.authorizeMaintenanceTask(task)
	} else {
		taskErr = s.authorizeGrowthTask(task)
	}
	if err := taskErr; err != nil {
		return err
	}
	encoded, found, err = s.get(groupKey(m.GroupID))
	if err != nil {
		return err
	}
	if !found {
		return errors.New("Group mutation descriptor is missing")
	}
	var group Group
	if err := decodeStreamJSON(encoded, &group); err != nil {
		return err
	}
	if group.GroupID != m.GroupID || group.StateKind != GroupStateCapability || group.Epoch != m.ExpectedEpoch {
		return ErrGroupMutationUnauthorized
	}
	if len(group.Members) == 0 && group.Epoch != 1 || len(group.Members) != 0 && validateGroupMembers(group.Members) != nil {
		return errors.New("Group mutation descriptor membership is invalid")
	}
	role := func(members []GroupMember, id uint64) string {
		for _, member := range members {
			if member.RaftID == id {
				return member.Role
			}
		}
		return ""
	}
	if role(group.Members, server) == "" && role(task.Members, server) == "" {
		return ErrGroupMutationUnauthorized
	}
	// Membership-task transfers are only for retiring/demoting this leader.
	// Arbitrary balancing is a different workflow, not an implied permission.
	if transfer && role(task.Members, server) == GroupMemberVoter {
		return ErrGroupMutationUnauthorized
	}
	desired := role(task.Members, m.Member)
	switch m.Operation {
	case raftpb.ConfChangeType_ConfChangeRemoveNode:
		if desired != "" || role(group.Members, m.Member) == "" {
			return ErrGroupMutationUnauthorized
		}
	case raftpb.ConfChangeType_ConfChangeAddNode:
		if desired != GroupMemberVoter {
			return ErrGroupMutationUnauthorized
		}
	case raftpb.ConfChangeType_ConfChangeAddLearnerNode:
		if desired == "" {
			return ErrGroupMutationUnauthorized
		}
	}
	if m.Operation != raftpb.ConfChangeType_ConfChangeRemoveNode {
		if _, member, err := s.rootMember(m.Member); err != nil {
			return err
		} else if !member {
			return ErrGroupMutationUnauthorized
		}
	}
	return nil
}
