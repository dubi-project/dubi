package groupmembership

import (
	"context"
	"errors"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

type LocalBackend interface {
	ReadIndex(ctx context.Context, groupID string) (uint64, error)
	Membership(groupID string) (*raftpb.ConfState, error)
	Status(groupID string) (raft.GroupStatus, error)
	MembershipCut(groupID string) (raft.MembershipCut, error)
	ChangeMembership(groupID string, expected raft.MembershipCut, raftID uint64, operation raftpb.ConfChangeType) error
	TransferLeadershipFenced(ctx context.Context, groupID string, target uint64, expected raft.MembershipCut, term uint64) (raft.GroupStatus, error)
}

type LocalControl struct {
	backend LocalBackend
	joiner  JoiningControl
	root    *rootstate.State
	local   uint64
}

func NewLocalControl(backend LocalBackend, joiner JoiningControl, root *rootstate.State, local uint64) (*LocalControl, error) {
	if backend == nil || joiner == nil || root == nil || local == 0 {
		return nil, errors.New("local Group membership control requires backend and join manager")
	}
	return &LocalControl{backend: backend, joiner: joiner, root: root, local: local}, nil
}

func (control *LocalControl) PrepareJoining(ctx context.Context, task rootstate.GroupTaskReference, raftID uint64) error {
	if err := task.Validate(); err != nil {
		return err
	}
	if _, err := control.backend.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	if err := control.root.AuthorizeGroupPreparation(control.local, control.local, task, raftID); err != nil {
		return err
	}
	if err := control.joiner.PrepareJoining(ctx, task, raftID); err != nil {
		return err
	}
	if _, err := control.backend.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	if err := control.root.AuthorizeGroupPreparation(control.local, control.local, task, raftID); err != nil {
		return err
	}
	return ctx.Err()
}

func (control *LocalControl) Membership(ctx context.Context, groupID string) (*raftpb.ConfState, error) {
	if _, err := control.backend.ReadIndex(ctx, groupID); err != nil {
		return nil, err
	}
	return control.backend.Membership(groupID)
}
func (control *LocalControl) Status(_ context.Context, groupID string) (raft.GroupStatus, error) {
	return control.backend.Status(groupID)
}
func (control *LocalControl) ChangeMembership(ctx context.Context, mutation rootstate.GroupMutation) error {
	if err := mutation.Validate(); err != nil {
		return err
	}
	if _, err := control.backend.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	if err := control.root.AuthorizeGroupMutation(control.local, control.local, mutation); err != nil {
		return err
	}
	if _, err := control.backend.ReadIndex(ctx, mutation.GroupID); err != nil {
		return err
	}
	cut, err := control.backend.MembershipCut(mutation.GroupID)
	if err != nil {
		return err
	}
	// Data ReadIndex can wait while the Root task or caller qualification changes.
	if _, err := control.backend.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	if err := control.root.AuthorizeGroupMutation(control.local, control.local, mutation); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return control.backend.ChangeMembership(mutation.GroupID, cut, mutation.Member, mutation.Operation)
}
func (control *LocalControl) TransferLeadership(
	ctx context.Context, task rootstate.GroupTaskReference, target uint64,
) (raft.GroupStatus, error) {
	if err := task.Validate(); err != nil {
		return raft.GroupStatus{}, err
	}
	if _, err := control.backend.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return raft.GroupStatus{}, err
	}
	if err := control.root.AuthorizeGroupTransfer(control.local, control.local, task, target); err != nil {
		return raft.GroupStatus{}, err
	}
	if _, err := control.backend.ReadIndex(ctx, task.GroupID); err != nil {
		return raft.GroupStatus{}, err
	}
	cut, err := control.backend.MembershipCut(task.GroupID)
	if err != nil {
		return raft.GroupStatus{}, err
	}
	current, err := control.backend.Status(task.GroupID)
	if err != nil {
		return raft.GroupStatus{}, err
	}
	if _, err := control.backend.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return raft.GroupStatus{}, err
	}
	if err := control.root.AuthorizeGroupTransfer(control.local, control.local, task, target); err != nil {
		return raft.GroupStatus{}, err
	}
	result, err := control.backend.TransferLeadershipFenced(ctx, task.GroupID, target, cut, current.Term)
	if err != nil {
		return result, err
	}
	if _, err := control.backend.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return raft.GroupStatus{}, err
	}
	if err := control.root.AuthorizeGroupTransfer(control.local, control.local, task, target); err != nil {
		return raft.GroupStatus{}, err
	}
	return result, ctx.Err()
}
