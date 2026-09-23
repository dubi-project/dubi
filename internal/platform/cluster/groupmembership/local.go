package groupmembership

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

type LocalBackend interface {
	ReadIndex(ctx context.Context, groupID string) (uint64, error)
	Membership(groupID string) (*raftpb.ConfState, error)
	Status(groupID string) (raft.GroupStatus, error)
	AddLearner(groupID string, raftID uint64) error
	PromoteLearner(groupID string, raftID uint64) error
	RemoveMember(groupID string, raftID uint64) error
	TransferLeadership(ctx context.Context, groupID string, target uint64) (raft.GroupStatus, error)
}

type LocalControl struct {
	backend LocalBackend
	joiner  JoiningControl
}

func NewLocalControl(backend LocalBackend, joiner JoiningControl) (*LocalControl, error) {
	if backend == nil || joiner == nil {
		return nil, errors.New("local Group membership control requires backend and join manager")
	}
	return &LocalControl{backend: backend, joiner: joiner}, nil
}

func (control *LocalControl) PrepareJoining(ctx context.Context, groupID string, raftID uint64) error {
	return control.joiner.PrepareJoining(ctx, groupID, raftID)
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
func (control *LocalControl) AddLearner(_ context.Context, groupID string, raftID uint64) error {
	return control.backend.AddLearner(groupID, raftID)
}
func (control *LocalControl) PromoteLearner(_ context.Context, groupID string, raftID uint64) error {
	return control.backend.PromoteLearner(groupID, raftID)
}
func (control *LocalControl) RemoveMember(_ context.Context, groupID string, raftID uint64) error {
	return control.backend.RemoveMember(groupID, raftID)
}
func (control *LocalControl) TransferLeadership(
	ctx context.Context, groupID string, target uint64,
) (raft.GroupStatus, error) {
	return control.backend.TransferLeadership(ctx, groupID, target)
}
