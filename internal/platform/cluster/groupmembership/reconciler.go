// Package groupmembership reconciles persistent Root membership tasks against
// a Group's durably applied Raft ConfState.
package groupmembership

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

var ErrLeadershipTransferred = errors.New("Group leadership transferred; resume reconciliation on the new leader")
var ErrVoterDemotion = errors.New("automatic Voter to Learner demotion is not supported")

type RootControl interface {
	Submit(groupID string, command []byte) error
	ReadIndex(ctx context.Context, groupID string) (uint64, error)
}

type GroupControl interface {
	PrepareJoining(ctx context.Context, groupID string, raftID uint64) error
	Membership(ctx context.Context, groupID string) (*raftpb.ConfState, error)
	Status(ctx context.Context, groupID string) (raft.GroupStatus, error)
	AddLearner(ctx context.Context, groupID string, raftID uint64) error
	PromoteLearner(ctx context.Context, groupID string, raftID uint64) error
	RemoveMember(ctx context.Context, groupID string, raftID uint64) error
	TransferLeadership(ctx context.Context, groupID string, target uint64) (raft.GroupStatus, error)
}

type Reconciler struct {
	rootControl  RootControl
	groupControl GroupControl
	root         *rootstate.State
}

func New(rootControl RootControl, groupControl GroupControl, root *rootstate.State) (*Reconciler, error) {
	if rootControl == nil || groupControl == nil || root == nil {
		return nil, errors.New("Group membership reconciler requires Root control, Group control, and Root state")
	}
	return &Reconciler{rootControl: rootControl, groupControl: groupControl, root: root}, nil
}

// Plan persists desired membership before any Raft ConfState mutation. Calling
// it again with the same plan is safe because the Root RequestID is stable.
func (reconciler *Reconciler) Plan(ctx context.Context, plan rootstate.GroupChangePlan) error {
	command := rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: commandRequestID("plan", plan.ChangeID), Type: rootstate.PlanGroupMembers,
		GroupPlan: &plan}
	return reconciler.submitRoot(ctx, command)
}

// Reconcile is restart-safe: each invocation derives the next action from the
// persistent Root task and the target Group's durably applied ConfState.
func (reconciler *Reconciler) Reconcile(ctx context.Context, changeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := reconciler.rootControl.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return fmt.Errorf("confirm Root membership task: %w", err)
	}
	change, found, err := reconciler.root.GroupChange(changeID)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("Group membership change is not registered")
	}
	if change.State == rootstate.GroupChangeCompleted {
		return nil
	}
	group, found, err := reconciler.root.Group(change.GroupID)
	if err != nil {
		return err
	}
	if !found || group.Epoch != change.ExpectedEpoch {
		return errors.New("Group membership task no longer matches the Root descriptor Epoch")
	}

	for {
		conf, err := reconciler.groupControl.Membership(ctx, change.GroupID)
		if err != nil {
			return err
		}
		actual, err := stableMembers(conf)
		if err != nil {
			return err
		}
		desired := memberRoles(change.Members)

		changed := false
		for _, member := range change.Members {
			role, present := actual[member.RaftID]
			if !present {
				if err := reconciler.groupControl.PrepareJoining(ctx, change.GroupID, member.RaftID); err != nil {
					return fmt.Errorf("prepare RaftID %d as joining Group: %w", member.RaftID, err)
				}
				if err := reconciler.groupControl.AddLearner(ctx, change.GroupID, member.RaftID); err != nil {
					return fmt.Errorf("add RaftID %d as Learner: %w", member.RaftID, err)
				}
				if err := reconciler.confirmTarget(ctx, change.GroupID); err != nil {
					return err
				}
				changed = true
				break
			}
			if member.Role == rootstate.GroupMemberVoter && role == rootstate.GroupMemberLearner {
				if err := reconciler.groupControl.PromoteLearner(ctx, change.GroupID, member.RaftID); err != nil {
					return fmt.Errorf("promote Learner %d: %w", member.RaftID, err)
				}
				if err := reconciler.confirmTarget(ctx, change.GroupID); err != nil {
					return err
				}
				changed = true
				break
			}
			if member.Role == rootstate.GroupMemberLearner && role == rootstate.GroupMemberVoter {
				return fmt.Errorf("RaftID %d: %w", member.RaftID, ErrVoterDemotion)
			}
		}
		if changed {
			continue
		}

		removed := false
		for _, raftID := range removalCandidates(conf, desired) {
			status, err := reconciler.groupControl.Status(ctx, change.GroupID)
			if err != nil {
				return err
			}
			if status.RaftID == raftID && status.LeaderID == raftID {
				target := transferTarget(change.Members, actual, raftID)
				if target == 0 {
					return errors.New("cannot remove local Leader without another desired Voter")
				}
				if _, err := reconciler.groupControl.TransferLeadership(ctx, change.GroupID, target); err != nil {
					return fmt.Errorf("transfer Group leadership to %d: %w", target, err)
				}
				return ErrLeadershipTransferred
			}
			if err := reconciler.groupControl.RemoveMember(ctx, change.GroupID, raftID); err != nil {
				return fmt.Errorf("remove RaftID %d: %w", raftID, err)
			}
			if err := reconciler.confirmTarget(ctx, change.GroupID); err != nil {
				return err
			}
			removed = true
			break
		}
		if removed {
			continue
		}
		if !sameMembers(actual, desired) {
			continue
		}
		completion := rootstate.Command{Version: rootstate.FormatVersion,
			RequestID: commandRequestID("complete", change.ChangeID), Type: rootstate.CompleteGroupMembers,
			GroupCompletion: &rootstate.GroupChangeCompletion{ChangeID: change.ChangeID, GroupID: change.GroupID}}
		return reconciler.submitRoot(ctx, completion)
	}
}

func (reconciler *Reconciler) ReconcilePending(ctx context.Context) error {
	if _, err := reconciler.rootControl.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return fmt.Errorf("confirm Root membership tasks: %w", err)
	}
	changes, err := reconciler.root.PendingGroupChanges()
	if err != nil {
		return err
	}
	var failures []error
	for _, change := range changes {
		if err := reconciler.Reconcile(ctx, change.ChangeID); err != nil {
			failures = append(failures, fmt.Errorf("reconcile Group membership change %q: %w", change.ChangeID, err))
		}
	}
	return errors.Join(failures...)
}

func (reconciler *Reconciler) confirmTarget(ctx context.Context, groupID string) error {
	_, err := reconciler.groupControl.Membership(ctx, groupID)
	return err
}

func (reconciler *Reconciler) submitRoot(ctx context.Context, command rootstate.Command) error {
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		return err
	}
	if err := reconciler.rootControl.Submit(raft.RootGroupID, encoded); err != nil {
		return fmt.Errorf("submit Root membership command: %w", err)
	}
	if _, err := reconciler.rootControl.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return fmt.Errorf("confirm Root membership command: %w", err)
	}
	result, found, err := reconciler.root.ResultForCommand(encoded)
	if err != nil {
		return err
	}
	if !found || result.Error != "" {
		return fmt.Errorf("Root membership command failed: found=%v error=%q", found, result.Error)
	}
	return nil
}

func stableMembers(conf *raftpb.ConfState) (map[uint64]string, error) {
	if conf == nil || len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return nil, errors.New("Group membership is in joint consensus")
	}
	members := make(map[uint64]string, len(conf.GetVoters())+len(conf.GetLearners()))
	for _, raftID := range conf.GetVoters() {
		members[raftID] = rootstate.GroupMemberVoter
	}
	for _, raftID := range conf.GetLearners() {
		if members[raftID] != "" {
			return nil, errors.New("RaftID has conflicting ConfState roles")
		}
		members[raftID] = rootstate.GroupMemberLearner
	}
	return members, nil
}

func memberRoles(members []rootstate.GroupMember) map[uint64]string {
	roles := make(map[uint64]string, len(members))
	for _, member := range members {
		roles[member.RaftID] = member.Role
	}
	return roles
}

func sameMembers(left, right map[uint64]string) bool {
	if len(left) != len(right) {
		return false
	}
	for raftID, role := range left {
		if right[raftID] != role {
			return false
		}
	}
	return true
}

func transferTarget(desired []rootstate.GroupMember, actual map[uint64]string, local uint64) uint64 {
	for _, member := range desired {
		if member.RaftID != local && member.Role == rootstate.GroupMemberVoter &&
			actual[member.RaftID] == rootstate.GroupMemberVoter {
			return member.RaftID
		}
	}
	return 0
}

func removalCandidates(conf *raftpb.ConfState, desired map[uint64]string) []uint64 {
	result := make([]uint64, 0, len(conf.GetLearners())+len(conf.GetVoters()))
	for _, raftID := range conf.GetLearners() {
		if desired[raftID] == "" {
			result = append(result, raftID)
		}
	}
	for _, raftID := range conf.GetVoters() {
		if desired[raftID] == "" {
			result = append(result, raftID)
		}
	}
	return result
}

func commandRequestID(stage, changeID string) string {
	digest := sha256.Sum256([]byte("dubi-group-membership-v1\x00" + stage + "\x00" + changeID))
	return "group-" + stage + "-" + hex.EncodeToString(digest[:12])
}
