package noderemoval

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var ErrLeaderChanged = errors.New("Node removal must resume on the current Root leader")

type RootHost interface {
	Submit(string, []byte) error
	ReadIndex(context.Context, string) (uint64, error)
	Status(string) (raft.GroupStatus, error)
	RemoveRootMember(*raftpb.ConfState, uint64) error
	TransferLeadership(context.Context, string, uint64) (raft.GroupStatus, error)
}

// GroupReconciler is implemented by groupmembership.Reconciler. No simulated
// Root descriptor updates may substitute for actual Raft membership changes.
type GroupReconciler interface {
	CheckAvailable(context.Context, string) error
	Plan(context.Context, rootstate.GroupChangePlan) error
	Reconcile(context.Context, string) error
}

type RaftExecutor struct {
	root   *rootstate.State
	host   RootHost
	groups GroupReconciler
}

func NewRaftExecutor(root *rootstate.State, host RootHost, groups GroupReconciler) (*RaftExecutor, error) {
	if root == nil || host == nil || groups == nil {
		return nil, errors.New("Node removal executor requires Root, Host and Group reconciler")
	}
	return &RaftExecutor{root: root, host: host, groups: groups}, nil
}

func (e *RaftExecutor) ConfirmRoot(ctx context.Context) error {
	if ctx == nil {
		return errors.New("Node removal context required")
	}
	// Reject followers before ReadIndex: a forwarded management request
	// must not spend its deadline waiting on a member that cannot execute it.
	status, err := e.host.Status(raft.RootGroupID)
	if err != nil {
		return err
	}
	if status.Role != etcdraft.StateLeader || status.LeaderID == 0 || status.RaftID != status.LeaderID {
		return ErrLeaderChanged
	}
	if _, err := e.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	status, err = e.host.Status(raft.RootGroupID)
	if err != nil {
		return err
	}
	if status.Role != etcdraft.StateLeader || status.LeaderID == 0 || status.RaftID != status.LeaderID {
		return ErrLeaderChanged
	}
	return nil
}

func (e *RaftExecutor) PlanGroup(ctx context.Context, plan rootstate.GroupChangePlan) error {
	if err := e.ConfirmRoot(ctx); err != nil {
		return err
	}
	if !plan.Maintenance {
		return errors.New("Node removal requires a maintenance Group plan")
	}
	return e.groups.Plan(ctx, plan)
}

func (e *RaftExecutor) CheckGroup(ctx context.Context, groupID string) error {
	if err := e.ConfirmRoot(ctx); err != nil {
		return err
	}
	if err := e.groups.CheckAvailable(ctx, groupID); err != nil {
		return fmt.Errorf("%w: %v", ErrGroupUnavailable, err)
	}
	return nil
}

func (e *RaftExecutor) ReconcileGroup(ctx context.Context, changeID string) error {
	if err := e.ConfirmRoot(ctx); err != nil {
		return err
	}
	return e.groups.Reconcile(ctx, changeID)
}

// RetireRegionGroup records the explicit administrative decision that an
// unreachable Region Group is no longer recoverable in its current identity.
// It changes only Root control state; data recovery and the same-name Region's
// next formation are handled by the normal join/repair path.
func (e *RaftExecutor) RetireRegionGroup(ctx context.Context, groupID string) error {
	if err := e.ConfirmRoot(ctx); err != nil {
		return err
	}
	progress, found, err := e.root.RegionProgress(groupID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("Region Group %q has no confirmed cohort", groupID)
	}
	command, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: fmt.Sprintf("retire-region-group-%s-%d", groupID, e.root.AppliedIndex()),
		Type:      rootstate.RetireRegionGroup, GroupRetirement: &rootstate.RegionGroupRetirement{
			GroupID: groupID, RegionID: progress.Current.RegionID, Generation: progress.Current.Generation,
			GroupEpoch: progress.Current.GroupEpoch, FenceIndex: progress.Current.GroupFenceIndex,
			Nodes: slices.Clone(progress.Current.Nodes),
		}})
	if err != nil {
		return err
	}
	proposalErr := e.host.Submit(raft.RootGroupID, command)
	result, err := e.waitCommandResult(ctx, command)
	if err != nil {
		return errors.Join(proposalErr, err)
	}
	if result.Error != "" {
		return errors.Join(proposalErr, fmt.Errorf("Region Group retirement not confirmed: error=%q", result.Error))
	}
	return nil
}

func (e *RaftExecutor) RemoveRootMember(ctx context.Context, target uint64) error {
	if err := e.ConfirmRoot(ctx); err != nil {
		return err
	}
	plan, err := Build(e.root, target)
	if err != nil {
		return err
	}
	if len(plan.Groups) != 0 {
		return ErrGroupStillPresent
	}
	if !plan.RootMember {
		return nil
	}
	status, err := e.host.Status(raft.RootGroupID)
	if err != nil {
		return err
	}
	if status.RaftID == target {
		for _, voter := range plan.RootMembership.GetVoters() {
			if voter != target {
				if _, err := e.host.TransferLeadership(ctx, raft.RootGroupID, voter); err != nil {
					return err
				}
				return ErrLeaderChanged
			}
		}
		return ErrRemovalWouldLoseMajority
	}
	proposalErr := e.host.RemoveRootMember(plan.RootMembership, target)
	if err := e.ConfirmRoot(ctx); err != nil {
		return errors.Join(proposalErr, err)
	}
	conf, err := e.root.RootMembership()
	if err != nil {
		return errors.Join(proposalErr, err)
	}
	for _, members := range [][]uint64{conf.GetVoters(), conf.GetVotersOutgoing(), conf.GetLearners(), conf.GetLearnersNext()} {
		if slices.Contains(members, target) {
			return errors.Join(proposalErr, ErrRootMemberStillPresent)
		}
	}
	return nil // Membership proves success even when the proposal reply was lost.
}

func (e *RaftExecutor) FenceNode(ctx context.Context, request rootstate.NodeFenceRequest) error {
	if err := e.ConfirmRoot(ctx); err != nil {
		return err
	}
	// A temporary rejection is durable. Bind retries to a fresh Root cut so a
	// later successful removal cannot inherit that old rejection.
	command, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: fmt.Sprintf("node-fence-%d-%d", request.RaftID, e.root.AppliedIndex()),
		Type:      rootstate.FenceNode, NodeFence: &request})
	if err != nil {
		return err
	}
	proposalErr := e.host.Submit(raft.RootGroupID, command)
	result, err := e.waitCommandResult(ctx, command)
	if err != nil {
		return errors.Join(proposalErr, err)
	}
	if result.Error != "" {
		return errors.Join(proposalErr, fmt.Errorf("Node Fence not confirmed: error=%q", result.Error))
	}
	return nil
}

// waitCommandResult closes the small race between Submit returning and the
// Root state machine applying the command. A successful Raft proposal may
// lose its reply during a leader change; the durable command result is the
// authority, not the first read after Submit.
func (e *RaftExecutor) waitCommandResult(ctx context.Context, command []byte) (rootstate.Result, error) {
	var zero rootstate.Result
	if ctx == nil {
		return zero, errors.New("Node removal context required")
	}
	for {
		if _, err := e.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			return zero, err
		}
		result, found, err := e.root.ResultForCommand(command)
		if err != nil {
			return zero, err
		}
		if found {
			return result, nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-timer.C:
		}
	}
}
