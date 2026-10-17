package roottopology

import (
	"context"
	"errors"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type Host interface {
	ReadIndex(context.Context, string) (uint64, error)
	Status(string) (raft.GroupStatus, error)
	ReconfigureVoters(string, *raftpb.ConfState, []uint64) error
	TransferLeadership(context.Context, string, uint64) (raft.GroupStatus, error)
}

type Coordinator struct {
	host      Host
	root      *rootstate.State
	clusterID string
}

func New(host Host, root *rootstate.State, clusterID string) (*Coordinator, error) {
	if host == nil || root == nil || identity.ValidateClusterID(clusterID) != nil {
		return nil, errors.New("automatic Root topology requires Host, Root state and Cluster identity")
	}
	return &Coordinator{host: host, root: root, clusterID: clusterID}, nil
}

// Reconcile derives each step from a confirmed persistent membership cut.
// Raft's joint configuration is the durable in-progress record and auto-leave
// resumes after restart; no scheduler RAM or local cache authorizes a change.
// Perform at most one role-change proposal or leadership transfer per round.
func (c *Coordinator) Reconcile(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	status, err := c.host.Status(raft.RootGroupID)
	if err != nil {
		return err
	}
	if status.Role != etcdraft.StateLeader || status.LeaderID == 0 || status.LeaderID != status.RaftID {
		return nil
	}
	if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	// A newly enrolled Node is first a Root learner.  Do not derive the
	// durable voter plan from that transitional membership until the joining
	// process has completed its Root admission and can serve authenticated
	// traffic.  Otherwise a cross-Region join can demote an existing voter
	// while the new Node is still starting, leaving the control plane with a
	// membership cut that no participant can confirm.
	pending, err := c.root.PendingRootJoins()
	if err != nil {
		return err
	}
	if len(pending) != 0 {
		return nil
	}
	cut, err := c.root.RootRoutingView(rootjoin.MaxViewPeers)
	if err != nil {
		return err
	}
	// An admitted join is still only a Root learner until its own process has
	// replayed the local Root state and published the activation fence. Waiting
	// here prevents a cross-Region plan from demoting an existing voter while
	// the new process is still starting.
	for _, learner := range append(slices.Clone(cut.Membership.GetLearners()), cut.Membership.GetLearnersNext()...) {
		ready, err := c.root.RootMemberReady(learner)
		if err != nil {
			return err
		}
		if !ready {
			return nil
		}
	}
	view, err := rootjoin.FromRootCut(c.clusterID, cut, 0)
	if err != nil {
		return err
	}
	desired, err := DesiredVoters(view)
	if errors.Is(err, ErrJoint) {
		return nil
	}
	if err != nil {
		return err
	}
	current := slices.Clone(view.Voters)
	slices.Sort(current)
	if slices.Equal(current, desired) {
		return nil
	}
	// Do not remove the current enrollment-authority members in the same
	// membership cut that introduces a new voter.  The authority coordinator
	// must first install the next signer epoch on that voter while the old
	// authority majority is still present.  Stage the change as a temporary
	// expansion; the next reconciliation round will publish the exact desired
	// voter set after the new authority cut is durable.
	authority, found, err := c.root.EnrollmentAuthority()
	if err != nil {
		return err
	}
	if found {
		staged := slices.Clone(desired)
		removedAuthority := false
		for _, member := range authority.Members {
			if slices.Contains(current, member) && !slices.Contains(desired, member) {
				removedAuthority = true
				if !slices.Contains(staged, member) {
					staged = append(staged, member)
				}
			}
		}
		if removedAuthority {
			slices.Sort(staged)
			if len(staged) <= MaxRootVoters && !slices.Equal(staged, current) {
				desired = staged
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !slices.Contains(desired, status.RaftID) {
		for _, id := range desired {
			if slices.Contains(current, id) {
				_, err := c.host.TransferLeadership(ctx, raft.RootGroupID, id)
				return err
			}
		}
		return errors.New("Root topology has no retained voter for leader transfer")
	}
	err = c.host.ReconfigureVoters(raft.RootGroupID, cut.Membership, desired)
	if errors.Is(err, raft.ErrTopologyChanged) || errors.Is(err, raft.ErrTopologyPending) {
		// Catch-up/concurrent admission is expected; the next bounded round
		// must rederive the plan instead of weakening it to the live subset.
		return nil
	}
	return err
}
