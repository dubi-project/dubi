// Package regiongroups forms engine-neutral regional Groups from immutable
// Root plans. Formation/routing publication does not enable LocalReady/GeoReady.
package regiongroups

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Host interface {
	Status(string) (raft.GroupStatus, error)
	Membership(string) (*raftpb.ConfState, error)
	ReadIndex(context.Context, string) (uint64, error)
	Submit(string, []byte) error
}
type Inspector interface {
	ReadIndex(context.Context, uint64, string) (uint64, error)
	InspectMembership(context.Context, uint64, string) (*raftpb.ConfState, uint64, error)
}
type Recovery interface{ RecoverLocalGroups(context.Context) error }
type Coordinator struct {
	host    Host
	root    *rootstate.State
	inspect Inspector
	recover Recovery
	cursor  string // Scheduling fairness only; never formation authority.
}

func New(host Host, root *rootstate.State, inspect Inspector, recovery Recovery) (*Coordinator, error) {
	if host == nil || root == nil || inspect == nil || recovery == nil {
		return nil, errors.New("regional formation requires Root, Host, inspection and recovery")
	}
	return &Coordinator{host: host, root: root, inspect: inspect, recover: recovery}, nil
}

func GroupID(region string) string {
	digest := sha256.Sum256([]byte(region))
	return "region-" + hex.EncodeToString(digest[:])
}

// Reconcile is called serially on EVERY node. Only actual Root leaders plan
// and publish; all nodes recover their own authorized initial participants.
func (c *Coordinator) Reconcile(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	status, err := c.host.Status(raft.RootGroupID)
	if err != nil {
		return err
	}
	leader := status.Role == etcdraft.StateLeader && status.LeaderID == status.RaftID && status.LeaderID != 0
	var planErr error
	if leader {
		planErr = c.plan(ctx)
	}
	recoverErr := c.recover.RecoverLocalGroups(ctx)
	if !leader || ctx.Err() != nil {
		return errors.Join(planErr, recoverErr, ctx.Err())
	}
	return errors.Join(planErr, recoverErr, c.publishOne(ctx))
}

func (c *Coordinator) plan(ctx context.Context) error {
	if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	// Root learners are not yet ready to host a regional Group.  Wait for the
	// join workflow to publish durable admission before capturing a cohort;
	// otherwise formation can race the learner's startup and publish a plan
	// containing a Node that cannot observe or confirm it.
	pending, err := c.root.PendingRootJoins()
	if err != nil {
		return err
	}
	if len(pending) != 0 {
		return nil
	}
	cut, err := c.root.RootRoutingView(rootstate.MaxRegionCohortNodes)
	if err != nil {
		return err
	}
	// Admission/topology may still be in joint consensus. Do not capture an
	// intermediate configuration as an initial formation cohort.
	if len(cut.Membership.GetVotersOutgoing()) != 0 || len(cut.Membership.GetLearnersNext()) != 0 || cut.Membership.GetAutoLeave() {
		return nil
	}
	regions := map[string][]uint64{}
	for _, node := range cut.Nodes {
		regions[node.RegionID] = append(regions[node.RegionID], node.RaftID)
	}
	keys := make([]string, 0, len(regions))
	for region := range regions {
		keys = append(keys, region)
	}
	sort.Strings(keys)
	for _, region := range keys {
		nodes := regions[region]
		if len(nodes) == 0 {
			continue
		}
		slices.Sort(nodes)
		head, found, err := c.root.RegionCohortHead(region)
		if err != nil {
			return err
		}
		id := GroupID(region)
		if found {
			// A retired Region keeps its history, but a fresh set of Nodes may
			// form the same named Group again. The two Root commands are
			// intentionally automatic and are not a user-facing rebuild flow.
			if retired, err := c.root.RegionGroupIsRetired(id); err != nil {
				return err
			} else if !retired {
				continue
			}
			if head.PendingGeneration != 0 {
				continue
			}
			if _, planned, err := c.root.RegionGroupRebuild(id); err != nil {
				return err
			} else if !planned {
				previous, previousFound, err := c.root.RegionCohort(region, head.CurrentGeneration)
				if err != nil || !previousFound {
					return errors.New("retired Region has no confirmed cohort")
				}
				old := make(map[uint64]bool, len(previous.Nodes))
				for _, oldID := range previous.Nodes {
					old[oldID] = true
					if _, member, err := c.root.RootMember(oldID); err != nil {
						return err
					} else if member {
						// Wait until the administrator has explicitly removed every
						// old Node. Recovery of one old Node must remain possible
						// before that decision is complete.
						old = nil
						break
					}
				}
				if old == nil {
					continue
				}
				fresh := make([]uint64, 0, len(nodes))
				for _, nodeID := range nodes {
					if !old[nodeID] {
						fresh = append(fresh, nodeID)
					}
				}
				if len(fresh) == 0 {
					continue
				}
				if err := c.submit(ctx, rootstate.Command{Type: rootstate.PlanRegionGroupRebuild, GroupRebuild: &rootstate.RegionGroupRebuild{
					GroupID: id, RegionID: region, PreviousGeneration: previous.Generation,
					PreviousGroupEpoch: previous.GroupEpoch, PreviousFenceIndex: previous.GroupFenceIndex,
					PreviousNodes: slices.Clone(previous.Nodes), TargetGeneration: previous.Generation + 1,
					TargetNodes: slices.Clone(fresh),
				}}); err != nil {
					return err
				}
				continue
			}
			if err := c.submit(ctx, rootstate.Command{Type: rootstate.PlanRegionCohort, RegionPlan: &rootstate.RegionCohortPlan{
				RegionID: region, GroupID: id, ExpectedGeneration: head.CurrentGeneration, Nodes: slices.Clone(nodes),
			}}); err != nil {
				return err
			}
			continue
		}
		group, found, err := c.root.Group(id)
		if err != nil {
			return err
		}
		if !found {
			if err := c.submit(ctx, rootstate.Command{Type: rootstate.RegisterGroup, Group: &rootstate.Group{GroupID: id, StateKind: rootstate.GroupStateEngine}}); err != nil {
				return err
			}
		} else if group.StateKind != rootstate.GroupStateEngine || group.Epoch != 1 || len(group.Members) != 0 {
			return errors.New("automatic regional GroupID is occupied by another formation")
		}
		if err := c.submit(ctx, rootstate.Command{Type: rootstate.PlanRegionCohort, RegionPlan: &rootstate.RegionCohortPlan{RegionID: region, GroupID: id, Nodes: nodes}}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) publishOne(ctx context.Context) error {
	if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	groups, err := c.root.Groups()
	if err != nil {
		return err
	}
	var pending []rootstate.Group
	for _, group := range groups {
		if group.GroupID == raft.RootGroupID || group.Epoch != 1 || len(group.Members) != 0 {
			continue
		}
		if _, found, err := c.root.PendingRegionFormation(group.GroupID); err != nil {
			return err
		} else if found {
			pending = append(pending, group)
			continue
		}
		if _, found, err := c.root.GroupInitialCohort(group.GroupID); err != nil {
			return err
		} else if found {
			pending = append(pending, group)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].GroupID < pending[j].GroupID })
	selected := pending[0]
	for _, group := range pending {
		if group.GroupID > c.cursor {
			selected = group
			break
		}
	}
	c.cursor = selected.GroupID // An unavailable Region does not monopolize rounds.
	cohort, found, err := c.root.PendingRegionFormation(selected.GroupID)
	if err != nil {
		return err
	}
	if !found {
		cohort, found, err = c.root.GroupInitialCohort(selected.GroupID)
	}
	if err != nil {
		return err
	}
	if !found || (!cohort.Formation && !cohort.Reformation) {
		return errors.New("regional formation disappeared")
	}
	// Bounded, one Region per round. All original participants must confirm a
	// real read barrier and exact stable membership, not merely be registered.
	attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	local, err := c.host.Status(raft.RootGroupID)
	if err != nil {
		return err
	}
	for _, id := range cohort.Nodes {
		conf, leader, err := c.inspectParticipant(attempt, local.RaftID, id, selected.GroupID)
		if err != nil {
			if pendingInspection(err) {
				return nil
			}
			return fmt.Errorf("regional formation awaiting Node %d: %w", id, err)
		}
		voters := slices.Clone(conf.GetVoters())
		slices.Sort(voters)
		if leader == 0 || !slices.Contains(cohort.Nodes, leader) || !slices.Equal(voters, cohort.Nodes) || len(conf.GetLearners()) != 0 || len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
			return errors.New("regional Group is not yet at its exact initial stable membership")
		}
	}
	members := make([]rootstate.GroupMember, 0, len(cohort.Nodes))
	for _, id := range cohort.Nodes {
		members = append(members, rootstate.GroupMember{RaftID: id, Role: rootstate.GroupMemberVoter})
	}
	return c.submit(ctx, rootstate.Command{Type: rootstate.PublishGroupMembers, Membership: &rootstate.GroupMembership{GroupID: selected.GroupID, ExpectedEpoch: 1, Members: members}})
}

func (c *Coordinator) inspectParticipant(ctx context.Context, local, target uint64, group string) (*raftpb.ConfState, uint64, error) {
	if target != local {
		if _, err := c.inspect.ReadIndex(ctx, target, group); err != nil {
			return nil, 0, err
		}
		return c.inspect.InspectMembership(ctx, target, group)
	}
	// The RPC client intentionally forbids self-targets. Local participants
	// still require the same quorum/applied barrier and actual Raft membership.
	if _, err := c.host.ReadIndex(ctx, group); err != nil {
		return nil, 0, err
	}
	conf, err := c.host.Membership(group)
	if err != nil {
		return nil, 0, err
	}
	status, err := c.host.Status(group)
	return conf, status.LeaderID, err
}

func pendingInspection(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, raft.ErrNoLeader) ||
		status.Code(err) == codes.Unavailable || status.Code(err) == codes.NotFound || status.Code(err) == codes.DeadlineExceeded
}

func (c *Coordinator) submit(ctx context.Context, command rootstate.Command) error {
	return submitRoot(ctx, c.host, c.root, command)
}

func submitRoot(ctx context.Context, host Host, root *rootstate.State, command rootstate.Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	command.Version, command.RequestID = rootstate.FormatVersion, "regional-intent"
	if command.Type == rootstate.ConfirmRegionCohort || command.Type == rootstate.AuthorizeRegionGrowth || command.Type == rootstate.PlanRegionCohort {
		// A concurrent Root transition can durably reject an otherwise identical
		// assertion. Bind retries to a fresh applied cut so the rejection's
		// idempotency record cannot poison every later attempt. A lost response
		// is still safe: the cohort itself is immutable after confirmation.
		command.RequestID = fmt.Sprintf("regional-confirmation-%d", root.AppliedIndex())
	}
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		return fmt.Errorf("encode regional Root command type=%q request=%q version=%d: %w", command.Type, command.RequestID, command.Version, err)
	}
	digest := sha256.Sum256(encoded)
	command.RequestID = "regional-" + hex.EncodeToString(digest[:])
	encoded, err = rootstate.EncodeCommand(command)
	if err != nil {
		return fmt.Errorf("encode regional Root command type=%q request=%q version=%d: %w", command.Type, command.RequestID, command.Version, err)
	}
	if err := host.Submit(raft.RootGroupID, encoded); err != nil {
		return err
	}
	if _, err := host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	result, found, err := root.ResultForCommand(encoded)
	if err != nil {
		return fmt.Errorf("inspect regional Root command type=%q request=%q: %w", command.Type, command.RequestID, err)
	}
	if !found {
		return errors.New("regional Root command not yet applied")
	}
	if result.Error != "" {
		return errors.New(result.Error)
	}
	return nil
}
