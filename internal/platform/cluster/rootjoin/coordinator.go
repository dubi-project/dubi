// Package rootjoin advances authenticated enrollment into non-voting Root
// membership. Root voter selection/Region placement is a separate policy; this
// coordinator never promotes an unqualified learner or shrinks a quorum.
package rootjoin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	ErrNotLeader     = errors.New("Root join requires the current Root leader")
	ErrJoinPending   = errors.New("Root learner membership has not yet been confirmed")
	ErrJoinRejected  = errors.New("Root join task was rejected")
	ErrMemberRemoved = errors.New("admitted Root member was removed; the old join cannot be re-admitted")
)

type Host interface {
	Submit(string, []byte) error
	ReadIndex(context.Context, string) (uint64, error)
	Status(string) (raft.GroupStatus, error)
	AddLearner(string, uint64) error
}

type Coordinator struct {
	host Host
	root *rootstate.State
}

func New(host Host, root *rootstate.State) (*Coordinator, error) {
	if host == nil || root == nil {
		return nil, errors.New("Root join coordinator requires Host and Root state")
	}
	return &Coordinator{host: host, root: root}, nil
}

// Ensure must be called by a currently verified Node transport whose identity
// matches start.Request. The Root state machine independently verifies its
// consumed grant, exact request digest and registered Node metadata.
// Returning an admitted task certifies durable Root learner/voter membership,
// NOT local catch-up, readiness, automatic voter promotion or data placement.
func (c *Coordinator) Ensure(ctx context.Context, start rootstate.RootJoinStart) (rootstate.RootJoin, error) {
	if err := c.leader(ctx); err != nil {
		return rootstate.RootJoin{}, err
	}
	encoded, err := identityenrollment.EncodeRequest(start.Request)
	if err != nil {
		return rootstate.RootJoin{}, err
	}
	// A rejected request must not occupy the legitimate CSR's idempotency key.
	// GrantID alone would let a different Node poison this grant's first attempt.
	digest := sha256.Sum256(append([]byte(start.GrantID+"\x00"), encoded...))
	command := rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.BeginRootJoin,
		RequestID: "root-join-start-" + hex.EncodeToString(digest[:]), RootJoinStart: &start}
	if err := c.confirm(ctx, command); err != nil {
		return rootstate.RootJoin{}, err
	}
	task, found, err := c.root.RootJoin(start.GrantID)
	if err != nil {
		return rootstate.RootJoin{}, err
	}
	if !found {
		return rootstate.RootJoin{}, ErrJoinPending
	}
	return c.advance(ctx, task)
}

// Reconcile resumes only durable Pending tasks. It never guesses that a
// missing member is a new join, or replays Admitted tasks after removal.
func (c *Coordinator) Reconcile(ctx context.Context) error {
	if err := c.leader(ctx); err != nil {
		return err
	}
	if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	tasks, err := c.root.PendingRootJoins()
	if err != nil {
		return err
	}
	var failures []error
	for _, task := range tasks {
		if err := c.leader(ctx); err != nil {
			return errors.Join(errors.Join(failures...), err)
		}
		if _, err := c.advance(ctx, task); err != nil {
			failures = append(failures, fmt.Errorf("Root join %s: %w", task.GrantID, err))
		}
	}
	return errors.Join(failures...)
}

func (c *Coordinator) advance(ctx context.Context, task rootstate.RootJoin) (rootstate.RootJoin, error) {
	if err := c.leader(ctx); err != nil {
		return rootstate.RootJoin{}, err
	}
	if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return rootstate.RootJoin{}, err
	}
	current, found, err := c.root.RootJoin(task.GrantID)
	if err != nil {
		return rootstate.RootJoin{}, err
	}
	if !found || current.NodeID != task.NodeID || current.RaftID != task.RaftID || current.RequestSHA256 != task.RequestSHA256 {
		return rootstate.RootJoin{}, ErrJoinRejected
	}
	task = current
	conf, err := c.root.RootMembership()
	if err != nil {
		return rootstate.RootJoin{}, err
	}
	if task.State == rootstate.RootJoinAdmitted {
		if !Contains(conf, task.RaftID) {
			return rootstate.RootJoin{}, ErrMemberRemoved
		}
		return task, nil
	}
	finish := rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.CompleteRootJoin,
		RequestID: "root-join-complete-" + task.GrantID, RootJoinFinish: &rootstate.RootJoinFinish{GrantID: task.GrantID}}
	encoded, err := rootstate.EncodeCommand(finish)
	if err != nil {
		return rootstate.RootJoin{}, err
	}
	// A prior deterministic completion rejection is a terminal operator-visible
	// failure. Do not re-add a member that maintenance removed in that window.
	result, found, err := c.root.ResultForCommand(encoded)
	if err != nil {
		return rootstate.RootJoin{}, err
	}
	if found && result.Error != "" {
		return rootstate.RootJoin{}, fmt.Errorf("%w: completion rejected", ErrJoinRejected)
	}
	if !Contains(conf, task.RaftID) {
		if err := c.leader(ctx); err != nil {
			return rootstate.RootJoin{}, err
		}
		proposalErr := c.host.AddLearner(raft.RootGroupID, task.RaftID)
		if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			return rootstate.RootJoin{}, errors.Join(proposalErr, err)
		}
		conf, err = c.root.RootMembership()
		if err != nil {
			return rootstate.RootJoin{}, errors.Join(proposalErr, err)
		}
		if !Contains(conf, task.RaftID) {
			return rootstate.RootJoin{}, errors.Join(proposalErr, ErrJoinPending)
		}
	}
	if err := c.confirm(ctx, finish); err != nil {
		return rootstate.RootJoin{}, err
	}
	task, found, err = c.root.RootJoin(task.GrantID)
	if err != nil {
		return rootstate.RootJoin{}, err
	}
	if !found || task.State != rootstate.RootJoinAdmitted {
		return rootstate.RootJoin{}, ErrJoinPending
	}
	conf, err = c.root.RootMembership()
	if err != nil {
		return rootstate.RootJoin{}, err
	}
	if !Contains(conf, task.RaftID) {
		return rootstate.RootJoin{}, ErrMemberRemoved
	}
	return task, nil
}

func (c *Coordinator) leader(ctx context.Context) error {
	if ctx == nil {
		return errors.New("Root join context required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := c.host.Status(raft.RootGroupID)
	if err != nil {
		return err
	}
	if state.LeaderID == 0 || state.RaftID != state.LeaderID || state.Role != etcdraft.StateLeader {
		return ErrNotLeader
	}
	return nil
}

func (c *Coordinator) confirm(ctx context.Context, command rootstate.Command) error {
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		return err
	}
	submitErr := c.host.Submit(raft.RootGroupID, encoded)
	if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return errors.Join(submitErr, err)
	}
	result, found, err := c.root.ResultForCommand(encoded)
	if err != nil {
		return errors.Join(submitErr, err)
	}
	if !found {
		return errors.Join(submitErr, ErrJoinPending)
	}
	if result.Error != "" {
		return fmt.Errorf("%w: %s", ErrJoinRejected, result.Error)
	}
	return nil // Recover a lost response using its exact committed command result.
}

func Contains(conf *raftpb.ConfState, id uint64) bool {
	if id == 0 {
		return false
	}
	for _, ids := range [][]uint64{conf.GetVoters(), conf.GetVotersOutgoing(), conf.GetLearners(), conf.GetLearnersNext()} {
		for _, candidate := range ids {
			if candidate == id {
				return true
			}
		}
	}
	return false
}
