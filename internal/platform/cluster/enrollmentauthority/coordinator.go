// Package enrollmentauthority keeps Root's signer-delegation authority cut in
// step with the current stable Root voters. It is deliberately small: Root
// decides the durable epoch and membership; private signer material remains
// local to the authority Nodes.
package enrollmentauthority

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
)

var ErrNotLeader = errors.New("enrollment authority requires the current Root leader")

type Host interface {
	Submit(string, []byte) error
	ReadIndex(context.Context, string) (uint64, error)
	Status(string) (raft.GroupStatus, error)
}

type Transferer interface {
	Transfer(context.Context, rootstate.Node, uint64) error
}

type Coordinator struct {
	host     Host
	root     *rootstate.State
	transfer Transferer
}

func (c *Coordinator) SetTransferer(transfer Transferer) { c.transfer = transfer }

func New(host Host, root *rootstate.State) (*Coordinator, error) {
	if host == nil || root == nil {
		return nil, errors.New("enrollment authority requires Host and Root state")
	}
	return &Coordinator{host: host, root: root}, nil
}

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
	conf, err := c.root.RootMembership()
	if err != nil {
		return err
	}
	if conf == nil || len(conf.Voters) == 0 || len(conf.VotersOutgoing) != 0 || len(conf.LearnersNext) != 0 {
		// During a joint Root change, wait for the topology coordinator to
		// complete the stable cut before advancing the authority epoch.
		return nil
	}
	members := slices.Clone(conf.Voters)
	slices.Sort(members)
	current, found, err := c.root.EnrollmentAuthority()
	if err != nil {
		return err
	}
	expected := uint64(0)
	epoch := uint64(1)
	if found {
		if current.Epoch == math.MaxUint64 {
			return errors.New("authority epoch exhausted")
		}
		expected = current.Epoch
		epoch = current.Epoch + 1
		if current.State == rootstate.AuthorityStateActive && slices.Equal(current.Members, members) {
			if c.transfer == nil || readySubset(members, current.ReadyMembers) {
				return nil
			}
			_, err := c.prepareMembers(ctx, members, current.Epoch, status.RaftID)
			return err
		}
	}
	var ready []uint64
	if found && readySubset(members, current.ReadyMembers) {
		// A removal can shrink the authority cut without moving any signer
		// material: every surviving target is already durably ready.
		ready = slices.Clone(members)
	} else {
		ready, err = c.prepareMembers(ctx, members, epoch, status.RaftID)
		if err != nil {
			return err
		}
	}
	// No authority is activated before at least the stable Root majority has
	// acknowledged a validated envelope and local directory Sync.
	plan := &rootstate.EnrollmentAuthorityPlan{ExpectedEpoch: expected, Epoch: epoch, Members: members, ReadyMembers: ready, State: rootstate.AuthorityStateActive}
	command := rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.PublishEnrollmentAuthority,
		RequestID: authorityRequestID(plan), EnrollmentAuthority: plan}
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		return err
	}
	if err := c.host.Submit(raft.RootGroupID, encoded); err != nil {
		return err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			return err
		}
		result, found, err := c.root.ResultForCommand(encoded)
		if err != nil {
			return err
		}
		if found {
			if result.Error != "" {
				return errors.New(result.Error)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func readySubset(members, ready []uint64) bool {
	for _, member := range members {
		if !slices.Contains(ready, member) {
			return false
		}
	}
	return true
}

func (c *Coordinator) prepareMembers(ctx context.Context, members []uint64, epoch, local uint64) ([]uint64, error) {
	if c.transfer == nil {
		return nil, errors.New("enrollment authority signer installation is unavailable")
	}
	var ready []uint64
	var failures []error
	for _, member := range members {
		if member == local {
			ready = append(ready, member)
			continue
		}
		node, found, err := c.root.Node(member)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.New("enrollment authority member is not registered")
		}
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = c.transfer.Transfer(attempt, node, epoch)
		cancel()
		if err != nil {
			failures = append(failures, fmt.Errorf("authority Node %d: %w", member, err))
		} else {
			ready = append(ready, member)
		}
	}
	if len(ready) < len(members)/2+1 {
		return nil, errors.Join(append(failures, errors.New("authority signer majority is not installed"))...)
	}
	return ready, nil
}

func authorityRequestID(plan *rootstate.EnrollmentAuthorityPlan) string {
	encoded, _ := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion, RequestID: "authority-digest", Type: rootstate.PublishEnrollmentAuthority, EnrollmentAuthority: plan})
	digest := sha256.Sum256(encoded)
	return "enrollment-authority-" + hex.EncodeToString(digest[:])
}
