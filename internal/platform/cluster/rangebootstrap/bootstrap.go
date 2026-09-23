// Package rangebootstrap reconciles a Root pending Range with its initial
// Owner Group Fence before making the Range routable.
package rangebootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

type Host interface {
	Submit(groupID string, command []byte) error
	ReadIndex(ctx context.Context, groupID string) (uint64, error)
}

type OwnerProof struct {
	RequestID    string
	AppliedIndex uint64
}

type OwnerControl interface {
	EnsureInitialRange(ctx context.Context, descriptor rootstate.Range) (OwnerProof, error)
}

type OwnerResolver func(groupID string) (OwnerControl, bool)

type LocalOwner struct {
	host  Host
	state *capstate.State
}

func NewLocalOwner(host Host, state *capstate.State) (*LocalOwner, error) {
	if host == nil || state == nil {
		return nil, errors.New("local Range Owner requires Host and capability state")
	}
	return &LocalOwner{host: host, state: state}, nil
}

type Bootstrapper struct {
	host   Host
	root   *rootstate.State
	owners OwnerResolver
}

func New(host Host, root *rootstate.State, owners OwnerResolver) (*Bootstrapper, error) {
	if host == nil || root == nil || owners == nil {
		return nil, errors.New("Range bootstrap requires Host, Root state, and Owner resolver")
	}
	return &Bootstrapper{host: host, root: root, owners: owners}, nil
}

// Reconcile is idempotent. Root's pending descriptor is the durable task; no
// in-memory progress is required to resume after a process restart.
func (bootstrapper *Bootstrapper) Reconcile(ctx context.Context, rangeID string) error {
	if rootstate.ValidateRangeID(rangeID) != nil {
		return errors.New("invalid Range bootstrap identity")
	}
	if _, err := bootstrapper.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return fmt.Errorf("confirm Root pending Range: %w", err)
	}
	descriptor, found, err := bootstrapper.root.Range(rangeID)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("Range bootstrap target is not registered")
	}
	if descriptor.State == rootstate.RangeStateActive {
		return nil
	}
	if descriptor.State != rootstate.RangeStatePending || descriptor.Epoch != 1 ||
		len(descriptor.Start) != 0 || len(descriptor.End) != 0 {
		return errors.New("Range bootstrap target is not an initial pending Range")
	}
	owner, found := bootstrapper.owners(descriptor.OwnerGroupID)
	if !found || owner == nil {
		return errors.New("Range Owner control is unavailable on this coordinator")
	}
	active := descriptor
	active.State = rootstate.RangeStateActive
	proof, err := owner.EnsureInitialRange(ctx, active)
	if err != nil {
		return err
	}
	if proof.RequestID != OwnerRequestID(active) || proof.AppliedIndex == 0 {
		return errors.New("Owner returned invalid Range Fence proof")
	}
	activation := rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: requestID("publish", active), Type: rootstate.ActivateRange,
		Activation: &rootstate.RangeActivation{RangeID: active.RangeID,
			OwnerGroupID: active.OwnerGroupID, Epoch: active.Epoch,
			OwnerRequestID: proof.RequestID, OwnerAppliedIndex: proof.AppliedIndex}}
	encoded, err := rootstate.EncodeCommand(activation)
	if err != nil {
		return err
	}
	if err := bootstrapper.host.Submit(raft.RootGroupID, encoded); err != nil {
		return fmt.Errorf("submit Root Range activation: %w", err)
	}
	if _, err := bootstrapper.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return fmt.Errorf("confirm Root Range activation: %w", err)
	}
	result, found, err := bootstrapper.root.ResultForCommand(encoded)
	if err != nil {
		return err
	}
	if !found || result.Error != "" {
		return fmt.Errorf("Root Range was not activated: found=%v error=%q", found, result.Error)
	}
	return nil
}

func (owner *LocalOwner) EnsureInitialRange(ctx context.Context, descriptor rootstate.Range) (OwnerProof, error) {
	if descriptor.Validate() != nil || descriptor.State != rootstate.RangeStateActive || descriptor.Epoch != 1 ||
		len(descriptor.Start) != 0 || len(descriptor.End) != 0 {
		return OwnerProof{}, errors.New("invalid initial Owner Range Fence")
	}
	if _, err := owner.host.ReadIndex(ctx, descriptor.OwnerGroupID); err != nil {
		return OwnerProof{}, fmt.Errorf("confirm Owner Range Fence: %w", err)
	}
	current, installed, err := owner.state.Range(descriptor.RangeID)
	if err != nil {
		return OwnerProof{}, err
	}
	if installed && !sameRange(current, descriptor) {
		return OwnerProof{}, errors.New("Owner has a conflicting Range Fence")
	}
	requestID := OwnerRequestID(descriptor)
	if !installed {
		command, err := routedstate.EncodeActivateRange(requestID, descriptor)
		if err != nil {
			return OwnerProof{}, err
		}
		if err := owner.host.Submit(descriptor.OwnerGroupID, command); err != nil {
			return OwnerProof{}, fmt.Errorf("submit Owner Range Fence: %w", err)
		}
		if _, err := owner.host.ReadIndex(ctx, descriptor.OwnerGroupID); err != nil {
			return OwnerProof{}, fmt.Errorf("confirm installed Owner Range Fence: %w", err)
		}
	}
	result, found, err := owner.state.ResultForRange(requestID, descriptor)
	if err != nil {
		return OwnerProof{}, err
	}
	if !found || !result.Applied || result.Error != "" || result.Index == 0 {
		return OwnerProof{}, fmt.Errorf("Owner Range Fence was not applied: found=%v error=%q", found, result.Error)
	}
	return OwnerProof{RequestID: requestID, AppliedIndex: result.Index}, nil
}

func OwnerRequestID(descriptor rootstate.Range) string { return requestID("install", descriptor) }

func requestID(stage string, descriptor rootstate.Range) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte("dubi-range-bootstrap-v1\x00" + stage + "\x00"))
	_, _ = digest.Write([]byte(descriptor.RangeID + "\x00" + descriptor.OwnerGroupID + "\x00"))
	_, _ = digest.Write([]byte(fmt.Sprintf("%d", descriptor.Epoch)))
	return "range-" + stage + "-" + hex.EncodeToString(digest.Sum(nil)[:12])
}

func sameRange(left, right rootstate.Range) bool {
	return left.RangeID == right.RangeID && left.Keyspace() == right.Keyspace() &&
		left.OwnerGroupID == right.OwnerGroupID && left.Epoch == right.Epoch && left.State == right.State &&
		bytes.Equal(left.Start, right.Start) && bytes.Equal(left.End, right.End)
}
