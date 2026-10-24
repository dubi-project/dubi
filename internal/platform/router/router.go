// Package router resolves DUBI's internal Keyspace routes from the Root Group.
// User authorization at the Tenant boundary must happen before calling it.
package router

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

var (
	ErrNoRoute          = errors.New("Keyspace has no active Range")
	ErrDirectoryInvalid = errors.New("Root Range directory is inconsistent")
	ErrCatalogBehind    = errors.New("Root catalog is behind the confirmed read index")
	ErrWrongOwner       = errors.New("route targets a different Owner Group")
	ErrRouteFenced      = errors.New("route generation is fenced")
)

// Target is the immutable routing generation carried from an entry Node to
// the Owner Group. Engine state machines must compare it with their
// locally applied Range fence in the same atomic transaction as a mutation.
type Target struct {
	Keyspace     rootstate.Keyspace `json:"keyspace"`
	RangeID      string             `json:"range_id"`
	OwnerGroupID string             `json:"owner_group_id"`
	Epoch        uint64             `json:"epoch"`
	RoutingKey   []byte             `json:"routing_key"`
}

func (target Target) Validate() error {
	if target.Keyspace.Validate() != nil || rootstate.ValidateRangeID(target.RangeID) != nil ||
		raft.ValidateGroupID(target.OwnerGroupID) != nil || target.OwnerGroupID == raft.RootGroupID ||
		target.Epoch == 0 {
		return ErrDirectoryInvalid
	}
	return nil
}

type ReadBarrier interface {
	ReadIndex(ctx context.Context, groupID string) (uint64, error)
}

type Catalog interface {
	AppliedIndex() uint64
	RouteKeyspace(keyspace rootstate.Keyspace, routingKey []byte) (rootstate.Range, bool, error)
}

type Router struct {
	barrier ReadBarrier
	catalog Catalog
}

type confirmedRootIndexContextKey struct{}

// WithConfirmedRootIndex attaches an already quorum-confirmed Root cut to an
// internal request context. It is intended for server-side admission paths
// that have just confirmed current Node membership; external callers cannot
// set this private context key.
func WithConfirmedRootIndex(ctx context.Context, index uint64) context.Context {
	if ctx == nil || index == 0 {
		return ctx
	}
	return context.WithValue(ctx, confirmedRootIndexContextKey{}, index)
}

// ConfirmedRootIndex returns a Root read cut previously attached by a trusted
// internal admission path.
func ConfirmedRootIndex(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	index, ok := ctx.Value(confirmedRootIndexContextKey{}).(uint64)
	return index, ok && index != 0
}

func New(barrier ReadBarrier, catalog Catalog) (*Router, error) {
	if barrier == nil || catalog == nil {
		return nil, errors.New("Root Router requires ReadIndex and catalog")
	}
	return &Router{barrier: barrier, catalog: catalog}, nil
}

// Resolve performs a quorum-confirmed Root read before returning an Owner
// Group and RangeEpoch. The result is routing information, not authorization.
func (r *Router) Resolve(ctx context.Context, keyspace rootstate.Keyspace, routingKey []byte) (rootstate.Range, error) {
	if err := keyspace.Validate(); err != nil {
		return rootstate.Range{}, err
	}
	index, err := r.barrier.ReadIndex(ctx, raft.RootGroupID)
	if err != nil {
		return rootstate.Range{}, fmt.Errorf("confirm Root route: %w", err)
	}
	return r.ResolveAtIndex(keyspace, routingKey, index)
}

// ResolveAtIndex resolves from the local Root catalog after the caller has
// already confirmed this Root cut. The catalog must have applied at least the
// cut; otherwise the caller should obtain a fresh cut and retry.
func (r *Router) ResolveAtIndex(keyspace rootstate.Keyspace, routingKey []byte, index uint64) (rootstate.Range, error) {
	if err := keyspace.Validate(); err != nil || index == 0 {
		return rootstate.Range{}, ErrDirectoryInvalid
	}
	if r.catalog.AppliedIndex() < index {
		return rootstate.Range{}, ErrCatalogBehind
	}
	value, found, err := r.catalog.RouteKeyspace(keyspace, routingKey)
	if err != nil {
		return rootstate.Range{}, err
	}
	if !found {
		return rootstate.Range{}, ErrNoRoute
	}
	if value.Keyspace() != keyspace || value.RangeID == "" || value.OwnerGroupID == "" || value.Epoch == 0 ||
		value.State != rootstate.RangeStateActive || !Contains(value, routingKey) {
		return rootstate.Range{}, ErrDirectoryInvalid
	}
	return value, nil
}

// ResolveTarget returns a detached route token suitable for forwarding to an
// Owner Group. Mutating routingKey after this call cannot alter the Target.
func (r *Router) ResolveTarget(ctx context.Context, keyspace rootstate.Keyspace, routingKey []byte) (Target, error) {
	value, err := r.Resolve(ctx, keyspace, routingKey)
	if err != nil {
		return Target{}, err
	}
	return targetFromRange(keyspace, routingKey, value), nil
}

// ResolveTargetAtIndex resolves a detached route using an already confirmed
// Root cut, avoiding a second serial Root ReadIndex on the same request.
func (r *Router) ResolveTargetAtIndex(keyspace rootstate.Keyspace, routingKey []byte, index uint64) (Target, error) {
	value, err := r.ResolveAtIndex(keyspace, routingKey, index)
	if err != nil {
		return Target{}, err
	}
	return targetFromRange(keyspace, routingKey, value), nil
}

func targetFromRange(keyspace rootstate.Keyspace, routingKey []byte, value rootstate.Range) Target {
	return Target{Keyspace: keyspace, RangeID: value.RangeID, OwnerGroupID: value.OwnerGroupID,
		Epoch: value.Epoch, RoutingKey: bytes.Clone(routingKey)}
}

// CheckOwner compares a forwarded Target with the exact Range fence already
// applied by the Owner Group. It is a pure comparison so engine state
// machines can invoke it inside their own atomic apply transaction.
//
// Calling this only before Raft submission is not a write fence: the applied
// Range generation may change before the mutation commits.
func CheckOwner(groupID string, current rootstate.Range, target Target) error {
	if raft.ValidateGroupID(groupID) != nil || target.Validate() != nil {
		return ErrDirectoryInvalid
	}
	if target.OwnerGroupID != groupID || current.OwnerGroupID != groupID {
		return ErrWrongOwner
	}
	if current.Keyspace() != target.Keyspace || current.RangeID != target.RangeID ||
		current.State != rootstate.RangeStateActive || !Contains(current, target.RoutingKey) {
		return ErrDirectoryInvalid
	}
	if current.Epoch != target.Epoch {
		return ErrRouteFenced
	}
	return nil
}

// Contains uses half-open [start,end) bounds; an empty bound is unbounded.
func Contains(value rootstate.Range, routingKey []byte) bool {
	if len(value.Start) != 0 && bytes.Compare(routingKey, value.Start) < 0 {
		return false
	}
	if len(value.End) != 0 && bytes.Compare(routingKey, value.End) >= 0 {
		return false
	}
	return len(value.Start) == 0 || len(value.End) == 0 || bytes.Compare(value.Start, value.End) < 0
}
