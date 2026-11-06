package runtimecontract

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

// Binding is created by server assembly. Module and Engine never come from
// client-supplied headers or payloads. It reuses an Engine without sharing its data.
type Binding struct {
	backend        Contract
	prepare        RangePreparer
	module, engine string
}

// RangePreparer is Runtime's durable, idempotent first-write initialization.
type RangePreparer interface {
	EnsureRange(context.Context, rootstate.Keyspace) error
}
type BoundClient struct {
	binding Binding
	space   rootstate.Keyspace
}
type BoundMutation struct {
	RequestID    string
	Key, Payload []byte
}
type BoundRead struct{ Key, Payload []byte }

func Bind(backend Contract, prepare RangePreparer, module, engine string) (Binding, error) {
	if backend == nil || prepare == nil || (rootstate.Keyspace{TenantID: "validation", ModuleID: module, EngineID: engine}).Validate() != nil {
		return Binding{}, errors.New("Runtime binding requires backend, initializer and server identities")
	}
	return Binding{backend: backend, prepare: prepare, module: module, engine: engine}, nil
}

// ForTenant accepts the Tenant already authenticated and authorized by the
// caller. It is not an authorization check and does not allocate storage.
func (b Binding) ForTenant(tenant string) (*BoundClient, error) {
	space := rootstate.Keyspace{TenantID: tenant, ModuleID: b.module, EngineID: b.engine}
	if b.backend == nil || b.prepare == nil || space.Validate() != nil {
		return nil, ErrInvalidRequest
	}
	return &BoundClient{binding: b, space: space}, nil
}
func (c *BoundClient) Mutate(ctx context.Context, r BoundMutation) (MutationResult, error) {
	if c == nil || validateAddress(ctx, c.space, r.Key) != nil || rootstate.ValidateRequestID(r.RequestID) != nil || len(r.Payload) == 0 {
		return MutationResult{}, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return MutationResult{}, err
	}
	if err := c.binding.prepare.EnsureRange(ctx, c.space); err != nil {
		return MutationResult{}, err
	}
	requestID, _ := c.space.BindRequestID(r.RequestID)
	result, err := c.binding.backend.Mutate(ctx, MutationRequest{RequestID: requestID, Keyspace: c.space, RoutingKey: r.Key, Payload: r.Payload})
	// Keep the caller's correlation identity; the scoped ID belongs to Runtime.
	result.RequestID = r.RequestID
	return result, err
}
func (c *BoundClient) Read(ctx context.Context, r BoundRead) (ReadResult, error) {
	if c == nil || validateAddress(ctx, c.space, r.Key) != nil || len(r.Payload) == 0 {
		return ReadResult{}, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return ReadResult{}, err
	}
	return c.binding.backend.Read(ctx, ReadRequest{Keyspace: c.space, RoutingKey: r.Key, Payload: r.Payload})
}
