// Package runtimecontract defines the narrow boundary exposed by the DUBI
// Runtime to reusable Engines. It deliberately contains no Raft, RPC, Pack,
// Pebble, or Node implementation details.
package runtimecontract

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

var (
	ErrInvalidRequest = errors.New("invalid Runtime Contract request")
	ErrInvalidRoute   = errors.New("Runtime Contract resolver returned an invalid route")
)

// RouteResolver is the only routing dependency visible to an Engine. The
// implementation confirms the current Root view and returns an immutable
// Route Target; it must not expose a mutable directory to the caller.
type RouteResolver interface {
	ResolveTarget(context.Context, rootstate.Keyspace, []byte) (router.Target, error)
}

var _ RouteResolver = (*router.Router)(nil)

// StateExecutor is the data-plane half of the contract. It receives a
// validated Route Target and opaque Engine payloads. Local execution,
// forwarding, Raft, fencing, and physical durability remain Runtime
// responsibilities behind this interface.
type StateExecutor interface {
	Mutate(context.Context, Mutation) (MutationResult, error)
	Read(context.Context, Read) (ReadResult, error)
}

// Contract is the stable Runtime surface used by Engines. The public methods
// resolve a Keyspace/routing key first, then execute against the resulting
// immutable Target.
type Contract interface {
	Resolve(context.Context, rootstate.Keyspace, []byte) (router.Target, error)
	Mutate(context.Context, MutationRequest) (MutationResult, error)
	Read(context.Context, ReadRequest) (ReadResult, error)
}

// MutationRequest is an opaque Engine mutation addressed by a logical key.
// RequestID is stable across retries and is owned by the Engine operation.
type MutationRequest struct {
	RequestID  string
	Keyspace   rootstate.Keyspace
	RoutingKey []byte
	Payload    []byte
}

// ReadRequest is an opaque Engine read addressed by a logical key. Payload
// carries the Engine's read operation (for example, a version or query).
type ReadRequest struct {
	Keyspace   rootstate.Keyspace
	RoutingKey []byte
	Payload    []byte
}

// Mutation is the validated request passed from the Contract to the Runtime
// executor. Engines never construct this type directly.
type Mutation struct {
	RequestID string
	Target    router.Target
	Payload   []byte
}

// Read is the validated request passed from the Contract to the Runtime
// executor. Engines never construct this type directly.
type Read struct {
	Target  router.Target
	Payload []byte
}

// MutationResult reports the Runtime's durable execution result without
// exposing a storage or consensus type. Rejection is a deterministic Engine
// or routing rejection; an error means the Runtime could not establish the
// requested execution result.
type MutationResult struct {
	RequestID    string
	Applied      bool
	Rejection    string
	Value        []byte
	AppliedIndex uint64
}

// ReadResult reports an opaque Engine value and the confirmed Owner read cut.
type ReadResult struct {
	Value          []byte
	Rejection      string
	ConfirmedIndex uint64
}

// Dispatcher is the small reference implementation of Contract. It keeps
// route resolution and state execution injectable so the contract can be
// tested without a Node, network, or disk. Production wiring supplies the
// Runtime's resolver and executor adapters.
type Dispatcher struct {
	resolver RouteResolver
	executor StateExecutor
}

var _ Contract = (*Dispatcher)(nil)

func New(resolver RouteResolver, executor StateExecutor) (*Dispatcher, error) {
	if resolver == nil || executor == nil {
		return nil, errors.New("Runtime Contract requires a route resolver and state executor")
	}
	return &Dispatcher{resolver: resolver, executor: executor}, nil
}

func (dispatcher *Dispatcher) Resolve(ctx context.Context, keyspace rootstate.Keyspace, routingKey []byte) (router.Target, error) {
	if dispatcher == nil || dispatcher.resolver == nil {
		return router.Target{}, errors.New("Runtime Contract is not initialized")
	}
	if err := validateAddress(ctx, keyspace, routingKey); err != nil {
		return router.Target{}, err
	}
	key := bytes.Clone(routingKey)
	target, err := dispatcher.resolver.ResolveTarget(ctx, keyspace, key)
	if err != nil {
		return router.Target{}, err
	}
	if err := validateResolvedTarget(target, keyspace, key); err != nil {
		return router.Target{}, err
	}
	return detachTarget(target), nil
}

func (dispatcher *Dispatcher) Mutate(ctx context.Context, request MutationRequest) (MutationResult, error) {
	if dispatcher == nil || dispatcher.executor == nil {
		return MutationResult{}, errors.New("Runtime Contract is not initialized")
	}
	if err := validateAddress(ctx, request.Keyspace, request.RoutingKey); err != nil ||
		rootstate.ValidateRequestID(request.RequestID) != nil || len(request.Payload) == 0 {
		return MutationResult{}, ErrInvalidRequest
	}
	target, err := dispatcher.Resolve(ctx, request.Keyspace, request.RoutingKey)
	if err != nil {
		return MutationResult{}, err
	}
	result, err := dispatcher.executor.Mutate(ctx, Mutation{RequestID: request.RequestID, Target: target, Payload: bytes.Clone(request.Payload)})
	if err != nil {
		return MutationResult{}, err
	}
	result.RequestID = request.RequestID
	result.Value = bytes.Clone(result.Value)
	return result, nil
}

func (dispatcher *Dispatcher) Read(ctx context.Context, request ReadRequest) (ReadResult, error) {
	if dispatcher == nil || dispatcher.executor == nil {
		return ReadResult{}, errors.New("Runtime Contract is not initialized")
	}
	if err := validateAddress(ctx, request.Keyspace, request.RoutingKey); err != nil || len(request.Payload) == 0 {
		return ReadResult{}, ErrInvalidRequest
	}
	target, err := dispatcher.Resolve(ctx, request.Keyspace, request.RoutingKey)
	if err != nil {
		return ReadResult{}, err
	}
	result, err := dispatcher.executor.Read(ctx, Read{Target: target, Payload: bytes.Clone(request.Payload)})
	if err != nil {
		return ReadResult{}, err
	}
	result.Value = bytes.Clone(result.Value)
	return result, nil
}

func validateAddress(ctx context.Context, keyspace rootstate.Keyspace, routingKey []byte) error {
	if ctx == nil || keyspace.Validate() != nil || len(routingKey) == 0 {
		return ErrInvalidRequest
	}
	return nil
}

func validateResolvedTarget(target router.Target, keyspace rootstate.Keyspace, routingKey []byte) error {
	if target.Validate() != nil || target.Keyspace != keyspace || !bytes.Equal(target.RoutingKey, routingKey) {
		return fmt.Errorf("%w: target does not match requested Keyspace or routing key", ErrInvalidRoute)
	}
	return nil
}

func detachTarget(target router.Target) router.Target {
	target.RoutingKey = bytes.Clone(target.RoutingKey)
	return target
}
