// Package enginedata is the in-process immutable-data entry for
// engine implementations. It is not a Tenant-authenticated public API.
package enginedata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

var ErrOwnerNotLocal = errors.New("current Range Owner is not hosted on this Node")

const routeAttemptTimeout = 8 * time.Second
const routeRetryDelay = 100 * time.Millisecond
const readAttemptTimeout = 60 * time.Second
const writeAttemptTimeout = 90 * time.Second

// Publication encodes a engine mutation which must publish exactly the
// supplied immutable descriptor with Transaction.SetRecordReference. The
// engine owns its schema; the platform verifies the resulting reference.
type Publication func(records.Descriptor) ([]byte, error)

type WriteRequest struct {
	RequestID string
	Keyspace  rootstate.Keyspace
	Key       []byte
	Body      []byte
	Publish   Publication
}

type WriteState string

const (
	// WriteUnknown includes pre-publication failure and uncertain submission.
	// It does not assert that a retry cannot later find a committed publication.
	WriteUnknown WriteState = "unknown"
	// WritePublishedUnconfirmed means the exact current publication was seen at
	// an Owner read cut, but this call did not prove LocalReady. A confirmation
	// may already have committed after an ACK was lost; retry the RequestID.
	WritePublishedUnconfirmed WriteState = "published_unconfirmed"
	WriteLocalReady           WriteState = "local_ready"
	WriteGeoReady             WriteState = "geo_ready"
)

type WriteResult struct {
	State WriteState
	Local recordwrite.LocalWrite
}

// Service is an in-process entry on any Node. It transparently dispatches to
// a current Owner member when the Owner Group is not held here. Tenant
// application authentication belongs to a separate external boundary: the
// in-process caller must already be authorized to use its Keyspace.
type Service struct {
	runtime  *node.Runtime
	client   *raftrpc.GroupClient
	geoRetry *recordwrite.GeoMetadataRetryStore
}

func New(runtime *node.Runtime, client *raftrpc.GroupClient) (*Service, error) {
	return NewWithGeoRetry(runtime, client, nil)
}

func NewWithGeoRetry(runtime *node.Runtime, client *raftrpc.GroupClient, geoRetry *recordwrite.GeoMetadataRetryStore) (*Service, error) {
	if runtime == nil || runtime.Host == nil || runtime.Root == nil || runtime.Engines == nil ||
		runtime.Records == nil || runtime.Identity.RaftID == 0 || runtime.Identity.NodeID == "" || client == nil {
		return nil, errors.New("engine data entry requires a complete local Node and Group client")
	}
	return &Service{runtime: runtime, client: client, geoRetry: geoRetry}, nil
}

// Write is the any-Node entry. A forwarded request carries the same stable
// RequestID and opaque mutation to each candidate; only the Owner executes
// the physical-majority protocol. This method does not expose an external API.
func (service *Service) Write(ctx context.Context, request WriteRequest) (WriteResult, error) {
	return service.write(ctx, request, true)
}

// WriteLocal is retained for Owner-local callers that must not relay.
func (service *Service) WriteLocal(ctx context.Context, request WriteRequest) (WriteResult, error) {
	return service.write(ctx, request, false)
}

func (service *Service) write(ctx context.Context, request WriteRequest, allowRemote bool) (WriteResult, error) {
	result := WriteResult{State: WriteUnknown}
	if service == nil || service.runtime == nil || service.client == nil || ctx == nil ||
		request.Publish == nil || request.Keyspace.Validate() != nil ||
		len(request.Key) == 0 || len(request.Key) > records.MaxKeyBytes {
		return result, errors.New("local engine write requires a valid Keyspace, key and publisher")
	}
	key, body := bytes.Clone(request.Key), bytes.Clone(request.Body)
	target, err := service.resolveTarget(ctx, request.Keyspace, key)
	if err != nil {
		return result, err
	}
	descriptor, err := records.Describe(request.Keyspace, target.OwnerGroupID, key, body)
	if err != nil {
		return result, err
	}
	forPublisher := descriptor
	forPublisher.Key = bytes.Clone(descriptor.Key)
	payload, err := request.Publish(forPublisher)
	if err != nil {
		return result, fmt.Errorf("encode engine publication: %w", err)
	}
	payload = bytes.Clone(payload)
	call := raftrpc.EngineDataWrite{RequestID: request.RequestID, Target: target,
		Descriptor: descriptor, Body: body, Payload: payload}
	if _, found := service.runtime.Engines.Resolve(target.OwnerGroupID); !found {
		if !allowRemote {
			return result, ErrOwnerNotLocal
		}
		result, err := service.forwardWrite(ctx, call)
		return result, err
	}
	writeCtx := ctx
	writeCancel := func() {}
	if allowRemote {
		alternates := 0
		for _, id := range service.client.GroupCandidates(target.OwnerGroupID) {
			if id != service.runtime.Identity.RaftID {
				alternates++
			}
		}
		if alternates > 0 {
			writeCtx, writeCancel = writeCandidateContext(ctx, alternates+1)
		}
	}
	completed, err := service.ExecuteWrite(writeCtx, call)
	writeCancel()
	local := fromWrite(completed)
	if err == nil || !allowRemote || ctx.Err() != nil {
		return local, err
	}
	// A timed-out attempt can still have published the exact command. All
	// candidates receive the same RequestID, descriptor, body and mutation;
	// only a fresh Owner confirmation may turn this into LocalReady.
	remote, routeErr := service.forwardWrite(ctx, call)
	if routeErr == nil {
		return remote, nil
	}
	if local.State == WritePublishedUnconfirmed && remote.State == WriteUnknown {
		remote.State = WritePublishedUnconfirmed
	}
	return remote, errors.Join(err, routeErr)
}

func fromWrite(result raftrpc.EngineDataWriteResult) WriteResult {
	state := WriteState(result.State)
	if state == "" {
		state = WriteUnknown
	}
	return WriteResult{State: state, Local: recordwrite.LocalWrite{Reference: result.Reference,
		ConfirmationIndex: result.ConfirmationIndex, RegionGeneration: result.RegionGeneration}}
}

func (service *Service) forwardWrite(ctx context.Context, request raftrpc.EngineDataWrite) (WriteResult, error) {
	result := WriteResult{State: WriteUnknown}
	var last error
	candidates := service.client.GroupCandidates(request.Target.OwnerGroupID)
	remaining := 0
	for _, id := range candidates {
		if id != service.runtime.Identity.RaftID {
			remaining++
		}
	}
	for _, id := range candidates {
		if id == service.runtime.Identity.RaftID {
			continue
		}
		attemptsLeft := remaining
		remaining--
		if err := ctx.Err(); err != nil {
			return result, err
		}
		node, found, err := service.runtime.Root.RootMember(id)
		if err != nil {
			return result, err
		}
		if !found {
			continue
		}
		attempt, done := writeCandidateContext(ctx, attemptsLeft)
		response, err := service.client.WriteEngineData(attempt, node, request)
		done()
		if err == nil {
			return fromWrite(response), nil
		}
		if response.State == string(WritePublishedUnconfirmed) {
			result.State = WritePublishedUnconfirmed
		}
		last = err
	}
	if last == nil {
		last = ErrOwnerNotLocal
	}
	return result, fmt.Errorf("route engine write to Owner Group %s: %w", request.Target.OwnerGroupID, last)
}

// ExecuteWrite is the narrow authenticated peer-RPC execution boundary. It
// never accepts a different Owner/Keyspace/descriptor from the routed request.
func (service *Service) ExecuteWrite(ctx context.Context, request raftrpc.EngineDataWrite) (raftrpc.EngineDataWriteResult, error) {
	result := raftrpc.EngineDataWriteResult{State: string(WriteUnknown)}
	if service == nil || service.runtime == nil || ctx == nil || request.Target.Validate() != nil ||
		request.Descriptor.Validate() != nil || request.Descriptor.Keyspace != request.Target.Keyspace ||
		request.Descriptor.GroupID != request.Target.OwnerGroupID || !bytes.Equal(request.Descriptor.Key, request.Target.RoutingKey) {
		return result, errors.New("engine data write differs from its Owner route")
	}
	actual, err := records.Describe(request.Target.Keyspace, request.Target.OwnerGroupID,
		request.Target.RoutingKey, request.Body)
	if err != nil {
		return result, errors.New("engine data body differs from descriptor")
	}
	actualID, _ := actual.ID()
	wantID, _ := request.Descriptor.ID()
	if actualID != wantID {
		return result, errors.New("engine data body differs from descriptor")
	}
	owner, found := service.runtime.Engines.Resolve(request.Target.OwnerGroupID)
	if !found || owner == nil {
		return result, ErrOwnerNotLocal
	}
	writer := recordwrite.Writer{Barrier: service.runtime, Root: service.runtime.Root,
		Owner: owner, LocalID: service.runtime.Identity.RaftID, LocalNodeID: service.runtime.Identity.NodeID,
		Store: service.runtime.Records, Sender: service.client, Observer: service.client,
		Submit: func(call context.Context, groupID string, command []byte) error {
			if err := call.Err(); err != nil {
				return err
			}
			return service.runtime.Host.Submit(groupID, command)
		}, MirrorGeo: service.client, MirrorRetry: service.geoRetry}
	if geo, err := service.geoPlan(ctx, request.Target); err != nil {
		return result, err
	} else if geo != nil {
		written, err := writer.WriteGeoFromRange(ctx, recordwrite.WriteRequest{RequestID: request.RequestID,
			Target: request.Target, Descriptor: request.Descriptor, Body: request.Body, Payload: request.Payload}, 16, 256)
		if err != nil {
			if service.currentPublication(ctx, owner, request.Target, request.Descriptor, request.RequestID) {
				result.State = string(WritePublishedUnconfirmed)
			}
			return result, err
		}
		result.State, result.Reference, result.ConfirmationIndex, result.RegionGeneration, result.RequiredRegions =
			string(WriteGeoReady), written.Reference, written.ConfirmationIndex, written.RegionGeneration, written.RequiredRegions
		return result, nil
	}
	ready, err := writer.WriteLocal(ctx, recordwrite.WriteRequest{RequestID: request.RequestID,
		Target: request.Target, Descriptor: request.Descriptor, Body: request.Body, Payload: request.Payload})
	if err != nil {
		if service.currentPublication(ctx, owner, request.Target, request.Descriptor, request.RequestID) {
			result.State = string(WritePublishedUnconfirmed)
		}
		return result, err
	}
	result.State, result.Reference, result.ConfirmationIndex, result.RegionGeneration =
		string(WriteLocalReady), ready.Reference, ready.ConfirmationIndex, ready.RegionGeneration
	return result, nil
}

// geoPlan returns a plan only when the confirmed local Region is too small to
// make a standalone local-majority promise. The plan is obtained at a Root
// read cut; the Writer rechecks it while staging the fixed Region majority.
func (service *Service) geoPlan(ctx context.Context, target router.Target) (*placement.GeoPlan, error) {
	progress, ready, err := service.runtime.Root.RegionProgress(target.OwnerGroupID)
	if err != nil {
		return nil, err
	}
	if !ready || !progress.Eligible || progress.Pending != nil || len(progress.Current.Nodes) >= 3 {
		return nil, nil
	}
	_, plan, err := placement.ReadRangeGeoPlan(ctx, service.runtime, service.runtime.Root, target.RangeID, 16, 256)
	if err != nil {
		return nil, err
	}
	policy, err := placement.SelectWritePolicy(len(progress.Current.Nodes), len(plan.Regions()))
	if err != nil {
		return nil, err
	}
	if !policy.RequiresGeoReady() {
		return nil, nil
	}
	return &plan, nil
}

func (service *Service) Read(ctx context.Context, keyspace rootstate.Keyspace, key []byte) (recordwrite.LocalRead, error) {
	return service.read(ctx, keyspace, key, true)
}

func (service *Service) ReadLocal(ctx context.Context, keyspace rootstate.Keyspace, key []byte) (recordwrite.LocalRead, error) {
	return service.read(ctx, keyspace, key, false)
}

func (service *Service) read(ctx context.Context, keyspace rootstate.Keyspace, key []byte, allowRemote bool) (recordwrite.LocalRead, error) {
	if service == nil || service.runtime == nil || service.client == nil || ctx == nil ||
		keyspace.Validate() != nil || len(key) == 0 || len(key) > records.MaxKeyBytes {
		return recordwrite.LocalRead{}, errors.New("local engine read requires a valid Keyspace and key")
	}
	key = bytes.Clone(key)
	target, err := service.resolveTarget(ctx, keyspace, key)
	if err != nil {
		return recordwrite.LocalRead{}, err
	}
	if _, found := service.runtime.Engines.Resolve(target.OwnerGroupID); !found {
		if !allowRemote {
			return recordwrite.LocalRead{}, ErrOwnerNotLocal
		}
		return service.forwardRead(ctx, target, key)
	}
	readCtx := ctx
	readCancel := func() {}
	if allowRemote {
		alternates := 0
		for _, id := range service.client.GroupCandidates(target.OwnerGroupID) {
			if id != service.runtime.Identity.RaftID {
				alternates++
			}
		}
		if alternates > 0 {
			readCtx, readCancel = readCandidateContext(ctx, alternates+1)
		}
	}
	got, err := service.ExecuteRead(readCtx, target, key)
	readCancel()
	if err != nil {
		if !allowRemote || ctx.Err() != nil {
			return recordwrite.LocalRead{}, err
		}
		// Hosting the Owner Group does not mean this Node's physical Store is
		// healthy. Another current Owner member may still have a verified data
		// majority; it runs the same read gate before returning any bytes.
		remote, routeErr := service.forwardRead(ctx, target, key)
		if routeErr == nil {
			return remote, nil
		}
		return recordwrite.LocalRead{}, errors.Join(err, routeErr)
	}
	return recordwrite.LocalRead{Body: got.Body, Reference: got.Reference, RegionGeneration: got.RegionGeneration, State: got.State}, nil
}

func (service *Service) forwardRead(ctx context.Context, target router.Target, key []byte) (recordwrite.LocalRead, error) {
	var last error
	candidates := service.client.GroupCandidates(target.OwnerGroupID)
	remaining := 0
	for _, id := range candidates {
		if id != service.runtime.Identity.RaftID {
			remaining++
		}
	}
	for _, id := range candidates {
		if id == service.runtime.Identity.RaftID {
			continue
		}
		attemptsLeft := remaining
		remaining--
		if err := ctx.Err(); err != nil {
			return recordwrite.LocalRead{}, err
		}
		node, found, err := service.runtime.Root.RootMember(id)
		if err != nil {
			return recordwrite.LocalRead{}, err
		}
		if !found {
			continue
		}
		attempt, done := readCandidateContext(ctx, attemptsLeft)
		response, err := service.client.ReadEngineData(attempt, node, target, key)
		done()
		if err == nil {
			return recordwrite.LocalRead{Body: response.Body, Reference: response.Reference,
				RegionGeneration: response.RegionGeneration, State: response.State}, nil
		}
		last = err
	}
	if last == nil {
		last = ErrOwnerNotLocal
	}
	return recordwrite.LocalRead{}, fmt.Errorf("route engine read to Owner Group %s: %w", target.OwnerGroupID, last)
}

// Each candidate gets a bounded attempt while preserving time for later
// current Owner members. Timeouts never weaken the independent Owner/Root/
// physical-majority gates for either read or write.
func readCandidateContext(ctx context.Context, remaining int) (context.Context, context.CancelFunc) {
	return candidateContext(ctx, remaining, readAttemptTimeout)
}

func writeCandidateContext(ctx context.Context, remaining int) (context.Context, context.CancelFunc) {
	return candidateContext(ctx, remaining, writeAttemptTimeout)
}

func candidateContext(ctx context.Context, remaining int, maximum time.Duration) (context.Context, context.CancelFunc) {
	if remaining < 1 {
		remaining = 1
	}
	limit := maximum
	if deadline, ok := ctx.Deadline(); ok {
		share := time.Until(deadline) / time.Duration(remaining)
		if share < limit {
			limit = share
		}
	}
	return context.WithTimeout(ctx, limit)
}

func (service *Service) ExecuteRead(ctx context.Context, target router.Target, key []byte) (raftrpc.EngineDataReadResult, error) {
	if service == nil || service.runtime == nil || ctx == nil || target.Validate() != nil ||
		!bytes.Equal(target.RoutingKey, key) {
		return raftrpc.EngineDataReadResult{}, errors.New("engine data read differs from its Owner route")
	}
	owner, found := service.runtime.Engines.Resolve(target.OwnerGroupID)
	if !found || owner == nil {
		return raftrpc.EngineDataReadResult{}, ErrOwnerNotLocal
	}
	reader := recordwrite.Reader{Barrier: service.runtime, Root: service.runtime.Root,
		Owner: owner, LocalID: service.runtime.Identity.RaftID, LocalNodeID: service.runtime.Identity.NodeID,
		Store: service.runtime.Records, Observer: service.client, Fetcher: service.client}
	got, err := reader.ReadLocal(ctx, target, key)
	if err != nil {
		geo, geoErr := reader.ReadGeo(ctx, target, key, 16, 256)
		if geoErr == nil {
			return raftrpc.EngineDataReadResult{Body: geo.Body, Reference: geo.Reference,
				RegionGeneration: geo.RegionGeneration, State: geo.State}, nil
		}
		return raftrpc.EngineDataReadResult{}, errors.Join(err, geoErr)
	}
	return raftrpc.EngineDataReadResult{Body: got.Body, Reference: got.Reference,
		RegionGeneration: got.RegionGeneration, State: got.State}, nil
}

func (service *Service) resolveTarget(ctx context.Context, keyspace rootstate.Keyspace, key []byte) (router.Target, error) {
	barrier := rootRouteBarrier{local: service.runtime}
	routes, err := router.New(barrier, service.runtime.Root)
	if err != nil {
		return router.Target{}, err
	}
	return retryRoute(ctx, routeAttemptTimeout, func(attempt context.Context) (router.Target, error) {
		return routes.ResolveTarget(attempt, keyspace, key)
	})
}

// A ReadIndex sent during a Root leader change can remain pending until its
// context expires. Retry a fresh, quorum-confirmed cut; never reuse a route
// from an unconfirmed or stale local directory view.
func retryRoute(ctx context.Context, attemptTimeout time.Duration,
	resolve func(context.Context) (router.Target, error)) (router.Target, error) {
	for {
		if err := ctx.Err(); err != nil {
			return router.Target{}, err
		}
		attempt, cancel := context.WithTimeout(ctx, attemptTimeout)
		target, err := resolve(attempt)
		cancel()
		if err == nil {
			return target, nil
		}
		if ctx.Err() != nil {
			return router.Target{}, ctx.Err()
		}
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, raft.ErrNoLeader) &&
			!errors.Is(err, router.ErrCatalogBehind) {
			return router.Target{}, err
		}
		timer := time.NewTimer(routeRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return router.Target{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (service *Service) currentPublication(ctx context.Context, owner *enginestate.State, target router.Target,
	descriptor records.Descriptor, requestID string) bool {
	id, err := descriptor.ID()
	if err != nil {
		return false
	}
	current := false
	_, err = owner.ReadRouted(ctx, service.runtime, target, func(view enginestate.ReadView) error {
		ref, found, err := view.RecordReference(descriptor.Key)
		if err != nil || !found {
			return err
		}
		refID, err := ref.Descriptor.ID()
		current = err == nil && refID == id && ref.RequestID == requestID && ref.OwnerGroupID == target.OwnerGroupID &&
			ref.RangeID == target.RangeID && ref.RangeEpoch == target.Epoch && bytes.Equal(ref.RoutingKey, target.RoutingKey)
		return err
	})
	return err == nil && current
}
