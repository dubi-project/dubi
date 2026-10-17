// Package node assembles the durable cluster components owned by one DUBI
// server process.
package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/engineruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
	"github.com/dubi-project/dubi/internal/platform/cluster/groupruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/rangebootstrap"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/protocol"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"google.golang.org/grpc"
)

type Options struct {
	StoreRoot          string
	TickInterval       time.Duration
	Transport          raft.Transport
	RootBootstrapPeers []uint64
	EngineMux          *engineruntime.Mux
}

// Runtime owns components in their shutdown order. Root is owned by Host once
// Open succeeds; callers must close Runtime rather than individual fields.
type Runtime struct {
	Identity        identity.Identity
	Host            *raft.Host
	Root            *rootstate.State
	Groups          *groupruntime.Registry
	Engines         *engineruntime.Runtime
	JoinManager     *groupmembership.JoinManager
	LocalMembership *groupmembership.LocalControl
	Volumes         *volume.Registry
	Packs           *pack.Pool
	Records         *records.Store
	Snapshots       *snapshot.Store
	RootJoin        *rootjoin.LocalStore
	// rootReadIndex optionally confirms a Root barrier through a remote voter.
	// Learners cannot always complete a local ReadIndex before promotion, so
	// production startup supplies this through the authenticated GroupClient.
	rootReadIndex func(context.Context) (uint64, error)

	mu                   sync.Mutex
	recoverMu            sync.Mutex
	closed               bool
	rpcRegistered        bool
	recoveryContext      context.Context
	cancelRecovery       context.CancelFunc
	closeDone            chan struct{}
	closeErr             error
	joinRecoveryDeferred bool
}

// SetRootReadIndex supplies the authenticated remote Root barrier used while
// an enrolled learner is recovering. It must be set before Run begins.
func (runtime *Runtime) SetRootReadIndex(read func(context.Context) (uint64, error)) {
	if runtime != nil {
		runtime.rootReadIndex = read
	}
}

// ReadRootIndex confirms a linearizable Root barrier through the configured
// remote voter path when local ReadIndex is unavailable. A joined Node can be
// a healthy Root voter while its local leader hint is still catching up, so
// callers must not equate a local ReadIndex timeout with cluster unreachability.
func (runtime *Runtime) ReadRootIndex(ctx context.Context) (uint64, error) {
	if runtime == nil || runtime.Host == nil || runtime.Root == nil {
		return 0, errors.New("Node runtime is incomplete")
	}
	// Use the server-supplied authenticated path whenever it exists. It first
	// tries the local voter and then a current remote Root voter, with bounded
	// RPC deadlines. Calling Host.ReadIndex directly here can keep startup in
	// a long retry cycle when the local follower's leader hint is stale.
	if runtime.rootReadIndex == nil {
		return runtime.Host.ReadIndex(ctx, raft.RootGroupID)
	}
	index, err := runtime.rootReadIndex(ctx)
	if err != nil {
		return 0, err
	}
	// Voters already apply the Root log locally as part of normal Raft
	// progress. Do not chase a moving remote barrier index during startup:
	// topology reconciliation can advance Root again before a WAN follower
	// observes the previous cut. Learners still need the explicit wait because
	// their remote barrier is the only proof that the local Root view is ready.
	if membership, membershipErr := runtime.Host.Membership(raft.RootGroupID); membershipErr == nil {
		for _, voter := range membership.GetVoters() {
			if voter == runtime.Identity.RaftID {
				return index, nil
			}
		}
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for runtime.Root.AppliedIndex() < index {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
		}
	}
	return index, nil
}

func (runtime *Runtime) confirmRoot(ctx context.Context) error {
	return confirmStartupRoot(ctx, func(attempt context.Context) error {
		_, err := runtime.ReadRootIndex(attempt)
		return err
	}, time.Second)
}

// RecoverLocalGroups reopens Groups whose published membership contains this
// Node, immutable initial regional formation participants, and joining Groups
// named by a pending membership task. It requires a
// Root quorum barrier and is intentionally separate from Open so the network
// listener can become available before recovery waits for cluster quorum.
func (runtime *Runtime) RecoverLocalGroups(ctx context.Context) error {
	if runtime == nil || runtime.Host == nil || runtime.Root == nil || runtime.Groups == nil {
		return errors.New("Node runtime is incomplete")
	}
	runtime.recoverMu.Lock()
	defer runtime.recoverMu.Unlock()
	runtime.mu.Lock()
	closed := runtime.closed
	runtime.mu.Unlock()
	if closed {
		return errors.New("Node runtime is closed")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if runtime.recoveryContext != nil {
		stop := context.AfterFunc(runtime.recoveryContext, cancel)
		defer stop()
	}
	if err := runtime.waitForRootJoinPosition(ctx); err != nil {
		return err
	}
	if err := runtime.confirmRoot(ctx); err != nil {
		return err
	}
	if err := runtime.activateRootJoin(); err != nil {
		return err
	}
	if runtime.RootJoin != nil {
		runtime.mu.Lock()
		deferRecovery := !runtime.joinRecoveryDeferred
		if deferRecovery {
			// The first post-enrollment recovery only establishes Root readiness.
			// Regional Groups may be part of the placement cut that promotes this
			// Node; defer opening them to the scheduler after startup so a slow or
			// transitional Group cannot block the Node's ready hand-off.
			runtime.joinRecoveryDeferred = true
		}
		runtime.mu.Unlock()
		if deferRecovery {
			return nil
		}
	}
	descriptors, err := runtime.Root.Groups()
	if err != nil {
		return err
	}
	targets := make(map[string]bool)
	initialPeers := make(map[string][]uint64)
	for _, descriptor := range descriptors {
		if containsMember(descriptor.Members, runtime.Identity.RaftID) {
			targets[descriptor.GroupID] = false
		}
		if descriptor.GroupID == raft.RootGroupID {
			continue
		}
		cohort, found, err := runtime.Root.GroupInitialCohort(descriptor.GroupID)
		if err != nil {
			return err
		}
		if !found || (!cohort.Formation && !cohort.Reformation) || !slices.Contains(cohort.Nodes, runtime.Identity.RaftID) {
			cohort, found, err = runtime.Root.PendingRegionFormation(descriptor.GroupID)
			if err != nil {
				return err
			}
			if !found || (!cohort.Formation && !cohort.Reformation) || !slices.Contains(cohort.Nodes, runtime.Identity.RaftID) {
				continue
			}
		}
		// Unpublished initial membership is authorized by the durable cohort,
		// never by an RPC-supplied peer list. Published removal remains removal.
		if len(descriptor.Members) != 0 && !containsMember(descriptor.Members, runtime.Identity.RaftID) {
			continue
		}
		if len(descriptor.Members) == 0 && descriptor.Epoch != 1 {
			return errors.New("unpublished regional Group has invalid Epoch")
		}
		node, member, err := runtime.Root.RootMember(runtime.Identity.RaftID)
		if err != nil {
			return err
		}
		if !member || node.NodeID != runtime.Identity.NodeID || node.RegionID != cohort.RegionID {
			return errors.New("regional formation requires current local Root membership and Region")
		}
		targets[descriptor.GroupID] = false
		initialPeers[descriptor.GroupID] = slices.Clone(cohort.Nodes)
	}
	changes, err := runtime.Root.PendingGroupChanges()
	if err != nil {
		return err
	}
	for _, change := range changes {
		if !containsMember(change.Members, runtime.Identity.RaftID) {
			continue
		}
		if _, found := targets[change.GroupID]; found {
			continue
		}
		_, found, err := runtime.Root.Group(change.GroupID)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("pending Group membership target is not registered")
		}
		targets[change.GroupID] = true
	}
	ids := make([]string, 0, len(targets))
	for groupID := range targets {
		if groupID != raft.RootGroupID {
			ids = append(ids, groupID)
		}
	}
	sort.Strings(ids)
	for _, groupID := range ids {
		if runtime.Host.HasGroup(groupID) {
			continue
		}
		// A remote membership reconciler may be preparing this same local
		// engine Group at the exact moment startup recovery reaches it. The
		// engine state is registered before Host.AddGroup, so wait for that
		// owner instead of opening a second state and colliding on its Store lock.
		if _, open := runtime.Engines.Resolve(groupID); open {
			if err := runtime.waitForGroupOpen(ctx, groupID); err != nil {
				return err
			}
			continue
		}
		joining := targets[groupID]
		descriptor, found, err := runtime.Root.Group(groupID)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("regional Group disappeared from Root")
		}
		state, err := runtime.Groups.NewStateFromDescriptor(ctx, descriptor)
		if err != nil {
			if errors.Is(err, engineruntime.ErrGroupStateAlreadyOpen) {
				if waitErr := runtime.waitForGroupOpen(ctx, groupID); waitErr != nil {
					return waitErr
				}
				continue
			}
			return err
		}
		if err := runtime.Host.AddGroup(groupID, initialPeers[groupID], joining, state); err != nil {
			_ = state.Close()
			return err
		}
	}
	return nil
}

func (runtime *Runtime) waitForGroupOpen(ctx context.Context, groupID string) error {
	return waitForGroupOpen(ctx, runtime.Host, groupID)
}

type groupPresence interface{ HasGroup(string) bool }

func waitForGroupOpen(ctx context.Context, host groupPresence, groupID string) error {
	if ctx == nil || host == nil {
		return errors.New("Group recovery context is required")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if host.HasGroup(groupID) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func containsMember(members []rootstate.GroupMember, raftID uint64) bool {
	for _, member := range members {
		if member.RaftID == raftID {
			return true
		}
	}
	return false
}

// InitializeRoot persists the first Node's membership during explicit init.
// A repeated init never replaces an existing membership, even after expansion.
// Serving a node uses Open without bootstrap peers and only recovers disk state.
func InitializeRoot(store string, volumePaths ...string) error {
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: store, TickInterval: time.Hour})
	if err != nil {
		return err
	}
	volumes, err := volume.Initialize(store, volumePaths)
	if err != nil {
		return errors.Join(err, host.Close())
	}
	packs, err := pack.OpenPool(volumes)
	if err != nil {
		return errors.Join(err, volumes.Close(), host.Close())
	}
	data, err := records.OpenNodeStore(packs, volumes)
	if err != nil {
		return errors.Join(err, packs.Close(), volumes.Close(), host.Close())
	}
	// init establishes this mandatory resource without exposing any data API.
	if err := errors.Join(data.Close(), packs.Close()); err != nil {
		return errors.Join(err, volumes.Close(), host.Close())
	}
	root, err := rootstate.Open(filepath.Join(store, "dubi-cluster-state"))
	if err != nil {
		return errors.Join(err, volumes.Close(), host.Close())
	}
	if root.AppliedIndex() != 0 {
		// Initialization is already durable. Runtime startup will validate and
		// reconcile this state against the Raft log before serving requests.
		return errors.Join(root.Close(), volumes.Close(), host.Close())
	}
	err = host.AddGroup(raft.RootGroupID, []uint64{host.Identity().RaftID}, false, root)
	if err != nil {
		return errors.Join(err, root.Close(), volumes.Close(), host.Close())
	}
	return errors.Join(volumes.Close(), host.Close())
}

func Open(options Options) (*Runtime, error) {
	if options.StoreRoot == "" || options.EngineMux == nil || options.TickInterval < 0 {
		return nil, errors.New("Node runtime requires StoreRoot, Engine Mux, and a non-negative tick interval")
	}
	if err := protocol.CurrentAdvertisement().Validate(); err != nil {
		return nil, fmt.Errorf("validate local protocol formats: %w", err)
	}
	value, err := identity.Load(options.StoreRoot)
	if err != nil {
		return nil, err
	}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: options.StoreRoot,
		TickInterval: options.TickInterval, Transport: options.Transport})
	if err != nil {
		return nil, err
	}
	var volumes *volume.Registry
	if len(options.RootBootstrapPeers) != 0 {
		// Internal bootstrap fixtures use this path; the production CLI has
		// already registered volumes during explicit init/join preparation.
		volumes, err = volume.Initialize(options.StoreRoot, nil)
	} else {
		volumes, err = volume.Open(options.StoreRoot)
	}
	if err != nil {
		return nil, errors.Join(err, host.Close())
	}
	packs, err := pack.OpenPool(volumes)
	if err != nil {
		return nil, errors.Join(err, volumes.Close(), host.Close())
	}
	data, err := records.OpenNodeStore(packs, volumes)
	if err != nil {
		return nil, errors.Join(err, packs.Close(), volumes.Close(), host.Close())
	}
	var localJoin *rootjoin.LocalStore
	var localJoinState rootjoin.LocalState
	if value.Origin == identity.OriginJoin && len(options.RootBootstrapPeers) == 0 {
		localJoin, err = rootjoin.OpenLocalStore(options.StoreRoot, packs.StoreID())
		if err != nil {
			return nil, errors.Join(err, data.Close(), packs.Close(), volumes.Close(), host.Close())
		}
		localJoinState, err = localJoin.Load()
		if errors.Is(err, rootjoin.ErrLocalStateMissing) {
			// Existing development Nodes still recover their actual Raft state;
			// absence of this marker never authorizes empty Root joining.
			if err := localJoin.Close(); err != nil {
				return nil, errors.Join(err, data.Close(), packs.Close(), volumes.Close(), host.Close())
			}
			localJoin = nil
		} else if err != nil {
			return nil, errors.Join(err, localJoin.Close(), data.Close(), packs.Close(), volumes.Close(), host.Close())
		}
	}
	artifacts, err := snapshot.OpenNodeStore(options.StoreRoot, packs.StoreID())
	if err != nil {
		return nil, errors.Join(err, localJoin.Close(), data.Close(), packs.Close(), volumes.Close(), host.Close())
	}
	recoveryContext, cancelRecovery := context.WithCancel(context.Background())
	engines, err := engineruntime.NewRuntimeWithSnapshots(options.StoreRoot, options.EngineMux, packs, artifacts, recoveryContext)
	if err != nil {
		cancelRecovery()
		return nil, errors.Join(err, localJoin.Close(), artifacts.Close(), data.Close(), packs.Close(), volumes.Close(), host.Close())
	}
	root, err := rootstate.Open(filepath.Join(options.StoreRoot, "dubi-cluster-state"))
	if err != nil {
		cancelRecovery()
		return nil, errors.Join(err, localJoin.Close(), artifacts.Close(), data.Close(), packs.Close(), volumes.Close(), host.Close())
	}
	fail := func(openErr error) (*Runtime, error) {
		cancelRecovery()
		return nil, errors.Join(openErr, localJoin.Close(), host.Close(), root.Close(), artifacts.Close(), data.Close(), packs.Close(), volumes.Close())
	}
	rootMachine, err := rootstate.WithSnapshots(root, artifacts, recoveryContext)
	if err != nil {
		return fail(err)
	}
	joining := localJoin != nil && localJoinState.Phase == rootjoin.LocalCatchingUp
	if err := host.AddGroup(raft.RootGroupID, options.RootBootstrapPeers, joining, rootMachine); err != nil {
		return fail(err)
	}
	if localJoin != nil {
		applied, membership, err := root.RootPosition()
		if err != nil {
			return fail(err)
		}
		if err := localJoinState.CheckRecovered(applied, membership); err != nil {
			return fail(err)
		}
	}
	groups, err := groupruntime.New(root, host)
	if err != nil {
		return fail(err)
	}
	if err := engines.Register(groups); err != nil {
		return fail(err)
	}
	joiningFactory := func(ctx context.Context, task rootstate.GroupTaskReference) (raft.StateMachine, error) {
		if _, err := host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			return nil, err
		}
		if err := root.AuthorizeGroupPreparation(value.RaftID, value.RaftID, task, value.RaftID); err != nil {
			return nil, err
		}
		state, err := groups.NewState(ctx, task.GroupID)
		if !errors.Is(err, engineruntime.ErrGroupStateAlreadyOpen) {
			return state, err
		}
		// Startup recovery may have registered the durable engine state a
		// moment before attaching the Raft Group. Wait for that owner instead of
		// reporting a transient duplicate-open failure to the scheduler.
		if waitErr := waitForGroupOpen(ctx, host, task.GroupID); waitErr != nil {
			return nil, waitErr
		}
		return nil, groupmembership.ErrJoiningGroupAlreadyOpen
	}
	joiner, err := groupmembership.NewJoinManager(host, value.RaftID, joiningFactory)
	if err != nil {
		return fail(err)
	}
	localMembership, err := groupmembership.NewLocalControl(host, joiner, root, value.RaftID)
	if err != nil {
		return fail(err)
	}
	return &Runtime{Identity: value, Host: host, Root: root, Groups: groups,
		Engines: engines, JoinManager: joiner, LocalMembership: localMembership, Volumes: volumes, Packs: packs, Records: data,
		Snapshots: artifacts, RootJoin: localJoin, recoveryContext: recoveryContext, cancelRecovery: cancelRecovery, closeDone: make(chan struct{})}, nil
}

// LocalOwner resolves only engine Groups currently open on this Node.
// Cluster-wide coordinators should combine it with remote Owner RPC routing.
func (runtime *Runtime) LocalOwner(groupID string) (rangebootstrap.OwnerControl, bool) {
	if runtime == nil || runtime.Host == nil || runtime.Engines == nil {
		return nil, false
	}
	state, found := runtime.Engines.Resolve(groupID)
	if !found {
		return nil, false
	}
	owner, err := rangebootstrap.NewLocalOwner(runtime.Host, state)
	return owner, err == nil
}

// RegisterInternalRPC attaches all inbound cluster services to one gRPC
// registrar. TLS listener configuration remains the server entrypoint's job;
// every handler still requires a certificate-backed AuthorizePeer callback.
func (runtime *Runtime) RegisterInternalRPC(registrar grpc.ServiceRegistrar,
	authorize raftrpc.AuthorizePeer, maxSize int,
) error {
	if runtime == nil || registrar == nil || authorize == nil {
		return errors.New("Node runtime RPC registration requires registrar and peer authorizer")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return errors.New("Node runtime is closed")
	}
	if runtime.rpcRegistered {
		return errors.New("Node runtime RPC services are already registered")
	}
	clusterID, raftID := runtime.Identity.ClusterID, runtime.Identity.RaftID
	raftServer, err := raftrpc.NewServer(clusterID, raftID, snapshotStepper{Host: runtime.Host, runtime: runtime}, authorize, maxSize)
	if err != nil {
		return err
	}
	groupServer, err := raftrpc.NewGroupServer(clusterID, raftID, runtime.Host, authorize, maxSize)
	if err != nil {
		return err
	}
	rangeServer, err := raftrpc.NewRangeControlServer(clusterID, raftID, runtime.LocalOwner, authorize)
	if err != nil {
		return err
	}
	engineServer, err := raftrpc.NewEngineServer(clusterID, raftID, runtime.Host,
		runtime.Engines.Resolve, authorize, maxSize)
	if err != nil {
		return err
	}
	membershipServer, err := raftrpc.NewMembershipServer(clusterID, raftID, runtime.Host,
		runtime.JoinManager, runtime.Root, authorize)
	if err != nil {
		return err
	}
	regionServer, err := raftrpc.NewRegionFenceServer(clusterID, raftID, runtime.Host, runtime.Root, runtime.Engines.Resolve, authorize)
	if err != nil {
		return err
	}
	regionServer.SetRootReadIndex(runtime.ReadRootIndex)
	holdingServer, err := raftrpc.NewRecordHoldingServer(runtime.Identity, runtime.Host, runtime.Root, runtime.Records, authorize)
	if err != nil {
		return err
	}
	cutServer, err := raftrpc.NewRecordCutServer(holdingServer, runtime.Engines.Resolve, runtime.Snapshots)
	if err != nil {
		return err
	}
	transferServer, err := raftrpc.NewRecordTransferServer(runtime.Identity, runtime.Host, runtime.Root, runtime.Records, authorize)
	if err != nil {
		return err
	}
	raftwire.RegisterRaftServiceServer(registrar, raftServer)
	raftwire.RegisterGroupServiceServer(registrar, groupServer)
	raftwire.RegisterRangeControlServiceServer(registrar, rangeServer)
	raftwire.RegisterEngineServiceServer(registrar, engineServer)
	raftwire.RegisterGroupMembershipServiceServer(registrar, membershipServer)
	raftwire.RegisterRegionFenceServiceServer(registrar, regionServer)
	raftwire.RegisterRecordHoldingServiceServer(registrar, holdingServer)
	raftwire.RegisterRecordCutServiceServer(registrar, cutServer)
	raftwire.RegisterRecordTransferServiceServer(registrar, transferServer)
	runtime.rpcRegistered = true
	return nil
}

// BeginShutdown cancels Node-owned recovery without releasing any resource.
// The server calls this before joining RPC handlers, some of which may be
// waiting on a durable snapshot restore. Close still performs the ownership
// barrier and must follow. Cancellation is safe to repeat concurrently.
func (runtime *Runtime) BeginShutdown() {
	if runtime != nil && runtime.cancelRecovery != nil {
		runtime.cancelRecovery()
	}
}

func (runtime *Runtime) Close() error {
	if runtime == nil {
		return nil
	}
	runtime.mu.Lock()
	if runtime.closeDone == nil {
		runtime.closeDone = make(chan struct{})
	}
	if runtime.closed {
		done := runtime.closeDone
		runtime.mu.Unlock()
		<-done
		return runtime.closeErr
	}
	runtime.closed = true
	host := runtime.Host
	runtime.mu.Unlock()
	runtime.BeginShutdown()
	// Cancel and join startup recovery before closing state or releasing Store
	// ownership. A concurrent caller that acquires this lock later sees closed.
	runtime.recoverMu.Lock()
	defer runtime.recoverMu.Unlock()
	// Close Group state before releasing Volume ownership. Host.Close also
	// releases the Store lock; the registry lock protects the short tail.
	var err error
	if runtime.RootJoin != nil {
		err = runtime.RootJoin.Close() // Stop all local metadata writes before Host releases the Store lock.
	}
	if host != nil {
		err = errors.Join(err, host.Close())
	}
	if runtime.Snapshots != nil {
		err = errors.Join(err, runtime.Snapshots.Close())
	}
	// The index must stop using Pool before Pool or its parent catalog close.
	if runtime.Records != nil {
		err = errors.Join(err, runtime.Records.Close())
	}
	if runtime.Packs != nil {
		err = errors.Join(err, runtime.Packs.Close())
	}
	if runtime.Volumes != nil {
		err = errors.Join(err, runtime.Volumes.Close())
	}
	runtime.mu.Lock()
	runtime.closeErr = err
	close(runtime.closeDone)
	runtime.mu.Unlock()
	return err
}
