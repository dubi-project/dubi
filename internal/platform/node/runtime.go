// Package node assembles the durable cluster components owned by one DUBI
// server process.
package node

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/capabilityruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
	"github.com/dubi-project/dubi/internal/platform/cluster/groupruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/rangebootstrap"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"google.golang.org/grpc"
)

type Options struct {
	StoreRoot          string
	TickInterval       time.Duration
	Transport          raft.Transport
	RootBootstrapPeers []uint64
	CapabilityMux      *capabilityruntime.Mux
}

// Runtime owns components in their shutdown order. Root is owned by Host once
// Open succeeds; callers must close Runtime rather than individual fields.
type Runtime struct {
	Identity        identity.Identity
	Host            *raft.Host
	Root            *rootstate.State
	Groups          *groupruntime.Registry
	Capabilities    *capabilityruntime.Runtime
	JoinManager     *groupmembership.JoinManager
	LocalMembership *groupmembership.LocalControl

	mu            sync.Mutex
	recoverMu     sync.Mutex
	closed        bool
	rpcRegistered bool
}

// RecoverLocalGroups reopens Groups whose published membership contains this
// Node, plus joining Groups named by a pending membership task. It requires a
// Root quorum barrier and is intentionally separate from Open so the network
// listener can become available before recovery waits for cluster quorum.
func (runtime *Runtime) RecoverLocalGroups(ctx context.Context) error {
	if runtime == nil || runtime.Host == nil || runtime.Root == nil || runtime.Groups == nil {
		return errors.New("Node runtime is incomplete")
	}
	runtime.recoverMu.Lock()
	defer runtime.recoverMu.Unlock()
	if _, err := runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	descriptors, err := runtime.Root.Groups()
	if err != nil {
		return err
	}
	targets := make(map[string]bool)
	for _, descriptor := range descriptors {
		if containsMember(descriptor.Members, runtime.Identity.RaftID) {
			targets[descriptor.GroupID] = false
		}
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
		joining := targets[groupID]
		state, err := runtime.Groups.NewState(ctx, groupID)
		if err != nil {
			return err
		}
		if err := runtime.Host.AddGroup(groupID, nil, joining, state); err != nil {
			_ = state.Close()
			return err
		}
	}
	return nil
}

func containsMember(members []rootstate.GroupMember, raftID uint64) bool {
	for _, member := range members {
		if member.RaftID == raftID {
			return true
		}
	}
	return false
}

func Open(options Options) (*Runtime, error) {
	if options.StoreRoot == "" || options.CapabilityMux == nil || options.TickInterval < 0 {
		return nil, errors.New("Node runtime requires StoreRoot, Capability Mux, and a non-negative tick interval")
	}
	value, err := identity.Load(options.StoreRoot)
	if err != nil {
		return nil, err
	}
	capabilities, err := capabilityruntime.NewRuntime(options.StoreRoot, options.CapabilityMux)
	if err != nil {
		return nil, err
	}
	root, err := rootstate.Open(filepath.Join(options.StoreRoot, "dubi-cluster-state"))
	if err != nil {
		return nil, err
	}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: options.StoreRoot,
		TickInterval: options.TickInterval, Transport: options.Transport})
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	fail := func(openErr error) (*Runtime, error) {
		return nil, errors.Join(openErr, host.Close(), root.Close())
	}
	if err := host.AddGroup(raft.RootGroupID, options.RootBootstrapPeers, false, root); err != nil {
		return fail(err)
	}
	groups, err := groupruntime.New(root, host)
	if err != nil {
		return fail(err)
	}
	if err := capabilities.Register(groups); err != nil {
		return fail(err)
	}
	joiner, err := groupmembership.NewJoinManager(host, value.RaftID, groups.NewState)
	if err != nil {
		return fail(err)
	}
	localMembership, err := groupmembership.NewLocalControl(host, joiner)
	if err != nil {
		return fail(err)
	}
	return &Runtime{Identity: value, Host: host, Root: root, Groups: groups,
		Capabilities: capabilities, JoinManager: joiner, LocalMembership: localMembership}, nil
}

// LocalOwner resolves only capability Groups currently open on this Node.
// Cluster-wide coordinators should combine it with remote Owner RPC routing.
func (runtime *Runtime) LocalOwner(groupID string) (rangebootstrap.OwnerControl, bool) {
	if runtime == nil || runtime.Host == nil || runtime.Capabilities == nil {
		return nil, false
	}
	state, found := runtime.Capabilities.Resolve(groupID)
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
	raftServer, err := raftrpc.NewServer(clusterID, raftID, runtime.Host, authorize, maxSize)
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
	capabilityServer, err := raftrpc.NewCapabilityServer(clusterID, raftID, runtime.Host,
		runtime.Capabilities.Resolve, authorize, maxSize)
	if err != nil {
		return err
	}
	membershipServer, err := raftrpc.NewMembershipServer(clusterID, raftID, runtime.Host,
		runtime.JoinManager, authorize)
	if err != nil {
		return err
	}
	raftwire.RegisterRaftServiceServer(registrar, raftServer)
	raftwire.RegisterGroupServiceServer(registrar, groupServer)
	raftwire.RegisterRangeControlServiceServer(registrar, rangeServer)
	raftwire.RegisterCapabilityServiceServer(registrar, capabilityServer)
	raftwire.RegisterGroupMembershipServiceServer(registrar, membershipServer)
	runtime.rpcRegistered = true
	return nil
}

func (runtime *Runtime) Close() error {
	if runtime == nil {
		return nil
	}
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return nil
	}
	runtime.closed = true
	host := runtime.Host
	runtime.mu.Unlock()
	if host == nil {
		return nil
	}
	return host.Close()
}
