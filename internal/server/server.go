// Package server assembles and runs one DUBI process around the platform Node
// runtime. The optional public listener is isolated from internal Node RPC.
package server

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginedata"
	"github.com/dubi-project/dubi/internal/platform/cluster/engineruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/enrollmentauthority"
	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
	"github.com/dubi-project/dubi/internal/platform/cluster/noderemoval"
	"github.com/dubi-project/dubi/internal/platform/cluster/rangeplacement"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/roottopology"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/nodeadminrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/nodecertrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/nodecertwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
)

type Options struct {
	StoreRoot          string
	Listener           net.Listener
	PublicListener     net.Listener
	ServerTLS          *tls.Config
	ClientTLS          *tls.Config
	AuthorizePeer      raftrpc.AuthorizePeer
	EngineMux          *engineruntime.Mux
	RootBootstrapPeers []uint64
	BootstrapDirectory raftrpc.Directory
	TickInterval       time.Duration
	MembershipInterval time.Duration
	MaxMessageSize     int
	ReportError        groupmembership.ErrorReporter
	// Only an existing managed authority may serve enrollment. Joining Nodes
	// never receive the CA signer. This is internal composition, not a CLI flag.
	EnrollmentSigner *nodepki.Signer
	// Normal managed CLI startup supplies this. Nil retains internal fixture
	// composition without automatic Node catalog publication.
	Network *NetworkOptions
}

type Server struct {
	Runtime              *node.Runtime
	GroupClient          *raftrpc.GroupClient
	EngineData           *enginedata.Service // In-process only; not Tenant-authenticated.
	MembershipReconciler *groupmembership.Reconciler
	NodeRemoval          *noderemoval.Coordinator

	listener        net.Listener
	publicListener  net.Listener
	publicHTTP      *http.Server
	publicTLS       *tls.Config
	grpc            *grpc.Server
	transport       *raftrpc.Transport
	scheduler       *groupmembership.Scheduler
	certificates    *certificateRenewal
	regional        *regionalFormation
	liveRepair      *liveRecordRepair
	repairProgress  *recordrepair.ProgressStore
	geoRetry        *recordwrite.GeoMetadataRetryStore
	geoRecordRepair *recordrepair.GeoRecordRepairStore
	geoRepair       *geoMetadataRepair
	volumeMigration *volumeMigrationWorker
	ready           chan struct{}
	localNode       *rootstate.Node
	desiredName     string
	storeRoot       string

	mu        sync.Mutex
	cancel    context.CancelFunc
	started   bool
	closed    bool
	workers   sync.WaitGroup
	closeDone chan struct{}
	closeErr  error
}

func closeRepairProgress(store *recordrepair.ProgressStore) error {
	if store == nil {
		return nil
	}
	return store.Close()
}

func closeGeoRetry(store *recordwrite.GeoMetadataRetryStore) error {
	if store == nil {
		return nil
	}
	return store.Close()
}

func closeGeoRecordRepair(store *recordrepair.GeoRecordRepairStore) error {
	if store == nil {
		return nil
	}
	return store.Close()
}

func Open(options Options) (*Server, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	// Multiple background workers share the CLI reporter. Do not assume an
	// arbitrary supplied io.Writer/callback is safe for concurrent invocation.
	if reporter := options.ReportError; reporter != nil {
		var reportMu sync.Mutex
		options.ReportError = func(err error) {
			reportMu.Lock()
			defer reportMu.Unlock()
			reporter(err)
		}
	}
	transportSlot := newTransportSlot()
	runtime, err := node.Open(node.Options{StoreRoot: options.StoreRoot,
		TickInterval: options.TickInterval, Transport: transportSlot,
		RootBootstrapPeers: options.RootBootstrapPeers, EngineMux: options.EngineMux})
	if err != nil {
		_ = options.Listener.Close()
		if options.PublicListener != nil {
			_ = options.PublicListener.Close()
		}
		return nil, err
	}
	fail := func(openErr error, transport *raftrpc.Transport, client *raftrpc.GroupClient) (*Server, error) {
		var failures []error
		if client != nil {
			failures = append(failures, client.Close())
		}
		if transport != nil {
			failures = append(failures, transport.Close())
		}
		failures = append(failures, runtime.Close(), options.Listener.Close())
		if options.PublicListener != nil {
			failures = append(failures, options.PublicListener.Close())
		}
		return nil, errors.Join(openErr, errors.Join(failures...))
	}
	var localNode *rootstate.Node
	if options.Network != nil {
		descriptor, err := resolveLocalNetwork(runtime, options.Listener.Addr().String(), *options.Network)
		if err != nil {
			return fail(err, nil, nil)
		}
		// Validate deployment input before exposing the listener or mutating Root.
		if _, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion, RequestID: "validate-local-node", Type: rootstate.RegisterNode, Node: &descriptor}); err != nil {
			return fail(err, nil, nil)
		}
		localNode = &descriptor
	}
	rootDirectory := raftrpc.NewRootDirectory(runtime.Root)
	directory := raftrpc.Directory(rootDirectory)
	if view, found, err := runtime.RootJoinView(); err != nil {
		return fail(err, nil, nil)
	} else if found {
		cached := raftrpc.NewStaticDirectory()
		for _, peer := range view.Peers {
			if err := cached.Update(peer.RaftID, peer.Address); err != nil {
				return fail(err, nil, nil)
			}
		}
		directory = fallbackDirectory{primary: rootDirectory, fallback: cached}
	}
	if options.BootstrapDirectory != nil {
		directory = fallbackDirectory{primary: directory, fallback: options.BootstrapDirectory}
	}
	clientCredentials := tlscredentials.NewTLS(normalizeClientTLS(options.ClientTLS))
	transport, err := raftrpc.NewTransport(raftrpc.TransportOptions{
		ClusterID: runtime.Identity.ClusterID, RaftID: runtime.Identity.RaftID,
		Directory: directory, Credentials: clientCredentials,
		MaxSize:      options.MaxMessageSize,
		OpenSnapshot: runtime.OpenSnapshot,
		OnFailure: func(groupID string, raftID uint64) {
			_ = runtime.Host.ReportUnreachable(groupID, raftID)
		},
		OnSnapshot: func(groupID string, raftID uint64, success bool) {
			_ = runtime.Host.ReportSnapshot(groupID, raftID, success)
		},
	})
	if err != nil {
		return fail(err, nil, nil)
	}
	if err := transportSlot.Bind(transport); err != nil {
		return fail(err, transport, nil)
	}
	client, err := raftrpc.NewGroupClient(raftrpc.GroupClientOptions{
		ClusterID: runtime.Identity.ClusterID, RaftID: runtime.Identity.RaftID,
		Directory: directory, Credentials: clientCredentials,
		MaxSize: options.MaxMessageSize, GroupRoutes: rootDirectory,
	})
	if err != nil {
		return fail(err, transport, nil)
	}
	runtime.SetRootReadIndex(func(ctx context.Context) (uint64, error) {
		status, _ := runtime.Host.Status(raft.RootGroupID)
		// A Root voter can always ask its local Raft Group to forward a
		// ReadIndex to the current leader. Prefer that path before dialing a
		// peer: startup recovery must not depend on a stale cross-node HTTP/2
		// stream merely because the local voter has not learned the leader hint
		// yet. Joined learners still use the remote fallback below until Root
		// promotes them.
		if membership, membershipErr := runtime.Host.Membership(raft.RootGroupID); membershipErr == nil {
			for _, voter := range membership.GetVoters() {
				if voter == runtime.Identity.RaftID {
					attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
					index, localErr := runtime.Host.ReadIndex(attempt, raft.RootGroupID)
					cancel()
					if localErr == nil {
						return index, nil
					}
					break
				}
			}
		}
		// A fresh bootstrap Root has a voter before its Node descriptor is
		// registered. The local leader can still prove its own single-node
		// barrier, so do not build a routing view (which requires descriptors)
		// until the local path has had a chance to succeed.
		if status.Role == etcdraft.StateLeader && status.LeaderID == runtime.Identity.RaftID {
			attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
			index, localErr := runtime.Host.ReadIndex(attempt, raft.RootGroupID)
			cancel()
			if localErr == nil {
				return index, nil
			}
		}
		cut, err := runtime.Root.RootRoutingView(rootjoin.MaxViewPeers)
		if err != nil {
			return 0, err
		}
		targets := make([]uint64, 0, len(cut.Membership.GetVoters())+1)
		seen := make(map[uint64]bool)
		add := func(id uint64) {
			if id != 0 && id != runtime.Identity.RaftID && !seen[id] {
				seen[id] = true
				targets = append(targets, id)
			}
		}
		add(status.LeaderID)
		for _, id := range cut.Membership.GetVoters() {
			add(id)
		}
		// A local Root leader must try its own barrier first. The old order
		// always skipped the local leader and tried voters in membership order;
		// that made a healthy cross-region leader wait on an unreachable voter
		// before it could confirm its own quorum. Each remote attempt is bounded
		// so one stale address cannot consume the caller's whole deadline.
		for _, target := range targets {
			attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
			index, remoteErr := client.ReadIndex(attempt, target, raft.RootGroupID)
			cancel()
			if remoteErr == nil {
				return index, nil
			}
		}
		return runtime.Host.ReadIndex(ctx, raft.RootGroupID)
	})
	localData, err := enginedata.New(runtime, client)
	if err != nil {
		return fail(err, transport, client)
	}
	routedMembership, err := raftrpc.NewRoutedMembershipControl(client, runtime.Root, runtime.LocalMembership)
	if err != nil {
		return fail(err, transport, client)
	}
	reconciler, err := groupmembership.New(runtime.Host, routedMembership, runtime.Root)
	if err != nil {
		return fail(err, transport, client)
	}
	nodeRemovalExecutor, err := noderemoval.NewRaftExecutor(runtime.Root, runtime.Host, reconciler)
	if err != nil {
		return fail(err, transport, client)
	}
	nodeRemoval, err := noderemoval.NewCoordinator(runtime.Root, nodeRemovalExecutor)
	if err != nil {
		return fail(err, transport, client)
	}
	rootJoinCoordinator, err := rootjoin.New(runtime.Host, runtime.Root)
	if err != nil {
		return fail(err, transport, client)
	}
	rootTasks := rootControlTasks{join: rootJoinCoordinator}
	authorityCoordinator, err := enrollmentauthority.New(runtime.Host, runtime.Root)
	if err != nil {
		return fail(err, transport, client)
	}
	if options.EnrollmentSigner != nil {
		authorityCoordinator.SetTransferer(enrollmentAuthorityTransfer{runtime: runtime, client: client, signer: options.EnrollmentSigner, store: options.StoreRoot})
	}
	rootTasks.authority = authorityCoordinator
	if options.Network != nil {
		topology, err := roottopology.New(runtime.Host, runtime.Root, runtime.Identity.ClusterID)
		if err != nil {
			return fail(err, transport, client)
		}
		rootTasks.topology = topology
	}
	scheduler, err := groupmembership.NewScheduler(runtime.Host, membershipTasks{root: rootTasks, data: reconciler},
		options.MembershipInterval, options.ReportError)
	if err != nil {
		return fail(err, transport, client)
	}
	grpcServer := newInternalRPCServer(options.ServerTLS, options.MaxMessageSize, connectionLifetimes())
	if err := runtime.RegisterInternalRPC(grpcServer, options.AuthorizePeer, options.MaxMessageSize); err != nil {
		grpcServer.Stop()
		return fail(err, transport, client)
	}
	var enrollmentServer *enrollmentrpc.Server
	var certificateServer *nodecertrpc.Server
	nodeAdminServer, err := nodeadminrpc.NewServer(runtime.Identity, options.AuthorizePeer, func(ctx context.Context, target uint64) error {
		_, err := nodeRemoval.Reconcile(ctx, target)
		return err
	})
	if err != nil {
		grpcServer.Stop()
		return fail(err, transport, client)
	}
	nodeAdminServer.SetRenameFunc(func(ctx context.Context, name string) error {
		return (&Server{Runtime: runtime}).RenameNode(ctx, name)
	})
	nodeAdminServer.SetRemoveByNameFunc(func(ctx context.Context, name string) (uint64, error) {
		plan, err := (&Server{Runtime: runtime, NodeRemoval: nodeRemoval}).RemoveNodeByName(ctx, name)
		return plan.Node.RaftID, err
	})
	adminRoute := &nodeAdminRoute{runtime: runtime, directory: rootDirectory, credentials: clientCredentials,
		clusterID: runtime.Identity.ClusterID, localRaft: runtime.Identity.RaftID}
	nodeAdminServer.SetRemoveRouter(adminRoute.remove)
	nodeAdminServer.SetRemoveByNameRouter(adminRoute.removeByName)
	nodeAdminServer.SetSignerWrappingKeyFunc(func(ctx context.Context) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nodepki.SignerWrappingPublicKey(options.StoreRoot, runtime.Identity, time.Now().UTC())
	})
	nodeAdminServer.SetTransferSignerFunc(func(ctx context.Context, from uint64, epoch uint64, envelope []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, found, err := runtime.Root.Node(runtime.Identity.RaftID); err != nil {
			return err
		} else if !found {
			return errors.New("signer transfer target is not registered in Root")
		}
		local, found, err := runtime.Root.Node(runtime.Identity.RaftID)
		if err != nil || !found {
			return errors.New("signer transfer target is not registered in Root")
		}
		if err := runtime.Root.AuthorizeEnrollmentTransfer(from, local.RaftID, epoch); err != nil {
			return err
		}
		if err := nodepki.InstallTransferredSigner(options.StoreRoot, runtime.Identity, epoch, envelope, time.Now().UTC()); err != nil {
			return err
		}
		signer, err := nodepki.OpenSigner(options.StoreRoot, runtime.Identity, time.Now().UTC())
		if err != nil {
			return err
		}
		if enrollmentServer != nil {
			enrollmentServer.SetSigner(signer)
		}
		if certificateServer != nil {
			certificateServer.SetSigner(signer)
		}
		return nil
	})
	nodeadminrpc.RegisterServer(grpcServer, nodeAdminServer)
	rootJoinServer, err := rootjoinrpc.NewServer(runtime.Host, runtime.Root, runtime.Identity.ClusterID)
	if err != nil {
		grpcServer.Stop()
		return fail(err, transport, client)
	}
	rootJoinServer.SetRootReadIndex(runtime.ReadRootIndex)
	rootjoinwire.RegisterRootJoinServiceServer(grpcServer, rootJoinServer)
	dataServer := newDataService(runtime, client, rootDirectory)
	datawire.RegisterDataServiceServer(grpcServer, dataServer)
	routedData, err := raftrpc.NewEngineDataServer(runtime.Identity, runtime.Host, runtime.Root,
		runtime.Records, options.AuthorizePeer, localData, options.MaxMessageSize)
	if err != nil {
		grpcServer.Stop()
		return fail(err, transport, client)
	}
	raftwire.RegisterEngineDataServiceServer(grpcServer, routedData)
	raftwire.RegisterRecordRepairServiceServer(grpcServer, newRecordRepairService(runtime, client, options.AuthorizePeer))
	rootControlHost, err := newRootControlHost(runtime.Host, client, runtime.Identity.RaftID)
	if err != nil {
		grpcServer.Stop()
		return fail(err, transport, client)
	}
	enrollmentServer, err = enrollmentrpc.NewServer(rootControlHost, runtime.Root, options.EnrollmentSigner, runtime.Identity)
	if err != nil {
		grpcServer.Stop()
		return fail(err, transport, client)
	}
	enrollmentServer.SetRootReadIndex(runtime.ReadRootIndex)
	enrollmentwire.RegisterEnrollmentServiceServer(grpcServer, enrollmentServer)
	certificateServer, err = nodecertrpc.NewServer(runtime.Host, runtime.Root, options.EnrollmentSigner, runtime.Identity)
	if err != nil {
		grpcServer.Stop()
		return fail(err, transport, client)
	}
	certificateServer.SetRootReadIndex(runtime.ReadRootIndex)
	nodecertwire.RegisterNodeCertificateServiceServer(grpcServer, certificateServer)
	var certificates *certificateRenewal
	var regional *regionalFormation
	var liveRepair *liveRecordRepair
	var repairProgress *recordrepair.ProgressStore
	var geoRetry *recordwrite.GeoMetadataRetryStore
	var geoRecordRepair *recordrepair.GeoRecordRepairStore
	var geoRepair *geoMetadataRepair
	var volumeMigration *volumeMigrationWorker
	if options.Network != nil {
		certificates = newCertificateRenewal(options.StoreRoot, runtime.Identity, runtime.Host, runtime.Root, options.EnrollmentSigner, options.ReportError)
		coordinator, err := regiongroups.New(runtime.Host, runtime.Root, client, runtime)
		if err != nil {
			grpcServer.Stop()
			return fail(err, transport, client)
		}
		confirmer, err := regiongroups.NewConfirmer(runtime.Host, runtime.Root, regionalParticipants{runtime: runtime, client: client})
		if err != nil {
			grpcServer.Stop()
			return fail(err, transport, client)
		}
		grower, err := regiongroups.NewGrower(runtime.Host, runtime.Root, regionalParticipants{runtime: runtime, client: client},
			regionalRepairer{runtime: runtime, client: client})
		if err != nil {
			grpcServer.Stop()
			return fail(err, transport, client)
		}
		placementCoordinator, err := rangeplacement.New(runtime.Host, runtime.Root)
		if err != nil {
			grpcServer.Stop()
			return fail(err, transport, client)
		}
		regional = &regionalFormation{coordinator: coordinator, confirmer: confirmer, grower: grower, placement: placementCoordinator, report: options.ReportError}
		repairProgress, err = recordrepair.OpenProgressStore(filepath.Join(options.StoreRoot, "dubi-repair-state"))
		if err != nil {
			grpcServer.Stop()
			return fail(err, transport, client)
		}
		geoRetry, err = recordwrite.OpenGeoMetadataRetryStore(filepath.Join(options.StoreRoot, "dubi-geo-metadata-retry-state"))
		if err != nil {
			_ = repairProgress.Close()
			grpcServer.Stop()
			return fail(err, transport, client)
		}
		geoRecordRepair, err = recordrepair.OpenGeoRecordRepairStore(filepath.Join(options.StoreRoot, "dubi-geo-record-repair-state"))
		if err != nil {
			_ = geoRetry.Close()
			_ = repairProgress.Close()
			grpcServer.Stop()
			return fail(err, transport, client)
		}
		geoRepair = &geoMetadataRepair{runtime: runtime, client: client, store: geoRetry, records: geoRecordRepair, report: options.ReportError}
		liveRepair = &liveRecordRepair{runtime: runtime, client: client, progress: repairProgress, geoRecordRepair: geoRecordRepair, report: options.ReportError}
		dataServer.repair = liveRepair
		dataServer.geoRetry = geoRetry
		dataServer.geoRecords = geoRecordRepair
	}
	volumeMigration = &volumeMigrationWorker{runtime: runtime, report: options.ReportError, budget: dataServer.maintenance}
	var publicHTTP *http.Server
	var publicTLS *tls.Config
	if options.PublicListener != nil {
		publicHTTP = newPublicHTTPServer(auth.Verifier{Barrier: runtime.Host, Catalog: runtime.Root})
		publicTLS = normalizeServerTLS(options.ServerTLS)
		publicTLS.ClientAuth = tls.NoClientCert
		publicTLS.ClientCAs = nil
		publicTLS.NextProtos = []string{"http/1.1"}
	}
	return &Server{Runtime: runtime, GroupClient: client, EngineData: localData, MembershipReconciler: reconciler, NodeRemoval: nodeRemoval,
		listener: options.Listener, publicListener: options.PublicListener, publicHTTP: publicHTTP, publicTLS: publicTLS,
		grpc: grpcServer, transport: transport,
		scheduler: scheduler, certificates: certificates, regional: regional, liveRepair: liveRepair, ready: make(chan struct{}), localNode: localNode,
		desiredName: func() string {
			if options.Network != nil {
				return options.Network.Name
			}
			return ""
		}(),
		storeRoot: options.StoreRoot, repairProgress: repairProgress, geoRetry: geoRetry, geoRecordRepair: geoRecordRepair, geoRepair: geoRepair, volumeMigration: volumeMigration, closeDone: make(chan struct{})}, nil
}

// RemoveNode runs one explicit operator-requested removal pass. It is not
// called by heartbeat/timeout code; callers must provide the operator's
// authorization and may call it again after a restart to resume durable Group
// membership changes.
func (server *Server) RemoveNode(ctx context.Context, raftID uint64) (noderemoval.Plan, error) {
	if server == nil || server.NodeRemoval == nil {
		return noderemoval.Plan{}, errors.New("Node removal is not configured")
	}
	server.mu.Lock()
	closed := server.closed
	server.mu.Unlock()
	if closed {
		return noderemoval.Plan{}, errors.New("server is closed")
	}
	return server.NodeRemoval.Reconcile(ctx, raftID)
}

// RemoveNodeByName resolves the unique active display label, then uses the
// same durable removal workflow as the RaftID form.
func (server *Server) RemoveNodeByName(ctx context.Context, name string) (noderemoval.Plan, error) {
	if server == nil || server.Runtime == nil {
		return noderemoval.Plan{}, errors.New("Node removal is not configured")
	}
	if ctx == nil {
		return noderemoval.Plan{}, errors.New("Node removal context is required")
	}
	if err := rootstate.ValidateNodeName(name); err != nil || name == "" {
		return noderemoval.Plan{}, errors.New("invalid Node name")
	}
	if _, err := server.Runtime.ReadRootIndex(ctx); err != nil {
		return noderemoval.Plan{}, err
	}
	node, found, err := server.Runtime.Root.NodeByName(name)
	if err != nil {
		return noderemoval.Plan{}, err
	}
	if !found {
		return noderemoval.Plan{}, fmt.Errorf("Node name %q is not registered", name)
	}
	return server.RemoveNode(ctx, node.RaftID)
}

// RenameNode changes only the optional operator-facing label in Root. The
// NodeID, RaftID, address and Region remain untouched.
func (server *Server) RenameNode(ctx context.Context, name string) error {
	if server == nil || server.Runtime == nil {
		return errors.New("Node rename is not configured")
	}
	if err := rootstate.ValidateNodeName(name); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("Node rename requires context")
	}
	node, found, err := server.Runtime.Root.Node(server.Runtime.Identity.RaftID)
	if err != nil {
		return err
	}
	if !found || node.NodeID != server.Runtime.Identity.NodeID {
		return errors.New("local Node is not registered in Root")
	}
	if node.Name == name {
		return nil
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s", node.RaftID, name)))
	command := rootstate.Command{Version: rootstate.FormatVersion, RequestID: "rename-node-" + hex.EncodeToString(digest[:]), Type: rootstate.RenameNode,
		NodeRename: &rootstate.NodeRename{NodeID: node.NodeID, RaftID: node.RaftID, Name: name}}
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		return err
	}
	if err := server.Runtime.Host.Submit(raft.RootGroupID, encoded); err != nil {
		return err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := server.Runtime.ReadRootIndex(ctx); err != nil {
			return err
		}
		result, found, err := server.Runtime.Root.ResultForCommand(encoded)
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

func (server *Server) StoreRoot() string {
	if server == nil {
		return ""
	}
	return server.storeRoot
}

// Ready closes only after the internal listener is serving and all local
// Groups selected by a confirmed Root view have reopened.
func (server *Server) Ready() <-chan struct{} {
	if server == nil {
		return nil
	}
	return server.ready
}

// LocalAddress is the actual bound internal listener. It is used only by the
// local CLI to remember a same-host endpoint after startup.
func (server *Server) LocalAddress() string {
	if server == nil || server.listener == nil {
		return ""
	}
	return server.listener.Addr().String()
}

// PublicAddress is empty when the optional Tenant HTTPS boundary is disabled.
func (server *Server) PublicAddress() string {
	if server == nil || server.publicListener == nil {
		return ""
	}
	return server.publicListener.Addr().String()
}

// Run serves until ctx is cancelled or the internal gRPC listener fails.
// Context cancellation is a clean shutdown and returns nil.
func (server *Server) Run(ctx context.Context) error {
	if server == nil {
		return errors.New("DUBI server is nil")
	}
	server.mu.Lock()
	if server.closed {
		server.mu.Unlock()
		return errors.New("DUBI server is closed")
	}
	if server.started {
		server.mu.Unlock()
		return errors.New("DUBI server is already running")
	}
	runCtx, cancelCause := context.WithCancelCause(ctx)
	cancel := func() { cancelCause(context.Canceled) }
	server.cancel = cancel
	server.started = true
	// Reserve the startup/supervisor lifetime before Close can begin waiting.
	// Nested worker registration happens while this lease remains held.
	server.workers.Add(1)
	server.mu.Unlock()
	result := server.run(runCtx, cancelCause)
	server.workers.Done()
	closeErr := server.Close()
	if runCtx.Err() != nil && (errors.Is(result, context.Canceled) || errors.Is(result, context.DeadlineExceeded)) {
		result = nil
	}
	if cause := context.Cause(runCtx); cause != nil && !errors.Is(cause, context.Canceled) && !errors.Is(cause, context.DeadlineExceeded) {
		result = errors.Join(result, cause)
	}
	return errors.Join(result, closeErr)
}

// run never closes owned resources. Close first cancels and joins this
// supervisor and its workers, then releases transport/state/Store ownership.
func (server *Server) run(runCtx context.Context, cancel context.CancelCauseFunc) error {
	if server.publicHTTP != nil {
		server.workers.Add(1)
		go func() {
			defer server.workers.Done()
			err := server.publicHTTP.Serve(tls.NewListener(server.publicListener, server.publicTLS))
			if runCtx.Err() == nil && !errors.Is(err, http.ErrServerClosed) {
				if err == nil {
					err = errors.New("public HTTPS listener stopped unexpectedly")
				}
				cancel(fmt.Errorf("serve public HTTPS: %w", err))
			}
		}()
	}
	server.workers.Add(1)
	go func() {
		defer server.workers.Done()
		err := server.grpc.Serve(server.listener)
		if runCtx.Err() == nil {
			if err == nil {
				err = errors.New("internal RPC listener stopped unexpectedly")
			}
			// Also interrupt startup waiting for a leader/quorum; otherwise a
			// failed listener could strand Run before its normal serving select.
			cancel(fmt.Errorf("serve internal RPC: %w", err))
		}
	}()
	if err := server.waitForRootLeader(runCtx); err != nil {
		return fmt.Errorf("wait for Root leader: %w", err)
	}
	if server.localNode != nil {
		if err := server.Runtime.EnsureLocalNodeWithName(runCtx, server.localNode.RegionID, server.localNode.Address, server.localNode.Name); err != nil {
			return fmt.Errorf("confirm local Node registration: %w", err)
		}
	}
	// Bootstrap registration must precede local Group recovery. A fresh
	// single-node Root has membership but no Node descriptor yet; recovering a
	// regional Group first would reject that valid startup with "Root member has
	// no registered address". Joined Nodes already have the descriptor, so the
	// same ordering also keeps their recovery path deterministic.
	if err := server.Runtime.RecoverLocalGroups(runCtx); err != nil {
		return fmt.Errorf("recover local Groups: %w", err)
	}
	if server.desiredName != "" {
		if current, found, err := server.Runtime.Root.Node(server.Runtime.Identity.RaftID); err != nil {
			return err
		} else if found && current.Name != server.desiredName {
			if err := server.RenameNode(runCtx, server.desiredName); err != nil {
				return fmt.Errorf("set local Node name: %w", err)
			}
		}
	}
	if err := server.publishLocalRootReady(runCtx); err != nil {
		return fmt.Errorf("publish local Root readiness: %w", err)
	}
	server.mu.Lock()
	if server.closed || runCtx.Err() != nil {
		server.mu.Unlock()
		return context.Canceled
	}
	close(server.ready)
	schedulerResult := make(chan error, 1)
	server.workers.Add(1)
	go func() {
		defer server.workers.Done()
		schedulerResult <- server.scheduler.Run(runCtx)
	}()
	if server.certificates != nil {
		server.workers.Add(1)
		go func() {
			defer server.workers.Done()
			server.certificates.run(runCtx)
		}()
	}
	if server.regional != nil {
		server.workers.Add(1)
		go func() { defer server.workers.Done(); server.regional.run(runCtx) }()
	}
	if server.liveRepair != nil {
		server.workers.Add(1)
		go func() { defer server.workers.Done(); server.liveRepair.run(runCtx) }()
	}
	if server.geoRepair != nil {
		server.workers.Add(1)
		go func() { defer server.workers.Done(); server.geoRepair.run(runCtx) }()
	}
	if server.volumeMigration != nil {
		server.workers.Add(1)
		go func() { defer server.workers.Done(); server.volumeMigration.run(runCtx) }()
	}
	server.mu.Unlock()

	var result error
	select {
	case <-runCtx.Done():
	case err := <-schedulerResult:
		if err != nil && !errors.Is(err, context.Canceled) {
			result = fmt.Errorf("run membership scheduler: %w", err)
		}
	}
	return result
}

// publishLocalRootReady is the final join hand-off. A local activation fence
// is not useful to the cluster until it is committed through Root, so Root
// topology can safely wait before promoting this learner or demoting a voter.
func (server *Server) publishLocalRootReady(ctx context.Context) error {
	if server.Runtime == nil || server.Runtime.RootJoin == nil {
		return nil
	}
	local, err := server.Runtime.RootJoin.Load()
	if err != nil {
		return err
	}
	if local.Phase != rootjoin.LocalActive || local.ActivatedIndex == 0 {
		return errors.New("local Root join is not active")
	}
	ready, found, err := server.Runtime.Root.NodeReady(server.Runtime.Identity.RaftID)
	if err != nil {
		return err
	}
	if found && ready.NodeID == server.Runtime.Identity.NodeID && ready.ActivatedIndex >= local.ActivatedIndex {
		return nil
	}
	command, err := rootstate.EncodeCommand(rootstate.Command{
		Version:   rootstate.FormatVersion,
		RequestID: fmt.Sprintf("node-ready-%d-%d", server.Runtime.Identity.RaftID, local.ActivatedIndex),
		Type:      rootstate.MarkNodeReady,
		NodeReady: &rootstate.NodeReadyRecord{NodeID: server.Runtime.Identity.NodeID, RaftID: server.Runtime.Identity.RaftID, ActivatedIndex: local.ActivatedIndex},
	})
	if err != nil {
		return err
	}
	status, err := server.Runtime.Host.Status(raft.RootGroupID)
	if err != nil {
		return err
	}
	cut, err := server.Runtime.Root.RootRoutingView(rootjoin.MaxViewPeers)
	if err != nil {
		return err
	}
	targets := make([]uint64, 0, len(cut.Membership.GetVoters())+1)
	seen := make(map[uint64]bool)
	add := func(id uint64) {
		if id != 0 && id != server.Runtime.Identity.RaftID && !seen[id] {
			seen[id] = true
			targets = append(targets, id)
		}
	}
	add(status.LeaderID)
	for _, id := range cut.Membership.GetVoters() {
		add(id)
	}
	for _, id := range cut.Membership.GetVotersOutgoing() {
		add(id)
	}
	if len(targets) == 0 {
		return errors.New("Root readiness has no remote voter target")
	}
	var failures []error
	for _, target := range targets {
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		submitDone := make(chan error, 1)
		go func() {
			_, submitErr := server.GroupClient.Submit(attempt, target, raft.RootGroupID, command)
			submitDone <- submitErr
		}()
		var submitErr error
		for {
			result, found, resultErr := server.Runtime.Root.ResultForCommand(command)
			if resultErr != nil {
				submitErr = resultErr
				break
			}
			if found {
				if result.Error != "" {
					submitErr = errors.New(result.Error)
				} else {
					cancel()
					return nil
				}
				break
			}
			select {
			case err := <-submitDone:
				if err != nil {
					submitErr = err
				}
			case <-attempt.Done():
				submitErr = attempt.Err()
			default:
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if attempt.Err() == nil {
				// The proposal RPC may have lost its reply after committing. Keep
				// polling the local Root result until the bounded attempt expires.
				continue
			}
			break
		}
		err := submitErr
		cancel()
		if err == nil {
			return nil
		} else {
			failures = append(failures, fmt.Errorf("target %d: %w", target, err))
		}
	}
	return errors.Join(failures...)
}

func (server *Server) waitForRootLeader(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := server.Runtime.Host.Status(raft.RootGroupID)
		if err != nil {
			return err
		}
		if status.LeaderID != 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (server *Server) Close() error {
	if server == nil {
		return nil
	}
	server.mu.Lock()
	if server.closeDone == nil {
		server.closeDone = make(chan struct{})
	}
	if server.closed {
		done := server.closeDone
		server.mu.Unlock()
		<-done
		return server.closeErr
	}
	server.closed = true
	if server.cancel != nil {
		server.cancel()
	}
	server.mu.Unlock()
	// Snapshot restoration uses the Node lifetime, not an RPC deadline. Cancel
	// that work before waiting for handlers, without closing their storage yet.
	server.Runtime.BeginShutdown()
	var publicErr error
	if server.publicHTTP != nil {
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		publicErr = server.publicHTTP.Shutdown(shutdown)
		cancel()
		if publicErr != nil {
			_ = server.publicHTTP.Close()
		}
	}
	server.grpc.Stop()
	listenerErr := closeListener(server.listener)
	if server.publicListener != nil {
		listenerErr = errors.Join(listenerErr, closeListener(server.publicListener))
	}
	server.workers.Wait()
	err := errors.Join(publicErr, listenerErr, server.GroupClient.Close(),
		server.transport.Close(), closeRepairProgress(server.repairProgress), closeGeoRetry(server.geoRetry), closeGeoRecordRepair(server.geoRecordRepair), server.Runtime.Close())
	server.mu.Lock()
	server.closeErr = err
	close(server.closeDone)
	server.mu.Unlock()
	return err
}

func closeListener(listener net.Listener) error {
	err := listener.Close()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func validateOptions(options Options) error {
	if options.StoreRoot == "" || options.Listener == nil || options.AuthorizePeer == nil ||
		options.EngineMux == nil || options.TickInterval < 0 || options.MembershipInterval < 0 {
		return errors.New("server requires StoreRoot, listener, peer authorizer, Engine Mux, and valid intervals")
	}
	if len(options.RootBootstrapPeers) > 1 && options.BootstrapDirectory == nil {
		return errors.New("multi-node Root bootstrap requires a peer address directory")
	}
	if err := validateServerTLS(options.ServerTLS); err != nil {
		return err
	}
	if err := validateClientTLS(options.ClientTLS); err != nil {
		return err
	}
	return nil
}

func validateServerTLS(config *tls.Config) error {
	if config == nil || (len(config.Certificates) == 0 && config.GetCertificate == nil) || config.ClientCAs == nil ||
		config.ClientAuth != tls.RequireAndVerifyClientCert ||
		(config.MaxVersion != 0 && config.MaxVersion < tls.VersionTLS13) {
		return errors.New("server TLS requires a certificate, Client CA, and mandatory verified client certificates")
	}
	return nil
}

func validateClientTLS(config *tls.Config) error {
	if config == nil || (len(config.Certificates) == 0 && config.GetClientCertificate == nil) || config.RootCAs == nil || config.InsecureSkipVerify ||
		(config.MaxVersion != 0 && config.MaxVersion < tls.VersionTLS13) {
		return errors.New("client TLS requires a certificate, Root CA, and certificate verification")
	}
	return nil
}

func normalizeServerTLS(config *tls.Config) *tls.Config {
	copy := config.Clone()
	if copy.MinVersion < tls.VersionTLS13 {
		copy.MinVersion = tls.VersionTLS13
	}
	return copy
}

func normalizeClientTLS(config *tls.Config) *tls.Config {
	copy := config.Clone()
	if copy.MinVersion < tls.VersionTLS13 {
		copy.MinVersion = tls.VersionTLS13
	}
	return copy
}

func messageSize(value int) int {
	if value <= 0 {
		return raftrpc.DefaultMaxMessageSize
	}
	return value
}

type fallbackDirectory struct {
	primary  raftrpc.Directory
	fallback raftrpc.Directory
}

func (directory fallbackDirectory) Resolve(raftID uint64) (string, bool) {
	if address, found := directory.primary.Resolve(raftID); found {
		return address, true
	}
	return directory.fallback.Resolve(raftID)
}

type transportSlot struct {
	mu        sync.RWMutex
	transport raft.Transport
}

func newTransportSlot() *transportSlot { return &transportSlot{} }

func (slot *transportSlot) Bind(transport raft.Transport) error {
	if transport == nil {
		return errors.New("Raft transport is nil")
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.transport != nil {
		return errors.New("Raft transport is already bound")
	}
	slot.transport = transport
	return nil
}

func (slot *transportSlot) Send(groupID string, message *raftpb.Message) error {
	slot.mu.RLock()
	transport := slot.transport
	slot.mu.RUnlock()
	if transport == nil {
		return errors.New("Raft transport is not ready")
	}
	return transport.Send(groupID, message)
}
