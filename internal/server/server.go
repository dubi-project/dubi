// Package server assembles and runs one DUBI process around the platform Node
// runtime. Public APIs are intentionally not part of this package yet.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/capabilityruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Options struct {
	StoreRoot          string
	Listener           net.Listener
	ServerTLS          *tls.Config
	ClientTLS          *tls.Config
	AuthorizePeer      raftrpc.AuthorizePeer
	CapabilityMux      *capabilityruntime.Mux
	RootBootstrapPeers []uint64
	BootstrapDirectory raftrpc.Directory
	TickInterval       time.Duration
	MembershipInterval time.Duration
	MaxMessageSize     int
	ReportError        groupmembership.ErrorReporter
}

type Server struct {
	Runtime              *node.Runtime
	GroupClient          *raftrpc.GroupClient
	MembershipReconciler *groupmembership.Reconciler

	listener  net.Listener
	grpc      *grpc.Server
	transport *raftrpc.Transport
	scheduler *groupmembership.Scheduler
	ready     chan struct{}

	mu      sync.Mutex
	cancel  context.CancelFunc
	started bool
	closed  bool
}

func Open(options Options) (*Server, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	transportSlot := newTransportSlot()
	runtime, err := node.Open(node.Options{StoreRoot: options.StoreRoot,
		TickInterval: options.TickInterval, Transport: transportSlot,
		RootBootstrapPeers: options.RootBootstrapPeers, CapabilityMux: options.CapabilityMux})
	if err != nil {
		_ = options.Listener.Close()
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
		return nil, errors.Join(openErr, errors.Join(failures...))
	}
	rootDirectory := raftrpc.NewRootDirectory(runtime.Root)
	directory := raftrpc.Directory(rootDirectory)
	if options.BootstrapDirectory != nil {
		directory = fallbackDirectory{primary: rootDirectory, fallback: options.BootstrapDirectory}
	}
	clientCredentials := credentials.NewTLS(normalizeClientTLS(options.ClientTLS))
	transport, err := raftrpc.NewTransport(raftrpc.TransportOptions{
		ClusterID: runtime.Identity.ClusterID, RaftID: runtime.Identity.RaftID,
		Directory: directory, Credentials: clientCredentials,
		MaxSize: options.MaxMessageSize,
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
	routedMembership, err := raftrpc.NewRoutedMembershipControl(client, runtime.Root, runtime.LocalMembership)
	if err != nil {
		return fail(err, transport, client)
	}
	reconciler, err := groupmembership.New(runtime.Host, routedMembership, runtime.Root)
	if err != nil {
		return fail(err, transport, client)
	}
	scheduler, err := groupmembership.NewScheduler(runtime.Host, reconciler,
		options.MembershipInterval, options.ReportError)
	if err != nil {
		return fail(err, transport, client)
	}
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(normalizeServerTLS(options.ServerTLS))),
		grpc.MaxRecvMsgSize(messageSize(options.MaxMessageSize)+1024),
	)
	if err := runtime.RegisterInternalRPC(grpcServer, options.AuthorizePeer, options.MaxMessageSize); err != nil {
		grpcServer.Stop()
		return fail(err, transport, client)
	}
	return &Server{Runtime: runtime, GroupClient: client, MembershipReconciler: reconciler,
		listener: options.Listener, grpc: grpcServer, transport: transport,
		scheduler: scheduler, ready: make(chan struct{})}, nil
}

// Ready closes only after the internal listener is serving and all local
// Groups selected by a confirmed Root view have reopened.
func (server *Server) Ready() <-chan struct{} {
	if server == nil {
		return nil
	}
	return server.ready
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
	runCtx, cancel := context.WithCancel(ctx)
	server.cancel = cancel
	server.started = true
	server.mu.Unlock()

	serveResult := make(chan error, 1)
	go func() { serveResult <- server.grpc.Serve(server.listener) }()
	if err := server.waitForRootLeader(runCtx); err != nil {
		closeErr := server.Close()
		if ctx.Err() != nil {
			return closeErr
		}
		return errors.Join(fmt.Errorf("wait for Root leader: %w", err), closeErr)
	}
	if err := server.Runtime.RecoverLocalGroups(runCtx); err != nil {
		closeErr := server.Close()
		if ctx.Err() != nil {
			return closeErr
		}
		return errors.Join(fmt.Errorf("recover local Groups: %w", err), closeErr)
	}
	close(server.ready)
	schedulerResult := make(chan error, 1)
	go func() { schedulerResult <- server.scheduler.Run(runCtx) }()

	var result error
	select {
	case <-ctx.Done():
	case err := <-serveResult:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
			result = fmt.Errorf("serve internal RPC: %w", err)
		}
	case err := <-schedulerResult:
		if err != nil && !errors.Is(err, context.Canceled) {
			result = fmt.Errorf("run membership scheduler: %w", err)
		}
	}
	if err := server.Close(); err != nil {
		result = errors.Join(result, err)
	}
	return result
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
	if server.closed {
		server.mu.Unlock()
		return nil
	}
	server.closed = true
	if server.cancel != nil {
		server.cancel()
	}
	server.mu.Unlock()
	server.grpc.Stop()
	return errors.Join(closeListener(server.listener), server.GroupClient.Close(),
		server.transport.Close(), server.Runtime.Close())
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
		options.CapabilityMux == nil || options.TickInterval < 0 || options.MembershipInterval < 0 {
		return errors.New("server requires StoreRoot, listener, peer authorizer, Capability Mux, and valid intervals")
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
	if config == nil || len(config.Certificates) == 0 || config.ClientCAs == nil ||
		config.ClientAuth != tls.RequireAndVerifyClientCert ||
		(config.MaxVersion != 0 && config.MaxVersion < tls.VersionTLS13) {
		return errors.New("server TLS requires a certificate, Client CA, and mandatory verified client certificates")
	}
	return nil
}

func validateClientTLS(config *tls.Config) error {
	if config == nil || len(config.Certificates) == 0 || config.RootCAs == nil || config.InsecureSkipVerify ||
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
