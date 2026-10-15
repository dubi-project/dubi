package enrollmentrpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enrollment"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinwire"
	"github.com/dubi-project/dubi/internal/platform/storage/diskpressure"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type networkFixture struct {
	store   string
	server  *Server
	id      identity.Identity
	root    *rootstate.State
	host    *raft.Host
	signer  *nodepki.Signer
	address string
	client  enrollmentwire.EnrollmentServiceClient
	nodeTLS *tls.Config
}

func newNetworkFixture(t *testing.T, wrap func(enrollment.Host) enrollment.Host) *networkFixture {
	return newNetworkFixtureWithInterceptor(t, wrap, nil)
}

func newNetworkFixtureWithInterceptor(t *testing.T, wrap func(enrollment.Host) enrollment.Host, interceptor grpc.UnaryServerInterceptor) *networkFixture {
	t.Helper()
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodepki.Ensure(store, id, nodepki.DefaultServerName, time.Time{}); err != nil {
		t.Fatal(err)
	}
	signer, err := nodepki.OpenSigner(store, id, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	root, err := rootstate.Open(filepath.Join(store, "root-state"))
	if err != nil {
		t.Fatal(err)
	}
	host, err := raft.OpenHost(raft.HostOptions{StoreRoot: store, TickInterval: time.Hour})
	if err != nil {
		_ = root.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := host.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := host.AddGroup(raft.RootGroupID, []uint64{id.RaftID}, false, root); err != nil {
		t.Fatal(err)
	}
	if err := host.Campaign(raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	address := listener.Addr().String()
	command, err := rootstate.EncodeCommand(rootstate.Command{Version: 1, RequestID: "register-network-issuer", Type: rootstate.RegisterNode,
		Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "local", Address: address}})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Submit(raft.RootGroupID, command); err != nil {
		t.Fatal(err)
	}
	backend := enrollment.Host(host)
	if wrap != nil {
		backend = wrap(host)
	}
	service, err := NewServer(backend, root, signer, id)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, clientTLS, err := config.LoadTLS(store, id)
	if err != nil {
		t.Fatal(err)
	}
	var serverOptions = []grpc.ServerOption{grpc.Creds(credentials.NewTLS(serverTLS)), grpc.MaxRecvMsgSize(MaxMessageSize)}
	if interceptor != nil {
		serverOptions = append(serverOptions, grpc.UnaryInterceptor(interceptor))
	}
	server := grpc.NewServer(serverOptions...)
	enrollmentwire.RegisterEnrollmentServiceServer(server, service)
	rootJoinServer, err := rootjoinrpc.NewServer(host, root, id.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	rootjoinwire.RegisterRootJoinServiceServer(server, rootJoinServer)
	authorize, err := identity.PeerAuthorizer(id.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	groupServer, err := raftrpc.NewGroupServer(id.ClusterID, id.RaftID, host, authorize, MaxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	raftServer, err := raftrpc.NewServer(id.ClusterID, id.RaftID, host, authorize, MaxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	raftwire.RegisterGroupServiceServer(server, groupServer)
	raftwire.RegisterRaftServiceServer(server, raftServer)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-done; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	})
	f := &networkFixture{store: store, server: service, root: root, id: id, host: host, signer: signer, address: address, nodeTLS: clientTLS}
	f.client = enrollmentwire.NewEnrollmentServiceClient(f.connect(t, clientTLS))
	return f
}

func (f *networkFixture) connect(t *testing.T, config *tls.Config) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+f.address, grpc.WithTransportCredentials(credentials.NewTLS(config)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageSize), grpc.MaxCallSendMsgSize(MaxMessageSize)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func (f *networkFixture) ticket(t *testing.T) identityenrollment.Ticket {
	t.Helper()
	draft, err := identityenrollment.NewGrantDraft(0, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	r := draft.Request()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := f.client.CreateGrant(ctx, &enrollmentwire.CreateGrantRequest{ClusterId: f.id.ClusterID, FromRaftId: f.id.RaftID,
		GrantId: r.GrantID, SecretSha256: r.SecretSHA256, PublicKeyDer: r.PublicKeyDER, ExpiresUnix: r.ExpiresUnix})
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := draft.Complete(identityenrollment.GrantBundle{ClusterID: response.GetClusterId(), GrantID: response.GetGrantId(),
		IssuerAddress: response.GetIssuerAddress(), ExpiresUnix: response.GetExpiresUnix(),
		CACertificatePEM: response.GetCaCertificatePem(), CertificatePEM: response.GetCertificatePem()}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	return ticket
}

func TestEnrollmentNetworkLeafOnlyInstallRetryAndRoleIsolation(t *testing.T) {
	f := newNetworkFixture(t, nil)
	ticket := f.ticket(t)
	store := t.TempDir()
	prepared, request, err := identityenrollment.Prepare(store, nodepki.DefaultServerName, "r2", "127.0.0.1:7444")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bundle, err := Enroll(ctx, ticket, request)
	if err != nil {
		t.Fatal(err)
	}
	registered, found, err := f.root.Node(prepared.RaftID)
	if err != nil || !found || registered.NodeID != prepared.NodeID {
		t.Fatal(registered, found, err)
	}
	if _, err := identityenrollment.SaveReceipt(store, ticket, request, bundle, time.Time{}); err != nil {
		t.Fatal(err)
	}
	_, material, err := identityenrollment.ResumeReceipt(store, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{material.Paths.WrappingKey, material.Paths.Envelope} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("joining Node received signer", path, err)
		}
	}
	if _, err := Enroll(ctx, ticket, request); err != nil {
		t.Fatal("same request retry", err)
	}
	_, different, err := identityenrollment.Prepare(t.TempDir(), nodepki.DefaultServerName, "r2", "127.0.0.1:7445")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Enroll(ctx, ticket, different); status.Code(err) != codes.PermissionDenied {
		t.Fatal("ticket reused", err)
	}
	if _, err := f.client.RevokeGrant(ctx, &enrollmentwire.RevokeGrantRequest{ClusterId: f.id.ClusterID, FromRaftId: f.id.RaftID, GrantId: ticket.GrantID()}); err == nil {
		t.Fatal("consumed grant revoked")
	}
	tlsConfig, err := ticket.ClientTLS(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	conn := f.connect(t, tlsConfig)
	joinClient := enrollmentwire.NewEnrollmentServiceClient(conn)
	if _, err := joinClient.CreateGrant(ctx, &enrollmentwire.CreateGrantRequest{ClusterId: f.id.ClusterID, FromRaftId: f.id.RaftID}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("enrollment managed grants", err)
	}
	if _, err := joinClient.RevokeGrant(ctx, &enrollmentwire.RevokeGrantRequest{ClusterId: f.id.ClusterID, FromRaftId: f.id.RaftID, GrantId: ticket.GrantID()}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("enrollment revoked grants", err)
	}
	if _, err := raftwire.NewGroupServiceClient(conn).ReadIndex(ctx, &raftwire.GroupReadIndexRequest{ClusterId: f.id.ClusterID, GroupId: raft.RootGroupID, FromRaftId: f.id.RaftID, ToRaftId: f.id.RaftID}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("enrollment used Node Group RPC", err)
	}
	if _, err := raftwire.NewRaftServiceClient(conn).Step(ctx, &raftwire.StepRequest{ClusterId: f.id.ClusterID, GroupId: raft.RootGroupID, FromRaftId: f.id.RaftID, ToRaftId: f.id.RaftID, Message: []byte("invalid")}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("enrollment used Node Raft RPC", err)
	}
	proof, err := ticket.AdmissionProof(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := identityenrollment.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.EnrollNode(ctx, &enrollmentwire.EnrollNodeRequest{ClusterId: f.id.ClusterID, RequestDocument: encoded, AdmissionProof: proof}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("Node role reused enrollment proof", err)
	}
	if _, err := joinClient.EnrollNode(ctx, &enrollmentwire.EnrollNodeRequest{ClusterId: f.id.ClusterID, RequestDocument: bytes.Repeat([]byte{'a'}, maxRequestDocument+1), AdmissionProof: proof}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("document bound", err)
	}
}

func TestEnrollmentNetworkRevokedGrantAndWrongTrustAnchor(t *testing.T) {
	f := newNetworkFixture(t, nil)
	ticket := f.ticket(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.client.RevokeGrant(ctx, &enrollmentwire.RevokeGrantRequest{ClusterId: f.id.ClusterID, FromRaftId: f.id.RaftID, GrantId: ticket.GrantID()}); err != nil {
		t.Fatal(err)
	}
	_, request, err := identityenrollment.Prepare(t.TempDir(), nodepki.DefaultServerName, "local", "127.0.0.1:7444")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Enroll(ctx, ticket, request); status.Code(err) != codes.PermissionDenied {
		t.Fatal("revoked ticket accepted", err)
	}
	if _, found, err := f.root.Node(request.RaftID); err != nil || found {
		t.Fatal("revoked ticket registered Node", found, err)
	}
	other := newNetworkFixture(t, nil)
	wrongCA := enrollmentwire.NewEnrollmentServiceClient(f.connect(t, other.nodeTLS))
	if _, err := wrongCA.RevokeGrant(ctx, &enrollmentwire.RevokeGrantRequest{ClusterId: f.id.ClusterID, FromRaftId: f.id.RaftID, GrantId: ticket.GrantID()}); status.Code(err) != codes.Unavailable {
		t.Fatal("foreign CA crossed TLS", err)
	}
	noClient := f.nodeTLS.Clone()
	noClient.Certificates = nil
	if _, err := enrollmentwire.NewEnrollmentServiceClient(f.connect(t, noClient)).CreateGrant(ctx, &enrollmentwire.CreateGrantRequest{ClusterId: f.id.ClusterID, FromRaftId: f.id.RaftID}); status.Code(err) != codes.Unavailable {
		t.Fatal("missing client certificate crossed TLS", err)
	}
}

type blockingReadHost struct {
	enrollment.Host
	entered chan struct{}
}

func (h blockingReadHost) ReadIndex(ctx context.Context, _ string) (uint64, error) {
	h.entered <- struct{}{}
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestEnrollmentNetworkWorkLimitAndCancellation(t *testing.T) {
	entered := make(chan struct{}, maxConcurrentRequests)
	f := newNetworkFixture(t, func(h enrollment.Host) enrollment.Host { return blockingReadHost{Host: h, entered: entered} })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for range maxConcurrentRequests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = f.client.CreateGrant(ctx, &enrollmentwire.CreateGrantRequest{ClusterId: f.id.ClusterID, FromRaftId: f.id.RaftID})
		}()
	}
	for range maxConcurrentRequests {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("request did not occupy a slot")
		}
	}
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), time.Second)
	defer rpcCancel()
	_, err := f.client.CreateGrant(rpcCtx, &enrollmentwire.CreateGrantRequest{ClusterId: f.id.ClusterID, FromRaftId: f.id.RaftID})
	if status.Code(err) != codes.ResourceExhausted || len(status.Convert(err).Details()) != 1 {
		t.Fatal("missing bounded admission and RetryInfo", err)
	}
	cancel()
	wg.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for len(f.server.slots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(f.server.slots) != 0 {
		t.Fatal("cancelled calls leaked enrollment slots")
	}
}

type transientHost struct {
	enrollment.Host
	failRead atomic.Bool
	pressure atomic.Bool
}

func (h *transientHost) ReadIndex(ctx context.Context, group string) (uint64, error) {
	if h.failRead.Load() {
		return 0, errors.New("injected quorum unavailable")
	}
	return h.Host.ReadIndex(ctx, group)
}
func (h *transientHost) Submit(group string, data []byte) error {
	if h.pressure.Load() {
		return diskpressure.ErrPressure
	}
	return h.Host.Submit(group, data)
}

func TestEnrollmentNetworkTransientFailureDoesNotConsumeGrant(t *testing.T) {
	h := &transientHost{}
	f := newNetworkFixture(t, func(host enrollment.Host) enrollment.Host { h.Host = host; return h })
	ticket := f.ticket(t)
	_, request, err := identityenrollment.Prepare(t.TempDir(), nodepki.DefaultServerName, "local", "127.0.0.1:7444")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h.failRead.Store(true)
	if _, err := Enroll(ctx, ticket, request); status.Code(err) != codes.Unavailable {
		t.Fatal("quorum failure misclassified as permission failure", err)
	}
	h.failRead.Store(false)
	h.pressure.Store(true)
	if _, err := Enroll(ctx, ticket, request); status.Code(err) != codes.ResourceExhausted || len(status.Convert(err).Details()) != 1 {
		t.Fatal("storage pressure lacks RetryInfo", err)
	}
	h.pressure.Store(false)
	grant, found, err := f.root.JoinGrant(ticket.GrantID())
	if err != nil || !found || grant.State != rootstate.JoinGrantActive {
		t.Fatal("failed proposal consumed grant", found, err)
	}
	if _, err := Enroll(ctx, ticket, request); err != nil {
		t.Fatal("same request did not recover", err)
	}
}
