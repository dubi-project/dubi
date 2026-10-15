package rootjoinrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinwire"
	"github.com/dubi-project/dubi/internal/platform/storage/diskpressure"
	etcdraft "go.etcd.io/raft/v3"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type controlledHost struct {
	rootjoin.Host
	mode     atomic.Int32
	reads    atomic.Int32
	entered  chan struct{}
	follower atomic.Bool
	onRead   func()
}

func (h *controlledHost) ReadIndex(ctx context.Context, group string) (uint64, error) {
	h.reads.Add(1)
	if h.onRead != nil {
		h.onRead()
	}
	switch h.mode.Load() {
	case 1:
		h.entered <- struct{}{}
		<-ctx.Done()
		return 0, ctx.Err()
	case 2:
		return 0, diskpressure.ErrPressure
	case 3:
		return 0, errors.New("quorum unavailable")
	}
	return h.Host.ReadIndex(ctx, group)
}

func (h *controlledHost) Status(group string) (raft.GroupStatus, error) {
	s, err := h.Host.Status(group)
	if h.follower.Load() {
		s.Role = etcdraft.StateFollower
	}
	return s, err
}

// This fixture exercises handler admission with a real Root Host and
// constructed verified-peer context. Real TLS role/key tests live alongside
// enrollmentrpc and in the formal Server integration test.
func handlerFixture(t *testing.T) (*Server, *controlledHost, identity.Identity, *x509.Certificate, *x509.Certificate) {
	t.Helper()
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	material, err := nodepki.Ensure(store, id, nodepki.DefaultServerName, time.Time{})
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
	command, err := rootstate.EncodeCommand(rootstate.Command{Version: 1, RequestID: "register", Type: rootstate.RegisterNode, Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "local", Address: "127.0.0.1:7443"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Submit(raft.RootGroupID, command); err != nil {
		t.Fatal(err)
	}
	pair, err := tls.LoadX509KeyPair(material.Paths.Certificate, material.Paths.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	backend := &controlledHost{Host: host, entered: make(chan struct{}, 8)}
	server, err := NewServer(backend, root, id.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	return server, backend, id, leaf, material.CACertificate
}

func nodeContext(ctx context.Context, leaf, ca *x509.Certificate) context.Context {
	return peer.NewContext(ctx, &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf, ca}}}}})
}

func TestRootJoinRPCBoundsCancellationAndTransientErrors(t *testing.T) {
	s, h, id, leaf, ca := handlerFixture(t)
	request := &rootjoinwire.DiscoverRootRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID}
	h.mode.Store(1)
	ctx, cancel := context.WithCancel(nodeContext(context.Background(), leaf, ca))
	defer cancel()
	results := make(chan error, 4)
	for range 4 {
		go func() { _, err := s.DiscoverRoot(ctx, request); results <- err }()
	}
	for range 4 {
		select {
		case <-h.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("admission did not enter ReadIndex")
		}
	}
	_, err := s.DiscoverRoot(ctx, request)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal("busy request not rejected", err)
	}
	foundRetry := false
	for _, detail := range status.Convert(err).Details() {
		if retry, ok := detail.(*errdetails.RetryInfo); ok && retry.RetryDelay.AsDuration() == time.Second {
			foundRetry = true
		}
	}
	if !foundRetry {
		t.Fatal("missing retry guidance", err)
	}
	cancel()
	for range 4 {
		select {
		case err := <-results:
			if status.Code(err) != codes.Canceled {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled call leaked slot")
		}
	}
	if len(s.slots) != 0 {
		t.Fatal("admission slots leaked")
	}
	ctx = nodeContext(context.Background(), leaf, ca)
	for mode, want := range map[int32]codes.Code{2: codes.ResourceExhausted, 3: codes.Unavailable, 0: codes.OK} {
		h.mode.Store(mode)
		_, err := s.DiscoverRoot(ctx, request)
		if status.Code(err) != want {
			t.Fatal("transient error misclassified", mode, err)
		}
	}
	if _, err := s.JoinRoot(ctx, &rootjoinwire.JoinRootRequest{ClusterId: id.ClusterID, RequestDocument: make([]byte, maxRequestDocument+1)}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("document limit missing", err)
	}
	if _, err := s.DiscoverRoot(ctx, &rootjoinwire.DiscoverRootRequest{ClusterId: string(make([]byte, MaxRequestSize+1)), FromRaftId: id.RaftID}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("message limit missing", err)
	}
	if _, err := s.JoinRoot(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if _, err := s.DiscoverRoot(ctx, nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
}

func TestRootJoinRPCRequiresCurrentCertificateAfterQuorum(t *testing.T) {
	s, h, id, leaf, ca := handlerFixture(t)
	request := &rootjoinwire.DiscoverRootRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID}
	for name, ctx := range map[string]context.Context{
		"missing":    context.Background(),
		"nil-peer":   peer.NewContext(context.Background(), nil),
		"unverified": peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}}}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := s.DiscoverRoot(ctx, request); status.Code(err) != codes.Unauthenticated {
				t.Fatal(err)
			}
		})
	}
	expired := *leaf
	// Inject expiry during the ReadIndex boundary, on this handler's goroutine.
	h.onRead = func() { expired.NotAfter = time.Now().Add(-time.Second) }
	if _, err := s.DiscoverRoot(nodeContext(context.Background(), &expired, ca), request); status.Code(err) != codes.PermissionDenied {
		t.Fatal("expiry during quorum wait ignored", err)
	}
	h.onRead = nil
	if h.reads.Load() != 1 {
		t.Fatal("auth reached wrong read boundary", h.reads.Load())
	}
	if _, err := s.DiscoverRoot(nodeContext(context.Background(), leaf, ca), request); err != nil {
		t.Fatal("valid connection unusable", err)
	}
	expiringCA := *ca
	h.onRead = func() { expiringCA.NotAfter = time.Now().Add(-time.Second) }
	if _, err := s.DiscoverRoot(nodeContext(context.Background(), leaf, &expiringCA), request); status.Code(err) != codes.PermissionDenied {
		t.Fatal("CA expiry during quorum wait ignored", err)
	}
}

func TestRootJoinRPCFollowerRedirectDoesNotPropose(t *testing.T) {
	s, h, _, _, ca := handlerFixture(t)
	// Construct a real enrolled CSR/leaf and register its metadata. No consumed
	// grant exists: follower redirection must not claim admission or propose.
	store := t.TempDir()
	id, request, err := identityenrollment.Prepare(store, nodepki.DefaultServerName, "r1", "127.0.0.1:7444")
	if err != nil {
		t.Fatal(err)
	}
	command, err := rootstate.NodeEnrollmentCommand(request)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Submit(raft.RootGroupID, encoded); err != nil {
		t.Fatal(err)
	}
	// This constructed context isolates handler redirect behavior; network
	// certificate verification is independently covered by the real TLS tests.
	id.ClusterID = s.clusterID
	uri, err := identity.NodeIdentityURI(id)
	if err != nil {
		t.Fatal(err)
	}
	key, err := identityenrollment.RequestPublicKey(request)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{Subject: ca.Subject, RawSubjectPublicKeyInfo: key, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	leaf.Subject.CommonName = id.NodeID
	leaf.URIs = append(leaf.URIs, uri)
	h.follower.Store(true)
	before := s.root.AppliedIndex()
	document, err := identityenrollment.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	response, err := s.JoinRoot(nodeContext(context.Background(), leaf, ca), &rootjoinwire.JoinRootRequest{ClusterId: s.clusterID, GrantId: "join_00000000000000000000000000000001", RequestDocument: document})
	if err != nil || response.AdmittedIndex != 0 || len(response.View.Peers) != 1 {
		t.Fatal(response, err)
	}
	if s.root.AppliedIndex() != before {
		t.Fatal("redirect mutated Root")
	}
	if _, err := s.JoinRoot(nodeContext(context.Background(), leaf, ca), &rootjoinwire.JoinRootRequest{ClusterId: s.clusterID, GrantId: "join_00000000000000000000000000000001", RequestDocument: append(document, '\n')}); status.Code(err) != codes.InvalidArgument {
		t.Fatal("noncanonical request accepted", err)
	}
}
