package nodecertrpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/pem"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	enrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/nodecertwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type fixture struct {
	host           *raft.Host
	root           *rootstate.State
	server         *Server
	signer         *nodepki.Signer
	issuer, node   identity.Identity
	store, address string
	clientTLS      *tls.Config
	client         nodecertwire.NodeCertificateServiceClient
}

func newFixture(t *testing.T, wrap func(RootReader) RootReader) *fixture {
	t.Helper()
	store := t.TempDir()
	issuer, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	issued := time.Now().UTC().Add(-61 * 24 * time.Hour)
	if _, err := nodepki.Ensure(store, issuer, nodepki.DefaultServerName, issued); err != nil {
		t.Fatal(err)
	}
	signer, err := nodepki.OpenSigner(store, issuer, time.Time{})
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
	if err := host.AddGroup(raft.RootGroupID, []uint64{issuer.RaftID}, false, root); err != nil {
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
	f := &fixture{host: host, root: root, issuer: issuer, signer: signer, address: listener.Addr().String()}
	register := func(id identity.Identity, address string) {
		command, err := rootstate.EncodeCommand(rootstate.Command{Version: 1, RequestID: "renewal-register-" + id.NodeID, Type: rootstate.RegisterNode, Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "local", Address: address}})
		if err != nil {
			t.Fatal(err)
		}
		if err := host.Submit(raft.RootGroupID, command); err != nil {
			t.Fatal(err)
		}
	}
	register(issuer, f.address)
	f.store = t.TempDir()
	id, request, err := enrollment.Prepare(f.store, nodepki.DefaultServerName, "local", "127.0.0.1:7444")
	if err != nil {
		t.Fatal(err)
	}
	id.ClusterID = issuer.ClusterID
	leaf, err := signer.IssueNode(id, request.ServerName, []byte(request.CSRPEM), issued)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enrollment.Install(f.store, enrollment.Bundle{Version: 1, ClusterID: id.ClusterID, NodeID: id.NodeID, RaftID: id.RaftID, ServerName: request.ServerName, CACertificatePEM: string(signer.CACertificatePEM()), CertificatePEM: string(leaf)}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	f.node, err = identity.Load(f.store)
	if err != nil {
		t.Fatal(err)
	}
	register(f.node, request.Address)
	backend := RootReader(host)
	if wrap != nil {
		backend = wrap(host)
	}
	f.server, err = NewServer(backend, root, signer, issuer)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, _, err := config.LoadTLS(store, issuer)
	if err != nil {
		t.Fatal(err)
	}
	_, f.clientTLS, err = config.LoadTLS(f.store, f.node)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)), grpc.MaxRecvMsgSize(MaxMessageSize), grpc.WaitForHandlers(true))
	nodecertwire.RegisterNodeCertificateServiceServer(s, f.server)
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	t.Cleanup(func() {
		s.Stop()
		if err := <-done; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	})
	f.client = f.connect(t, f.clientTLS)
	return f
}

func (f *fixture) connect(t *testing.T, configuration *tls.Config) nodecertwire.NodeCertificateServiceClient {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+f.address, grpc.WithTransportCredentials(credentials.NewTLS(configuration)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageSize), grpc.MaxCallSendMsgSize(MaxMessageSize)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return nodecertwire.NewNodeCertificateServiceClient(conn)
}

func (f *fixture) request() *nodecertwire.RenewNodeRequest {
	return &nodecertwire.RenewNodeRequest{ClusterId: f.node.ClusterID, FromRaftId: f.node.RaftID}
}

func TestNodeRenewalNetworkMembershipAndIdentity(t *testing.T) {
	f := newFixture(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	before := f.root.AppliedIndex()
	if _, err := f.client.RenewNode(ctx, f.request()); status.Code(err) != codes.PermissionDenied {
		t.Fatal("pending node renewed", err)
	}
	if f.root.AppliedIndex() != before {
		t.Fatal("renewal changed Root")
	}
	if err := f.host.AddLearner(raft.RootGroupID, f.node.RaftID); err != nil {
		t.Fatal(err)
	}
	old, err := nodepki.Load(f.store, f.node, nodepki.DefaultServerName, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	response, err := f.client.RenewNode(ctx, f.request())
	if err != nil {
		t.Fatal(err)
	}
	if response.NodeId != f.node.NodeID || response.RaftId != f.node.RaftID || response.ClusterId != f.node.ClusterID {
		t.Fatal("identity changed")
	}
	updated, err := nodepki.InstallRenewedLeaf(f.store, f.node, nodepki.DefaultServerName, response.CertificatePem, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !updated.Certificate.NotAfter.After(old.Certificate.NotAfter) || !bytes.Equal(updated.Certificate.RawSubjectPublicKeyInfo, old.Certificate.RawSubjectPublicKeyInfo) {
		t.Fatal("renewal did not preserve identity/key")
	}
	// A fresh connection loads the installed leaf. A needless retry returns it
	// unchanged, avoiding continuous serial-number churn or validity shortening.
	_, newTLS, err := config.LoadTLS(f.store, f.node)
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.connect(t, newTLS).RenewNode(ctx, f.request())
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(current.CertificatePem)
	if block == nil || !bytes.Equal(block.Bytes, updated.Certificate.Raw) {
		t.Fatal("unneeded renewal changed leaf")
	}
	if err := f.host.RemoveMember(raft.RootGroupID, f.node.RaftID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	before = f.root.AppliedIndex()
	if _, err := f.client.RenewNode(ctx, f.request()); status.Code(err) != codes.PermissionDenied {
		t.Fatal("removed node renewed on old TLS session", err)
	}
	if f.root.AppliedIndex() != before {
		t.Fatal("rejected renewal changed Root")
	}
}

func TestNodeRenewalNetworkClientDoesNotPublishAndPinsResponse(t *testing.T) {
	f := newFixture(t, nil)
	if err := f.host.AddLearner(raft.RootGroupID, f.node.RaftID); err != nil {
		t.Fatal(err)
	}
	path := nodepki.StorePaths(f.store).Certificate
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaf, err := RequestRenewal(ctx, f.store, f.address)
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("client wrote PKI", err)
	}
	material, err := nodepki.Load(f.store, f.node, nodepki.DefaultServerName, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"cluster", "node", "raft", "certificate"} {
		response := &nodecertwire.RenewNodeResponse{ClusterId: f.node.ClusterID, NodeId: f.node.NodeID, RaftId: f.node.RaftID, CertificatePem: leaf}
		switch change {
		case "cluster":
			response.ClusterId = "wrong"
		case "node":
			response.NodeId = f.issuer.NodeID
		case "raft":
			response.RaftId = f.issuer.RaftID
		case "certificate":
			response.CertificatePem = []byte("broken")
		}
		if _, err := validateResponse(material, f.node, response, time.Now()); err == nil {
			t.Fatal("accepted response substitution", change)
		}
	}
	if _, err := nodepki.InstallRenewedLeaf(f.store, f.node, nodepki.DefaultServerName, leaf, time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func TestNodeRenewalNetworkRejectsEnrollmentRoleAndImpersonation(t *testing.T) {
	f := newFixture(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := f.client.RenewNode(ctx, &nodecertwire.RenewNodeRequest{ClusterId: f.node.ClusterID, FromRaftId: f.issuer.RaftID}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("impersonated issuer", err)
	}
	draft, err := enrollment.NewGrantDraft(0, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	r := draft.Request()
	leaf, err := f.signer.IssueEnrollment(r.GrantID, r.PublicKeyDER, time.Unix(r.ExpiresUnix, 0), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := draft.Complete(enrollment.GrantBundle{ClusterID: f.node.ClusterID, GrantID: r.GrantID, IssuerAddress: f.address, ExpiresUnix: r.ExpiresUnix, CACertificatePEM: string(f.signer.CACertificatePEM()), CertificatePEM: string(leaf)}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := ticket.ClientTLS(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.connect(t, configuration).RenewNode(ctx, f.request()); status.Code(err) != codes.PermissionDenied {
		t.Fatal("enrollment role renewed Node leaf", err)
	}
}

type hookedReader struct {
	RootReader
	after func()
}

func (h *hookedReader) ReadIndex(ctx context.Context, group string) (uint64, error) {
	index, err := h.RootReader.ReadIndex(ctx, group)
	if err == nil && h.after != nil {
		h.after()
	}
	return index, err
}

func TestNodeRenewalRechecksExpiryAfterQuorum(t *testing.T) {
	var backend *hookedReader
	f := newFixture(t, func(host RootReader) RootReader { backend = &hookedReader{RootReader: host}; return backend })
	if err := f.host.AddLearner(raft.RootGroupID, f.node.RaftID); err != nil {
		t.Fatal(err)
	}
	material, err := nodepki.Load(f.store, f.node, nodepki.DefaultServerName, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	f.server.now = func() time.Time { return time.Unix(0, now.Load()) }
	backend.after = func() { now.Store(material.Certificate.NotAfter.UnixNano()) }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := f.client.RenewNode(ctx, f.request()); status.Code(err) != codes.Unauthenticated {
		t.Fatal("expired during quorum wait", err)
	}
}

func TestNodeRenewalRechecksMembershipAfterSigning(t *testing.T) {
	var backend *hookedReader
	f := newFixture(t, func(host RootReader) RootReader { backend = &hookedReader{RootReader: host}; return backend })
	if err := f.host.AddLearner(raft.RootGroupID, f.node.RaftID); err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int64
	backend.after = func() {
		if reads.Add(1) == 2 {
			if err := f.host.RemoveMember(raft.RootGroupID, f.node.RaftID); err != nil {
				t.Error(err)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := f.client.RenewNode(ctx, f.request()); status.Code(err) != codes.PermissionDenied {
		t.Fatal("returned signed leaf after removal", err)
	}
	if reads.Load() != 2 {
		t.Fatal("post-signing confirmation missing")
	}
}

type blockedReader struct{ entered chan struct{} }

func (b *blockedReader) ReadIndex(ctx context.Context, _ string) (uint64, error) {
	b.entered <- struct{}{}
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestNodeRenewalNetworkWorkBudgetAndCancellation(t *testing.T) {
	backend := &blockedReader{entered: make(chan struct{}, maxConcurrentRequests)}
	f := newFixture(t, func(RootReader) RootReader { return backend })
	// Work-budget admission is exercised with a currently admitted Node. A
	// pending Node is rejected by the local membership preflight before it needs
	// a Root barrier, and is covered by TestNodeRenewalNetworkMembershipAndIdentity.
	if err := f.host.AddLearner(raft.RootGroupID, f.node.RaftID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, maxConcurrentRequests)
	for range maxConcurrentRequests {
		go func() { _, err := f.client.RenewNode(ctx, f.request()); done <- err }()
	}
	for range maxConcurrentRequests {
		select {
		case <-backend.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("requests did not reach Root")
		}
	}
	limited, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err := f.client.RenewNode(limited, f.request()); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("unbounded renewal work", err)
	}
	cancel()
	for range maxConcurrentRequests {
		select {
		case err := <-done:
			if status.Code(err) != codes.Canceled {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("cancellation did not finish")
		}
	}
	deadline := time.Now().Add(time.Second)
	for len(f.server.slots) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("renewal slots leaked")
		}
		time.Sleep(time.Millisecond)
	}
}
