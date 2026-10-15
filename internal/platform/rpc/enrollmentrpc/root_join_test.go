package enrollmentrpc

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinwire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func enrolledRootJoin(t *testing.T, f *networkFixture) (string, identityenrollment.Ticket, identityenrollment.Request, *tls.Config) {
	t.Helper()
	ticket := f.ticket(t)
	store := t.TempDir()
	_, request, err := identityenrollment.Prepare(store, nodepki.DefaultServerName, "r2", "127.0.0.1:7444")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bundle, err := Enroll(ctx, ticket, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := identityenrollment.SaveReceipt(store, ticket, request, bundle, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := identityenrollment.ResumeReceipt(store, time.Time{}); err != nil {
		t.Fatal(err)
	}
	id, err := identity.Load(store)
	if err != nil {
		t.Fatal(err)
	}
	_, clientTLS, err := config.LoadTLS(store, id)
	if err != nil {
		t.Fatal(err)
	}
	return store, ticket, request, clientTLS
}

func TestRootJoinNetworkAdmissionRetriesRolesAndRemoval(t *testing.T) {
	f := newNetworkFixture(t, nil)
	store, ticket, request, clientTLS := enrolledRootJoin(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	encoded, err := identityenrollment.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	command := &rootjoinwire.JoinRootRequest{ClusterId: f.id.ClusterID, GrantId: ticket.GrantID(), RequestDocument: encoded}
	client := rootjoinwire.NewRootJoinServiceClient(f.connect(t, clientTLS))
	before := f.root.AppliedIndex()
	// The issuer's valid Node certificate cannot impersonate the joining Node.
	if _, err := rootjoinwire.NewRootJoinServiceClient(f.connect(t, f.nodeTLS)).JoinRoot(ctx, command); status.Code(err) != codes.PermissionDenied {
		t.Fatal("issuer impersonated joiner", err)
	}
	enrollmentTLS, err := ticket.ClientTLS(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	temporary := rootjoinwire.NewRootJoinServiceClient(f.connect(t, enrollmentTLS))
	if _, err := temporary.JoinRoot(ctx, command); status.Code(err) != codes.PermissionDenied {
		t.Fatal("temporary role joined Root", err)
	}
	if _, err := temporary.DiscoverRoot(ctx, &rootjoinwire.DiscoverRootRequest{ClusterId: f.id.ClusterID, FromRaftId: request.RaftID}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("temporary role discovered Root", err)
	}
	if f.root.AppliedIndex() != before {
		t.Fatal("unauthorized RPC mutated Root")
	}
	discovery, err := client.DiscoverRoot(ctx, &rootjoinwire.DiscoverRootRequest{ClusterId: f.id.ClusterID, FromRaftId: request.RaftID})
	if err != nil || len(discovery.View.Peers) != 1 || discovery.View.Peers[0].RaftId != f.id.RaftID {
		t.Fatal("discovery advertised nonmember", discovery, err)
	}
	// Concurrent retries all recover one durable admission, never change quorum.
	var wg sync.WaitGroup
	responses := make(chan *rootjoinwire.JoinRootResponse, 3)
	for range 3 {
		wg.Go(func() {
			response, err := rootjoinrpc.Join(ctx, store, nil)
			if err != nil {
				t.Error(err)
				return
			}
			responses <- response
		})
	}
	wg.Wait()
	close(responses)
	var admitted uint64
	for response := range responses {
		if admitted == 0 {
			admitted = response.AdmittedIndex
		}
		if admitted == 0 || response.AdmittedIndex != admitted || len(response.View.Voters) != 1 || response.View.Voters[0] != f.id.RaftID || len(response.View.Learners) != 1 || response.View.Learners[0] != request.RaftID {
			t.Fatal("inconsistent admission", response)
		}
	}
	if admitted == 0 {
		t.Fatal("no admission succeeded")
	}
	task, found, err := f.root.RootJoin(ticket.GrantID())
	if err != nil || !found || task.State != rootstate.RootJoinAdmitted || task.AdmittedIndex != admitted {
		t.Fatal("admission not persisted", task, found, err)
	}
	if err := f.host.RemoveMember(raft.RootGroupID, request.RaftID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	if _, err := rootjoinrpc.Join(ctx, store, nil); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("removed Node was silently re-added", err)
	}
	conf, err := f.root.RootMembership()
	if err != nil || len(conf.Learners) != 0 {
		t.Fatal(conf, err)
	}
}

func TestRootJoinNetworkRejectsSameIdentityDifferentKeyAndWrongGrant(t *testing.T) {
	f := newNetworkFixture(t, nil)
	store, ticket, request, clientTLS := enrolledRootJoin(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	encoded, err := identityenrollment.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	// Issue a valid same-identity Node leaf for another key. Identity-only
	// checking would accept this; the CSR binding must reject it before proposal.
	id, err := identity.Load(store)
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	unbound := id
	unbound.ClusterID = ""
	uri, err := identity.JoinIdentityURI(unbound)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: id.NodeID}, DNSNames: []string{nodepki.DefaultServerName}, URIs: []*url.URL{uri}}, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := f.signer.IssueNode(id, nodepki.DefaultServerName, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csr}), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certificate, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}))
	if err != nil {
		t.Fatal(err)
	}
	wrongTLS := clientTLS.Clone()
	wrongTLS.Certificates = []tls.Certificate{pair}
	wrongClient := rootjoinwire.NewRootJoinServiceClient(f.connect(t, wrongTLS))
	// Successful discovery proves that this test crossed real mTLS with the
	// same Node identity; JoinRoot must additionally bind the enrolled key.
	if _, err := wrongClient.DiscoverRoot(ctx, &rootjoinwire.DiscoverRootRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID}); err != nil {
		t.Fatal("same-identity fixture failed TLS", err)
	}
	before := f.root.AppliedIndex()
	if _, err := wrongClient.JoinRoot(ctx, &rootjoinwire.JoinRootRequest{ClusterId: id.ClusterID, GrantId: ticket.GrantID(), RequestDocument: encoded}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("same identity with different key was admitted", err)
	}
	if f.root.AppliedIndex() != before {
		t.Fatal("wrong key mutated Root")
	}
	// A changed metadata request preserves the key but must not poison the
	// legitimate request's idempotency record for this grant.
	changed := request
	changed.Address = "127.0.0.1:7999"
	wrong, err := identityenrollment.EncodeRequest(changed)
	if err != nil {
		t.Fatal(err)
	}
	client := rootjoinwire.NewRootJoinServiceClient(f.connect(t, clientTLS))
	if _, err := client.JoinRoot(ctx, &rootjoinwire.JoinRootRequest{ClusterId: f.id.ClusterID, GrantId: ticket.GrantID(), RequestDocument: wrong}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("unconsumed CSR admitted", err)
	}
	if _, err := client.JoinRoot(ctx, &rootjoinwire.JoinRootRequest{ClusterId: f.id.ClusterID, GrantId: "join_000000000000000000000000000000ff", RequestDocument: encoded}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("unknown grant admitted", err)
	}
	if _, err := rootjoinrpc.Join(ctx, store, nil); err != nil {
		t.Fatal("bad request poisoned good request", err)
	}
}
