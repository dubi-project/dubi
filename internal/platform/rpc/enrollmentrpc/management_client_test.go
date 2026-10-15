package enrollmentrpc

import (
	"context"
	"encoding/pem"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type managementReply struct {
	enrollmentwire.UnimplementedEnrollmentServiceServer
	create func(context.Context, *enrollmentwire.CreateGrantRequest) (*enrollmentwire.CreateGrantResponse, error)
}

func (s managementReply) CreateGrant(ctx context.Context, request *enrollmentwire.CreateGrantRequest) (*enrollmentwire.CreateGrantResponse, error) {
	return s.create(ctx, request)
}

func serveManagementReply(t *testing.T, f *networkFixture, create func(context.Context, *enrollmentwire.CreateGrantRequest) (*enrollmentwire.CreateGrantResponse, error)) string {
	t.Helper()
	serverTLS, _, err := config.LoadTLS(f.store, f.id)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	enrollmentwire.RegisterEnrollmentServiceServer(server, managementReply{create: create})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-done; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	})
	return listener.Addr().String()
}

func TestEnrollmentNetworkManagementRetriesSamePublicDraftAndRevokes(t *testing.T) {
	f := newNetworkFixture(t, nil)
	var calls atomic.Int32
	var first *enrollmentwire.CreateGrantRequest
	var firstMu sync.Mutex
	address := serveManagementReply(t, f, func(ctx context.Context, request *enrollmentwire.CreateGrantRequest) (*enrollmentwire.CreateGrantResponse, error) {
		firstMu.Lock()
		count := calls.Add(1)
		if count == 1 {
			first = proto.Clone(request).(*enrollmentwire.CreateGrantRequest)
		} else if !proto.Equal(first, request) {
			firstMu.Unlock()
			return nil, status.Error(codes.InvalidArgument, "retry changed grant request")
		}
		firstMu.Unlock()
		response, err := f.server.CreateGrant(ctx, request)
		if err == nil && count == 1 {
			return nil, status.Error(codes.Unavailable, "injected reply loss after Root confirmation")
		}
		return response, err
	})
	draft, err := identityenrollment.NewGrantDraft(0, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticket, err := CreateTicket(ctx, f.store, address, draft)
	if err != nil || calls.Load() != 2 || ticket.GrantID() != draft.Request().GrantID || ticket.Address() != f.address {
		t.Fatal("lost reply retry failed", calls.Load(), err)
	}
	before := f.root.AppliedIndex()
	if _, err := CreateTicket(ctx, f.store, address, draft); err != nil || f.root.AppliedIndex() != before {
		t.Fatal("same draft allocated another grant", err)
	}
	if err := RevokeGrant(ctx, f.store, f.address, ticket.GrantID()); err != nil {
		t.Fatal(err)
	}
	grant, found, err := f.root.JoinGrant(ticket.GrantID())
	if err != nil || !found || grant.State != rootstate.JoinGrantRevoked {
		t.Fatal("revocation not confirmed", found, err)
	}
	beforeCalls := calls.Load()
	if _, err := CreateTicket(ctx, f.store, address, draft); err == nil || calls.Load() != beforeCalls+1 {
		t.Fatal("terminal grant retried or resurrected", calls.Load(), err)
	}
}

func TestEnrollmentNetworkManagementPinsResponseClusterAndCA(t *testing.T) {
	f := newNetworkFixture(t, nil)
	alternate, err := nodepki.Ensure(t.TempDir(), f.id, nodepki.DefaultServerName, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	// A different self-signed CA for the SAME Cluster identity is still not the
	// trust anchor loaded from the operator's managed Node Store.
	ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: alternate.CACertificate.Raw}))
	for _, kind := range []string{"cluster", "ca"} {
		t.Run(kind, func(t *testing.T) {
			address := serveManagementReply(t, f, func(ctx context.Context, request *enrollmentwire.CreateGrantRequest) (*enrollmentwire.CreateGrantResponse, error) {
				response, err := f.server.CreateGrant(ctx, request)
				if err != nil {
					return nil, err
				}
				if kind == "cluster" {
					response.ClusterId = "cluster_00000000000000000000000000000001"
				} else {
					response.CaCertificatePem = ca
				}
				return response, nil
			})
			draft, err := identityenrollment.NewGrantDraft(0, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := CreateTicket(context.Background(), f.store, address, draft); err == nil || !strings.Contains(err.Error(), "changed") {
				t.Fatal("issuer replaced trust anchor", err)
			}
		})
	}
}
