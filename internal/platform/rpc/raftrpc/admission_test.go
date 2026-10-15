package raftrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/diskpressure"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type pressureBackend struct {
	rejection error
	reads     int
}

func (b *pressureBackend) Submit(string, []byte) error { return b.rejection }
func (b *pressureBackend) ReadIndex(context.Context, string) (uint64, error) {
	b.reads++
	return 2, nil
}
func (b *pressureBackend) Status(string) (raft.GroupStatus, error) {
	return raft.GroupStatus{LeaderID: 1}, nil
}

func TestDiskAdmissionRPCClassificationAndRetryDetails(t *testing.T) {
	for _, rejection := range []error{diskpressure.ErrPressure, fmt.Errorf("wrapped: %w", diskpressure.ErrProbe)} {
		err := mutationError("propose", rejection, codes.Unavailable)
		if status.Code(err) != codes.ResourceExhausted || retryableGroupRPC(context.Background(), err) {
			t.Fatal("pressure incorrectly triggers failover", err)
		}
		details := status.Convert(err).Details()
		if len(details) != 1 {
			t.Fatal("missing retry detail")
		}
		retry, ok := details[0].(*errdetails.RetryInfo)
		if !ok || retry.GetRetryDelay().AsDuration() != diskpressure.RetryDelay {
			t.Fatal("wrong retry delay", details)
		}
		if forwarded := mutationError("forward", err, codes.FailedPrecondition); forwarded != err {
			t.Fatal("forwarding lost capacity status")
		}
	}
	if status.Code(mutationError("operation", errors.New("other"), codes.FailedPrecondition)) != codes.FailedPrecondition {
		t.Fatal("changed unrelated failure classification")
	}
}

func TestGroupAdmissionRejectsWithoutReadBarrierAndReadsRemainAvailable(t *testing.T) {
	backend := &pressureBackend{rejection: diskpressure.ErrPressure}
	server, err := NewGroupServer("cluster", 1, backend, func(context.Context, uint64) error { return nil }, 1024)
	if err != nil {
		t.Fatal(err)
	}
	// Only the admission/error path is under test here; real mTLS transport and
	// certificate binding are exercised by group_test.go and transport_test.go.
	cert := &x509.Certificate{}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
		VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}}})
	_, err = server.Submit(ctx, &raftwire.GroupSubmitRequest{ClusterId: "cluster", GroupId: "data", FromRaftId: 2, ToRaftId: 1, Command: []byte("work")})
	if status.Code(err) != codes.ResourceExhausted || backend.reads != 0 {
		t.Fatal("rejected proposal reached confirmation", err, backend.reads)
	}
	if _, err := server.ReadIndex(ctx, &raftwire.GroupReadIndexRequest{ClusterId: "cluster", GroupId: "data", FromRaftId: 2, ToRaftId: 1}); err != nil || backend.reads != 1 {
		t.Fatal("disk pressure blocked read", err)
	}
	backend.rejection = nil
	if _, err := server.Submit(ctx, &raftwire.GroupSubmitRequest{ClusterId: "cluster", GroupId: "data", FromRaftId: 2, ToRaftId: 1, Command: []byte("work")}); err != nil {
		t.Fatal("recovery failed", err)
	}
}
