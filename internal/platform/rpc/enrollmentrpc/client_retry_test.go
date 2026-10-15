package enrollmentrpc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enrollment"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type onceUnavailableHost struct {
	enrollment.Host
	fail atomic.Bool
}

type enrollmentCall struct {
	enrollmentwire.EnrollmentServiceClient
	call func(context.Context, *enrollmentwire.EnrollNodeRequest) (*enrollmentwire.EnrollNodeResponse, error)
}

func (c enrollmentCall) EnrollNode(ctx context.Context, request *enrollmentwire.EnrollNodeRequest, _ ...grpc.CallOption) (*enrollmentwire.EnrollNodeResponse, error) {
	return c.call(ctx, request)
}

func TestEnrollmentRetryBoundsIdenticalRequestAndCancellation(t *testing.T) {
	request := &enrollmentwire.EnrollNodeRequest{ClusterId: testRetryCluster, RequestDocument: []byte("public-csr-fixture"), AdmissionProof: []byte("test-proof")}
	original := proto.Clone(request).(*enrollmentwire.EnrollNodeRequest)
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.PermissionDenied, codes.Unauthenticated, codes.InvalidArgument, codes.FailedPrecondition, codes.ResourceExhausted, codes.Internal} {
		t.Run(code.String(), func(t *testing.T) {
			calls := 0
			client := enrollmentCall{call: func(ctx context.Context, got *enrollmentwire.EnrollNodeRequest) (*enrollmentwire.EnrollNodeResponse, error) {
				calls++
				if !proto.Equal(got, original) {
					t.Fatal("retry changed request identity/proof")
				}
				if end, ok := ctx.Deadline(); !ok || time.Until(end) > 5*time.Second {
					t.Fatal("RPC lacks finite attempt deadline")
				}
				if calls == 1 {
					return nil, status.Error(code, "injected")
				}
				return &enrollmentwire.EnrollNodeResponse{BundleDocument: []byte("reply")}, nil
			}}
			_, err := enrollWithRetry(context.Background(), client, request)
			retry := code == codes.Unavailable || code == codes.DeadlineExceeded
			if retry && (err != nil || calls != 2) || !retry && (status.Code(err) != code || calls != 1) {
				t.Fatal("incorrect retry classification", calls, err)
			}
		})
	}
	t.Run("finite-attempt-count", func(t *testing.T) {
		calls := 0
		client := enrollmentCall{call: func(context.Context, *enrollmentwire.EnrollNodeRequest) (*enrollmentwire.EnrollNodeResponse, error) {
			calls++
			return nil, status.Error(codes.Unavailable, "offline")
		}}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := enrollWithRetry(ctx, client, request); status.Code(err) != codes.Unavailable || calls != 8 {
			t.Fatal(calls, err)
		}
	})
	t.Run("cancel-between-attempts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		client := enrollmentCall{call: func(context.Context, *enrollmentwire.EnrollNodeRequest) (*enrollmentwire.EnrollNodeResponse, error) {
			calls++
			cancel()
			return nil, status.Error(codes.Unavailable, "cancelled")
		}}
		if _, err := enrollWithRetry(ctx, client, request); !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatal(calls, err)
		}
	})
}

const testRetryCluster = "cluster_00000000000000000000000000000001"

func (h *onceUnavailableHost) ReadIndex(ctx context.Context, group string) (uint64, error) {
	if h.fail.Swap(false) {
		return 0, errors.New("injected transient leadership change")
	}
	return h.Host.ReadIndex(ctx, group)
}

func TestEnrollmentNetworkRetriesEnrollmentAfterTransientRootLoss(t *testing.T) {
	h := &onceUnavailableHost{}
	f := newNetworkFixture(t, func(host enrollment.Host) enrollment.Host { h.Host = host; return h })
	ticket := f.ticket(t)
	_, request, err := identityenrollment.Prepare(t.TempDir(), nodepki.DefaultServerName, "local", "127.0.0.1:7444")
	if err != nil {
		t.Fatal(err)
	}
	h.fail.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bundle, err := Enroll(ctx, ticket, request)
	if err != nil || bundle.NodeID != request.NodeID || bundle.RaftID != request.RaftID {
		t.Fatal("one-call enrollment did not recover original identity", err)
	}
	grant, found, err := f.root.JoinGrant(ticket.GrantID())
	if err != nil || !found || grant.State != rootstate.JoinGrantConsumed {
		t.Fatal("original grant not consumed", found, err)
	}
}
