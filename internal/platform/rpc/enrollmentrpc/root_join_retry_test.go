package enrollmentrpc

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRootJoinNetworkRetriesSameReceiptAcrossTemporaryFailure(t *testing.T) {
	for _, mode := range []string{"before-confirmation", "lost-confirmed-reply", "pressure", "denied", "unavailable-limit", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			calls := 0
			var first *rootjoinwire.JoinRootRequest
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			f := newNetworkFixtureWithInterceptor(t, nil, func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if !strings.HasSuffix(info.FullMethod, "/JoinRoot") {
					return handler(ctx, request)
				}
				mu.Lock()
				calls++
				count := calls
				current := request.(*rootjoinwire.JoinRootRequest)
				if first == nil {
					first = proto.Clone(current).(*rootjoinwire.JoinRootRequest)
				}
				equal := proto.Equal(first, current)
				mu.Unlock()
				if !equal {
					return nil, status.Error(codes.InvalidArgument, "retry changed receipt request")
				}
				switch mode {
				case "pressure":
					return nil, status.Error(codes.ResourceExhausted, "busy")
				case "denied":
					return nil, status.Error(codes.PermissionDenied, "denied")
				case "unavailable-limit":
					return nil, status.Error(codes.Unavailable, "offline")
				case "cancel":
					cancel()
					return nil, status.Error(codes.Unavailable, "cancel")
				case "before-confirmation":
					if count == 1 {
						return nil, status.Error(codes.Unavailable, "joint transition")
					}
				}
				response, err := handler(ctx, request)
				if mode == "lost-confirmed-reply" && count == 1 && err == nil {
					return nil, status.Error(codes.Unavailable, "reply lost")
				}
				return response, err
			})
			store, ticket, _, _ := enrolledRootJoin(t, f)
			response, err := rootjoinrpc.Join(ctx, store, nil)
			mu.Lock()
			count := calls
			mu.Unlock()
			if mode == "before-confirmation" || mode == "lost-confirmed-reply" {
				if err != nil || response.GetAdmittedIndex() == 0 || count != 2 {
					t.Fatal("same receipt retry did not recover", count, err)
				}
				task, found, err := f.root.RootJoin(ticket.GrantID())
				if err != nil || !found || task.State != rootstate.RootJoinAdmitted || task.AdmittedIndex != response.GetAdmittedIndex() {
					t.Fatal("retry changed durable admission", task, found, err)
				}
			} else {
				wantCount := 1
				if mode == "unavailable-limit" {
					wantCount = 8
				}
				if err == nil || count != wantCount {
					t.Fatal("retry limit/classification", mode, count, err)
				}
				if _, found, err := f.root.RootJoin(ticket.GrantID()); err != nil || found {
					t.Fatal("failed call created admission", found, err)
				}
			}
		})
	}
}
