package enrollmentrpc

import (
	"context"
	"testing"
	"time"

	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEnrollmentNetworkManagementRequiresCurrentRootMember(t *testing.T) {
	for _, phase := range []string{"pending", "member", "removed"} {
		t.Run(phase, func(t *testing.T) {
			f := newNetworkFixture(t, nil)
			store, _, request, clientTLS := enrolledRootJoin(t, f)
			client := enrollmentwire.NewEnrollmentServiceClient(f.connect(t, clientTLS))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if phase != "pending" {
				if _, err := rootjoinrpc.Join(ctx, store, nil); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "removed" {
				// Establish and authorize this TLS session while the learner is a
				// current member; removal must affect subsequent calls on it.
				live := f.ticket(t)
				if _, err := client.RevokeGrant(ctx, &enrollmentwire.RevokeGrantRequest{ClusterId: f.id.ClusterID, FromRaftId: request.RaftID, GrantId: live.GrantID()}); err != nil {
					t.Fatal(err)
				}
				if err := f.host.RemoveMember(raft.RootGroupID, request.RaftID); err != nil {
					t.Fatal(err)
				}
				if _, err := f.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
					t.Fatal(err)
				}
			}
			target := f.ticket(t)
			draft, err := identityenrollment.NewGrantDraft(0, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			r := draft.Request()
			before := f.root.AppliedIndex()
			_, createErr := client.CreateGrant(ctx, &enrollmentwire.CreateGrantRequest{ClusterId: f.id.ClusterID, FromRaftId: request.RaftID,
				GrantId: r.GrantID, SecretSha256: r.SecretSHA256, PublicKeyDer: r.PublicKeyDER, ExpiresUnix: r.ExpiresUnix})
			_, revokeErr := client.RevokeGrant(ctx, &enrollmentwire.RevokeGrantRequest{ClusterId: f.id.ClusterID, FromRaftId: request.RaftID, GrantId: target.GrantID()})
			if phase == "member" {
				if createErr != nil || revokeErr != nil {
					t.Fatal("current learner denied", createErr, revokeErr)
				}
				return
			}
			if status.Code(createErr) != codes.PermissionDenied || status.Code(revokeErr) != codes.PermissionDenied {
				t.Errorf("nonmember managed credentials: create=%v revoke=%v", createErr, revokeErr)
			}
			if f.root.AppliedIndex() != before {
				t.Fatal("unauthorized management mutated Root")
			}
		})
	}
}
