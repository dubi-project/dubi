package enrollmentrpc

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentwire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEnrollmentNetworkRejectsUnusableAddressBeforeGrantConsumption(t *testing.T) {
	f := newNetworkFixture(t, nil)
	ticket := f.ticket(t)
	_, request, err := identityenrollment.Prepare(t.TempDir(), nodepki.DefaultServerName, "r2", "127.0.0.1:7444")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tlsConfig, err := ticket.ClientTLS(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	proof, err := ticket.AdmissionProof(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	client := enrollmentwire.NewEnrollmentServiceClient(f.connect(t, tlsConfig))
	before := f.root.AppliedIndex()
	for _, address := range []string{"0.0.0.0:7444", "[::]:7444", "224.0.0.1:7444", "host/path:7444", "host:07444", "user@host:7444"} {
		candidate := request
		candidate.Address = address
		encoded, err := json.Marshal(candidate)
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.EnrollNode(ctx, &enrollmentwire.EnrollNodeRequest{ClusterId: f.id.ClusterID, RequestDocument: encoded, AdmissionProof: proof})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("unusable endpoint %q was not rejected before admission: %v", address, err)
		}
		grant, found, err := f.root.JoinGrant(ticket.GrantID())
		if err != nil || !found || grant.State != rootstate.JoinGrantActive || f.root.AppliedIndex() != before {
			t.Fatal("bad address consumed grant or mutated Root", grant, err)
		}
	}
	if _, err := Enroll(ctx, ticket, request); err != nil {
		t.Fatal("valid request could not reuse unconsumed grant", err)
	}
}
