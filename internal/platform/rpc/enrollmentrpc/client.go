package enrollmentrpc

import (
	"context"
	"errors"
	"time"

	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Enroll returns a validated, leaf-only bundle. The caller must durably save it
// through SaveReceipt before ResumeReceipt installs it; this function never changes a
// local Store or starts Raft. A retry must use the same persisted Request.
func Enroll(ctx context.Context, ticket identityenrollment.Ticket, request identityenrollment.Request) (identityenrollment.Bundle, error) {
	if ctx == nil {
		return identityenrollment.Bundle{}, errors.New("enrollment context required")
	}
	if err := ctx.Err(); err != nil {
		return identityenrollment.Bundle{}, err
	}
	encoded, err := identityenrollment.EncodeRequest(request)
	if err != nil {
		return identityenrollment.Bundle{}, err
	}
	if len(encoded) > maxRequestDocument {
		return identityenrollment.Bundle{}, status.Error(codes.ResourceExhausted, "Node enrollment request exceeds limit")
	}
	config, err := ticket.ClientTLS(time.Time{})
	if err != nil {
		return identityenrollment.Bundle{}, err
	}
	proof, err := ticket.AdmissionProof(time.Time{})
	if err != nil {
		return identityenrollment.Bundle{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///"+ticket.Address(), grpc.WithTransportCredentials(tlscredentials.NewTLS(config)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageSize), grpc.MaxCallSendMsgSize(MaxMessageSize)))
	if err != nil {
		return identityenrollment.Bundle{}, err
	}
	defer conn.Close()
	response, err := enrollWithRetry(ctx, enrollmentwire.NewEnrollmentServiceClient(conn), &enrollmentwire.EnrollNodeRequest{
		ClusterId: ticket.ClusterID(), RequestDocument: encoded, AdmissionProof: proof})
	if err != nil {
		return identityenrollment.Bundle{}, err
	}
	bundle, err := identityenrollment.DecodeBundle(response.GetBundleDocument())
	if err != nil {
		return identityenrollment.Bundle{}, err
	}
	if err := ticket.ValidateNodeBundle(bundle, request, time.Time{}); err != nil {
		return identityenrollment.Bundle{}, err
	}
	return bundle, nil
}

// Root role changes may temporarily move the leader away from the signer.
// Reuse the identical proof/CSR throughout a finite retry window; do not retry
// authentication, validation or resource-pressure failures. Each RPC and the
// enclosing Enroll call have separate finite limits, with no detached work.
func enrollWithRetry(ctx context.Context, client enrollmentwire.EnrollmentServiceClient, request *enrollmentwire.EnrollNodeRequest) (*enrollmentwire.EnrollNodeResponse, error) {
	delay := 100 * time.Millisecond
	for attempt := 0; attempt < 8; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		response, err := client.EnrollNode(callCtx, request)
		cancel()
		if err == nil {
			return response, nil
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded || attempt == 7 {
			return nil, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, time.Second)
	}
	panic("unreachable enrollment retry state")
}
