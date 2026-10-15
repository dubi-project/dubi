package enrollmentrpc

import (
	"context"
	"crypto/x509"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateTicket sends only the draft's public request over managed Node mTLS.
// Keep the same draft to retry a lost reply; neither private key nor secret is
// sent to the issuer. A returned CA must equal the caller's existing trust root.
func CreateTicket(ctx context.Context, store, address string, draft *identityenrollment.GrantDraft) (identityenrollment.Ticket, error) {
	if ctx == nil || draft == nil {
		return identityenrollment.Ticket{}, errors.New("ticket creation requires context and draft")
	}
	if err := ctx.Err(); err != nil {
		return identityenrollment.Ticket{}, err
	}
	request := draft.Request()
	if err := request.Validate(time.Time{}); err != nil {
		return identityenrollment.Ticket{}, err
	}
	id, roots, conn, err := openManagementClient(store, address)
	if err != nil {
		return identityenrollment.Ticket{}, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	client := enrollmentwire.NewEnrollmentServiceClient(conn)
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return identityenrollment.Ticket{}, err
		}
		callCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		response, err := client.CreateGrant(callCtx, &enrollmentwire.CreateGrantRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID,
			GrantId: request.GrantID, SecretSha256: request.SecretSHA256, PublicKeyDer: request.PublicKeyDER, ExpiresUnix: request.ExpiresUnix})
		stop()
		if err == nil {
			return completeTicketReply(id.ClusterID, roots, draft, response)
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			return identityenrollment.Ticket{}, err
		}
		last = err
		if attempt == 2 {
			break
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return identityenrollment.Ticket{}, ctx.Err()
		case <-timer.C:
		}
	}
	return identityenrollment.Ticket{}, last
}

func completeTicketReply(clusterID string, roots *x509.CertPool, draft *identityenrollment.GrantDraft, response *enrollmentwire.CreateGrantResponse) (identityenrollment.Ticket, error) {
	if response == nil || response.ClusterId != clusterID {
		return identityenrollment.Ticket{}, errors.New("ticket issuer changed Cluster identity")
	}
	ca, err := nodepki.ValidateClusterCA(clusterID, []byte(response.CaCertificatePem), time.Time{})
	if err != nil {
		return identityenrollment.Ticket{}, err
	}
	pinned := x509.NewCertPool()
	pinned.AddCert(ca)
	if roots == nil || !roots.Equal(pinned) {
		return identityenrollment.Ticket{}, errors.New("ticket issuer changed the pinned Cluster CA")
	}
	return draft.Complete(identityenrollment.GrantBundle{ClusterID: response.ClusterId, GrantID: response.GrantId,
		IssuerAddress: response.IssuerAddress, ExpiresUnix: response.ExpiresUnix,
		CACertificatePEM: response.CaCertificatePem, CertificatePEM: response.CertificatePem}, time.Time{})
}

// RevokeGrant revokes an unused enrollment grant, not an already enrolled
// Node's certificate or its Raft membership.
func RevokeGrant(ctx context.Context, store, address, grantID string) error {
	if ctx == nil {
		return errors.New("grant revocation requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := identity.ValidateJoinGrantID(grantID); err != nil {
		return err
	}
	id, _, conn, err := openManagementClient(store, address)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	_, err = enrollmentwire.NewEnrollmentServiceClient(conn).RevokeGrant(ctx, &enrollmentwire.RevokeGrantRequest{
		ClusterId: id.ClusterID, FromRaftId: id.RaftID, GrantId: grantID})
	return err
}

func openManagementClient(store, address string) (identity.Identity, *x509.CertPool, *grpc.ClientConn, error) {
	if err := identityenrollment.ValidateNodeAddress(address); err != nil {
		return identity.Identity{}, nil, nil, err
	}
	id, err := identity.Load(store)
	if err != nil {
		return identity.Identity{}, nil, nil, err
	}
	_, clientTLS, err := config.LoadTLS(store, id)
	if err != nil {
		return identity.Identity{}, nil, nil, err
	}
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(tlscredentials.NewTLS(clientTLS)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageSize), grpc.MaxCallSendMsgSize(MaxMessageSize)))
	if err != nil {
		return identity.Identity{}, nil, nil, err
	}
	return id, clientTLS.RootCAs, conn, nil
}
