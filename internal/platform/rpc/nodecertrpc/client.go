package nodecertrpc

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/rpc/nodecertwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RequestRenewal uses the existing managed identity and pinned CA. It performs
// no local writes; the Store-owning runtime must publish the validated result
// through InstallRenewedLeaf. Cancellation or network failure leaves PKI intact.
func RequestRenewal(ctx context.Context, store, address string) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("Node renewal context required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := enrollment.ValidateIssuerAddress(address); err != nil {
		return nil, err
	}
	id, err := identity.Load(store)
	if err != nil {
		return nil, err
	}
	material, err := nodepki.Load(store, id, nodepki.DefaultServerName, time.Time{})
	if err != nil {
		return nil, err
	}
	_, configuration, err := config.LoadTLS(store, id)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(material.CACertificate)
	if configuration.RootCAs == nil || !configuration.RootCAs.Equal(pool) {
		return nil, errors.New("Node renewal trust root changed during preparation")
	}
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(tlscredentials.NewTLS(configuration)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageSize), grpc.MaxCallSendMsgSize(MaxMessageSize)))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	client := nodecertwire.NewNodeCertificateServiceClient(conn)
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		callCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		response, err := client.RenewNode(callCtx, &nodecertwire.RenewNodeRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID})
		stop()
		if err == nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return validateResponse(material, id, response, time.Now().UTC())
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			return nil, err
		}
		last = err
		if attempt < 2 {
			timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return nil, last
}

func validateResponse(current nodepki.Material, id identity.Identity, response *nodecertwire.RenewNodeResponse, now time.Time) ([]byte, error) {
	if response == nil || response.ClusterId != id.ClusterID || response.NodeId != id.NodeID || response.RaftId != id.RaftID || len(response.CertificatePem) > MaxMessageSize {
		return nil, errors.New("Node renewal response does not match managed identity")
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: current.CACertificate.Raw})
	if err := nodepki.ValidateEnrollmentRetry(current, id, nodepki.DefaultServerName, caPEM, response.CertificatePem, now); err != nil {
		return nil, err
	}
	block, _ := pem.Decode(response.CertificatePem)
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(leaf.Raw, current.Certificate.Raw) && (!leaf.NotAfter.After(current.Certificate.NotAfter) || leaf.NotBefore.Before(current.Certificate.NotBefore)) {
		return nil, errors.New("Node renewal response rolls back or does not extend validity")
	}
	return bytes.Clone(response.CertificatePem), nil
}
