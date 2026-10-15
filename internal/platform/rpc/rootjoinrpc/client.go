package rootjoinrpc

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Join uses installed managed Node TLS and an immutable enrollment receipt.
// It never mutates Store, bootstraps Root, starts Raft or promotes a learner.
// The caller must durably publish a local join phase/peer cache before opening
// a joining runtime. Seeds are optional previously confirmed addresses; the
// receipt issuer is always a fallback. Temporary ticket expiry is irrelevant.
func Join(ctx context.Context, store string, seeds []string) (*rootjoinwire.JoinRootResponse, error) {
	if ctx == nil {
		return nil, errors.New("Root join context required")
	}
	if len(seeds) > MaxPeers {
		return nil, errors.New("too many Root join seeds")
	}
	id, err := identity.Load(store)
	if err != nil {
		return nil, err
	}
	receipt, err := identityenrollment.LoadReceipt(store)
	if err != nil {
		return nil, err
	}
	_, clientTLS, err := config.LoadTLS(store, id)
	if err != nil {
		return nil, err
	}
	ca, err := nodepki.ValidateClusterCA(id.ClusterID, []byte(receipt.Bundle.CACertificatePEM), time.Time{})
	if err != nil {
		return nil, err
	}
	pinned := x509.NewCertPool()
	pinned.AddCert(ca)
	if clientTLS.RootCAs == nil || !clientTLS.RootCAs.Equal(pinned) {
		return nil, errors.New("managed Node CA differs from enrollment receipt")
	}
	if len(clientTLS.Certificates) != 1 || len(clientTLS.Certificates[0].Certificate) == 0 {
		return nil, errors.New("managed Node certificate required")
	}
	leaf, err := x509.ParseCertificate(clientTLS.Certificates[0].Certificate[0])
	if err != nil {
		return nil, err
	}
	key, err := identityenrollment.RequestPublicKey(receipt.Request)
	if err != nil || !bytes.Equal(leaf.RawSubjectPublicKeyInfo, key) || receipt.Bundle.ClusterID != id.ClusterID ||
		receipt.Request.NodeID != id.NodeID || receipt.Request.RaftID != id.RaftID {
		return nil, errors.New("receipt does not match installed Node identity and key")
	}
	encoded, err := identityenrollment.EncodeRequest(receipt.Request)
	if err != nil {
		return nil, err
	}
	if len(encoded) > maxRequestDocument {
		return nil, status.Error(codes.ResourceExhausted, "Root join document exceeds limit")
	}
	request := &rootjoinwire.JoinRootRequest{ClusterId: id.ClusterID, GrantId: receipt.GrantID, RequestDocument: encoded}
	queue := append(append([]string(nil), seeds...), receipt.IssuerAddress)
	for _, address := range queue {
		if err := identityenrollment.ValidateIssuerAddress(address); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	seen := make(map[string]int)
	var lastErr error
	attempts := 0
	for len(queue) > 0 && attempts < 16 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		address := queue[0]
		queue = queue[1:]
		key := endpointKey(address)
		if seen[key] >= 8 {
			continue
		}
		seen[key]++
		if seen[key] > 1 {
			delay := min(100*time.Millisecond*time.Duration(1<<min(seen[key]-2, 4)), time.Second)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
		attempts++
		conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(tlscredentials.NewTLS(clientTLS)),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxResponseSize), grpc.MaxCallSendMsgSize(MaxRequestSize)))
		if err != nil {
			lastErr = err
			continue
		}
		attemptCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		response, callErr := rootjoinwire.NewRootJoinServiceClient(conn).JoinRoot(attemptCtx, request)
		stop()
		closeErr := conn.Close()
		if callErr != nil {
			switch status.Code(callErr) {
			case codes.Unavailable, codes.DeadlineExceeded:
				lastErr = callErr
				// Try other known routes first, then revisit this endpoint after
				// bounded backoff. Leader/joint transitions must not require a
				// new receipt. Count EVERY RPC against the global 16-call limit.
				if seen[key] < 8 && len(queue) < MaxPeers+1 {
					queue = append(queue, address)
				}
			default:
				return nil, callErr
			}
			continue
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if err := ValidateView(response.GetView(), id.ClusterID); err != nil {
			return nil, err
		}
		if response.AdmittedIndex != 0 {
			if response.AdmittedIndex > response.View.AppliedIndex || !viewContains(response.View, id.RaftID) {
				return nil, errors.New("Root admission response lacks confirmed membership")
			}
			for _, peer := range response.View.Peers {
				if peer.RaftId == id.RaftID && (peer.NodeId != id.NodeID || peer.RegionId != receipt.Request.RegionID || peer.Address != receipt.Request.Address) {
					return nil, errors.New("Root admission response changed enrolled Node metadata")
				}
			}
			return response, nil
		}
		var learned []string
		for _, peer := range response.View.Peers {
			if peer.RaftId == response.View.LeaderId {
				learned = append(learned, peer.Address)
			}
		}
		for _, peer := range response.View.Peers {
			if peer.RaftId != response.View.LeaderId {
				learned = append(learned, peer.Address)
			}
		}
		// Replace with the latest confirmed cut instead of appending one full
		// 4096-peer list per redirect. Per-endpoint and total RPC limits also
		// bound redirects that cycle through already visited endpoints.
		queue = append(learned, queue...)
		if len(queue) > MaxPeers+1 {
			queue = queue[:MaxPeers+1]
		}
		lastErr = status.Error(codes.Unavailable, "Root leader redirected admission; retry the same receipt")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lastErr == nil {
		lastErr = status.Error(codes.Unavailable, "Root admission was not confirmed")
	}
	return nil, lastErr
}
