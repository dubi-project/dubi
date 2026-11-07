package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"net/url"
	"testing"
	"time"
)

func TestMonitorRPCRequiresCurrentAuthenticatedMember(t *testing.T) {
	id, err := identity.Bootstrap(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	s := &dataService{runtime: &node.Runtime{Identity: id, Root: root}}
	req := &datawire.MonitorRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID, EndUnix: time.Now().Unix(), Seconds: 900}
	if _, err = s.ObserveMonitor(context.Background(), req); status.Code(err) != codes.PermissionDenied {
		t.Fatal("anonymous accepted", err)
	}
	uri, err := identity.NodeIdentityURI(id)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: id.NodeID}, URIs: []*url.URL{uri}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}}})
	if _, err = s.ObserveMonitor(ctx, req); status.Code(err) != codes.PermissionDenied {
		t.Fatal("nonmember accepted", err)
	}
	req.FromRaftId++
	if _, err = s.ObserveMonitor(ctx, req); status.Code(err) != codes.PermissionDenied {
		t.Fatal("spoofed sender accepted", err)
	}
	req.Seconds = 1
	if _, err = s.ObserveMonitor(ctx, req); status.Code(err) != codes.InvalidArgument {
		t.Fatal("unbounded query accepted", err)
	}
}
