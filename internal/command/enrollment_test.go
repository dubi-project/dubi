package command

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestJoinCredentialCLIRejectsInvalidInputWithoutCreatingStore(t *testing.T) {
	store := filepath.Join(t.TempDir(), "absent")
	t.Setenv("DUBI_STORE", store)
	for _, args := range [][]string{{"join", "token", "--ttl", "0"}, {"join", "token", "--ttl", "-1s"},
		{"join", "token", "--ttl", "25h"}, {"join", "token", "--store", ""},
		{"join", "token", "--server", "0.0.0.0:7443"}, {"join", "revoke"}, {"join", "revoke", "--grant", "invalid"}} {
		var output, diagnostics bytes.Buffer
		if code := Run(context.Background(), args, &output, &diagnostics); code == 0 || output.Len() != 0 {
			t.Fatal("invalid credential command accepted")
		}
		if _, err := os.Stat(store); !os.IsNotExist(err) {
			t.Fatal("invalid credential input created Store", err)
		}
	}
}

func TestManagedCLIRegistersNetworkAndServesExistingEnrollmentAuthority(t *testing.T) {
	store := filepath.Join(t.TempDir(), "node")
	if code := Run(context.Background(), []string{"init", "--store", store}, io.Discard, io.Discard); code != 0 {
		t.Fatal("init", code)
	}
	process, err := openServer([]string{"--store", store, "--listen", "127.0.0.1:0", "--region", "west"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan error, 1)
	go func() { done <- process.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	select {
	case <-process.Ready():
	case <-ctx.Done():
		t.Fatal("managed CLI startup not ready")
	}
	id := process.Runtime.Identity
	node, found, err := process.Runtime.Root.Node(id.RaftID)
	if err != nil || !found || node.RegionID != "west" {
		t.Fatal(node, found, err)
	}
	_, clientTLS, err := config.LoadTLS(store, id)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient("passthrough:///"+node.Address, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	draft, err := identityenrollment.NewGrantDraft(0, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	r := draft.Request()
	response, err := enrollmentwire.NewEnrollmentServiceClient(conn).CreateGrant(ctx, &enrollmentwire.CreateGrantRequest{
		ClusterId: id.ClusterID, FromRaftId: id.RaftID, GrantId: r.GrantID,
		SecretSha256: r.SecretSHA256, PublicKeyDer: r.PublicKeyDER, ExpiresUnix: r.ExpiresUnix})
	if err != nil || response.GetIssuerAddress() != node.Address {
		t.Fatal("managed CLI did not serve its registered authority", response, err)
	}
	if _, err := draft.Complete(identityenrollment.GrantBundle{ClusterID: response.ClusterId, GrantID: response.GrantId,
		IssuerAddress: response.IssuerAddress, ExpiresUnix: response.ExpiresUnix,
		CACertificatePEM: response.CaCertificatePem, CertificatePEM: response.CertificatePem}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	var output, diagnostics bytes.Buffer
	if code := Run(ctx, []string{"join", "token", "--store", store, "--server", node.Address}, &output, &diagnostics); code != 0 {
		t.Fatal("credential CLI failed", code) // Never print potentially secret output.
	}
	ticket, err := identityenrollment.DecodeTicket(strings.TrimSpace(output.String()), time.Time{})
	if err != nil || ticket.Address() != node.Address {
		t.Fatal("CLI output is not a valid ticket", err)
	}
	if strings.Contains(diagnostics.String(), "djt1.") || strings.Contains(diagnostics.String(), "PRIVATE KEY") || !strings.Contains(diagnostics.String(), ticket.GrantID()) {
		t.Fatal("credential diagnostics leaked secret or omitted revocation handle")
	}
	output.Reset()
	diagnostics.Reset()
	if code := Run(ctx, []string{"join", "revoke", "--store", store, "--server", node.Address, "--grant", ticket.GrantID()}, &output, &diagnostics); code != 0 {
		t.Fatal("revoke CLI failed", code)
	}
	grant, found, err := process.Runtime.Root.JoinGrant(ticket.GrantID())
	if err != nil || !found || grant.State != rootstate.JoinGrantRevoked {
		t.Fatal("CLI revoke did not commit", found, err)
	}
}
