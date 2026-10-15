package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
)

func TestTenantTokenAdministrativeLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store := filepath.Join(t.TempDir(), "node")
	if err := runInit([]string{"--store", store}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	process, err := openServer([]string{"--store", store, "--listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	_, ready := startJoinTestServer(t, process)
	ready()
	command, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: "register-token-tenant", Type: rootstate.RegisterTenant,
		Tenant: &rootstate.Tenant{TenantID: "token-tenant"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Runtime.Host.Submit(raft.RootGroupID, command); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	endpoint := process.LocalAddress()
	var output, diagnostics bytes.Buffer
	args := []string{"tenant", "token", "create", "--store", store, "--server", endpoint,
		"--request-id", "issue-token-once", "token-tenant"}
	if code := Run(ctx, args, &output, &diagnostics); code != 0 {
		t.Fatal("issue Token", code, diagnostics.String())
	}
	var issued datawire.CreateTenantTokenResponse
	if err := json.Unmarshal(output.Bytes(), &issued); err != nil || issued.Token == "" || issued.CredentialId == "" {
		t.Fatal("issued Token response is incomplete", err)
	}
	if bytes.Contains(diagnostics.Bytes(), []byte(issued.Token)) {
		t.Fatal("Token leaked to diagnostics")
	}
	verifier := auth.Verifier{Barrier: process.Runtime.Host, Catalog: process.Runtime.Root}
	principal, err := verifier.Verify(ctx, issued.Token)
	if err != nil || principal.TenantID() != "token-tenant" {
		t.Fatal("issued Token was not authorized", err)
	}
	output.Reset()
	diagnostics.Reset()
	if code := Run(ctx, args, &output, &diagnostics); code == 0 || bytes.Contains(output.Bytes(), []byte(issued.Token)) {
		t.Fatal("Token plaintext was reissued for an existing RequestID", code)
	}
	if credential, found, err := process.Runtime.Root.TenantCredential(issued.CredentialId); err != nil || !found ||
		credential.RevokedIndex != 0 {
		t.Fatal("issued credential missing from Root", found, err)
	}
	output.Reset()
	diagnostics.Reset()
	revoke := []string{"tenant", "token", "revoke", "--store", store, "--server", endpoint,
		"--request-id", "revoke-token-once", issued.CredentialId}
	if code := Run(ctx, revoke, &output, &diagnostics); code != 0 {
		t.Fatal("revoke Token", code, diagnostics.String())
	}
	if _, err := verifier.Verify(ctx, issued.Token); !errors.Is(err, auth.ErrInvalidCredential) {
		t.Fatal("revoked Token remained authorized", err)
	}
	output.Reset()
	diagnostics.Reset()
	if code := Run(ctx, revoke, &output, &diagnostics); code != 0 {
		t.Fatal("same RequestID did not idempotently recover revocation", code, diagnostics.String())
	}
}

func TestManagedServerCanBindSeparateTenantHTTPS(t *testing.T) {
	store := filepath.Join(t.TempDir(), "node")
	if err := runInit([]string{"--store", store}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	process, err := openServer([]string{"--store", store, "--listen", "127.0.0.1:0",
		"--api-listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if process.PublicAddress() == "" || process.PublicAddress() == process.LocalAddress() {
		t.Fatal("Tenant HTTPS did not bind separately from Node RPC")
	}
	if err := process.Close(); err != nil {
		t.Fatal(err)
	}
}
