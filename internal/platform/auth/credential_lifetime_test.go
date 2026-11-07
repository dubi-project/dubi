package auth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"strings"
	"testing"
	"time"
)

func TestShortCredentialIDsNeverExpiryAndLegacyTokens(t *testing.T) {
	now := time.Unix(1000, 0)
	root, e := rootstate.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	applyAuthCommand(t, root, rootstate.Command{RequestID: "tenant", Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: "t"}})
	token, c, e := Generate("t", now, 0)
	if e != nil || c.ExpiresUnix != 0 || !strings.HasPrefix(c.CredentialID, "credential_") || len(c.CredentialID) != 23 {
		t.Fatal(c, e)
	}
	id, e := tokenID(token)
	if e != nil || id != c.CredentialID {
		t.Fatal(id, e)
	}
	parts := strings.Split(token, ".")
	secret, e := base64.RawURLEncoding.DecodeString(parts[2])
	if e != nil || len(secret) != 32 {
		t.Fatal("secret entropy changed")
	}
	applyAuthCommand(t, root, rootstate.Command{RequestID: "create", Type: rootstate.CreateTenantCredential, TenantCredential: &c})
	verify := func(token string) {
		t.Helper()
		_, e := (Verifier{Barrier: fixedBarrier{index: root.AppliedIndex()}, Catalog: root, Now: func() time.Time { return now.AddDate(20, 0, 0) }}).Verify(context.Background(), token)
		if e != nil {
			t.Fatal(e)
		}
	}
	verify(token)
	// Tokens already issued with a 128-bit ID remain usable without rewriting secrets.
	legacy := "dubi." + strings.Repeat("a", 32) + "." + base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	digest := sha256.Sum256([]byte(legacy))
	old := c
	old.CredentialID = "cred_" + strings.Repeat("a", 32)
	old.TokenSHA256 = hex.EncodeToString(digest[:])
	applyAuthCommand(t, root, rootstate.Command{RequestID: "legacy", Type: rootstate.CreateTenantCredential, TenantCredential: &old})
	verify(legacy)
	access, a, e := GenerateAccess("admin", "", now, 0)
	if e != nil {
		t.Fatal(e)
	}
	applyAuthCommand(t, root, rootstate.Command{RequestID: "admin", Type: rootstate.CreateAccessCredential, AccessCredential: &a})
	_, e = (AccessVerifier{Barrier: fixedBarrier{index: root.AppliedIndex()}, Catalog: root, Now: func() time.Time { return now.AddDate(20, 0, 0) }}).Verify(context.Background(), access)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = GenerateAccess("session", a.CredentialID, now, 0); e == nil {
		t.Fatal("browser session cannot be permanent")
	}
	applyAuthCommand(t, root, rootstate.Command{RequestID: "revoke", Type: rootstate.RevokeTenantCredential, TenantRevocation: &rootstate.TenantCredentialRevocation{CredentialID: c.CredentialID}})
	applyAuthCommand(t, root, rootstate.Command{RequestID: "delete", Type: rootstate.DeleteTenantCredential, TenantRevocation: &rootstate.TenantCredentialRevocation{CredentialID: c.CredentialID}})
	if _, e = (Verifier{Barrier: fixedBarrier{index: root.AppliedIndex()}, Catalog: root}).Verify(context.Background(), token); e == nil {
		t.Fatal("deleted token authenticated")
	}
}
