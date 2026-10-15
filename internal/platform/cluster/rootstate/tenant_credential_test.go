package rootstate

import (
	"context"
	"strings"
	"testing"
)

func TestTenantCredentialDurableIssueRevokeAndSnapshot(t *testing.T) {
	state := rootFileFixture(t)
	value := TenantCredential{CredentialID: "cred_0123456789abcdef0123456789abcdef", TenantID: "tenant",
		TokenSHA256: strings.Repeat("a", 64), CreatedUnix: 100, ExpiresUnix: 200}
	applyRootFileCommand(t, state, Command{Type: CreateTenantCredential, TenantCredential: &value})
	read, found, err := state.TenantCredential(value.CredentialID)
	if err != nil || !found || read != value {
		t.Fatal("created credential not durable", read, found, err)
	}
	applyRootFileCommand(t, state, Command{Type: RevokeTenantCredential,
		TenantRevocation: &TenantCredentialRevocation{CredentialID: value.CredentialID}})
	read, found, err = state.TenantCredential(value.CredentialID)
	if err != nil || !found || read.RevokedIndex != state.AppliedIndex() {
		t.Fatal("revocation not durable", read, found, err)
	}
	store := rootArtifacts(t, t.TempDir())
	encoded, index, conf := buildRootFile(t, state, store)
	restored := rootFileState(t, t.TempDir())
	if err := restored.RestoreFrom(context.Background(), store, index, encoded, conf); err != nil {
		t.Fatal("restore credential snapshot", err)
	}
	read, found, err = restored.TenantCredential(value.CredentialID)
	if err != nil || !found || read.RevokedIndex != state.AppliedIndex() || read.TokenSHA256 != value.TokenSHA256 {
		t.Fatal("revoked credential lost across snapshot", read, found, err)
	}
}

func TestTenantCredentialRejectsInvalidOrUnknownOwner(t *testing.T) {
	state := rootFileState(t, t.TempDir())
	value := TenantCredential{CredentialID: "cred_0123456789abcdef0123456789abcdef", TenantID: "missing",
		TokenSHA256: strings.Repeat("b", 64), CreatedUnix: 100, ExpiresUnix: 200}
	encoded, err := EncodeCommand(Command{Version: FormatVersion, RequestID: "credential-missing-tenant",
		Type: CreateTenantCredential, TenantCredential: &value})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Apply(1, encoded); err != nil {
		t.Fatal(err)
	}
	result, found, err := state.Result("credential-missing-tenant")
	if err != nil || !found || result.Error == "" {
		t.Fatal("credential without Tenant accepted", result, found, err)
	}
	if _, found, err := state.TenantCredential(value.CredentialID); err != nil || found {
		t.Fatal("rejected credential became visible", found, err)
	}
	for _, invalid := range []TenantCredential{
		{CredentialID: "cred_0123456789abcdef0123456789abcdef", TenantID: "tenant", TokenSHA256: "secret", CreatedUnix: 1, ExpiresUnix: 2},
		{CredentialID: "cred_0123456789abcdef0123456789abcdef", TenantID: "tenant", TokenSHA256: strings.Repeat("a", 64), CreatedUnix: 1, ExpiresUnix: 1},
		{CredentialID: "cred_wrong", TenantID: "tenant", TokenSHA256: strings.Repeat("a", 64), CreatedUnix: 1, ExpiresUnix: 2},
	} {
		if _, err := EncodeCommand(Command{Version: FormatVersion, RequestID: "invalid", Type: CreateTenantCredential,
			TenantCredential: &invalid}); err == nil {
			t.Fatal("invalid credential was encodable", invalid)
		}
	}
}
