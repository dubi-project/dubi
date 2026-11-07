package rootstate

import (
	"context"
	"strings"
	"testing"
)

func TestRevokedCredentialDeletionAndPermanentParentSnapshot(t *testing.T) {
	s := rootFileFixture(t)
	apply := func(id string, c Command) Result { return applyBackupCommand(t, s, id, c) }
	a := AccessCredential{CredentialID: "credential_001122334455", Role: "admin", TokenSHA256: strings.Repeat("a", 64), CreatedUnix: 100, ExpiresUnix: 0}
	if r := apply("admin", Command{Type: CreateAccessCredential, AccessCredential: &a}); r.Error != "" {
		t.Fatal(r)
	}
	child := AccessCredential{CredentialID: "credential_001122334466", Role: "session", ParentID: a.CredentialID, TokenSHA256: strings.Repeat("b", 64), CreatedUnix: 100, ExpiresUnix: 1000}
	if r := apply("session", Command{Type: CreateAccessCredential, AccessCredential: &child}); r.Error != "" {
		t.Fatal(r)
	}
	store := rootArtifacts(t, t.TempDir())
	encoded, index, conf := buildRootFile(t, s, store)
	restored := rootFileState(t, t.TempDir())
	if e := restored.RestoreFrom(context.Background(), store, index, encoded, conf); e != nil {
		t.Fatal(e)
	}
	rev := &TenantCredentialRevocation{CredentialID: a.CredentialID}
	if r := apply("active-delete", Command{Type: DeleteAccessCredential, AccessRevocation: rev}); r.Error == "" {
		t.Fatal("deleted active credential")
	}
	apply("revoke", Command{Type: RevokeAccessCredential, AccessRevocation: rev})
	if r := apply("delete", Command{Type: DeleteAccessCredential, AccessRevocation: rev}); r.Error != "" {
		t.Fatal(r)
	}
	for _, id := range []string{a.CredentialID, child.CredentialID} {
		if _, ok, e := s.AccessCredential(id); e != nil || ok {
			t.Fatal("deleted credential or child remains", e)
		}
	}
	if r := apply("delete-again", Command{Type: DeleteAccessCredential, AccessRevocation: rev}); r.Error != "" {
		t.Fatal(r)
	}
	apply("tenant", Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: "never"}})
	c := TenantCredential{CredentialID: "credential_aabbccddeeff", TenantID: "never", TokenSHA256: strings.Repeat("c", 64), CreatedUnix: 100, ExpiresUnix: 0}
	apply("issue-tenant", Command{Type: CreateTenantCredential, TenantCredential: &c})
	collision := a
	collision.CredentialID = c.CredentialID
	if r := apply("collision", Command{Type: CreateAccessCredential, AccessCredential: &collision}); r.Error == "" {
		t.Fatal("cross-kind ID collision accepted")
	}
	tr := &TenantCredentialRevocation{CredentialID: c.CredentialID}
	if r := apply("active-tenant-delete", Command{Type: DeleteTenantCredential, TenantRevocation: tr}); r.Error == "" {
		t.Fatal("active tenant credential deleted")
	}
	apply("revoke-tenant", Command{Type: RevokeTenantCredential, TenantRevocation: tr})
	apply("delete-tenant", Command{Type: DeleteTenantCredential, TenantRevocation: tr})
	if _, ok, e := s.TenantCredential(c.CredentialID); e != nil || ok {
		t.Fatal("tenant credential remains", e)
	}
	store = rootArtifacts(t, t.TempDir())
	encoded, index, conf = buildRootFile(t, s, store)
	restored = rootFileState(t, t.TempDir())
	if e := restored.RestoreFrom(context.Background(), store, index, encoded, conf); e != nil {
		t.Fatal("deletion left invalid snapshot references", e)
	}
}
