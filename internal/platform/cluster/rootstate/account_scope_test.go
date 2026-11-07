package rootstate

import (
	"context"
	"strings"
	"testing"
)

func TestAccountScopeLastAdminAndRoleRevocation(t *testing.T) {
	s := rootFileFixture(t)
	for _, tenant := range []string{"scope-a", "scope-b"} {
		if r := applyBackupCommand(t, s, "tenant-"+tenant, Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: tenant, Modules: []string{"config"}}}); r.Error != "" {
			t.Fatal(r)
		}
	}
	makeAccount := func(id, username, tenant, role string) AdminAccount {
		return AdminAccount{ID: "account_" + id, Username: username, Name: username, TenantID: tenant, Role: role, PasswordSalt: strings.Repeat("a", 32), PasswordHash: strings.Repeat("b", 64), PasswordScheme: "argon2id-v19-m65536-t3-p1", CreatedUnix: 100}
	}
	save := func(request string, a AdminAccount) Result {
		return applyBackupCommand(t, s, request, Command{Type: PutAdminAccount, AdminAccountUpdate: &AdminAccountUpdate{Account: a, ExpectedRevision: a.Revision}})
	}
	system := makeAccount("111111111111", "system", "", "admin")
	a := makeAccount("222222222222", "scope.admin", "scope-a", "admin")
	b := makeAccount("333333333333", "other.admin", "scope-b", "admin")
	for _, v := range []AdminAccount{system, a, b} {
		if r := save("save-"+v.ID, v); r.Error != "" {
			t.Fatal(r)
		}
	}
	system, _, _ = s.AdminAccount(system.ID)
	a, _, _ = s.AdminAccount(a.ID)
	b, _, _ = s.AdminAccount(b.ID)
	for _, v := range []AdminAccount{system, a, b} {
		v.Disabled = true
		if r := save("disable-"+v.ID, v); r.Error == "" {
			t.Fatal("other scope counted as admin", v)
		}
	}
	moved := a
	moved.TenantID = "scope-b"
	if r := save("move", moved); r.Error == "" {
		t.Fatal("scope moved")
	}
	duplicate := makeAccount("444444444444", a.Username, "scope-b", "admin")
	if r := save("duplicate", duplicate); r.Error == "" {
		t.Fatal("username not globally unique")
	}
	second := makeAccount("555555555555", "scope.second", "scope-a", "admin")
	if r := save("second", second); r.Error != "" {
		t.Fatal(r)
	}
	session := AccessCredential{CredentialID: "credential_abcdef123456", Role: "session", AccountID: a.ID, AccountVersion: a.AuthVersion, CreatedUnix: 100, ExpiresUnix: 1000, TokenSHA256: strings.Repeat("c", 64)}
	if r := applyBackupCommand(t, s, "session", Command{Type: CreateAccessCredential, AccessCredential: &session}); r.Error != "" {
		t.Fatal(r)
	}
	a.Role = "readonly"
	if r := save("demote", a); r.Error != "" {
		t.Fatal(r)
	}
	if _, ok, e := s.AccessCredential(session.CredentialID); e != nil || ok {
		t.Fatal("demotion retained session", e)
	}
	a, _, _ = s.AdminAccount(a.ID)
	if r := applyBackupCommand(t, s, "remove-member", Command{Type: DeleteAdminAccount, AdminAccountDelete: &AdminAccountDelete{ID: a.ID, ExpectedRevision: a.Revision}}); r.Error != "" {
		t.Fatal(r)
	}
	second, _, _ = s.AdminAccount(second.ID)
	if r := applyBackupCommand(t, s, "remove-last", Command{Type: DeleteAdminAccount, AdminAccountDelete: &AdminAccountDelete{ID: second.ID, ExpectedRevision: second.Revision}}); r.Error == "" {
		t.Fatal("removed last tenant admin")
	}
	store := rootArtifacts(t, t.TempDir())
	encoded, index, conf := buildRootFile(t, s, store)
	restored := rootFileState(t, t.TempDir())
	if e := restored.RestoreFrom(context.Background(), store, index, encoded, conf); e != nil {
		t.Fatal(e)
	}
	got, ok, e := restored.AdminAccount(second.ID)
	if e != nil || !ok || got.TenantID != "scope-a" || got.EffectiveRole() != "admin" {
		t.Fatal(got, e)
	}
}
