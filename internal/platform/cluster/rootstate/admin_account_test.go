package rootstate

import (
	"context"
	"strings"
	"testing"
)

func TestAdminAccountSnapshotAndSessionVersion(t *testing.T) {
	s := rootFileFixture(t)
	a := AdminAccount{ID: "account_123456abcdef", Username: "admin", Name: "管理员", PasswordSalt: strings.Repeat("a", 32), PasswordHash: strings.Repeat("b", 64), PasswordScheme: "argon2id-v19-m65536-t3-p1", CreatedUnix: 100}
	save := func(request string, a AdminAccount, revision uint64) Result {
		return applyBackupCommand(t, s, request, Command{Type: PutAdminAccount, AdminAccountUpdate: &AdminAccountUpdate{Account: a, ExpectedRevision: revision}})
	}
	if r := save("create", a, 0); r.Error != "" {
		t.Fatal(r)
	}
	a, _, _ = s.AdminAccount(a.ID)
	session := AccessCredential{CredentialID: "cred_" + strings.Repeat("c", 32), Role: "session", AccountID: a.ID, AccountVersion: a.AuthVersion, CreatedUnix: 100, ExpiresUnix: 1000, TokenSHA256: strings.Repeat("d", 64)}
	if r := applyBackupCommand(t, s, "session", Command{Type: CreateAccessCredential, AccessCredential: &session}); r.Error != "" {
		t.Fatal(r)
	}
	prior := a.AuthVersion
	a.Name = "另一个名字"
	if r := save("rename", a, a.Revision); r.Error != "" {
		t.Fatal(r)
	}
	a, _, _ = s.AdminAccount(a.ID)
	if a.AuthVersion != prior {
		t.Fatal("rename invalidated sessions")
	}
	store := rootArtifacts(t, t.TempDir())
	encoded, index, conf := buildRootFile(t, s, store)
	restored := rootFileState(t, t.TempDir())
	if e := restored.RestoreFrom(context.Background(), store, index, encoded, conf); e != nil {
		t.Fatal(e)
	}
	got, ok, e := restored.AdminAccount(a.ID)
	if e != nil || !ok || got.Name != a.Name {
		t.Fatal(got, e)
	}
	if _, ok, e = restored.AccessCredential(session.CredentialID); e != nil || !ok {
		t.Fatal("snapshot lost session", e)
	}
	a.PasswordHash = strings.Repeat("e", 64)
	if r := save("password", a, a.Revision); r.Error != "" {
		t.Fatal(r)
	}
	if _, ok, e = s.AccessCredential(session.CredentialID); e != nil || ok {
		t.Fatal("old session retained", e)
	}
	if r := applyBackupCommand(t, s, "stale-session", Command{Type: CreateAccessCredential, AccessCredential: &session}); r.Error == "" {
		t.Fatal("stale login was admitted")
	}
}

func TestDeleteAdminAccountRemovesSessionsAndUsername(t *testing.T) {
	s := rootFileFixture(t)
	save := func(id, username string) AdminAccount {
		t.Helper()
		a := AdminAccount{ID: id, Username: username, Name: username, PasswordSalt: strings.Repeat("a", 32), PasswordHash: strings.Repeat("b", 64), PasswordScheme: "argon2id-v19-m65536-t3-p1", CreatedUnix: 100}
		r := applyBackupCommand(t, s, "create-"+id, Command{Type: PutAdminAccount, AdminAccountUpdate: &AdminAccountUpdate{Account: a}})
		if r.Error != "" {
			t.Fatal(r)
		}
		a, _, _ = s.AdminAccount(id)
		return a
	}
	a := save("account_111111111111", "first")
	b := save("account_222222222222", "second")
	session := AccessCredential{CredentialID: "credential_abcdef123456", Role: "session", AccountID: a.ID, AccountVersion: a.AuthVersion, CreatedUnix: 100, ExpiresUnix: 1000, TokenSHA256: strings.Repeat("d", 64)}
	if r := applyBackupCommand(t, s, "session-delete", Command{Type: CreateAccessCredential, AccessCredential: &session}); r.Error != "" {
		t.Fatal(r)
	}
	remove := func(request string, a AdminAccount, revision uint64) Result {
		return applyBackupCommand(t, s, request, Command{Type: DeleteAdminAccount, AdminAccountDelete: &AdminAccountDelete{ID: a.ID, ExpectedRevision: revision}})
	}
	if r := remove("stale-delete", a, a.Revision+1); r.Error == "" {
		t.Fatal("stale revision deleted")
	}
	if r := remove("delete-a", a, a.Revision); r.Error != "" {
		t.Fatal(r)
	}
	if _, ok, e := s.AdminAccount(a.ID); ok || e != nil {
		t.Fatal("account retained", e)
	}
	if _, ok, e := s.AccessCredential(session.CredentialID); ok || e != nil {
		t.Fatal("session retained", e)
	}
	if r := remove("delete-again", a, a.Revision); r.Error != "" {
		t.Fatal(r)
	}
	if r := remove("delete-last", b, b.Revision); r.Error == "" {
		t.Fatal("deleted last enabled admin")
	}
	replacement := save("account_333333333333", "first")
	if replacement.ID == a.ID {
		t.Fatal("identity reused")
	}
	if r := applyBackupCommand(t, s, "old-session", Command{Type: CreateAccessCredential, AccessCredential: &session}); r.Error == "" {
		t.Fatal("deleted identity returned")
	}
	store := rootArtifacts(t, t.TempDir())
	encoded, index, conf := buildRootFile(t, s, store)
	restored := rootFileState(t, t.TempDir())
	if e := restored.RestoreFrom(context.Background(), store, index, encoded, conf); e != nil {
		t.Fatal("dangling account references", e)
	}
}
