package rootstate

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func quotaApply(t *testing.T, s *State, kind string, q QuotaTicket, applied bool) string {
	t.Helper()
	id := fmt.Sprintf("quota-test-%d", s.AppliedIndex()+1)
	b, err := EncodeCommand(Command{Version: FormatVersion, RequestID: id, Type: kind, Quota: &QuotaMutation{Ticket: q, Applied: applied}})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Apply(s.AppliedIndex()+1, b); err != nil {
		t.Fatal(err)
	}
	r, ok, err := s.Result(id)
	if err != nil || !ok {
		t.Fatal(err)
	}
	return r.Error
}
func quotaTestTicket(key, request string, n uint64) QuotaTicket {
	space := Keyspace{TenantID: "quota", ModuleID: "config", EngineID: "state"}
	return QuotaTicket{ID: QuotaID(space, request), TenantID: space.TenantID, ModuleID: space.ModuleID, EngineID: space.EngineID, Key: []byte(key), RequestID: request, Version: strings.Repeat("a", 64), Bytes: n, CreatedUnix: 100}
}
func TestTenantQuotaAdmissionOverwriteDeleteAndRestore(t *testing.T) {
	s := rootFileFixture(t)
	applyRootFileCommand(t, s, Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: "quota", Name: "租户", Modules: []string{"config"}, StorageLimit: 100, Accounting: true}})
	a := quotaTestTicket("a", "a1", 60)
	if err := quotaApply(t, s, ReserveTenantQuota, a, false); err != "" {
		t.Fatal(err)
	}
	if err := quotaApply(t, s, ReserveTenantQuota, a, false); err != "" {
		t.Fatal("replay", err)
	}
	if u, _ := s.TenantUsage("quota"); u != (TenantUsage{Reserved: 60}) {
		t.Fatal(u)
	}
	b := quotaTestTicket("b", "b1", 50)
	if err := quotaApply(t, s, ReserveTenantQuota, b, false); err != "Tenant quota exceeded" {
		t.Fatal(err)
	}
	if err := quotaApply(t, s, CloseTenantQuota, a, false); err != "" {
		t.Fatal(err)
	}
	if u, _ := s.TenantUsage("quota"); u.Reserved != 60 {
		t.Fatal("close released unresolved capacity", u)
	}
	// It may have applied before cancellation reached its Owner.
	if err := quotaApply(t, s, FinishTenantQuota, a, true); err != "" {
		t.Fatal(err)
	}
	a2 := quotaTestTicket("a", "a2", 90)
	if err := quotaApply(t, s, ReserveTenantQuota, a2, false); err != "" {
		t.Fatal(err)
	}
	if u, _ := s.TenantUsage("quota"); u != (TenantUsage{Used: 60, Reserved: 30}) {
		t.Fatal(u)
	}
	a3 := quotaTestTicket("a", "a3", 1)
	if err := quotaApply(t, s, ReserveTenantQuota, a3, false); err != "Tenant quota key is busy" {
		t.Fatal(err)
	}
	// Restore an in-flight reservation through a real Raft snapshot.
	store := rootArtifacts(t, t.TempDir())
	image, index, conf := buildRootFile(t, s, store)
	restored := rootFileState(t, t.TempDir())
	if err := restored.RestoreFrom(context.Background(), store, index, image, conf); err != nil {
		t.Fatal(err)
	}
	if u, _ := restored.TenantUsage("quota"); u != (TenantUsage{Used: 60, Reserved: 30}) {
		t.Fatal("restore lost reservation", u)
	}
	if err := quotaApply(t, restored, FinishTenantQuota, a2, false); err != "" {
		t.Fatal(err)
	}
	current, _, _ := restored.Tenant("quota")
	current.StorageLimit = 10
	applyRootFileCommand(t, restored, Command{Type: UpdateTenant, Tenant: &current})
	// Shrink and deletion are allowed even below current usage.
	if err := quotaApply(t, restored, ReserveTenantQuota, a3, false); err != "" {
		t.Fatal(err)
	}
	if err := quotaApply(t, restored, FinishTenantQuota, a3, true); err != "" {
		t.Fatal(err)
	}
	del := quotaTestTicket("a", "delete", 0)
	if err := quotaApply(t, restored, ReserveTenantQuota, del, false); err != "" {
		t.Fatal(err)
	}
	if err := quotaApply(t, restored, FinishTenantQuota, del, true); err != "" {
		t.Fatal(err)
	}
	if u, _ := restored.TenantUsage("quota"); u != (TenantUsage{}) {
		t.Fatal("delete did not free quota", u)
	}
	if err := quotaApply(t, restored, FinishTenantQuota, del, true); err != "" {
		t.Fatal("finish replay", err)
	}
}
func TestTenantSettingsCASAndQuotaModuleIsolation(t *testing.T) {
	s := rootFileFixture(t)
	applyRootFileCommand(t, s, Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: "quota", Name: "配置", Modules: []string{"config"}, Accounting: true}})
	q := quotaTestTicket("a", "one", 5)
	q.ModuleID = "other"
	q.ID = QuotaID(q.Space(), q.RequestID)
	if err := quotaApply(t, s, ReserveTenantQuota, q, false); err != "Tenant module is forbidden" {
		t.Fatal(err)
	}
	old, _, _ := s.Tenant("quota")
	changed := old
	changed.Name = "新名称"
	changed.Modules = []string{}
	applyRootFileCommand(t, s, Command{Type: UpdateTenant, Tenant: &changed})
	b, err := EncodeCommand(Command{Version: FormatVersion, Type: UpdateTenant, RequestID: "stale-tenant", Tenant: &old})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Apply(s.AppliedIndex()+1, b); err != nil {
		t.Fatal(err)
	}
	result, _, _ := s.Result("stale-tenant")
	if result.Error != "Tenant revision conflict" {
		t.Fatal(result)
	}
	updated, _, _ := s.Tenant("quota")
	if updated.Name != "新名称" || updated.AllowsModule("config") {
		t.Fatal(updated)
	}
}

func TestQuotaSharedAcrossModulesAndEngineKeys(t *testing.T) {
	s := rootFileFixture(t)
	applyRootFileCommand(t, s, Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: "quota", Modules: []string{"config", "session"}, StorageLimit: 10, Accounting: true}})
	a := quotaTestTicket("same-key", "a", 7)
	if err := quotaApply(t, s, ReserveTenantQuota, a, false); err != "" {
		t.Fatal(err)
	}
	b := quotaTestTicket("same-key", "b", 4)
	b.ModuleID = "session"
	b.ID = QuotaID(b.Space(), b.RequestID)
	if err := quotaApply(t, s, ReserveTenantQuota, b, false); err != "Tenant quota exceeded" {
		t.Fatal("module got independent budget", err)
	}
	b.Bytes = 3
	if err := quotaApply(t, s, ReserveTenantQuota, b, false); err != "" {
		t.Fatal(err)
	}
	if u, _ := s.TenantUsage("quota"); u.Reserved != 10 {
		t.Fatal(u)
	}
}
