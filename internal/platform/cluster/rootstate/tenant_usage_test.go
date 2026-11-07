package rootstate

import (
	"context"
	"strings"
	"testing"
)

func TestAsynchronousUsageBatchReplayReorderingAndSnapshot(t *testing.T) {
	s := rootFileFixture(t)
	applyRootFileCommand(t, s, Command{Type: RegisterTenant, Tenant: &Tenant{TenantID: "soft", Modules: []string{"config", "session"}, Accounting: true, StorageLimit: 10}})
	reports := []UsageReport{}
	for _, module := range []string{"config", "session"} {
		r := RangeSeed{TenantID: "soft", ModuleID: module, EngineID: "state", RangeID: module, OwnerGroupID: "data"}
		applyRootFileCommand(t, s, Command{Type: PrepareRange, InitialRange: &r})
		applyRootFileCommand(t, s, Command{Type: ActivateRange, Activation: &RangeActivation{RangeID: module, OwnerGroupID: "data", Epoch: 1, OwnerRequestID: "activate-" + module, OwnerAppliedIndex: 1}})
		reports = append(reports, UsageReport{Space: r.Keyspace(), Key: []byte("same"), RequestID: "write", RangeID: module, OwnerGroupID: "data", Epoch: 1, Index: 2, Bytes: 8})
	}
	apply := func(rs ...UsageReport) {
		t.Helper()
		applyRootFileCommand(t, s, Command{Type: ReportTenantUsage, UsageReports: rs})
	}
	check := func(want uint64) {
		t.Helper()
		u, err := s.TenantUsage("soft")
		if err != nil || u.Used != want || u.Reserved != 0 {
			t.Fatalf("usage=%+v want=%d err=%v", u, want, err)
		}
	}
	// Existing completed accounting is replaced by the absolute report,
	// without scanning or migrating business values.
	prior := QuotaTicket{ID: QuotaID(reports[0].Space, "previous"), TenantID: "soft", ModuleID: "config", EngineID: "state", Key: []byte("same"), RequestID: "previous", Version: strings.Repeat("a", 64), Bytes: 4, CreatedUnix: 1}
	if e := quotaApply(t, s, ReserveTenantQuota, prior, false); e != "" {
		t.Fatal(e)
	}
	if e := quotaApply(t, s, FinishTenantQuota, prior, true); e != "" {
		t.Fatal(e)
	}
	apply(reports...)
	check(16) // Accepted writes are counted even over the soft limit.
	apply(reports...)
	check(16) // Different reporters / lost ACK replay do not double count.
	newer := reports[0]
	newer.Index = 4
	newer.Bytes = 3
	newer.RequestID = "shrink"
	apply(newer)
	apply(reports[0])
	check(11)
	stale := newer
	stale.OwnerGroupID = "empty"
	stale.Index = 100
	stale.Bytes = 1
	apply(stale)
	check(11)
	newer.Index = 5
	newer.Bytes = 0
	newer.RequestID = "delete"
	apply(newer)
	check(8)
	store := rootArtifacts(t, t.TempDir())
	image, index, conf := buildRootFile(t, s, store)
	restored := rootFileState(t, t.TempDir())
	if err := restored.RestoreFrom(context.Background(), store, index, image, conf); err != nil {
		t.Fatal(err)
	}
	s = restored
	apply(reports[0])
	check(8)
}
