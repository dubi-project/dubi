package rootstate

import (
	"context"
	"fmt"
	"testing"
)

func TestDiagnosticRetentionRetryAndSnapshot(t *testing.T) {
	s := rootFileFixture(t)
	makeRecord := func(id string) DiagnosticRecord {
		return DiagnosticRecord{ID: id, CreatedUnix: 100, Nodes: []DiagnosticNode{{NodeID: "node_00000000000000000000000000000001", RegionID: "west", Checks: []DiagnosticCheck{{Kind: "reachability", State: "passed"}, {Kind: "readiness", State: "passed"}, {Kind: "volumes", State: "unknown"}, {Kind: "maintenance", State: "warning"}}}}}
	}
	for i := 0; i < 52; i++ {
		id := fmt.Sprintf("check-%03d", i)
		d := makeRecord(id)
		r := applyBackupCommand(t, s, id, Command{Type: SaveDiagnostic, Diagnostic: &d})
		if r.Error != "" {
			t.Fatal(r)
		}
	}
	all, err := s.Diagnostics()
	if err != nil || len(all) != 50 || all[0].ID != "check-051" || all[49].ID != "check-002" {
		t.Fatal("retention/order", len(all), err)
	}
	d := makeRecord("check-051")
	applyBackupCommand(t, s, d.ID, Command{Type: SaveDiagnostic, Diagnostic: &d})
	again, _ := s.Diagnostics()
	if len(again) != 50 || again[0].CreatedIndex != all[0].CreatedIndex {
		t.Fatal("retry duplicated")
	}
	store := rootArtifacts(t, t.TempDir())
	encoded, index, conf := buildRootFile(t, s, store)
	restored := rootFileState(t, t.TempDir())
	if err := restored.RestoreFrom(context.Background(), store, index, encoded, conf); err != nil {
		t.Fatal(err)
	}
	records, err := restored.Diagnostics()
	if err != nil || len(records) != 50 || records[0].ID != all[0].ID {
		t.Fatal("snapshot lost diagnostic", err)
	}
	d.Nodes[0].Checks[0].Kind = "reachability|readiness"
	if d.Validate() == nil {
		t.Fatal("unknown check accepted")
	}
}
