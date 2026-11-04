package command

import (
	"bytes"
	"strings"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"google.golang.org/protobuf/proto"
)

func TestRepairScanProgressWireAndHealth(t *testing.T) {
	response := &datawire.LocalRepairStatusResponse{NodeId: "node-1", RaftId: 1, WorkerEnabled: true,
		ScanProgressSupported: true, TotalScanGroups: 1, ScanProgress: []*datawire.RepairScanProgress{{
			GroupId: "regional-west", Phase: "waiting_owner", Scanned: 80, NeedsCleanSweep: true,
			StartedUnix: 100, LastProgressUnix: 200, LastCleanUnix: 50}}}
	encoded, err := proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &datawire.LocalRepairStatusResponse{}
	if err := proto.Unmarshal(encoded, decoded); err != nil {
		t.Fatal(err)
	}
	peer := decodePeerRepairStatus(rootstate.Node{NodeID: "node-1", RaftID: 1}, decoded)
	if !peer.Reachable || !peer.ScanProgressSupported || len(peer.ScanProgress) != 1 || peer.ScanProgress[0].Scanned != 80 || peer.ScanProgress[0].Phase != "waiting_owner" {
		t.Fatalf("wire progress was lost: %+v", peer)
	}
	// A missing issue summary must not turn an explicit debt latch healthy.
	health := applyPeerRepairHealth(clusterHealthView{State: "available"}, []peerRepairView{peer})
	if health.State != "degraded" {
		t.Fatal("known scan debt reported available", health)
	}
	local := deriveLocalHealth(&volume.Report{}, "", &localRepairView{WorkerEnabled: true, LastCheckUnix: 200, ScanProgress: peer.ScanProgress}, "")
	if local.State != "degraded" {
		t.Fatal("known local scan debt reported healthy", local)
	}
}

func TestRepairScanProgressRejectsMalformedAndDistinguishesUnsupported(t *testing.T) {
	progress, err := decodeScanProgress(&datawire.LocalRepairStatusResponse{})
	if err != nil || len(progress) != 0 {
		t.Fatal("older server must remain readable without inventing progress", progress, err)
	}
	for name, response := range map[string]*datawire.LocalRepairStatusResponse{
		"too_many":       {ScanProgressSupported: true, TotalScanGroups: 65, ScanProgress: make([]*datawire.RepairScanProgress, 65)},
		"unadvertised":   {TotalScanGroups: 1},
		"bad_total":      {ScanProgressSupported: true, ScanProgress: []*datawire.RepairScanProgress{{GroupId: "g", Phase: "idle"}}},
		"nil_entry":      {ScanProgressSupported: true, TotalScanGroups: 1, ScanProgress: []*datawire.RepairScanProgress{nil}},
		"bad_phase":      {ScanProgressSupported: true, TotalScanGroups: 1, ScanProgress: []*datawire.RepairScanProgress{{GroupId: "g", Phase: "healthy"}}},
		"negative_time":  {ScanProgressSupported: true, TotalScanGroups: 1, ScanProgress: []*datawire.RepairScanProgress{{GroupId: "g", Phase: "idle", LastCleanUnix: -1}}},
		"unlatched_debt": {ScanProgressSupported: true, TotalScanGroups: 1, ScanProgress: []*datawire.RepairScanProgress{{GroupId: "g", Phase: "idle", Unresolved: 1}}},
		"duplicate":      {ScanProgressSupported: true, TotalScanGroups: 2, ScanProgress: []*datawire.RepairScanProgress{{GroupId: "g", Phase: "idle"}, {GroupId: "g", Phase: "idle"}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeScanProgress(response); err == nil {
				t.Fatal("accepted malformed progress")
			}
		})
	}
	items := make([]*datawire.RepairScanProgress, 0, 64)
	for i := 0; i < 64; i++ {
		items = append(items, &datawire.RepairScanProgress{GroupId: "group-" + strings.Repeat("a", i+1), Phase: "pending"})
	}
	got, err := decodeScanProgress(&datawire.LocalRepairStatusResponse{ScanProgressSupported: true, TotalScanGroups: 65, ScanProgress: items})
	if err != nil || len(got) != 64 {
		t.Fatal("valid bounded truncation rejected", err)
	}
}

func TestDoctorRepairProgressShowsStageAndNeverClaimsUnseenCompletion(t *testing.T) {
	var out bytes.Buffer
	repair := &localRepairView{ScanProgressSupported: true, TotalScanGroups: 2, ScanProgress: []repairScanView{{
		GroupID: "regional-west", Phase: "waiting_root", Scanned: 80, NeedsCleanSweep: true, LastProgressUnix: 200}}}
	if err := writeRepairScanProgress(&out, repair); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"waiting_root", "scanned=80", "needs-clean-sweep=true", "last-progress=1970-01-01T00:03:20Z", "last-clean=never", "showing 1 of 2"} {
		if !strings.Contains(out.String(), text) {
			t.Fatal("missing actionable progress", text, out.String())
		}
	}
	out.Reset()
	if err := writeRepairScanProgress(&out, &localRepairView{}); err != nil || !strings.Contains(out.String(), "unavailable") {
		t.Fatal("older server looked fully scanned", out.String(), err)
	}
	out.Reset()
	if err := writeRepairScanProgress(&out, &localRepairView{ScanProgressSupported: true}); err != nil || !strings.Contains(out.String(), "no eligible local Groups") {
		t.Fatal(out.String(), err)
	}
}

func TestCapacityBalanceIsRoutineMaintenanceUntilItFails(t *testing.T) {
	repair := &localRepairView{WorkerEnabled: true, LastCheckUnix: 100, VolumeMigration: &volumeMigrationView{
		Active: true, SourceVolumeID: "source", TargetVolumeID: "target", SourcePackID: "pack"}}
	health := deriveLocalHealth(&volume.Report{}, "", repair, "")
	if health.State != "healthy" {
		t.Fatal("routine balancing falsely degraded healthy storage", health)
	}
	repair.VolumeMigration.LastError = "target disk is full"
	if health := deriveLocalHealth(&volume.Report{}, "", repair, ""); health.State != "degraded" {
		t.Fatal("failed balancing was hidden", health)
	}
}
