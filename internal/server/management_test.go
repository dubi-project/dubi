package server

import (
	"github.com/dubi-project/dubi/internal/platform/management"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"testing"
	"time"
)

func TestManagementObservationUsesRealCapacityAndUnknownProgress(t *testing.T) {
	m := management.Member{NodeID: "n", RaftID: 1}
	r := &datawire.LocalRepairStatusResponse{NodeId: "n", RaftId: 1, ObservedUnix: time.Now().Unix(), VolumeDetailsSupported: true, TotalVolumes: 2, CapacitySupported: true, FilesystemTotalBytes: 100, FilesystemAvailableBytes: 60, WritableBytes: 40,
		Volumes:       []*datawire.VolumeHealthStatus{{VolumeId: "a", Path: "/a", State: "active", Health: "healthy", CapacityKnown: true, FilesystemTotalBytes: 100, FilesystemAvailableBytes: 60}, {VolumeId: "b", Path: "/b", State: "active", Health: "missing"}},
		WorkerEnabled: true, ScanProgressSupported: true, LastCheckUnix: time.Now().Unix(), TotalScanGroups: 1, ScanProgress: []*datawire.RepairScanProgress{{Phase: "scan", Scanned: 10, Unresolved: 2, NeedsCleanSweep: true}}}
	o, err := managementObservation(m, r)
	if err != nil {
		t.Fatal(err)
	}
	if o.Capacity.FilesystemTotalBytes != 100 || o.Volumes[1].Capacity != nil || o.Health.Status != "degraded" {
		t.Fatalf("capacity %+v", o)
	}
	if o.Maintenance[0].Total != nil || o.Maintenance[0].Pending == nil || *o.Maintenance[0].Pending != 2 || o.Maintenance[1].Supported || o.Maintenance[1].Phase != "unknown" {
		t.Fatalf("fabricated progress %+v", o.Maintenance)
	}
	r.FilesystemTotalBytes = 0
	r.FilesystemAvailableBytes = 0
	r.Volumes[0].Health = "missing"
	r.Volumes[0].CapacityKnown = false
	missing, err := managementObservation(m, r)
	if err != nil || missing.Capacity != nil || missing.Complete {
		t.Fatalf("missing filesystems fabricated capacity: %+v %v", missing, err)
	}
	r.TotalVolumes = 3
	if _, err = managementObservation(m, r); err == nil {
		t.Fatal("incomplete volume directory accepted")
	}
	r.TotalVolumes = 2
	r.NodeId = "wrong"
	if _, err = managementObservation(m, r); err == nil {
		t.Fatal("wrong responder accepted")
	}
}
