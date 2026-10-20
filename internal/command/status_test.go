package command

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

func TestRootStatusRequiresConfirmedIndexNotOnlyLeaderHint(t *testing.T) {
	if err := validateRootReadCut(&raftwire.GroupReadIndexResponse{LeaderRaftId: 42}); err == nil {
		t.Fatal("Root leader hint without a confirmed read cut was accepted")
	}
	if err := validateRootReadCut(&raftwire.GroupReadIndexResponse{LeaderRaftId: 42, ConfirmedIndex: 99}); err != nil {
		t.Fatal("confirmed Root read cut was rejected", err)
	}
}

func TestStatusQueriesLiveRootWithManagedNodeIdentity(t *testing.T) {
	type statusResult struct {
		ObservedNodeID    string           `json:"observed_node_id"`
		Root              rootjoin.View    `json:"root"`
		Health            localHealthView  `json:"health"`
		LocalStorage      *volume.Report   `json:"local_storage"`
		LocalStorageError string           `json:"local_storage_error"`
		LocalRepair       *localRepairView `json:"local_repair"`
		LocalRepairError  string           `json:"local_repair_error"`
	}
	store := filepath.Join(t.TempDir(), "node")
	if code := Run(context.Background(), []string{"init", "--store", store}, io.Discard, io.Discard); code != 0 {
		t.Fatal("init failed", code)
	}
	process, err := openServer([]string{"--store", store, "--listen", "127.0.0.1:0", "--region", "west"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	done := make(chan error, 1)
	go func() { done <- process.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	select {
	case <-process.Ready():
	case <-ctx.Done():
		t.Fatal("managed server did not become ready")
	}
	id := process.Runtime.Identity
	node, found, err := process.Runtime.Root.Node(id.RaftID)
	if err != nil || !found {
		t.Fatal("local Node is not registered", err)
	}
	var output, diagnostics bytes.Buffer
	if code := Run(ctx, []string{"status", "--store", store, "--server", node.Address}, &output, &diagnostics); code != 0 {
		t.Fatalf("status failed: %s", diagnostics.String())
	}
	var result statusResult
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.ObservedNodeID != id.NodeID ||
		result.Root.ClusterID != id.ClusterID || result.Root.AppliedIndex == 0 || !result.Root.Contains(id.RaftID) ||
		result.Health.Scope != "local" ||
		len(result.Root.Peers) != 1 || result.Root.Peers[0].RegionID != "west" ||
		result.LocalStorage == nil || result.LocalStorage.NodeID != id.NodeID ||
		len(result.LocalStorage.Volumes) != 1 || result.LocalStorage.Volumes[0].Health != "healthy" ||
		result.LocalStorageError != "" || result.LocalRepair == nil ||
		!result.LocalRepair.WorkerEnabled || result.LocalRepairError != "" {
		t.Fatal("status did not report the live confirmed Root view", result, err)
	}
	volumePath, offlinePath := filepath.Join(store, "volume"), filepath.Join(store, "volume.offline")
	if err := os.Rename(volumePath, offlinePath); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Rename(offlinePath, volumePath); err != nil {
			t.Error("restore test Volume:", err)
		}
	}()
	output.Reset()
	diagnostics.Reset()
	if code := Run(ctx, []string{"status", "--store", store, "--server", node.Address}, &output, &diagnostics); code != 0 {
		t.Fatalf("status hid live Root view after local Volume loss: %s", diagnostics.String())
	}
	result = statusResult{}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.Root.AppliedIndex == 0 ||
		result.LocalStorage == nil || len(result.LocalStorage.Volumes) != 1 ||
		result.LocalStorage.Volumes[0].Health != "missing" || result.LocalStorageError != "" || result.Health.State != "degraded" {
		t.Fatal("status did not expose the missing local Volume", result, err)
	}
	manifestPath, manifestOffline := filepath.Join(store, "DUBI_VOLUMES"), filepath.Join(store, "DUBI_VOLUMES.offline")
	if err := os.Rename(manifestPath, manifestOffline); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Rename(manifestOffline, manifestPath); err != nil {
			t.Error("restore test Volume manifest:", err)
		}
	}()
	output.Reset()
	diagnostics.Reset()
	if code := Run(ctx, []string{"status", "--store", store, "--server", node.Address}, &output, &diagnostics); code != 0 {
		t.Fatalf("status lost live Root view after local registry error: %s", diagnostics.String())
	}
	result = statusResult{}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.Root.AppliedIndex == 0 ||
		result.LocalStorage != nil || result.LocalStorageError == "" {
		t.Fatal("status hid the local registry error or lost Root view", result, err)
	}
	foreign := filepath.Join(t.TempDir(), "foreign")
	if code := Run(context.Background(), []string{"init", "--store", foreign}, io.Discard, io.Discard); code != 0 {
		t.Fatal("second init failed", code)
	}
	output.Reset()
	diagnostics.Reset()
	if code := Run(ctx, []string{"status", "--store", foreign, "--server", node.Address}, &output, &diagnostics); code == 0 || output.Len() != 0 {
		t.Fatal("foreign Cluster identity was allowed to read live status")
	}
}

func TestStatusRejectsInvalidInputBeforeNetwork(t *testing.T) {
	for _, args := range [][]string{{"status", "--store", ""}, {"status", "--server", "0.0.0.0:7443"},
		{"status", "extra"}, {"status", "--server", "bad:port"}} {
		var output, diagnostics bytes.Buffer
		if code := Run(context.Background(), args, &output, &diagnostics); code == 0 || output.Len() != 0 {
			t.Fatal("invalid status input accepted", args)
		}
	}
}

func TestDeriveLocalHealthNeverClaimsHealthyWithoutLocalObservations(t *testing.T) {
	if got := deriveLocalHealth(nil, "manifest unavailable", nil, ""); got.State != "degraded" || len(got.Reasons) < 1 {
		t.Fatal("storage failure was not surfaced", got)
	}
	if got := deriveLocalHealth(&volume.Report{Volumes: []volume.Status{{Record: volume.Record{ID: "v1"}, Health: "missing"}}}, "", &localRepairView{WorkerEnabled: true, LastCheckUnix: 10}, ""); got.State != "degraded" || len(got.Reasons) != 1 {
		t.Fatal("missing Volume was not surfaced", got)
	}
	if got := deriveLocalHealth(&volume.Report{Volumes: []volume.Status{{Record: volume.Record{ID: "v1"}, Health: "healthy"}}}, "", &localRepairView{WorkerEnabled: true, LastCheckUnix: 10}, ""); got.State != "healthy" || len(got.Reasons) != 0 {
		t.Fatal("clean local observations were not marked healthy", got)
	}
	if got := deriveLocalHealth(&volume.Report{Volumes: []volume.Status{{Record: volume.Record{ID: "v1", State: volume.Removed}, Health: "removed"}}}, "", &localRepairView{WorkerEnabled: true, LastCheckUnix: 10}, ""); got.State != "healthy" || len(got.Reasons) != 0 {
		t.Fatal("removed Volume tombstone was treated as a storage failure", got)
	}
	if got := deriveLocalHealth(&volume.Report{Volumes: []volume.Status{{Record: volume.Record{ID: "v1"}, Health: "healthy"}}}, "", &localRepairView{WorkerEnabled: true, TotalIssues: 1, LastCheckUnix: 10}, ""); got.State != "degraded" || len(got.Reasons) != 1 {
		t.Fatal("repair debt was not surfaced", got)
	}
	if got := deriveLocalHealth(&volume.Report{Volumes: []volume.Status{{Record: volume.Record{ID: "v1"}, Health: "healthy"}}}, "", &localRepairView{WorkerEnabled: true, LastCheckUnix: 10, GeoRecordDebt: 1, VolumeMigration: &volumeMigrationView{Active: true}}, ""); got.State != "degraded" || len(got.Reasons) != 2 {
		t.Fatal("maintenance and Geo debt were not surfaced", got)
	}
}

func TestDeriveClusterHealthShowsRootMajorityAndRegions(t *testing.T) {
	view := rootjoin.View{Voters: []uint64{1, 2, 3}, Peers: []rootstate.Node{
		{RaftID: 1, RegionID: "west"}, {RaftID: 2, RegionID: "west"}, {RaftID: 3, RegionID: "east"},
	}}
	local := localHealthView{Scope: "local", State: "healthy"}
	got := deriveClusterHealth(view, local, true)
	if got.State != "available" || !got.RootQuorumConfirmed || got.RootVoters != 3 || got.RootQuorum != 2 || len(got.Regions) != 2 {
		t.Fatal("cluster majority/regions were not summarized", got)
	}
	local.State = "degraded"
	local.Reasons = []string{"volume missing"}
	got = deriveClusterHealth(view, local, true)
	if got.State != "degraded" || len(got.Reasons) != 1 || got.Reasons[0] != "volume missing" {
		t.Fatal("local degradation was not carried into cluster status", got)
	}
}

func TestApplyPeerRepairHealthSurfacesUnreachableAndDebt(t *testing.T) {
	base := clusterHealthView{State: "available", RootQuorumConfirmed: true}
	got := applyPeerRepairHealth(base, []peerRepairView{
		{RaftID: 1, NodeID: "node-a", Reachable: true, RepairDebt: 2},
		{RaftID: 2, NodeID: "node-b", Reachable: false, Error: "timeout"},
	})
	if got.State != "degraded" || len(got.Repairs) != 2 || len(got.Reasons) != 2 {
		t.Fatalf("peer repair observations were not reflected: %+v", got)
	}
}
