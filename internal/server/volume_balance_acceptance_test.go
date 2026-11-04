package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

// Opt in only with two disposable, independently mounted test filesystems.
// The test never fills or faults an existing Node's Volume or filesystem.
func TestAutomaticVolumeBalanceOnIndependentFilesystems(t *testing.T) {
	sourceMount, targetMount := os.Getenv("DUBI_TEST_BALANCE_SOURCE"), os.Getenv("DUBI_TEST_BALANCE_TARGET")
	if sourceMount == "" || targetMount == "" {
		t.Skip("requires two disposable filesystems with a deliberate capacity difference")
	}
	source, err := os.MkdirTemp(sourceMount, "dubi-source-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(source)
	target, err := os.MkdirTemp(targetMount, "dubi-target-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(target)
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	registry, err := volume.Initialize(root, []string{source})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	pool, err := pack.OpenPool(registry)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := records.OpenNodeStore(pool, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	payload := make([]byte, 8<<20)
	descriptor, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "state", DatasetID: "items"}, "region-test", []byte("balance"), payload)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := store.Put(ctx, descriptor, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Add(target); err != nil {
		t.Fatal(err)
	}
	before, err := registry.Status()
	if err != nil {
		t.Fatal(err)
	}
	if before.Volumes[0].Device == before.Volumes[1].Device {
		t.Fatal("acceptance inputs share a filesystem")
	}
	plan, needed := volume.PlanBalance(before)
	if !needed {
		t.Fatal("acceptance filesystems have no meaningful capacity gap", before)
	}
	if plan.Source != before.Volumes[0].ID || plan.Target != before.Volumes[1].ID {
		t.Fatal("capacity planner picked the wrong direction", plan)
	}
	scheduler := resource.NewMaintenance(0)
	worker := &volumeMigrationWorker{runtime: &node.Runtime{Records: store, Packs: pool, Volumes: registry}, io: scheduler}
	for range 4 {
		attempt, finish, err := scheduler.TryStart(ctx, "volume-balance")
		if err != nil {
			t.Fatal(err)
		}
		err = worker.step(attempt)
		finish()
		if err != nil {
			t.Fatal(err)
		}
		status, err := store.VolumeMigration()
		if err != nil {
			t.Fatal(err)
		}
		if !status.Active {
			break
		}
	}
	status, err := store.VolumeMigration()
	if err != nil || status.Active {
		t.Fatal("automatic balancing did not finish", status, err)
	}
	image, err := store.SnapshotIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var index records.IndexSnapshot
	if err := json.Unmarshal(image, &index); err != nil || len(index.Entries) != 1 || index.Entries[0].Location.VolumeID != plan.Target {
		t.Fatal("automatic worker did not publish verified target location", index, err)
	}
	if _, err := store.Verify(ctx, descriptor); err != nil {
		t.Fatal("balanced bytes are unreadable", err)
	}
	after, err := registry.Status()
	if err != nil {
		t.Fatal(err)
	}
	if after.Volumes[0].State != volume.Active || after.Volumes[0].Capacity.AvailableBytes <= before.Volumes[0].Capacity.AvailableBytes {
		t.Fatal("source space was not reclaimed while keeping Volume Active", before, after)
	}
}

func TestVolumeWorkerContinuesSmallRecordMigrationPages(t *testing.T) {
	root := t.TempDir()
	if _, err := identity.Bootstrap(root); err != nil {
		t.Fatal(err)
	}
	registry, err := volume.Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	pool, err := pack.OpenPool(registry)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := records.OpenNodeStore(pool, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var descriptors []records.Descriptor
	for i := 0; i < 12; i++ {
		d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "state", DatasetID: "items"}, "region-test", []byte(fmt.Sprintf("small-%d", i)), []byte("body"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Put(context.Background(), d, []byte("body")); err != nil {
			t.Fatal(err)
		}
		descriptors = append(descriptors, d)
	}
	report, err := registry.Status()
	if err != nil {
		t.Fatal(err)
	}
	source := report.Volumes[0].ID
	if _, err := registry.Add(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := pool.DrainVolume(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if err := store.BeginVolumeMigration(source); err != nil {
		t.Fatal(err)
	}
	worker := &volumeMigrationWorker{runtime: &node.Runtime{Records: store, Packs: pool, Volumes: registry}, io: resource.NewMaintenance(0)}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); worker.run(ctx) }()
	defer func() { cancel(); <-done }()
	for {
		status, err := store.VolumeMigration()
		if err != nil {
			t.Fatal(err)
		}
		if !status.Active {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("small records waited an idle interval between every page", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	for _, d := range descriptors {
		if _, err := store.Verify(context.Background(), d); err != nil {
			t.Fatal("continued migration lost data", err)
		}
	}
}
