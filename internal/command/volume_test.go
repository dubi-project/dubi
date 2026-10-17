package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

func volumeCommand(t *testing.T, args ...string) []byte {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if code := Run(context.Background(), args, &stdout, &stderr); code != 0 {
		t.Fatalf("%v: exit=%d stderr=%s", args, code, stderr.String())
	}
	return stdout.Bytes()
}

func TestVolumeCLIExplicitInitAddAndServerRecovery(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	store := t.TempDir()
	t.Setenv("DUBI_STORE", store)
	first, second := t.TempDir(), t.TempDir()
	volumeCommand(t, "init", "--volume", first, "--volume", second)
	volumeCommand(t, "init")
	if _, err := os.Stat(filepath.Join(store, "volume")); !os.IsNotExist(err) {
		t.Fatal("explicit init created default volume")
	}
	var before volume.Report
	if err := json.Unmarshal(volumeCommand(t, "volume", "list"), &before); err != nil {
		t.Fatal(err)
	}
	if len(before.Volumes) != 2 {
		t.Fatalf("volumes: %+v", before)
	}
	process, err := openServer([]string{"--listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	stop, ready := startJoinTestServer(t, process)
	ready()
	address := process.LocalAddress()
	if err := saveLocalContext(store, process.Runtime.Identity, address, address, address); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DUBI_STORE", "")
	// Status is available while the Node owns its Store and volume locks.
	volumeCommand(t, "volume", "list")
	third := t.TempDir()
	var added volume.Record
	if err := json.Unmarshal(volumeCommand(t, "volume", "add", third), &added); err != nil {
		t.Fatal(err)
	}
	if live, err := process.Runtime.Volumes.Status(); err != nil || len(live.Volumes) != 3 || live.Volumes[2].ID != added.ID {
		t.Fatal("running Node did not adopt the new Volume", live, err)
	}
	volumeCommand(t, "volume", "add", third) // Live retry is idempotent.
	stop()
	volumeCommand(t, "volume", "add", third) // Offline retry preserves the same ID.
	process, err = openServer([]string{"--listen", address}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	after, err := process.Runtime.Volumes.Status()
	if err != nil {
		t.Fatal(err)
	}
	if after.StoreID != before.StoreID || len(after.Volumes) != 3 || after.Volumes[0].ID != before.Volumes[0].ID || after.Volumes[2].ID != added.ID {
		t.Fatalf("server lost volume identity: %+v", after)
	}
	if _, err := os.Stat(filepath.Join(store, "volume")); !os.IsNotExist(err) {
		t.Fatal("restart created default volume")
	}
}

func TestVolumeCLIDefaultMissingRemainsObservable(t *testing.T) {
	store := t.TempDir()
	t.Setenv("DUBI_STORE", store)
	volumeCommand(t, "init")
	volumeCommand(t, "volume", "add", t.TempDir())
	defaultPath := filepath.Join(store, "volume")
	if err := os.Rename(defaultPath, defaultPath+"-offline"); err != nil {
		t.Fatal(err)
	}
	process, err := openServer([]string{"--listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	var report volume.Report
	if err := json.Unmarshal(volumeCommand(t, "volume", "list"), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Volumes) != 2 || report.Volumes[0].Health != "missing" || report.Volumes[1].Health != "healthy" {
		t.Fatalf("degraded volumes: %+v", report)
	}
	if _, err := os.Stat(defaultPath); !os.IsNotExist(err) {
		t.Fatal("server recreated missing default")
	}
}

func TestVolumeCLIOnlineAddWritesAfterLostDefault(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	store := t.TempDir()
	t.Setenv("DUBI_STORE", store)
	volumeCommand(t, "init")
	process, err := openServer([]string{"--listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	stop, ready := startJoinTestServer(t, process)
	ready()
	address := process.LocalAddress()
	space := rootstate.Keyspace{TenantID: "online-volume", EngineID: "data-alpha", DatasetID: "shared"}
	first, err := records.Describe(space, "default", []byte("before"), []byte("old-pack"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := process.Runtime.Records.Put(context.Background(), first, []byte("old-pack")); err != nil {
		t.Fatal(err)
	}
	oldPath := filepath.Join(store, "volume")
	if err := os.Rename(oldPath, oldPath+".offline"); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Runtime.Records.Verify(context.Background(), first); !errors.Is(err, records.ErrUnavailable) {
		t.Fatal("lost default Volume remained readable", err)
	}
	var added volume.Record
	if err := json.Unmarshal(volumeCommand(t, "volume", "add", "--server", address, t.TempDir()), &added); err != nil {
		t.Fatal(err)
	}
	second, err := records.Describe(space, "default", []byte("after"), []byte("new-pack"))
	if err != nil {
		t.Fatal(err)
	}
	holding, err := process.Runtime.Records.Put(context.Background(), second, []byte("new-pack"))
	if err != nil || holding.VolumeID != added.ID {
		t.Fatal("live Pack pool did not adopt replacement Volume", holding, err)
	}
	report, err := volume.Inspect(store)
	if err != nil || len(report.Volumes) != 2 || report.Volumes[0].Health != "missing" || report.Volumes[1].Health != "healthy" {
		t.Fatal("live Volume status is wrong", report, err)
	}
	stop()
	reopened, err := openServer([]string{"--listen", address}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if recovered, err := reopened.Runtime.Records.Verify(context.Background(), second); err != nil || recovered.VolumeID != added.ID {
		t.Fatal("online-added Volume did not survive restart", recovered, err)
	}
}

func TestVolumeCLIOnlineAddPinsLocalNode(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	store := t.TempDir()
	volumeCommand(t, "init", "--store", store)
	owner, err := openServer([]string{"--store", store, "--listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	_, ready := startJoinTestServer(t, owner)
	ready()
	otherStore := t.TempDir()
	volumeCommand(t, "init", "--store", otherStore)
	other, err := openServer([]string{"--store", otherStore, "--listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	_, ready = startJoinTestServer(t, other)
	ready()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var stderr bytes.Buffer
	path := t.TempDir()
	if code := Run(ctx, []string{"volume", "add", "--store", store, "--server", other.LocalAddress(), path}, io.Discard, &stderr); code == 0 {
		t.Fatal("different Node accepted a local Volume mutation")
	}
	report, err := volume.Inspect(store)
	if err != nil || len(report.Volumes) != 1 {
		t.Fatal("wrong endpoint changed local registry", report, err)
	}
	if entries, err := os.ReadDir(path); err != nil || len(entries) != 0 {
		t.Fatal("wrong endpoint wrote the candidate Volume", entries, err)
	}
}

func TestVolumeCLICompactionUsesRunningNodeWhenStoreIsOwned(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	store := t.TempDir()
	volumeCommand(t, "init", "--store", store)
	process, err := openServer([]string{"--store", store, "--listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	_, ready := startJoinTestServer(t, process)
	ready()
	defer process.Close()

	var response map[string]any
	encoded := volumeCommand(t, "volume", "compact", "--store", store, "--server", process.LocalAddress())
	if err := json.Unmarshal(encoded, &response); err != nil {
		t.Fatal("invalid compaction response:", err)
	}
	if response == nil {
		t.Fatal("empty compaction response")
	}
}

func TestJoinPrepareRegistersExplicitVolumes(t *testing.T) {
	store := t.TempDir()
	t.Setenv("DUBI_STORE", store)
	path := t.TempDir()
	first := volumeCommand(t, "join", "prepare", "--advertise", "node-b:7443", "--volume", path)
	second := volumeCommand(t, "join", "prepare", "--advertise", "node-b:7443")
	if !bytes.Equal(first, second) {
		t.Fatal("join retry changed request")
	}
	var report volume.Report
	if err := json.Unmarshal(volumeCommand(t, "volume", "list"), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Volumes) != 1 || report.Volumes[0].Health != "healthy" {
		t.Fatalf("join volumes: %+v", report)
	}
	if _, err := os.Stat(filepath.Join(store, "volume")); !os.IsNotExist(err) {
		t.Fatal("join created unwanted default")
	}
}

func TestVolumeCLIDoesNotExposeUnsafeRemoval(t *testing.T) {
	for _, args := range [][]string{{"volume"}, {"volume", "remove", "/data/volume"}, {"volume", "add"}, {"volume", "list", "extra"}} {
		if code := Run(context.Background(), args, io.Discard, io.Discard); code == 0 {
			t.Fatalf("accepted %v", args)
		}
	}
}
