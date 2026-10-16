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

	"github.com/dubi-project/dubi/internal/platform/cluster/enginedata"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"github.com/dubi-project/dubi/internal/server"
)

// Both operator paths retain the Store identity and let the managed worker
// relocate the same published record after the original Volume disappears.
func TestManagedRepairRelocatesLostVolumeAfterAddAndRestart(t *testing.T) {
	testManagedRepairRelocatesLostVolume(t, false)
}

func TestManagedRepairRelocatesLostVolumeAfterOnlineAdd(t *testing.T) {
	testManagedRepairRelocatesLostVolume(t, true)
}

func testManagedRepairRelocatesLostVolume(t *testing.T, online bool) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	mux := referenceTestMux(t)
	stores := []string{filepath.Join(t.TempDir(), "n1"), filepath.Join(t.TempDir(), "n2"), filepath.Join(t.TempDir(), "n3")}
	if err := runInit([]string{"--store", stores[0]}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	first, err := openServerWithMux([]string{"--store", stores[0], "--listen", "127.0.0.1:0"}, io.Discard, mux)
	if err != nil {
		t.Fatal(err)
	}
	_, ready := startJoinTestServer(t, first)
	ready()
	nodes := []*server.Server{first}
	firstNode, found, err := first.Runtime.Root.Node(first.Runtime.Identity.RaftID)
	if err != nil || !found {
		t.Fatal("bootstrap Node is not registered", err)
	}
	var damagedStop func()
	var damagedAddress string
	var damagedID uint64
	var damagedStore = stores[1]
	var damaged = first
	for i := 1; i < 3; i++ {
		_, ticket := joinTestTicket(t, stores[0], firstNode.Address)
		joined, err := openJoiningServerWithMux(ctx, []string{"--store", stores[i],
			"--listen", "127.0.0.1:0", "--ticket-file", ticket}, nil, io.Discard, mux)
		if err != nil {
			t.Fatal(err)
		}
		stop, wait := startJoinTestServer(t, joined)
		wait()
		nodes = append(nodes, joined)
		if i == 1 {
			damaged, damagedStop, damagedID = joined, stop, joined.Runtime.Identity.RaftID
			node, found, err := first.Runtime.Root.Node(damagedID)
			if err != nil || !found {
				t.Fatal("joined Node is not registered", err)
			}
			damagedAddress = node.Address
		}
	}
	group := regiongroups.GroupID("default")
	for {
		progress, found, err := first.Runtime.Root.RegionProgress(group)
		if err == nil && found && progress.Eligible && progress.Pending == nil && len(progress.Current.Nodes) == 3 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("three-Node Region did not form", err, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	space := rootstate.Keyspace{TenantID: "volume-repair", EngineID: "data-alpha", DatasetID: "shared"}
	tenant, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: "volume-repair-tenant", Type: rootstate.RegisterTenant,
		Tenant: &rootstate.Tenant{TenantID: space.TenantID}})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Runtime.Host.Submit(raft.RootGroupID, tenant); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := first.Runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			t.Fatal(err)
		}
		result, found, err := first.Runtime.Root.ResultForCommand(tenant)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			break
		}
	}
	activateEngineTestDataset(t, ctx, first, group, space)
	key, body := []byte("lost-volume-key"), []byte("repair-on-restart-body")
	request := enginedata.WriteRequest{RequestID: "write-before-volume-loss", Keyspace: space,
		Key: key, Body: body, Publish: func(d records.Descriptor) ([]byte, error) { return json.Marshal(d) }}
	for {
		attempt, done := context.WithTimeout(ctx, 10*time.Second)
		result, err := first.EngineData.Write(attempt, request)
		done()
		if err == nil && result.State == enginedata.WriteLocalReady {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("initial immutable write did not become ready", result, err, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	d, err := records.Describe(space, group, key, body)
	if err != nil {
		t.Fatal(err)
	}
	// The writer may reach its majority without placing bytes on every Node.
	// Ensure the chosen target has a physical record to lose.
	if _, err := damaged.Runtime.Records.Verify(ctx, d); errors.Is(err, records.ErrNotHeld) {
		if _, err := damaged.Runtime.Records.Put(ctx, d, body); err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	before, err := damaged.Runtime.Records.Verify(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	// Do not remove a holding before the other two current Nodes also hold
	// verified bytes; otherwise a successful later repair would say nothing
	// about physical majority surviving the outage.
	for {
		allHeld := true
		for _, member := range nodes {
			if _, err := member.Runtime.Records.Verify(ctx, d); errors.Is(err, records.ErrNotHeld) {
				allHeld = false
			} else if err != nil {
				t.Fatal("verify current cohort before loss", err)
			}
		}
		if allHeld {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("background repair did not fill the current cohort before loss", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	conf, err := first.Runtime.Host.Membership(group)
	if err != nil || len(conf.GetVoters()) != 3 {
		t.Fatal("Owner Group has no stable three-voter membership before loss", conf, err)
	}
	if !online {
		damagedStop()
	}
	oldPath := filepath.Join(damagedStore, "volume")
	if err := os.Rename(oldPath, oldPath+".offline"); err != nil {
		t.Fatal(err)
	}
	readCtx, readDone := context.WithTimeout(ctx, 45*time.Second)
	survivingRead, readErr := first.EngineData.Read(readCtx, space, key)
	readDone()
	if readErr != nil || !bytes.Equal(survivingRead.Body, body) {
		for _, member := range nodes {
			if member == damaged && !online {
				continue
			}
			status, statusErr := member.Runtime.Host.Status(group)
			t.Logf("surviving Owner %x: status=%+v err=%v", member.Runtime.Identity.RaftID, status, statusErr)
		}
		t.Fatal("healthy physical majority could not read after one Node lost its Volume", readErr)
	}
	writeCtx, writeDone := context.WithTimeout(ctx, 45*time.Second)
	degraded := enginedata.WriteRequest{RequestID: "write-during-volume-outage", Keyspace: space,
		Key: []byte("new-key-during-outage"), Body: []byte("surviving-majority-write"),
		Publish: func(d records.Descriptor) ([]byte, error) { return json.Marshal(d) }}
	degradedResult, writeErr := first.EngineData.Write(writeCtx, degraded)
	writeDone()
	if writeErr != nil || degradedResult.State != enginedata.WriteLocalReady {
		t.Fatal("healthy majority could not publish a new record during Volume outage", degradedResult, writeErr)
	}
	if online {
		// The damaged Node still accepts traffic. Its local Store fails, so the
		// any-Node entry must route the same write to a healthy Owner member.
		fromDamaged := degraded
		fromDamaged.RequestID = "write-through-damaged-node"
		fromDamaged.Key = []byte("new-key-through-damaged-node")
		writeCtx, writeDone = context.WithTimeout(ctx, 45*time.Second)
		forwarded, forwardErr := damaged.EngineData.Write(writeCtx, fromDamaged)
		writeDone()
		if forwardErr != nil || forwarded.State != enginedata.WriteLocalReady {
			t.Fatal("damaged Node could not route write to healthy physical majority", forwarded, forwardErr)
		}
	}
	newPath := t.TempDir()
	var added volume.Record
	addArgs := []string{"volume", "add", "--store", damagedStore}
	if online {
		addArgs = append(addArgs, "--server", damagedAddress)
	}
	addArgs = append(addArgs, newPath)
	if err := json.Unmarshal(volumeCommand(t, addArgs...), &added); err != nil {
		t.Fatal(err)
	}
	if added.ID == before.VolumeID {
		t.Fatal("replacement reused lost Volume identity")
	}
	removeArgs := []string{"volume", "remove", "--store", damagedStore}
	if online {
		removeArgs = append(removeArgs, "--server", damagedAddress)
	}
	removeArgs = append(removeArgs, before.VolumeID)
	var removed volume.Record
	if err := json.Unmarshal(volumeCommand(t, removeArgs...), &removed); err != nil || removed.ID != before.VolumeID || removed.State != volume.Removed {
		t.Fatal("explicit removal of failed Volume did not persist", removed, err)
	}
	repairNode, repairStop := damaged, damagedStop
	if !online {
		restarted, err := openServerWithMux([]string{"--store", damagedStore, "--listen", damagedAddress}, io.Discard, mux)
		if err != nil {
			t.Fatal("restart with a missing registered Volume", err)
		}
		repairStop, ready = startJoinTestServer(t, restarted)
		ready()
		repairNode = restarted
		if restarted.Runtime.Identity.RaftID != damagedID {
			t.Fatal("restart changed Node identity")
		}
	}
	for {
		holding, err := repairNode.Runtime.Records.Verify(ctx, d)
		if err == nil {
			if holding.VolumeID != added.ID {
				t.Fatal("automatic repair did not use the new Volume", holding.VolumeID, added.ID)
			}
			break
		}
		if !errors.Is(err, records.ErrUnavailable) {
			t.Fatal("unexpected holding state before managed repair", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("managed repair did not relocate lost Pack bytes", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatal("lost default Volume was recreated", err)
	}
	status, err := volume.Inspect(damagedStore)
	if err != nil || len(status.Volumes) != 2 || status.Volumes[0].State != volume.Removed || status.Volumes[0].Health != "removed" || status.Volumes[1].Health != "healthy" {
		t.Fatal("Volume registration/health changed during repair", status, err)
	}
	read, err := repairNode.EngineData.Read(ctx, space, key)
	if err != nil || !bytes.Equal(read.Body, body) {
		t.Fatal("repaired Node cannot serve original data", err)
	}
	repairStop()
	final, err := openServerWithMux([]string{"--store", damagedStore, "--listen", damagedAddress}, io.Discard, mux)
	if err != nil {
		t.Fatal("reopen repaired Store while old Volume remains missing", err)
	}
	_, ready = startJoinTestServer(t, final)
	ready()
	holding, err := final.Runtime.Records.Verify(ctx, d)
	if err != nil || holding.VolumeID != added.ID {
		t.Fatal("repaired Pack location did not survive another restart", holding, err)
	}
}
