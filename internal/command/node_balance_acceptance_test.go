//go:build linux

package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginedata"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/server"
	"golang.org/x/sys/unix"
)

// Run only on explicitly supplied disposable mounts. The normal nine-node
// cluster is never opened. Same-host mTLS/independent filesystems verify the
// mechanism, not independent hardware or WAN failure domains.
func TestManagedNodeCapacityBalanceOnDisposableFilesystems(t *testing.T) {
	var mounts []string
	if value := os.Getenv("DUBI_TEST_NODE_BALANCE_MOUNTS"); value == "" {
		t.Skip("requires three disposable mounted filesystems")
	} else if json.Unmarshal([]byte(value), &mounts) != nil || len(mounts) != 3 {
		t.Fatal("invalid disposable mount list")
	}
	t.Setenv("HOME", t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	mux := referenceTestMux(t)
	var stores, volumes []string
	devices := make(map[uint64]bool)
	for _, mount := range mounts {
		var stat unix.Stat_t
		if err := unix.Stat(mount, &stat); err != nil {
			t.Fatal(err)
		}
		if devices[uint64(stat.Dev)] {
			t.Fatal("acceptance mounts share a filesystem")
		}
		devices[uint64(stat.Dev)] = true
		path, err := os.MkdirTemp(mount, "dubi-node-volume-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(path) })
		volumes, stores = append(volumes, path), append(stores, t.TempDir())
	}
	if err := runInit([]string{"--store", stores[0], "--volume", volumes[0]}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	first, err := openServerWithMux([]string{"--store", stores[0], "--listen", "127.0.0.1:0"}, io.Discard, mux)
	if err != nil {
		t.Fatal(err)
	}
	stop, ready := startJoinTestServer(t, first)
	ready()
	nodes := []*server.Server{first}
	stops := []func(){stop}
	for i := 1; i < 3; i++ {
		_, ticket := joinTestTicket(t, stores[0], first.LocalAddress())
		s, err := openJoiningServerWithMux(ctx, []string{"--store", stores[i], "--listen", "127.0.0.1:0", "--ticket-file", ticket, "--volume", volumes[i]}, nil, io.Discard, mux)
		if err != nil {
			t.Fatal(err)
		}
		stop, ready := startJoinTestServer(t, s)
		ready()
		nodes, stops = append(nodes, s), append(stops, stop)
	}
	poll := func(label string, check func() bool) {
		t.Helper()
		for !check() {
			select {
			case <-ctx.Done():
				t.Fatal(label, ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	group := regiongroups.GroupID("default")
	poll("initial Region", func() bool {
		h, found, err := first.Runtime.Root.RegionCohortHead("default")
		return err == nil && found && h.CurrentGeneration == 1 && h.PendingGeneration == 0
	})
	space := rootstate.Keyspace{TenantID: "balance", EngineID: "data-alpha", DatasetID: "shared"}
	command, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion, RequestID: "balance-tenant",
		Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: space.TenantID}})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Runtime.Host.Submit(raft.RootGroupID, command); err != nil {
		t.Fatal(err)
	}
	activateEngineTestDataset(t, ctx, first, group, space)
	key, body := []byte("capacity-handoff"), bytes.Repeat([]byte("immutable"), (2<<20)/9)
	written, err := first.EngineData.WriteLocal(ctx, enginedata.WriteRequest{RequestID: "before-capacity-move", Keyspace: space, Key: key, Body: body,
		Publish: func(d records.Descriptor) ([]byte, error) { return json.Marshal(d) }})
	if err != nil || written.State != enginedata.WriteLocalReady {
		t.Fatal("initial write", written, err)
	}
	descriptor, err := records.Describe(space, group, key, body)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := router.New(first.Runtime, first.Runtime.Root)
	if err != nil {
		t.Fatal(err)
	}
	target, err := routes.ResolveTarget(ctx, space, key)
	if err != nil {
		t.Fatal(err)
	}
	poll("Range placement", func() bool {
		_, _, err := placement.ReadRangeGeoPlan(ctx, first.Runtime, first.Runtime.Root, target.RangeID, 256, rootstate.MaxRegionCohortNodes)
		return err == nil
	})
	plan, err := placement.ReadLocalPlan(ctx, first.Runtime, first.Runtime.Root, group)
	if err != nil {
		t.Fatal(err)
	}
	var keepers []rootstate.Node
	for _, n := range plan.Nodes() {
		if n.RaftID != first.Runtime.Identity.RaftID {
			keepers = append(keepers, n)
		}
	}
	// Persist a handoff and its actual mTLS keeper pins, then restart the
	// source before deleting anything. Normal startup must resume that task.
	task, err := first.Runtime.Records.BeginNodeBalance(ctx, descriptor, plan.RetentionScope(), keepers)
	if err != nil {
		t.Fatal(err)
	}
	for _, keeper := range keepers {
		if _, err := first.GroupClient.RetainRecordOnNode(ctx, keeper, task.Sequence, task.Scope, descriptor); err != nil {
			t.Fatal("retention RPC", err)
		}
	}
	if err := first.Runtime.Packs.SealAll(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := first.Runtime.Volumes.Status()
	if err != nil {
		t.Fatal(err)
	}
	address, originalIdentity := first.LocalAddress(), first.Runtime.Identity
	stops[0]()
	restarted, err := openServerWithMux([]string{"--store", stores[0], "--listen", address}, io.Discard, mux)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Runtime.Identity != originalIdentity {
		t.Fatal("restart changed identity")
	}
	nodes[0] = restarted
	stops[0], ready = startJoinTestServer(t, restarted)
	ready()
	poll("automatic source eviction and pin release", func() bool {
		task, err := restarted.Runtime.Records.NodeBalanceTask()
		if err != nil || task != nil {
			return false
		}
		_, err = restarted.Runtime.Records.Verify(ctx, descriptor)
		return errors.Is(err, records.ErrNotHeld)
	})
	poll("old Pack reclaimed", func() bool {
		after, err := restarted.Runtime.Volumes.Status()
		return err == nil && after.Capacity.AvailableBytes >= before.Capacity.AvailableBytes+uint64(len(body))/2
	})
	for _, keeper := range nodes[1:] {
		if _, err := keeper.Runtime.Records.Verify(ctx, descriptor); err != nil {
			t.Fatal("keeper lost the majority", err)
		}
		pins, _, _, err := keeper.Runtime.Records.RetentionPage(ctx, "")
		if err != nil || len(pins) != 0 {
			t.Fatal("finished handoff leaked keeper pins", pins, err)
		}
	}
	read, err := restarted.EngineData.Read(ctx, space, key)
	if err != nil || !bytes.Equal(read.Body, body) || read.Reference.Index != written.Local.Reference.Index {
		t.Fatal("any-Node read after capacity handoff", err)
	}
	if _, err := restarted.Runtime.Records.Put(ctx, descriptor, body); err != nil {
		t.Fatal(err)
	}
	rootCommand := func(command rootstate.Command) []byte {
		t.Helper()
		command.Version = rootstate.FormatVersion
		data, err := rootstate.EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := restarted.Runtime.Host.Submit(raft.RootGroupID, data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	awaitRoot := func(data []byte) {
		t.Helper()
		poll("Root test control", func() bool {
			result, found, err := restarted.Runtime.Root.ResultForCommand(data)
			if err != nil {
				t.Fatal(err)
			}
			if found && result.Error != "" {
				t.Fatal(result.Error)
			}
			return found
		})
	}
	// Hold each worker's lease on behalf of a DIFFERENT current Node. Leasing
	// to the worker's own identity lets its normal background loop resume and
	// complete the backup while this test is waiting for the balance tick.
	// This verifies the concurrency guard, not Backup/Restore data transfer.
	awaitRoot(rootCommand(rootstate.Command{RequestID: "balance-repo", Type: rootstate.CreateRepository,
		Repository: &repository.Repository{Name: "balance", Type: repository.TypeFile, Path: filepath.Join(t.TempDir(), "backup")}}))
	controls := [][]byte{rootCommand(rootstate.Command{RequestID: "balance-job", Type: rootstate.CreateBackupJob,
		BackupJob: &rootstate.BackupJob{JobID: "capacity-snapshot", Repository: "balance", ClusterID: originalIdentity.ClusterID}})}
	for i, node := range nodes {
		claimant := nodes[(i+1)%len(nodes)].Runtime.Identity
		controls = append(controls, rootCommand(rootstate.Command{RequestID: fmt.Sprintf("balance-claim-%d", node.Runtime.Identity.RaftID),
			Type: rootstate.ClaimBackupJob, BackupJobClaim: &rootstate.BackupJobClaim{JobID: "capacity-snapshot", RaftID: node.Runtime.Identity.RaftID,
				NodeID: node.Runtime.Identity.NodeID, ClaimantRaftID: claimant.RaftID, ClaimantNodeID: claimant.NodeID,
				ObservedUnix: time.Now().Unix(), LeaseExpiresUnix: time.Now().Add(time.Hour).Unix()}}))
	}
	for _, control := range controls {
		awaitRoot(control)
	}
	blocked, err := restarted.Runtime.Records.BeginNodeBalance(ctx, descriptor, plan.RetentionScope(), keepers)
	if err != nil {
		t.Fatal(err)
	}
	poll("backup cancels discretionary deletion", func() bool {
		active, err := restarted.Runtime.Records.NodeBalanceTask()
		return err == nil && active == nil
	})
	if _, err := restarted.Runtime.Records.Verify(ctx, descriptor); err != nil {
		t.Fatal("active backup lost its source holding", err)
	}
	awaitRoot(rootCommand(rootstate.Command{RequestID: "balance-end-test-job", Type: rootstate.FailBackupJob,
		BackupJobFailure: &rootstate.BackupJobFailure{JobID: "capacity-snapshot", RaftID: originalIdentity.RaftID,
			NodeID: originalIdentity.NodeID, ClaimantRaftID: nodes[1].Runtime.Identity.RaftID, ClaimantNodeID: nodes[1].Runtime.Identity.NodeID,
			LeaseEpoch: 1, Error: "end isolated backup guard test"}}))
	// Create pressure only in the disposable source filesystem, then let the
	// production worker discover and carry out the next handoff on its own.
	spaceBefore, err := restarted.Runtime.Volumes.Status()
	if err != nil {
		t.Fatal(err)
	}
	fillerPath := filepath.Join(mounts[0], "capacity-test-filler")
	filler, err := os.OpenFile(fillerPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(fillerPath) })
	err = unix.Fallocate(int(filler.Fd()), 0, 0, int64(spaceBefore.Capacity.AvailableBytes*45/100))
	err = errors.Join(err, filler.Close())
	if err != nil {
		t.Fatal(err)
	}
	pressureBefore, err := restarted.Runtime.Volumes.Status()
	if err != nil {
		t.Fatal(err)
	}
	poll("automatic capacity discovery", func() bool {
		_, missing := restarted.Runtime.Records.Verify(ctx, descriptor)
		through, _, err := restarted.Runtime.Records.NodeBalanceReclaim()
		return errors.Is(missing, records.ErrNotHeld) && err == nil && through > blocked.Sequence
	})
	poll("automatically discovered Pack reclaimed", func() bool {
		after, err := restarted.Runtime.Volumes.Status()
		return err == nil && after.Capacity.AvailableBytes >= pressureBefore.Capacity.AvailableBytes+uint64(len(body))/2
	})
}
