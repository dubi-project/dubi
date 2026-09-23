package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	etcdraft "go.etcd.io/raft/v3"
)

func hostTestState(t *testing.T, root, groupID string) *testState {
	t.Helper()
	path := filepath.Join(root, "states", groupID)
	if err := os.MkdirAll(path, 0o750); err != nil {
		t.Fatal(err)
	}
	return openTestState(t, path)
}

func TestHostMultipleGroupsRestartAndStoreLock(t *testing.T) {
	root := t.TempDir()
	id, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	host, err := OpenHost(HostOptions{StoreRoot: root, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if host.Identity() != id {
		t.Fatalf("Host identity = %+v, want %+v", host.Identity(), id)
	}
	if _, err := OpenHost(HostOptions{StoreRoot: root}); err == nil {
		t.Fatal("second Host opened the same Store")
	}
	rootState := hostTestState(t, root, "root-state")
	rangeState := hostTestState(t, root, "range-state")
	if err := host.AddGroup(RootGroupID, []uint64{id.RaftID}, false, rootState); err != nil {
		t.Fatal(err)
	}
	if err := host.AddGroup("range-1", []uint64{id.RaftID}, false, rangeState); err != nil {
		t.Fatal(err)
	}
	duplicate := hostTestState(t, root, "duplicate")
	if err := host.AddGroup("range-1", nil, false, duplicate); err == nil || duplicate.closed {
		t.Fatalf("duplicate AddGroup = %v, state closed = %v", err, duplicate.closed)
	}
	_ = duplicate.Close()
	for _, groupID := range []string{RootGroupID, "range-1"} {
		if err := host.Campaign(groupID); err != nil {
			t.Fatal(err)
		}
		status, err := host.Status(groupID)
		if err != nil || status.Role != etcdraft.StateLeader {
			t.Fatalf("Group %q status = %+v, %v", groupID, status, err)
		}
	}
	if err := host.Submit(RootGroupID, []byte("cluster metadata")); err != nil {
		t.Fatal(err)
	}
	if err := host.Submit("range-1", []byte("opaque range command")); err != nil {
		t.Fatal(err)
	}
	if len(rootState.disk.Commands) != 1 || string(rootState.disk.Commands[0]) != "cluster metadata" {
		t.Fatalf("Root Group commands = %q", rootState.disk.Commands)
	}
	if len(rangeState.disk.Commands) != 1 || string(rangeState.disk.Commands[0]) != "opaque range command" {
		t.Fatalf("Range Group commands = %q", rangeState.disk.Commands)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	if !rootState.closed || !rangeState.closed {
		t.Fatal("Host did not close all Group StateMachines")
	}
	if err := host.TickAll(); !errors.Is(err, ErrHostClosed) {
		t.Fatalf("tick closed Host = %v", err)
	}
	restarted, err := OpenHost(HostOptions{StoreRoot: root, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	recoveredRoot := hostTestState(t, root, "root-state")
	recoveredRange := hostTestState(t, root, "range-state")
	if err := restarted.AddGroup(RootGroupID, nil, false, recoveredRoot); err != nil {
		t.Fatal(err)
	}
	if err := restarted.AddGroup("range-1", nil, false, recoveredRange); err != nil {
		t.Fatal(err)
	}
	if len(recoveredRoot.disk.Commands) != 1 || string(recoveredRoot.disk.Commands[0]) != "cluster metadata" {
		t.Fatalf("recovered Root Group commands = %q", recoveredRoot.disk.Commands)
	}
	if len(recoveredRange.disk.Commands) != 1 || string(recoveredRange.disk.Commands[0]) != "opaque range command" {
		t.Fatalf("recovered Range Group commands = %q", recoveredRange.disk.Commands)
	}
}

func TestHostRequiresBoundIdentityAndReleasesLock(t *testing.T) {
	root := t.TempDir()
	if _, err := identity.PrepareJoin(root); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenHost(HostOptions{StoreRoot: root}); err == nil {
		t.Fatal("unbound Node opened a Host")
	}
	bootstrap := t.TempDir()
	id, err := identity.Bootstrap(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := identity.BindCluster(root, id.ClusterID); err != nil {
		t.Fatal(err)
	}
	host, err := OpenHost(HostOptions{StoreRoot: root})
	if err != nil {
		t.Fatalf("Host lock was retained after failed OpenHost: %v", err)
	}
	defer host.Close()
	if err := host.Step("missing", nil); err == nil {
		t.Fatal("unknown Group accepted a message")
	}
}

func TestHostTickerElectsSingleNode(t *testing.T) {
	root := t.TempDir()
	id, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	host, err := OpenHost(HostOptions{StoreRoot: root, TickInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if err := host.AddGroup(RootGroupID, []uint64{id.RaftID}, false, hostTestState(t, root, "tick-state")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status, err := host.Status(RootGroupID)
		if err != nil {
			t.Fatal(err)
		}
		if status.Role == etcdraft.StateLeader {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Host ticker did not elect single-node Group")
}
