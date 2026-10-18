package node

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/engineruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeInstallsRootRestoreSnapshotThroughRaft(t *testing.T) {
	sourceRoot := t.TempDir()
	source, err := rootstate.Open(filepath.Join(sourceRoot, "root"))
	if err != nil {
		t.Fatal(err)
	}
	if err := source.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{11}}); err != nil {
		t.Fatal(err)
	}
	if err := source.ApplyMembership(2, &raftpb.ConfState{Voters: []uint64{11}}); err != nil {
		t.Fatal(err)
	}
	image, index, _, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	destinationRoot := t.TempDir()
	id, err := identity.Bootstrap(destinationRoot)
	if err != nil {
		t.Fatal(err)
	}
	destinationConf := &raftpb.ConfState{Voters: []uint64{id.RaftID}, AutoLeave: proto.Bool(false)}
	rewritten, err := rootstate.RewriteSnapshotForRestore(image, rootstate.RestoreIdentityMap{
		RaftIDs: map[uint64]uint64{11: id.RaftID},
		Nodes:   map[string]rootstate.Node{"node_00000000000000000000000000000001": {NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "west", Address: "new:7443"}},
	}, destinationConf)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitializeRoot(destinationRoot); err != nil {
		t.Fatal(err)
	}
	registry := engineruntime.NewRegistry()
	if err := registry.Register("record", &nodeTestHandler{}); err != nil {
		t.Fatal(err)
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := Open(Options{StoreRoot: destinationRoot, TickInterval: time.Hour, RootBootstrapPeers: []uint64{id.RaftID}, EngineMux: mux})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if err := runtime.InstallRestoreSnapshots(context.Background(), []RestoreGroupSnapshot{{GroupID: "root", Index: index, Term: 1, Data: rewritten, Conf: destinationConf}}); err != nil {
		t.Fatal(err)
	}
	if runtime.Root.AppliedIndex() != index || len(runtime.Root.ConfState().GetVoters()) != 1 || runtime.Root.ConfState().GetVoters()[0] != id.RaftID {
		t.Fatalf("restored Root state index=%d conf=%v", runtime.Root.AppliedIndex(), runtime.Root.ConfState())
	}
}
