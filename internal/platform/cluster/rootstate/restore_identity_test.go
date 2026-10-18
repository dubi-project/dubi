package rootstate

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestRewriteSnapshotForRestoreMapsRootIdentities(t *testing.T) {
	source := rootFileFixture(t)
	image, index, _, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	oldNode := "node_00000000000000000000000000000001"
	newNode := Node{NodeID: "node_00000000000000000000000000000002", RaftID: 21, RegionID: "west", Address: "new:7443"}
	rewritten, err := RewriteSnapshotForRestore(image, RestoreIdentityMap{
		RaftIDs: map[uint64]uint64{11: 21}, Nodes: map[string]Node{oldNode: newNode},
	}, &raftpb.ConfState{Voters: []uint64{21}})
	if err != nil {
		t.Fatal(err)
	}
	destination, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if err := destination.Restore(index, rewritten, &raftpb.ConfState{Voters: []uint64{21}}); err != nil {
		t.Fatal("restore rewritten Root snapshot:", err)
	}
	node, found, err := destination.Node(21)
	if err != nil || !found || node != newNode {
		t.Fatalf("restored Node = %+v, found=%v, err=%v", node, found, err)
	}
	group, found, err := destination.Group("data")
	if err != nil || !found || len(group.Members) != 1 || group.Members[0].RaftID != 21 {
		t.Fatalf("restored Group = %+v, found=%v, err=%v", group, found, err)
	}
}
