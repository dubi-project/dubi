package rootstate

import (
	"strings"
	"testing"
)

func TestShortNodeRegistrationReservesHistoricalIdentity(t *testing.T) {
	s := rootFileState(t, t.TempDir())
	n := Node{NodeID: "node_aabbccddeeff", RaftID: 1, RegionID: "west", Address: "localhost:17443"}
	applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: &n})
	duplicate := n
	duplicate.RaftID = 2
	r := applyBackupCommand(t, s, "duplicate-short-node", Command{Type: RegisterNode, Node: &duplicate})
	if !strings.Contains(r.Error, "NodeID is already registered") {
		t.Fatal("collision accepted", r)
	}
	applyRootFileCommand(t, s, Command{Type: FenceNode, NodeFence: &NodeFenceRequest{NodeID: n.NodeID, RaftID: n.RaftID}})
	r = applyBackupCommand(t, s, "reuse-fenced-short-node", Command{Type: RegisterNode, Node: &duplicate})
	if !strings.Contains(r.Error, "NodeID is already registered") {
		t.Fatal("historical identity reused", r)
	}
}
