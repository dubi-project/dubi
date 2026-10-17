package rootstate

import (
	"fmt"
	"testing"
)

func TestNodeReadyRequiresRegisteredIdentityAndPersists(t *testing.T) {
	state, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	node := Node{NodeID: "node_0123456789abcdef0123456789abcdef", RaftID: 7, RegionID: "west", Address: "127.0.0.1:17443"}
	register, err := EncodeCommand(Command{Version: FormatVersion, RequestID: "register-ready-node", Type: RegisterNode, Node: &node})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Apply(1, register); err != nil {
		t.Fatal(err)
	}
	readyCommand, err := EncodeCommand(Command{Version: FormatVersion, RequestID: "mark-ready-node", Type: MarkNodeReady,
		NodeReady: &NodeReadyRecord{NodeID: node.NodeID, RaftID: node.RaftID, ActivatedIndex: 12}})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Apply(2, readyCommand); err != nil {
		t.Fatal(err)
	}
	ready, found, err := state.NodeReady(node.RaftID)
	if err != nil || !found || ready.NodeID != node.NodeID || ready.ActivatedIndex != 12 {
		t.Fatalf("ready=%+v found=%v err=%v", ready, found, err)
	}
	if got, err := state.RootMemberReady(node.RaftID); err != nil || !got {
		t.Fatalf("legacy learner readiness got=%v err=%v", got, err)
	}
	if _, err := EncodeCommand(Command{Version: FormatVersion, RequestID: fmt.Sprintf("bad-ready-%d", node.RaftID), Type: MarkNodeReady,
		NodeReady: &NodeReadyRecord{NodeID: node.NodeID, RaftID: node.RaftID}}); err == nil {
		t.Fatal("zero activation fence accepted")
	}
}
