package rootstate

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRootRoutingAtomicCutBoundsAndIsolation(t *testing.T) {
	s := rootFileState(t, t.TempDir())
	for id := uint64(1); id <= 5; id++ {
		applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: &Node{NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: "local", Address: fmt.Sprintf("n%d:7443", id)}})
	}
	conf := &raftpb.ConfState{Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 3}, Learners: []uint64{4}, LearnersNext: []uint64{3}, AutoLeave: proto.Bool(true)}
	if err := s.ApplyMembership(6, conf); err != nil {
		t.Fatal(err)
	}
	view, err := s.RootRoutingView(4)
	if err != nil || view.AppliedIndex != 6 || len(view.Nodes) != 4 || !proto.Equal(view.Membership, conf) {
		t.Fatal(view, err)
	}
	for i, node := range view.Nodes {
		if node.RaftID != uint64(i+1) {
			t.Fatal("unsorted or nonmember node", view.Nodes)
		}
	}
	if _, err := s.RootRoutingView(3); !errors.Is(err, ErrRootRoutingLimit) {
		t.Fatal("missing bound", err)
	}
	if _, err := s.RootRoutingView(0); !errors.Is(err, ErrRootRoutingLimit) {
		t.Fatal("invalid bound", err)
	}
	view.Nodes[0].Address = "changed:7443"
	view.Membership.Voters[0] = 99
	again, err := s.RootRoutingView(4)
	if err != nil || again.Nodes[0].Address != "n1:7443" || !proto.Equal(again.Membership, conf) {
		t.Fatal("caller mutated Root", again, err)
	}
	// Every sampled membership and applied index must belong to the same cut.
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				cut, err := s.RootRoutingView(5)
				if err != nil {
					t.Error(err)
					return
				}
				if cut.AppliedIndex > 6 && (len(cut.Nodes) != 1 || cut.Nodes[0].RaftID != 1+cut.AppliedIndex%2 || cut.Membership.Voters[0] != cut.Nodes[0].RaftID) {
					t.Error("mixed routing cut", cut)
					return
				}
			}
		})
	}
	for i := uint64(7); i < 30; i++ {
		if err := s.ApplyMembership(i, &raftpb.ConfState{Voters: []uint64{1 + i%2}}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RootRoutingView(5); err == nil {
		t.Fatal("closed Root supplied routes")
	}
}

func TestRootRoutingFailsClosedOnMissingOrCorruptDescriptor(t *testing.T) {
	for _, scenario := range []string{"missing", "key-mismatch", "unknown-field", "duplicate-node", "zero-id"} {
		t.Run(scenario, func(t *testing.T) {
			s := rootFileState(t, t.TempDir())
			applyRootFileCommand(t, s, Command{Type: RegisterNode, Node: &Node{NodeID: "node_00000000000000000000000000000001", RaftID: 1, RegionID: "local", Address: "n1:7443"}})
			conf := &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
			if scenario == "zero-id" {
				conf.Learners = []uint64{0}
			}
			if err := s.ApplyMembership(s.AppliedIndex()+1, conf); err != nil {
				t.Fatal(err)
			}
			var encoded []byte
			switch scenario {
			case "key-mismatch":
				encoded = []byte(`{"node_id":"node_00000000000000000000000000000002","raft_id":3,"region_id":"local","address":"n2:7443"}`)
			case "unknown-field":
				encoded = []byte(`{"node_id":"node_00000000000000000000000000000002","raft_id":2,"region_id":"local","address":"n2:7443","extra":true}`)
			case "duplicate-node":
				encoded = []byte(`{"node_id":"node_00000000000000000000000000000001","raft_id":2,"region_id":"local","address":"n2:7443"}`)
			}
			if encoded != nil {
				if err := s.db.Set(nodeKey(2), encoded, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.RootRoutingView(5); err == nil {
				t.Fatal("invalid route was published")
			}
		})
	}
}
