package rootjoin

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRootJoinCoreViewOwnsOneCanonicalCut(t *testing.T) {
	cluster := "cluster_00000000000000000000000000000001"
	conf := &raftpb.ConfState{Voters: []uint64{3, 1}, VotersOutgoing: []uint64{2, 1}, Learners: []uint64{4}, LearnersNext: []uint64{2}, AutoLeave: proto.Bool(true)}
	cut := rootstate.RootRouting{AppliedIndex: 21, Membership: conf}
	for _, id := range []uint64{4, 2, 3, 1} {
		cut.Nodes = append(cut.Nodes, rootstate.Node{NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: "r1", Address: fmt.Sprintf("n%d:7443", id)})
	}
	view, err := FromRootCut(cluster, cut, 2)
	if err != nil {
		t.Fatal(err)
	}
	if view.AppliedIndex != 21 || view.LeaderID != 2 || !slices.Equal(view.Voters, []uint64{1, 3}) || !slices.Equal(view.VotersOutgoing, []uint64{1, 2}) {
		t.Fatal("wrong cut", view)
	}
	for id := uint64(1); id <= 4; id++ {
		if !view.Contains(id) || view.Peers[id-1].RaftID != id {
			t.Fatal("lost member", id)
		}
	}
	if view.Contains(0) || view.Contains(99) {
		t.Fatal("invented member")
	}
	if conf.Voters[0] != 3 || cut.Nodes[0].RaftID != 4 {
		t.Fatal("canonicalization mutated caller")
	}
	cloned := view.CloneCanonical()
	cloned.Voters[0], cloned.VotersOutgoing[0], cloned.Learners[0], cloned.LearnersNext[0] = 99, 99, 99, 99
	cloned.Peers[0].Address = "changed:7443"
	if err := view.Validate(cluster); err != nil || view.Peers[0].Address != "n1:7443" {
		t.Fatal("clone aliases source", err)
	}
	slices.Reverse(view.Peers)
	slices.Reverse(view.Voters)
	slices.Reverse(view.VotersOutgoing)
	want, err := FromRootCut(cluster, cut, 2)
	if err != nil {
		t.Fatal(err)
	}
	left, err := json.Marshal(view.CloneCanonical())
	if err != nil {
		t.Fatal(err)
	}
	right, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(left) != string(right) {
		t.Fatal("equivalent cuts have unstable serialization")
	}
	for _, hint := range []uint64{0, 4, 99} {
		view, err := FromRootCut(cluster, cut, hint)
		if err != nil || view.LeaderID != 0 {
			t.Fatal("nonvoter hint retained", view, err)
		}
	}
	if _, err := FromRootCut(cluster, rootstate.RootRouting{}, 0); err == nil {
		t.Fatal("empty cut accepted")
	}
}

func TestRootJoinEndpointEquivalenceDoesNotResolveDNS(t *testing.T) {
	for _, pair := range [][2]string{{"N1.Example.:7443", "n1.example:7443"}, {"[0:0:0:0:0:0:0:1]:7443", "[::1]:7443"}, {"[::ffff:127.0.0.1]:7443", "127.0.0.1:7443"}} {
		if EndpointKey(pair[0]) != EndpointKey(pair[1]) {
			t.Fatal("equivalent routes not deduplicated", pair)
		}
	}
	if EndpointKey("localhost:7443") == EndpointKey("127.0.0.1:7443") {
		t.Fatal("DNS lookup leaked into canonicalization")
	}
	if EndpointKey("n1:7443") == EndpointKey("n1:7444") {
		t.Fatal("ports collapsed")
	}
}
