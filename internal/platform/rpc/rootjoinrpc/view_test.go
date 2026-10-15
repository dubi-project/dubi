package rootjoinrpc

import (
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinwire"
	"google.golang.org/protobuf/proto"
)

const testCluster = "cluster_00000000000000000000000000000001"

func validView() *rootjoinwire.RootView {
	return &rootjoinwire.RootView{ClusterId: testCluster, AppliedIndex: 17, Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 3}, Learners: []uint64{4}, LearnersNext: []uint64{3}, AutoLeave: true, LeaderId: 1,
		Peers: []*rootjoinwire.RootPeer{
			{RaftId: 1, NodeId: fmt.Sprintf("node_%032x", 1), RegionId: "r1", Address: "n1:7443"},
			{RaftId: 2, NodeId: fmt.Sprintf("node_%032x", 2), RegionId: "r1", Address: "n2:7443"},
			{RaftId: 3, NodeId: fmt.Sprintf("node_%032x", 3), RegionId: "r2", Address: "n3:7443"},
			{RaftId: 4, NodeId: fmt.Sprintf("node_%032x", 4), RegionId: "r2", Address: "n4:7443"},
		}}
}

func TestRootJoinViewRejectsUntrustedRouting(t *testing.T) {
	if err := ValidateView(validView(), testCluster); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*rootjoinwire.RootView){
		"wrong-cluster":     func(v *rootjoinwire.RootView) { v.ClusterId = "cluster_00000000000000000000000000000002" },
		"zero-cut":          func(v *rootjoinwire.RootView) { v.AppliedIndex = 0 },
		"no-voters":         func(v *rootjoinwire.RootView) { v.Voters = nil },
		"duplicate-voter":   func(v *rootjoinwire.RootView) { v.Voters = []uint64{1, 1, 2} },
		"zero-member":       func(v *rootjoinwire.RootView) { v.Learners = append(v.Learners, 0) },
		"missing-peer":      func(v *rootjoinwire.RootView) { v.Peers = v.Peers[:3] },
		"nil-peer":          func(v *rootjoinwire.RootView) { v.Peers[0] = nil },
		"duplicate-peer":    func(v *rootjoinwire.RootView) { v.Peers[1] = v.Peers[0] },
		"duplicate-node":    func(v *rootjoinwire.RootView) { v.Peers[1].NodeId = v.Peers[0].NodeId },
		"duplicate-address": func(v *rootjoinwire.RootView) { v.Peers[1].Address = "N1:7443" },
		"duplicate-fqdn":    func(v *rootjoinwire.RootView) { v.Peers[1].Address = "N1.:7443" },
		"duplicate-ipv6": func(v *rootjoinwire.RootView) {
			v.Peers[0].Address = "[::1]:7443"
			v.Peers[1].Address = "[0:0:0:0:0:0:0:1]:7443"
		},
		"resolver-uri":                 func(v *rootjoinwire.RootView) { v.Peers[0].Address = "dns:///evil:7443" },
		"wildcard":                     func(v *rootjoinwire.RootView) { v.Peers[0].Address = "0.0.0.0:7443" },
		"zero-port":                    func(v *rootjoinwire.RootView) { v.Peers[0].Address = "n1:0" },
		"invalid-node":                 func(v *rootjoinwire.RootView) { v.Peers[0].NodeId = "node_user-supplied" },
		"invalid-region":               func(v *rootjoinwire.RootView) { v.Peers[0].RegionId = "../../r1" },
		"foreign-peer":                 func(v *rootjoinwire.RootView) { v.Peers[0].RaftId = 9 },
		"leader-is-learner":            func(v *rootjoinwire.RootView) { v.LeaderId = 4 },
		"foreign-leader":               func(v *rootjoinwire.RootView) { v.LeaderId = 99 },
		"learner-also-voter":           func(v *rootjoinwire.RootView) { v.Learners = []uint64{1, 4} },
		"next-also-incoming":           func(v *rootjoinwire.RootView) { v.LearnersNext = []uint64{1, 3} },
		"next-not-outgoing":            func(v *rootjoinwire.RootView) { v.LearnersNext = []uint64{4} },
		"joint-state-without-outgoing": func(v *rootjoinwire.RootView) { v.VotersOutgoing = nil },
		"too-many-peers":               func(v *rootjoinwire.RootView) { v.Peers = make([]*rootjoinwire.RootPeer, MaxPeers+1) },
		"too-many-voters":              func(v *rootjoinwire.RootView) { v.Voters = make([]uint64, MaxPeers+1) },
	}
	for name, change := range mutations {
		t.Run(name, func(t *testing.T) {
			v := validView()
			change(v)
			if err := ValidateView(v, testCluster); err == nil {
				t.Fatal("invalid view accepted", v)
			}
		})
	}
	if err := ValidateView(nil, testCluster); err == nil {
		t.Fatal("nil view accepted")
	}
	if err := ValidateView(validView(), "bad-cluster"); err == nil {
		t.Fatal("invalid expected identity")
	}
	v := validView()
	v.LeaderId = 0
	if err := ValidateView(v, testCluster); err != nil {
		t.Fatal("election without hint must remain readable", err)
	}
}

func FuzzRootJoinView(f *testing.F) {
	data, err := proto.Marshal(validView())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxResponseSize {
			return
		}
		var view rootjoinwire.RootView
		if err := proto.Unmarshal(data, &view); err != nil {
			return
		}
		if err := ValidateView(&view, testCluster); err == nil {
			if len(view.Peers) > MaxPeers || !viewContains(&view, view.Voters[0]) {
				t.Fatal("accepted invalid membership")
			}
		}
	})
}

func TestRootJoinWireViewConversionOwnsItsMemory(t *testing.T) {
	wire := validView()
	value, err := DecodeView(wire, testCluster)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeView(value)
	if err != nil || !proto.Equal(encoded, wire) {
		t.Fatal("view round trip changed", encoded, err)
	}
	wire.Voters[0], wire.VotersOutgoing[0], wire.Learners[0], wire.LearnersNext[0] = 99, 99, 99, 99
	wire.Peers[0].Address = "mutated:7443"
	if err := value.Validate(testCluster); err != nil || value.Peers[0].Address != "n1:7443" {
		t.Fatal("wire mutation changed validated core", err)
	}
	value.Voters[0], value.VotersOutgoing[0], value.Learners[0], value.LearnersNext[0] = 88, 88, 88, 88
	value.Peers[0].Address = "mutated-again:7443"
	if err := ValidateView(encoded, testCluster); err != nil || encoded.Peers[0].Address != "n1:7443" {
		t.Fatal("core mutation changed encoded view", err)
	}
	if _, err := DecodeView(wire, testCluster); err == nil {
		t.Fatal("invalid wire converted")
	}
	if _, err := encodeView(value); err == nil {
		t.Fatal("invalid core encoded")
	}
}
