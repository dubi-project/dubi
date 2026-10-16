package node

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

type formationDiscardTransport struct{}

func (formationDiscardTransport) Send(string, *raftpb.Message) error { return nil }

// A real local Raft store with a single Root voter and two admitted learners.
// No regional quorum is simulated: recovery must open the immutable plan even
// before its other participants exist, without publishing data readiness.
func TestRuntimeRegionFormationRestartsBeforeRoutingPublication(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	open := func(initial bool) *Runtime {
		options := Options{StoreRoot: store, TickInterval: time.Hour, Transport: formationDiscardTransport{}, EngineMux: nodeTestMux(t)}
		if initial {
			options.RootBootstrapPeers = []uint64{id.RaftID}
		}
		r, err := Open(options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.Close() })
		if err := r.Host.Campaign(raft.RootGroupID); err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := open(true)
	peers := []uint64{id.RaftID}
	for candidate := uint64(1); len(peers) < 3; candidate++ {
		if candidate != id.RaftID {
			peers = append(peers, candidate)
		}
	}
	for _, peer := range peers {
		nodeID := fmt.Sprintf("node_%032x", peer)
		if peer == id.RaftID {
			nodeID = id.NodeID
		}
		applyRoot(t, r, rootstate.Command{Version: 1, RequestID: fmt.Sprintf("node-%d", peer), Type: rootstate.RegisterNode, Node: &rootstate.Node{NodeID: nodeID, RaftID: peer, RegionID: "west", Address: "localhost:7443"}})
		if peer != id.RaftID {
			if err := r.Host.AddLearner(raft.RootGroupID, peer); err != nil {
				t.Fatal(err)
			}
		}
	}
	slices.Sort(peers)
	group := regiongroups.GroupID("west")
	applyRoot(t, r, rootstate.Command{Version: 1, RequestID: "group", Type: rootstate.RegisterGroup, Group: &rootstate.Group{GroupID: group, StateKind: rootstate.GroupStateEngine}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.JoinManager.PrepareJoining(ctx, rootstate.GroupTaskReference{GroupID: group, ChangeID: "missing", ExpectedEpoch: 1}, id.RaftID); err == nil || r.Host.HasGroup(group) {
		t.Fatal("learner opened without Root membership task", err)
	}
	applyRoot(t, r, rootstate.Command{Version: 1, RequestID: "plan", Type: rootstate.PlanRegionCohort, RegionPlan: &rootstate.RegionCohortPlan{RegionID: "west", GroupID: group, Nodes: peers}})
	if err := r.JoinManager.PrepareJoining(ctx, rootstate.GroupTaskReference{GroupID: group, ChangeID: "missing", ExpectedEpoch: 1}, id.RaftID); err == nil || r.Host.HasGroup(group) {
		t.Fatal("learner preempted initial formation", err)
	}
	// Stop with only the Root intent; restart must create the original local
	// participant, then reopen it on another restart without inventing peers.
	for n := 0; n < 2; n++ {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		r = open(false)
		if err := r.RecoverLocalGroups(ctx); err != nil {
			t.Fatal(err)
		}
		conf, err := r.Host.Membership(group)
		voters := slices.Clone(conf.GetVoters())
		slices.Sort(voters)
		if err != nil || !slices.Equal(voters, peers) || len(conf.GetLearners()) != 0 {
			t.Fatal(conf, err)
		}
		descriptor, found, err := r.Root.Group(group)
		if err != nil || !found || descriptor.Epoch != 1 || len(descriptor.Members) != 0 {
			t.Fatal("local open published unconfirmed routing", descriptor, found, err)
		}
	}
}
