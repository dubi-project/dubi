package raft

import (
	"errors"
	"slices"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func topologyFixture(t *testing.T) (*queuedTransport, map[uint64]string) {
	t.Helper()
	transport := &queuedTransport{groups: make(map[uint64]*Group), blocked: make(map[uint64]bool)}
	roots := make(map[uint64]string)
	for _, id := range []uint64{81, 82, 83} {
		root := t.TempDir()
		roots[id] = root
		options := GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: id, Joining: id != 81,
			State: openTestState(t, root), Transport: transport}
		if id == 81 {
			options.BootstrapPeers = []uint64{id}
		}
		group, err := OpenGroup(options)
		if err != nil {
			t.Fatal(err)
		}
		transport.groups[id] = group
	}
	t.Cleanup(func() {
		for _, group := range transport.groups {
			_ = group.Close()
		}
	})
	leader := transport.groups[81]
	if err := leader.Campaign(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{82, 83} {
		if err := leader.AddLearner(id); err != nil {
			t.Fatal(err)
		}
		transport.deliver(t)
	}
	return transport, roots
}

func TestGroupTopologyJointExpansionAndDemotion(t *testing.T) {
	transport, _ := topologyFixture(t)
	leader := transport.groups[81]
	before, _ := leader.Membership()
	if err := leader.ReconfigureVoters(before, []uint64{83, 81, 82}); err != nil {
		t.Fatal(err)
	}
	joint, err := leader.Membership()
	if err != nil || !joint.GetAutoLeave() || len(joint.GetVoters()) != 3 || !slices.Equal(joint.GetVotersOutgoing(), []uint64{81}) {
		t.Fatalf("expansion did not enter joint old=1/new=3: %+v, %v", joint, err)
	}
	if err := leader.ReconfigureVoters(joint, []uint64{81, 82, 83}); !errors.Is(err, ErrTopologyPending) {
		t.Fatal("accepted a second plan in joint state", err)
	}
	transport.deliver(t)
	stable, _ := leader.Membership()
	if len(stable.GetVoters()) != 3 || len(stable.GetVotersOutgoing()) != 0 {
		t.Fatalf("joint did not leave automatically: %+v", stable)
	}
	if err := leader.ReconfigureVoters(before, []uint64{81, 82, 83}); !errors.Is(err, ErrTopologyChanged) {
		t.Fatal("stale topology plan was accepted", err)
	}
	if err := leader.ReconfigureVoters(stable, []uint64{82, 83}); !errors.Is(err, ErrTopologyTransfer) {
		t.Fatal("local leader demotion did not require transfer", err)
	}
	if err := leader.ReconfigureVoters(stable, []uint64{81}); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	for id, group := range transport.groups {
		conf, err := group.Membership()
		if err != nil || !slices.Equal(conf.GetVoters(), []uint64{81}) || !slices.Equal(conf.GetLearners(), []uint64{82, 83}) || len(conf.GetVotersOutgoing()) != 0 {
			t.Fatalf("node %d demotion did not retain learners: %+v, %v", id, conf, err)
		}
	}
}

func TestGroupTopologyRejectsUnsafePlansWithoutLogMutation(t *testing.T) {
	transport, _ := topologyFixture(t)
	leader := transport.groups[81]
	before, _ := leader.Membership()
	last, _ := leader.storage.LastIndex()
	for _, voters := range [][]uint64{nil, {0}, {81, 81}, {81, 99}} {
		if err := leader.ReconfigureVoters(before, voters); err == nil {
			t.Fatalf("accepted invalid desired voters %v", voters)
		}
	}
	wrong := proto.Clone(before).(*raftpb.ConfState)
	wrong.Learners = nil
	if err := leader.ReconfigureVoters(wrong, []uint64{81, 82, 83}); !errors.Is(err, ErrTopologyChanged) {
		t.Fatal(err)
	}
	transport.blocked[83] = true
	for i := 0; i < 20; i++ {
		if err := leader.Tick(); err != nil {
			t.Fatal(err)
		}
		transport.deliver(t)
	}
	if err := leader.ReconfigureVoters(before, []uint64{81, 82, 83}); !errors.Is(err, ErrTopologyPending) {
		t.Fatal("inactive desired voter accepted", err)
	}
	if after, err := leader.storage.LastIndex(); err != nil || after != last {
		t.Fatalf("rejected plan changed log %d -> %d: %v", last, after, err)
	}
}

func TestGroupTopologyRestartDuringJointExpansion(t *testing.T) {
	transport, roots := topologyFixture(t)
	leader := transport.groups[81]
	before, _ := leader.Membership()
	if err := leader.ReconfigureVoters(before, []uint64{81, 82, 83}); err != nil {
		t.Fatal(err)
	}
	// Restart before delivering the joint change to any learner. Neither a
	// cached desired set nor a second bootstrap operation may finish the change.
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	transport.queue = nil
	root := roots[81]
	reopened, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "regional", RaftID: 81,
		State: openTestState(t, root), Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	transport.groups[81] = reopened
	if conf, _ := reopened.Membership(); len(conf.GetVotersOutgoing()) == 0 || !conf.GetAutoLeave() {
		t.Fatalf("restart lost durable joint configuration: %+v", conf)
	}
	for i := 0; i < 80; i++ {
		for _, group := range transport.groups {
			if err := group.Tick(); err != nil {
				t.Fatal(err)
			}
		}
		transport.deliver(t)
		conf, _ := reopened.Membership()
		if len(conf.GetVotersOutgoing()) == 0 && len(conf.GetVoters()) == 3 {
			return
		}
	}
	t.Fatal("restarted joint consensus did not converge")
}

func TestGroupTopologyDoesNotShrinkAroundInactiveVoter(t *testing.T) {
	transport, _ := topologyFixture(t)
	leader := transport.groups[81]
	before, _ := leader.Membership()
	if err := leader.ReconfigureVoters(before, []uint64{81, 82, 83}); err != nil {
		t.Fatal(err)
	}
	transport.deliver(t)
	stable, _ := leader.Membership()
	transport.blocked[83] = true
	for i := 0; i < 20; i++ {
		if err := leader.Tick(); err != nil {
			t.Fatal(err)
		}
		transport.deliver(t)
	}
	last, _ := leader.storage.LastIndex()
	if err := leader.ReconfigureVoters(stable, []uint64{81}); !errors.Is(err, ErrTopologyPending) {
		t.Fatal("offline old voter was demoted to reduce quorum", err)
	}
	if after, _ := leader.Membership(); !proto.Equal(after, stable) {
		t.Fatal("rejected shrink changed membership")
	}
	if after, _ := leader.storage.LastIndex(); after != last {
		t.Fatal("rejected shrink appended log")
	}
	if err := transport.groups[82].ReconfigureVoters(stable, []uint64{81, 82, 83}); err == nil {
		t.Fatal("follower accepted topology operation")
	}
	delete(transport.blocked, 83)
	for i := 0; i < 3; i++ {
		if err := leader.Tick(); err != nil {
			t.Fatal(err)
		}
		transport.deliver(t)
	}
	pressure := errors.New("test disk pressure")
	leader.proposalGuard = func() error { return pressure }
	if err := leader.ReconfigureVoters(stable, []uint64{81}); !errors.Is(err, pressure) {
		t.Fatal("topology bypassed disk pressure", err)
	}
}
