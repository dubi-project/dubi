package capstate

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type offlineMembershipTransport struct{}

func (offlineMembershipTransport) Send(string, *raftpb.Message) error { return nil }

func TestMembershipCutRaftRejectsABAAfterReopenWithoutRejectingOrdinaryWrites(t *testing.T) {
	store := t.TempDir()
	open := func() (*State, *raft.Group) {
		s, err := Open(filepath.Join(store, "state"), "data-1", &testHandler{})
		if err != nil {
			t.Fatal(err)
		}
		a, err := routedstate.New(s)
		if err != nil {
			t.Fatal(err)
		}
		g, err := raft.OpenGroup(raft.GroupOptions{StoreRoot: store, GroupID: "data-1", RaftID: 71, BootstrapPeers: []uint64{71}, State: a, Transport: offlineMembershipTransport{}})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Campaign(); err != nil {
			t.Fatal(err)
		}
		return s, g
	}
	s, g := open()
	cut, err := g.MembershipCut()
	if err != nil {
		t.Fatal(err)
	}
	activation, err := routedstate.EncodeActivateRange("range", rootstate.Range{RangeID: "r", TenantID: "tenant", CapabilityID: "record", DatasetID: "data", OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Submit(activation); err != nil {
		t.Fatal(err)
	}
	if s.AppliedIndex() <= cut.Index {
		t.Fatal("ordinary command not applied")
	}
	if err := g.ChangeMembership(cut, 72, raftpb.ConfChangeType_ConfChangeAddLearnerNode); err != nil {
		t.Fatal("ordinary write invalidated membership-only CAS", err)
	}
	before, err := g.MembershipCut()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.ChangeMembership(before, 72, raftpb.ConfChangeType_ConfChangeRemoveNode); err != nil {
		t.Fatal(err)
	}
	removed, err := g.MembershipCut()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.ChangeMembership(removed, 72, raftpb.ConfChangeType_ConfChangeAddLearnerNode); err != nil {
		t.Fatal(err)
	}
	after, err := g.MembershipCut()
	if err != nil || after.Index <= before.Index || !proto.Equal(after.Conf, before.Conf) {
		t.Fatal(before, after, err)
	}
	for round := 0; round < 2; round++ {
		applied := s.AppliedIndex()
		if err := g.ChangeMembership(before, 72, raftpb.ConfChangeType_ConfChangeRemoveNode); !errors.Is(err, raft.ErrMembershipChanged) {
			t.Fatal("ABA accepted", err)
		}
		if s.AppliedIndex() != applied {
			t.Fatal("rejected change advanced log/state")
		}
		if round == 0 {
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			s, g = open()
		}
	}
	defer g.Close()
	current, err := g.MembershipCut()
	if err != nil || current.Index != after.Index {
		t.Fatal(current, after, err)
	}
	if err := g.ChangeMembership(current, 72, raftpb.ConfChangeType_ConfChangeRemoveNode); err != nil {
		t.Fatal("fresh cut rejected", err)
	}
}
