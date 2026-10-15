package logstore

import (
	"errors"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3/raftpb"
)

func TestGroupFormationPersistsBeforeBootstrapAndCannotChange(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	peers, err := s.BindGroup("regional", 2, []uint64{3, 2, 1}, false)
	if err != nil || !slices.Equal(peers, []uint64{1, 2, 3}) {
		t.Fatal(peers, err)
	}
	peers[0] = 999
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	peers, err = s.BindGroup("regional", 2, nil, false)
	if err != nil || !slices.Equal(peers, []uint64{1, 2, 3}) {
		t.Fatal("lost bootstrap intent", peers, err)
	}
	for _, tc := range []struct {
		group   string
		id      uint64
		peers   []uint64
		joining bool
	}{
		{"other", 2, nil, false}, {"regional", 3, nil, false}, {"regional", 2, []uint64{2}, false}, {"regional", 2, nil, true}, {"regional", 2, []uint64{1, 2, 2}, false},
	} {
		if _, err := s.BindGroup(tc.group, tc.id, tc.peers, tc.joining); err == nil {
			t.Fatal("changed formation accepted", tc)
		}
	}
	if err := s.Save(hard(1, 2, 1), []*raftpb.Entry{entry(1, 1, "bootstrap")}, nil); err != nil {
		t.Fatal(err)
	}
	if peers, err := s.BindGroup("regional", 2, nil, false); err != nil || len(peers) != 0 {
		t.Fatal("already started Group requested bootstrap", peers, err)
	}
}

func TestGroupFormationLearnerCannotBootstrap(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if peers, err := s.BindGroup("regional", 2, nil, true); err != nil || len(peers) != 0 {
		t.Fatal(peers, err)
	}
	s.Close()
	s, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.BindGroup("regional", 2, []uint64{2}, false); err == nil {
		t.Fatal("learner bootstrapped new Group")
	}
	if peers, err := s.BindGroup("regional", 2, nil, false); err != nil || len(peers) != 0 {
		t.Fatal(peers, err)
	}
}

func TestGroupFormationRejectsHistoryWithoutIntentAndLostStartedLog(t *testing.T) {
	for _, intent := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbound-history", true: "started-log-missing"}[intent], func(t *testing.T) {
			root := t.TempDir()
			s, err := Open(root)
			if err != nil {
				t.Fatal(err)
			}
			if intent {
				if _, err := s.BindGroup("regional", 1, []uint64{1}, false); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Save(hard(1, 1, 1), []*raftpb.Entry{entry(1, 1, "first")}, nil); err != nil {
				t.Fatal(err)
			}
			if intent {
				b := s.db.NewBatch()
				if err := errors.Join(b.DeleteRange(entryPrefix, entryUpper, nil), b.Delete(hardStateKey, nil)); err != nil {
					t.Fatal(err)
				}
				if err := b.Commit(pebble.Sync); err != nil {
					t.Fatal(err)
				}
				b.Close()
			}
			s.Close()
			s, err = Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if _, err := s.BindGroup("regional", 1, []uint64{1}, false); err == nil {
				t.Fatal("unsafe bootstrap/adoption accepted")
			}
		})
	}
}
