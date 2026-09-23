package logstore

import (
	"bytes"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// This test deliberately uses no File state machine. It verifies that the
// persisted Store can drive an etcd/raft Group through bootstrap and a write.
func TestRawNodeBootstrapCommitAndRecover(t *testing.T) {
	root := t.TempDir()
	storage, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := raft.NewRawNode(&raft.Config{
		ID: 7, ElectionTick: 10, HeartbeatTick: 1, Storage: storage,
		MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 16, CheckQuorum: true, PreVote: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Bootstrap([]raft.Peer{{ID: 7}}); err != nil {
		t.Fatal(err)
	}
	var committed [][]byte
	drain := func() {
		t.Helper()
		for turn := 0; raw.HasReady(); turn++ {
			if turn >= 100 {
				t.Fatal("RawNode Ready did not quiesce")
			}
			ready := raw.Ready()
			if err := storage.Save(ready.HardState, ready.Entries, ready.Snapshot); err != nil {
				t.Fatal(err)
			}
			for _, entry := range ready.CommittedEntries {
				switch entry.GetType() {
				case raftpb.EntryType_EntryConfChange:
					change := new(raftpb.ConfChange)
					if err := proto.Unmarshal(entry.GetData(), change); err != nil {
						t.Fatal(err)
					}
					if err := storage.SaveConfState(raw.ApplyConfChange(change)); err != nil {
						t.Fatal(err)
					}
				case raftpb.EntryType_EntryNormal:
					if len(entry.GetData()) > 0 {
						committed = append(committed, bytes.Clone(entry.GetData()))
					}
				default:
					t.Fatalf("unexpected committed entry type %s", entry.GetType())
				}
			}
			raw.Advance(ready)
		}
	}
	drain()
	if err := raw.Campaign(); err != nil {
		t.Fatal(err)
	}
	drain()
	if raw.BasicStatus().SoftState.RaftState != raft.StateLeader {
		t.Fatal("single-node Group did not elect its leader")
	}
	if err := raw.Propose([]byte("dubi-group-command")); err != nil {
		t.Fatal(err)
	}
	drain()
	if len(committed) != 1 || string(committed[0]) != "dubi-group-command" {
		t.Fatalf("committed commands = %q", committed)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	hard, conf, err := reopened.InitialState()
	if err != nil || hard.GetCommit() == 0 || len(conf.GetVoters()) != 1 || conf.GetVoters()[0] != 7 {
		t.Fatalf("recovered Raft state = hard %v conf %v err %v", hard, conf, err)
	}
	last, err := reopened.LastIndex()
	if err != nil || last < hard.GetCommit() {
		t.Fatalf("recovered last index = %d, %v; commit %d", last, err, hard.GetCommit())
	}
}
