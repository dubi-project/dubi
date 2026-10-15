package raft

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/raft/logstore"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// An imported snapshot can precede AddLearner. A process can die after
// persisting the snapshot + committed membership tail but before applying
// that tail. The historical cut alone must not classify the local Node as a
// removed member; nor may an uncommitted tail silently authorize membership.
func TestGroupRecoveryReplaysMembershipAfterHistoricalSnapshot(t *testing.T) {
	for _, persistedCut := range []bool{false, true} {
		for _, tc := range []struct {
			name                                 string
			joining, commitTail, removed, wantOK bool
		}{
			{name: "committed-add", commitTail: true, wantOK: true},
			{name: "joining-committed-add", joining: true, commitTail: true, wantOK: true},
			{name: "authorized-wait-for-add", joining: true, wantOK: true},
			{name: "uncommitted-add-is-not-membership"},
			{name: "committed-removal-stays-removed", removed: true, commitTail: true},
		} {
			t.Run(fmt.Sprintf("%s/persisted-cut=%v", tc.name, persistedCut), func(t *testing.T) {
				store := t.TempDir()
				root, err := PrepareGroupRoot(store, "joining")
				if err != nil {
					t.Fatal(err)
				}
				conf := &raftpb.ConfState{Voters: []uint64{1}}
				operation := raftpb.ConfChangeType_ConfChangeAddLearnerNode
				if tc.removed {
					conf.Learners = []uint64{2}
					operation = raftpb.ConfChangeType_ConfChangeRemoveNode
				}
				encodedConf, err := proto.Marshal(conf)
				if err != nil {
					t.Fatal(err)
				}
				cut, err := json.Marshal(testStateDisk{Applied: 4, Conf: encodedConf, Commands: [][]byte{[]byte("before-join")}})
				if err != nil {
					t.Fatal(err)
				}
				image := &raftpb.Snapshot{Data: cut, Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(4), Term: proto.Uint64(1), ConfState: conf}}
				change, err := proto.Marshal(&raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{Type: operation.Enum(), NodeId: proto.Uint64(2)}}})
				if err != nil {
					t.Fatal(err)
				}
				entry := &raftpb.Entry{Index: proto.Uint64(5), Term: proto.Uint64(1), Type: raftpb.EntryType_EntryConfChangeV2.Enum(), Data: change}
				commit := uint64(4)
				if tc.commitTail {
					commit = 5
				}
				storage, err := logstore.Open(root)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := storage.BindGroup("joining", 2, nil, true); err != nil {
					t.Fatal(err)
				}
				if err := storage.Save(&raftpb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(commit)}, []*raftpb.Entry{entry}, image); err != nil {
					_ = storage.Close()
					t.Fatal(err)
				}
				if err := storage.Close(); err != nil {
					t.Fatal(err)
				}
				state := openTestState(t, store)
				if persistedCut {
					if err := state.Restore(4, cut, conf); err != nil {
						t.Fatal(err)
					}
				}
				group, err := OpenGroup(GroupOptions{StoreRoot: store, GroupID: "joining", RaftID: 2,
					Joining: tc.joining, State: state, Transport: preparedSnapshotTransport{}})
				if !tc.wantOK {
					if err == nil {
						_ = group.Close()
						t.Fatal("non-member recovered as an authorized member")
					}
					if !state.closed {
						t.Fatal("rejected recovery leaked state")
					}
					return
				}
				if err != nil {
					t.Fatal("historical snapshot prevented authorized membership recovery", err)
				}
				defer group.Close()
				if state.AppliedIndex() != commit || containsMember(state.ConfState(), 2) != tc.commitTail ||
					len(state.disk.Commands) != 1 || string(state.disk.Commands[0]) != "before-join" {
					t.Fatal("membership/data recovery differs from committed tail", state.disk)
				}
			})
		}
	}
}
