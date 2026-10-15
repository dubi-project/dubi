package logstore

import (
	"bytes"
	"errors"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestStoragePersistsCanonicalProtobufEntry(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close() }()
	entry := entry(1, 1, string(bytes.Repeat([]byte("raft-entry-"), 8192)))
	if err = storage.Save(nil, []*raftpb.Entry{entry}, nil); err != nil {
		t.Fatal(err)
	}
	want, err := proto.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	stored, closer, err := storage.db.Get(entryKey(entry.GetIndex()))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, want) {
		_ = closer.Close()
		t.Fatal("deferred Raft entry encoding changed the persisted protobuf format")
	}
	if err = closer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageSaveSnapshotCompactAndRecover(t *testing.T) {
	root := t.TempDir()
	storage, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	hardState, confState, err := storage.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if hardState != nil || confState == nil {
		t.Fatalf("unexpected initial state: hard=%v conf=%v", hardState, confState)
	}
	if first, _ := storage.FirstIndex(); first != 1 {
		t.Fatalf("initial first index = %d, want 1", first)
	}
	if last, _ := storage.LastIndex(); last != 0 {
		t.Fatalf("initial last index = %d, want 0", last)
	}

	entries := []*raftpb.Entry{
		entry(1, 1, "one"),
		entry(2, 2, "two"),
		entry(3, 2, "three"),
	}
	hard := hard(2, 7, 3)
	if err := storage.Save(hard, entries, nil); err != nil {
		t.Fatal(err)
	}
	limited, err := storage.Entries(1, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 1 || limited[0].GetIndex() != 1 {
		t.Fatalf("size-limited entries = %+v, want only index 1", limited)
	}

	conf := &raftpb.ConfState{Voters: []uint64{7, 8, 9}}
	snapshot := &raftpb.Snapshot{
		Data: []byte("state-machine-snapshot"),
		Metadata: &raftpb.SnapshotMetadata{
			Index:     new(uint64(2)),
			Term:      new(uint64(2)),
			ConfState: conf,
		},
	}
	if err := storage.Save(nil, nil, snapshot); err != nil {
		t.Fatal(err)
	}
	if first, _ := storage.FirstIndex(); first != 3 {
		t.Fatalf("first index after snapshot = %d, want 3", first)
	}
	if term, err := storage.Term(2); err != nil || term != 2 {
		t.Fatalf("snapshot boundary term = %d, %v", term, err)
	}
	if _, err := storage.Entries(2, 3, 1<<20); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("compacted Entries error = %v", err)
	}
	if err := storage.Compact(3); err != nil {
		t.Fatal(err)
	}
	if first, _ := storage.FirstIndex(); first != 4 {
		t.Fatalf("first index after compaction = %d, want 4", first)
	}
	if term, err := storage.Term(3); err != nil || term != 2 {
		t.Fatalf("compaction boundary term = %d, %v", term, err)
	}
	if err := storage.Save(nil, []*raftpb.Entry{entry(4, 3, "four")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	recoveredHard, recoveredConf, err := reopened.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(recoveredHard, hard) || !proto.Equal(recoveredConf, conf) {
		t.Fatalf("recovered state differs: hard=%v conf=%v", recoveredHard, recoveredConf)
	}
	recoveredSnapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(recoveredSnapshot, snapshot) {
		t.Fatalf("recovered snapshot differs: %v", recoveredSnapshot)
	}
	if first, _ := reopened.FirstIndex(); first != 4 {
		t.Fatalf("recovered first index = %d, want 4", first)
	}
	if last, _ := reopened.LastIndex(); last != 4 {
		t.Fatalf("recovered last index = %d, want 4", last)
	}
	recovered, err := reopened.Entries(4, 5, 1<<20)
	if err != nil || len(recovered) != 1 || !proto.Equal(recovered[0], entry(4, 3, "four")) {
		t.Fatalf("recovered entries = %v, %v", recovered, err)
	}
}

func TestStorageOverwritesConflictingSuffix(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close() }()
	if err := storage.Save(nil, []*raftpb.Entry{
		entry(1, 1, "one"), entry(2, 1, "old-two"), entry(3, 1, "old-three"),
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := storage.Save(nil, []*raftpb.Entry{
		entry(2, 2, "new-two"), entry(3, 2, "new-three"), entry(4, 2, "four"),
	}, nil); err != nil {
		t.Fatal(err)
	}
	entries, err := storage.Entries(1, 5, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 || string(entries[1].GetData()) != "new-two" || entries[3].GetIndex() != 4 {
		t.Fatalf("unexpected overwritten entries: %+v", entries)
	}
	if err := storage.Save(nil, []*raftpb.Entry{entry(2, 3, "short-suffix")}, nil); err != nil {
		t.Fatal(err)
	}
	if last, _ := storage.LastIndex(); last != 2 {
		t.Fatalf("last index after suffix truncation = %d, want 2", last)
	}
	if _, err := storage.Entries(3, 4, 1<<20); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("truncated suffix error = %v", err)
	}
}

func TestStorageRejectsGapsAndStateRegression(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close() }()
	if err := storage.Save(hard(1, 1, 1), []*raftpb.Entry{entry(1, 1, "one")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := storage.Save(nil, []*raftpb.Entry{entry(3, 2, "gap")}, nil); err == nil {
		t.Fatal("log gap unexpectedly succeeded")
	}
	if err := storage.Save(hard(0, 0, 0), nil, nil); err != nil {
		t.Fatalf("empty HardState should be ignored: %v", err)
	}
	if err := storage.Save(hard(2, 1, 2), nil, nil); err == nil {
		t.Fatal("commit beyond last index unexpectedly succeeded")
	}
	if err := storage.Save(hard(1, 1, 0), nil, nil); err == nil {
		t.Fatal("commit regression unexpectedly succeeded")
	}
}

func TestStorageSmallByteLimitDoesNotPreallocateWholeLog(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	var entries []*raftpb.Entry
	for index := uint64(1); index <= 1000; index++ {
		entries = append(entries, entry(index, 1, "data"))
	}
	if err := storage.Save(nil, entries, nil); err != nil {
		t.Fatal(err)
	}
	for _, bound := range []uint64{0, 1, uint64(proto.Size(entries[0]))} {
		got, err := storage.Entries(1, 1001, bound)
		if err != nil || len(got) != 1 || cap(got) > 64 {
			t.Fatal("byte-limited read allocated the full range", len(got), cap(got), err)
		}
	}
}

func TestLocalSnapshotPreservesNewerMembershipAndLogSuffix(t *testing.T) {
	root := t.TempDir()
	storage, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	if err := storage.Save(hard(1, 1, 3), []*raftpb.Entry{entry(1, 1, "one"), entry(2, 1, "two"), entry(3, 1, "three")}, nil); err != nil {
		t.Fatal(err)
	}
	oldConf := &raftpb.ConfState{Voters: []uint64{1}}
	newConf := &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	if err := storage.SaveConfState(newConf); err != nil {
		t.Fatal(err)
	}
	snapshot := &raftpb.Snapshot{Data: []byte("captured-at-two"), Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(2), Term: proto.Uint64(1), ConfState: oldConf}}
	if err := storage.SaveLocalSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	_, current, err := storage.InitialState()
	if err != nil || !proto.Equal(current, newConf) {
		t.Fatal("local snapshot rolled back membership", current, err)
	}
	stored, err := storage.Snapshot()
	if err != nil || !proto.Equal(stored, snapshot) {
		t.Fatal("snapshot lost its historical membership", err)
	}
	suffix, err := storage.Entries(3, 4, 1024)
	if err != nil || len(suffix) != 1 || string(suffix[0].GetData()) != "three" {
		t.Fatal("newer log suffix lost", err)
	}
	for _, invalid := range []*raftpb.Snapshot{
		{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(4), Term: proto.Uint64(1), ConfState: newConf}},
		{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(3), Term: proto.Uint64(2), ConfState: newConf}},
	} {
		if err := storage.SaveLocalSnapshot(invalid); err == nil {
			t.Fatal("invalid local cut accepted")
		}
	}
	if after, _ := storage.Snapshot(); !proto.Equal(after, stored) {
		t.Fatal("rejection changed snapshot")
	}
}

func entry(index, term uint64, data string) *raftpb.Entry {
	return &raftpb.Entry{Index: new(index), Term: new(term), Data: []byte(data)}
}

func hard(term, vote, commit uint64) *raftpb.HardState {
	return &raftpb.HardState{Term: new(term), Vote: new(vote), Commit: new(commit)}
}
