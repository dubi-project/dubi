package server

import (
	"context"
	"errors"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/raft"
)

type rootControlLocalFake struct {
	status    raft.GroupStatus
	statusErr error
	submits   int
	reads     int
	submitErr error
}

func (f *rootControlLocalFake) Submit(string, []byte) error { f.submits++; return f.submitErr }
func (f *rootControlLocalFake) ReadIndex(context.Context, string) (uint64, error) {
	f.reads++
	return f.status.AppliedIndex, nil
}
func (f *rootControlLocalFake) Status(string) (raft.GroupStatus, error) { return f.status, f.statusErr }

type rootControlClientFake struct {
	submits []uint64
	reads   []uint64
	err     error
}

func (f *rootControlClientFake) Submit(_ context.Context, target uint64, _ string, _ []byte) (uint64, error) {
	f.submits = append(f.submits, target)
	return 7, f.err
}
func (f *rootControlClientFake) ReadIndex(_ context.Context, target uint64, _ string) (uint64, error) {
	f.reads = append(f.reads, target)
	return 8, f.err
}

func TestRootControlHostForwardsFollowerRootWrites(t *testing.T) {
	local := &rootControlLocalFake{status: raft.GroupStatus{RaftID: 11, LeaderID: 22}}
	remote := &rootControlClientFake{}
	host, err := newRootControlHost(local, remote, 11)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Submit(raft.RootGroupID, []byte("command")); err != nil {
		t.Fatal(err)
	}
	if len(remote.submits) != 1 || remote.submits[0] != 22 {
		t.Fatalf("forwarded targets = %v, want [22]", remote.submits)
	}
	if local.submits != 0 {
		t.Fatalf("follower local submits = %d, want 0", local.submits)
	}
}

func TestRootControlHostUsesLocalLeaderAndForwardsRead(t *testing.T) {
	local := &rootControlLocalFake{status: raft.GroupStatus{RaftID: 11, LeaderID: 11, AppliedIndex: 3}}
	remote := &rootControlClientFake{}
	host, err := newRootControlHost(local, remote, 11)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Submit(raft.RootGroupID, []byte("command")); err != nil {
		t.Fatal(err)
	}
	if local.submits != 1 || len(remote.submits) != 0 {
		t.Fatalf("local submits=%d remote submits=%v", local.submits, remote.submits)
	}
	local.status.LeaderID = 22
	if _, err := host.ReadIndex(context.Background(), raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	if len(remote.reads) != 1 || remote.reads[0] != 22 {
		t.Fatalf("forwarded reads = %v, want [22]", remote.reads)
	}
}

func TestRootControlHostRetriesRefreshedLeaderAfterStaleSubmit(t *testing.T) {
	local := &rootControlLocalFake{status: raft.GroupStatus{RaftID: 11, LeaderID: 11}, submitErr: errors.New("not leader")}
	remote := &rootControlClientFake{}
	host, err := newRootControlHost(local, remote, 11)
	if err != nil {
		t.Fatal(err)
	}
	// The fake keeps the same status, so no target is available for the retry.
	if err := host.Submit(raft.RootGroupID, []byte("command")); err == nil {
		t.Fatal("stale local error was swallowed")
	}
}
