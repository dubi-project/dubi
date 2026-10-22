package server

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/raft"
)

type repairBarrierLocalFake struct {
	index   uint64
	readErr error
	status  raft.GroupStatus
	block   bool
}

func (fake repairBarrierLocalFake) ReadIndex(ctx context.Context, _ string) (uint64, error) {
	if fake.block {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return fake.index, fake.readErr
}

func (fake repairBarrierLocalFake) Status(string) (raft.GroupStatus, error) {
	return fake.status, nil
}

type repairBarrierClientFake struct {
	candidates []uint64
	indices    map[uint64]uint64
	errors     map[uint64]error
	calls      []uint64
}

func (fake *repairBarrierClientFake) GroupCandidates(string) []uint64 {
	return append([]uint64(nil), fake.candidates...)
}

func (fake *repairBarrierClientFake) ReadIndex(_ context.Context, target uint64, _ string) (uint64, error) {
	fake.calls = append(fake.calls, target)
	return fake.indices[target], fake.errors[target]
}

func TestReadRepairOwnerIndexUsesLocalBarrierFirst(t *testing.T) {
	client := &repairBarrierClientFake{candidates: []uint64{11, 12}}
	index, err := readRepairOwnerIndex(context.Background(), repairBarrierLocalFake{index: 7}, client, 10, "region-a")
	if err != nil || index != 7 || len(client.calls) != 0 {
		t.Fatalf("local Owner read = %d, calls=%v, err=%v", index, client.calls, err)
	}
}

func TestReadRepairOwnerIndexFallsBackToLeaderThenMembers(t *testing.T) {
	localErr := raft.ErrNoLeader
	client := &repairBarrierClientFake{candidates: []uint64{11, 12, 13},
		indices: map[uint64]uint64{12: 19}, errors: map[uint64]error{11: raft.ErrNoLeader}}
	local := repairBarrierLocalFake{readErr: localErr, status: raft.GroupStatus{LeaderID: 12}}
	index, err := readRepairOwnerIndex(context.Background(), local, client, 10, "region-a")
	if err != nil || index != 19 || !reflect.DeepEqual(client.calls, []uint64{12}) {
		t.Fatalf("remote leader read = %d, calls=%v, err=%v", index, client.calls, err)
	}
}

func TestReadRepairOwnerIndexTriesRemainingMembersAfterStaleLeader(t *testing.T) {
	client := &repairBarrierClientFake{candidates: []uint64{11, 12, 13},
		indices: map[uint64]uint64{13: 23},
		errors:  map[uint64]error{11: raft.ErrNoLeader, 12: raft.ErrNoLeader}}
	local := repairBarrierLocalFake{readErr: raft.ErrNoLeader, status: raft.GroupStatus{LeaderID: 11}}
	index, err := readRepairOwnerIndex(context.Background(), local, client, 10, "region-a")
	if err != nil || index != 23 || !reflect.DeepEqual(client.calls, []uint64{11, 12, 13}) {
		t.Fatalf("fallback read = %d, calls=%v, err=%v", index, client.calls, err)
	}
}

func TestReadRepairOwnerIndexFallsBackWhenLocalReadStalls(t *testing.T) {
	client := &repairBarrierClientFake{candidates: []uint64{11, 12},
		indices: map[uint64]uint64{12: 23}}
	local := repairBarrierLocalFake{block: true, status: raft.GroupStatus{LeaderID: 12}}
	started := time.Now()
	index, err := readRepairOwnerIndex(context.Background(), local, client, 10, "region-a")
	if err != nil || index != 23 || !reflect.DeepEqual(client.calls, []uint64{12}) {
		t.Fatalf("stalled local read fallback = %d, calls=%v, err=%v", index, client.calls, err)
	}
	if elapsed := time.Since(started); elapsed >= repairLocalGroupReadAttemptTimeout+repairGroupReadAttemptTimeout {
		t.Fatalf("fallback waited for the whole repair deadline: %s", elapsed)
	}
}

func TestReadRepairOwnerIndexPreservesFailureWhenNoMemberCanConfirm(t *testing.T) {
	localErr := raft.ErrNoLeader
	client := &repairBarrierClientFake{candidates: []uint64{11, 12}, errors: map[uint64]error{11: raft.ErrNoLeader, 12: errors.New("offline")}}
	index, err := readRepairOwnerIndex(context.Background(), repairBarrierLocalFake{readErr: localErr}, client, 10, "region-a")
	if index != 0 || !errors.Is(err, raft.ErrNoLeader) || err == nil {
		t.Fatalf("unavailable Owner read = %d, err=%v", index, err)
	}
}
