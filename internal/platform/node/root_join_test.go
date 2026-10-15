package node

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/capabilityruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
	"go.etcd.io/raft/v3/raftpb"
)

// These model inputs isolate the startup fence; the Server integration test
// separately obtains its real receipt through enrollment over mTLS.
func joinPositionState() rootjoin.LocalState {
	return rootjoin.LocalState{Version: 1, ClusterID: "cluster_" + strings.Repeat("1", 32),
		NodeID: "node_" + strings.Repeat("2", 32), RaftID: 2, StoreID: "store_" + strings.Repeat("3", 32),
		GrantID: "join_" + strings.Repeat("4", 32), RequestSHA256: strings.Repeat("5", 64),
		AdmittedIndex: 10, InitialViewIndex: 12, Phase: rootjoin.LocalCatchingUp,
		View: rootjoin.View{ClusterID: "cluster_" + strings.Repeat("1", 32), AppliedIndex: 12,
			Voters: []uint64{1}, Learners: []uint64{2}, Peers: []rootstate.Node{
				{NodeID: "node_" + strings.Repeat("1", 32), RaftID: 1, RegionID: "r1", Address: "leader:7443"},
				{NodeID: "node_" + strings.Repeat("2", 32), RaftID: 2, RegionID: "r2", Address: "learner:7443"}}}}
}

func TestRootJoinStartupWaitsForAppliedAdmissionNotHeartbeatOrMembershipAlone(t *testing.T) {
	local := joinPositionState()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	indices := []uint64{0, 5, 10, 11, 12}
	calls := 0
	err := waitRootJoinPosition(ctx, local, func() (uint64, *raftpb.ConfState, error) {
		index := indices[min(calls, len(indices)-1)]
		calls++
		conf := &raftpb.ConfState{}
		if index > 0 {
			conf.Voters, conf.Learners = []uint64{1}, []uint64{2}
		}
		return index, conf, nil
	})
	if err != nil || calls != len(indices) {
		t.Fatal("startup did not wait for confirmed cut", calls, err)
	}
}

func TestRootJoinStartupFenceRejectsRemovalDamageAndCancellation(t *testing.T) {
	storageErr := errors.New("injected Root read failure")
	for _, tc := range []struct {
		name          string
		active        bool
		index         uint64
		conf          *raftpb.ConfState
		readErr, want error
	}{
		{name: "removed", index: 10, conf: &raftpb.ConfState{Voters: []uint64{1}}, want: rootjoin.ErrMemberRemoved},
		{name: "active-behind", active: true, index: 11, conf: &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}, want: rootjoin.ErrRecoveryBehind},
		{name: "storage", readErr: storageErr, want: storageErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			local := joinPositionState()
			if tc.active {
				local.Phase, local.ActivatedIndex = rootjoin.LocalActive, 12
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := waitRootJoinPosition(ctx, local, func() (uint64, *raftpb.ConfState, error) { return tc.index, tc.conf, tc.readErr })
			if !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := waitRootJoinPosition(ctx, joinPositionState(), func() (uint64, *raftpb.ConfState, error) {
		calls++
		cancel()
		return 0, &raftpb.ConfState{}, nil
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal("catch-up wait ignored cancellation", calls, err)
	}
}

func TestRootJoinStartupBarrierRetriesLostReplyAndElectionOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	err := confirmStartupRoot(ctx, func(attempt context.Context) error {
		calls++
		switch calls {
		case 1:
			return raft.ErrNoLeader
		case 2:
			<-attempt.Done() // A dropped response, not a successful barrier.
			return attempt.Err()
		default:
			return nil
		}
	}, time.Millisecond)
	if err != nil || calls != 3 {
		t.Fatal(calls, err)
	}
	for _, fault := range []error{errors.New("storage failure"), context.DeadlineExceeded} {
		calls = 0
		err := confirmStartupRoot(ctx, func(context.Context) error { calls++; return fault }, time.Millisecond)
		if !errors.Is(err, fault) || calls != 1 {
			t.Fatal("masked non-attempt failure", calls, err)
		}
	}
	stopping, stop := context.WithCancel(context.Background())
	err = confirmStartupRoot(stopping, func(context.Context) error { stop(); return raft.ErrNoLeader }, time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("shutdown retried", err)
	}
}

func TestRootJoinStartupRecoveryCancellationJoinsRuntimeClose(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	registry := capabilityruntime.NewRegistry()
	if err := registry.Register("record", &nodeTestHandler{}); err != nil {
		t.Fatal(err)
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := Open(Options{StoreRoot: store, TickInterval: time.Hour, RootBootstrapPeers: []uint64{id.RaftID}, CapabilityMux: mux})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	done := make(chan error, 1)
	go func() { done <- runtime.RecoverLocalGroups(context.Background()) }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for runtime.recoverMu.TryLock() {
		runtime.recoverMu.Unlock()
		select {
		case <-deadline.C:
			t.Fatal("recovery did not start")
		case <-time.After(time.Millisecond):
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- runtime.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.C:
		t.Fatal("Close did not cancel background-context startup")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("startup succeeded without leader")
		}
	case <-time.After(time.Second):
		t.Fatal("recovery survived Close")
	}
}

func TestRootJoinStartupBeginShutdownCancelsWithoutReleasingStore(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	mux, err := capabilityruntime.NewRegistry().Seal()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := Open(Options{StoreRoot: store, TickInterval: time.Hour, RootBootstrapPeers: []uint64{id.RaftID}, CapabilityMux: mux})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	runtime.BeginShutdown()
	runtime.BeginShutdown()
	if !errors.Is(runtime.recoveryContext.Err(), context.Canceled) {
		t.Fatal("node restoration not canceled")
	}
	if _, err := runtime.Host.Status(raft.RootGroupID); err != nil {
		t.Fatal("resources closed before handlers joined", err)
	}
	if lock, err := storelock.Acquire(store); err == nil {
		lock.Release()
		t.Fatal("shutdown prelude released Store")
	}
	if err := runtime.RecoverLocalGroups(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatal("new recovery ignored shutdown", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	lock, err := storelock.Acquire(store)
	if err != nil {
		t.Fatal(err)
	}
	lock.Release()
}
