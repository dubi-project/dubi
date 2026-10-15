package raft

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestHostBackgroundTicksIsolateBlockedGroup(t *testing.T) {
	root := t.TempDir()
	id, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	host, err := OpenHost(HostOptions{StoreRoot: root, TickInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if err := host.AddGroup("slow", []uint64{id.RaftID}, false, hostTestState(t, root, "slow")); err != nil {
		t.Fatal(err)
	}
	slow, err := host.group("slow")
	if err != nil {
		t.Fatal(err)
	}
	// Holding the slow Group lock models snapshot installation/disk IO. Its
	// background worker must not stop the Root election or read barriers.
	slow.mu.Lock()
	defer slow.mu.Unlock()
	if err := host.AddGroup(RootGroupID, []uint64{id.RaftID}, false, hostTestState(t, root, "root")); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := host.Status(RootGroupID)
		if err != nil {
			t.Fatal(err)
		}
		if status.Role == etcdraft.StateLeader {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("blocked Group stopped other Group's background election")
		case <-ticker.C:
		}
	}
	if err := host.Submit(RootGroupID, []byte("independent")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := host.ReadIndex(ctx, RootGroupID); err != nil {
		t.Fatal(err)
	}
	host.mu.RLock()
	busy := host.ticking["slow"]
	host.mu.RUnlock()
	if !busy {
		t.Fatal("slow Group was never scheduled")
	}
}

type openingRestoreState struct {
	*testState
	entered chan struct{}
	release chan struct{}
	closes  atomic.Int32
}

func (s *openingRestoreState) Restore(index uint64, data []byte, conf *raftpb.ConfState) error {
	close(s.entered)
	<-s.release
	return s.testState.Restore(index, data, conf)
}
func (s *openingRestoreState) Close() error {
	s.closes.Add(1)
	return s.testState.Close()
}

func TestHostOpeningRecoveryDoesNotHoldHostLockAndCloseWaits(t *testing.T) {
	root := t.TempDir()
	id, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	host, err := OpenHost(HostOptions{StoreRoot: root, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.AddGroup("recovering", []uint64{id.RaftID}, false, hostTestState(t, root, "original")); err != nil {
		t.Fatal(err)
	}
	if err := host.Campaign("recovering"); err != nil {
		t.Fatal(err)
	}
	if err := host.Submit("recovering", []byte("recover me")); err != nil {
		t.Fatal(err)
	}
	if _, err := host.CreateSnapshot("recovering"); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	host, err = OpenHost(HostOptions{StoreRoot: root, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	state := &openingRestoreState{testState: hostTestState(t, root, "unapplied"), entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(state.release) })
	opened := make(chan error, 1)
	go func() { opened <- host.AddGroup("recovering", nil, false, state) }()
	waitPrepared(t, state.entered)
	duplicate := hostTestState(t, root, "duplicate-opening")
	duplicateResult := make(chan error, 1)
	go func() { duplicateResult <- host.AddGroup("recovering", nil, false, duplicate) }()
	if err := waitPreparedResult(t, duplicateResult); err == nil || duplicate.closed {
		t.Fatal("concurrent opening accepted or caller's state closed", err)
	}
	duplicate.Close()
	rootState := hostTestState(t, root, "root")
	rootOpened := make(chan error, 1)
	go func() { rootOpened <- host.AddGroup(RootGroupID, []uint64{id.RaftID}, false, rootState) }()
	if err := waitPreparedResult(t, rootOpened); err != nil {
		t.Fatal("unrelated Group blocked during recovery", err)
	}
	if err := host.Campaign(RootGroupID); err != nil {
		t.Fatal(err)
	}
	if err := host.Submit(RootGroupID, []byte("available during recovery")); err != nil {
		t.Fatal(err)
	}
	closed, alsoClosed := make(chan error, 1), make(chan error, 1)
	go func() { closed <- host.Close() }()
	// Wait for the shutdown transition without acquiring a recovering Group.
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for host.HasGroup(RootGroupID) {
		select {
		case <-deadline.C:
			t.Fatal("Host shutdown blocked behind opening Group")
		case <-ticker.C:
		}
	}
	go func() { alsoClosed <- host.Close() }()
	select {
	case err := <-closed:
		t.Fatal("Host closed before pending recovery finished", err)
	case err := <-alsoClosed:
		t.Fatal("concurrent Close returned before resource shutdown", err)
	default:
	}
	if other, err := OpenHost(HostOptions{StoreRoot: root}); err == nil {
		other.Close()
		t.Fatal("Store lock released while recovery owned files")
	}
	release.Do(func() { close(state.release) })
	if err := waitPreparedResult(t, opened); !errors.Is(err, ErrHostClosed) {
		t.Fatal("late opening was published after shutdown", err)
	}
	for _, result := range []chan error{closed, alsoClosed} {
		if err := waitPreparedResult(t, result); err != nil {
			t.Fatal(err)
		}
	}
	if state.closes.Load() != 1 {
		t.Fatal("late Group state not closed exactly once", state.closes.Load())
	}
}
