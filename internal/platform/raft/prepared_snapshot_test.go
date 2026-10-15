package raft

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

type preparedTestState struct {
	*testState
	started  chan struct{}
	release  chan struct{}
	buildErr error
	closeErr error
	released atomic.Int32
}

type preparedSnapshotTransport struct{}

func (preparedSnapshotTransport) Send(string, *raftpb.Message) error { return nil }

func (s *preparedTestState) PrepareSnapshot() (*PreparedSnapshot, error) {
	data, index, conf, err := s.testState.Snapshot()
	if err != nil {
		return nil, err
	}
	buildErr, closeErr := s.buildErr, s.closeErr
	return &PreparedSnapshot{Index: index, Conf: conf,
		Build: func(ctx context.Context) ([]byte, error) {
			s.started <- struct{}{}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-s.release:
				return data, buildErr
			}
		}, Close: func() error { s.released.Add(1); return closeErr }}, nil
}

func preparedGroup(t *testing.T) (*Group, *preparedTestState) {
	t.Helper()
	root := t.TempDir()
	state := &preparedTestState{testState: openTestState(t, root), started: make(chan struct{}, 2), release: make(chan struct{})}
	group, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "prepared", RaftID: 1, BootstrapPeers: []uint64{1}, State: state, Transport: preparedSnapshotTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := group.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := group.Campaign(); err != nil {
		t.Fatal(err)
	}
	if err := group.Submit([]byte("before-capture")); err != nil {
		t.Fatal(err)
	}
	return group, state
}

func waitPrepared(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot build did not start")
	}
}

func waitPreparedResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("operation blocked behind snapshot build")
		return nil
	}
}

func TestPreparedSnapshotBuildAllowsWritesReadsAndTicks(t *testing.T) {
	group, state := preparedGroup(t)
	before, err := group.Status()
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(state.release) }) }
	defer unblock()
	result := make(chan error, 1)
	go func() {
		index, err := group.CreateSnapshot()
		if err == nil && index != before.AppliedIndex {
			err = errors.New("snapshot did not preserve captured cut")
		}
		result <- err
	}()
	waitPrepared(t, state.started)
	operations := make(chan error, 1)
	go func() {
		if _, err := group.CreateSnapshot(); !errors.Is(err, ErrSnapshotBusy) {
			operations <- errors.New("concurrent snapshot did not reject")
			return
		}
		if err := group.Submit([]byte("after-capture")); err != nil {
			operations <- err
			return
		}
		if err := group.Tick(); err != nil {
			operations <- err
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := group.ReadIndex(ctx)
		operations <- err
	}()
	if err := waitPreparedResult(t, operations); err != nil {
		t.Fatal(err)
	}
	unblock()
	if err := waitPreparedResult(t, result); err != nil {
		t.Fatal(err)
	}
	if state.released.Load() != 1 {
		t.Fatal("snapshot resources not released exactly once")
	}
	snapshot, err := group.storage.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var captured testStateDisk
	if err := json.Unmarshal(snapshot.GetData(), &captured); err != nil {
		t.Fatal(err)
	}
	if captured.Applied != before.AppliedIndex || len(captured.Commands) != 1 || string(captured.Commands[0]) != "before-capture" {
		t.Fatal("image mixed old and new state", captured)
	}
	current, err := group.Status()
	if err != nil || current.AppliedIndex <= captured.Applied {
		t.Fatal("newer applied state lost", current, err)
	}
	entries, err := group.storage.Entries(captured.Applied+1, current.AppliedIndex+1, 1<<20)
	if err != nil || len(entries) != 1 || string(entries[0].GetData()) != "after-capture" {
		t.Fatal("compaction lost post-capture command", err)
	}
}

func TestPreparedSnapshotCloseCancelsBuilderAndReleasesBeforeStateClose(t *testing.T) {
	group, state := preparedGroup(t)
	result := make(chan error, 1)
	go func() { _, err := group.CreateSnapshot(); result <- err }()
	waitPrepared(t, state.started)
	closed := make(chan error, 2)
	for range 2 {
		go func() { closed <- group.Close() }()
	}
	for range 2 {
		if err := waitPreparedResult(t, closed); err != nil {
			t.Fatal(err)
		}
	}
	if err := waitPreparedResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal("build did not observe cancellation", err)
	}
	if state.released.Load() != 1 || !state.closed {
		t.Fatal("shutdown did not release snapshot and state")
	}
}

func TestPreparedSnapshotBuildAndReleaseErrorsDoNotPoisonConsensus(t *testing.T) {
	for _, phase := range []string{"build", "release"} {
		t.Run(phase, func(t *testing.T) {
			group, state := preparedGroup(t)
			injected := errors.New("injected snapshot error")
			if phase == "build" {
				state.buildErr = injected
			} else {
				state.closeErr = injected
			}
			close(state.release)
			if _, err := group.CreateSnapshot(); !errors.Is(err, injected) {
				t.Fatal("snapshot error lost", err)
			}
			if snapshot, err := group.storage.Snapshot(); err != nil || snapshot.GetMetadata().GetIndex() != 0 {
				t.Fatal("failed build compacted log", err)
			}
			if state.released.Load() != 1 {
				t.Fatal("failed build leaked resources")
			}
			if err := group.Submit([]byte("still-usable")); err != nil {
				t.Fatal("build error poisoned consensus", err)
			}
			state.buildErr, state.closeErr = nil, nil
			if _, err := group.CreateSnapshot(); err != nil {
				t.Fatal("retry failed", err)
			}
			if state.released.Load() != 2 {
				t.Fatal("retry leaked resources")
			}
		})
	}
}

func TestPreparedSnapshotDoesNotRollBackMembershipAppliedDuringBuild(t *testing.T) {
	group, state := preparedGroup(t)
	var once sync.Once
	unblock := func() { once.Do(func() { close(state.release) }) }
	defer unblock()
	result := make(chan error, 1)
	go func() { _, err := group.CreateSnapshot(); result <- err }()
	waitPrepared(t, state.started)
	changed := make(chan error, 1)
	go func() { changed <- group.AddLearner(2) }()
	if err := waitPreparedResult(t, changed); err != nil {
		t.Fatal(err)
	}
	unblock()
	if err := waitPreparedResult(t, result); err != nil {
		t.Fatal(err)
	}
	root := state.root
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenGroup(GroupOptions{StoreRoot: root, GroupID: "prepared", RaftID: 1,
		State: openTestState(t, root), Transport: preparedSnapshotTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	conf, err := reopened.Membership()
	if err != nil || len(conf.GetLearners()) != 1 || conf.GetLearners()[0] != 2 {
		t.Fatal("current membership regressed", conf, err)
	}
	snapshot, err := reopened.storage.Snapshot()
	if err != nil || len(snapshot.GetMetadata().GetConfState().GetLearners()) != 0 {
		t.Fatal("historical cut acquired future membership", snapshot, err)
	}
}
