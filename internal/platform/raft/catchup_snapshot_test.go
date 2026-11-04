package raft

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

func readyCatchupGroup(t *testing.T) (*Group, *preparedTestState) {
	t.Helper()
	g, s := preparedGroup(t)
	close(s.release)
	for i := 0; i < 20; i++ {
		if err := g.Submit([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	return g, s
}

func TestCatchupSnapshotOnlyForLaggingParticipantAndNewLogGap(t *testing.T) {
	g, s := readyCatchupGroup(t)
	if built, err := g.snapshotForCatchup(8); built || err != nil {
		t.Fatal("healthy single voter created an image", built, err)
	}
	if err := g.AddLearner(2); err != nil {
		t.Fatal(err)
	}
	if built, err := g.snapshotForCatchup(catchupSnapshotEntries); built || err != nil {
		t.Fatal("small backlog created an image", built, err)
	}
	if built, err := g.snapshotForCatchup(8); !built || err != nil {
		t.Fatal("lagging learner did not trigger compaction", built, err)
	}
	image, err := g.storage.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	first, err := g.storage.FirstIndex()
	if err != nil || first != image.GetMetadata().GetIndex()+1 || first <= 20 {
		t.Fatal("successful image did not compact the persisted log", first, err)
	}
	if built, err := g.snapshotForCatchup(8); built || err != nil || s.released.Load() != 1 {
		t.Fatal("same backlog repeatedly built images", built, err, s.released.Load())
	}
}

func TestCatchupSnapshotFailurePreservesLogAndRetries(t *testing.T) {
	for _, mode := range []string{"build", "admission"} {
		t.Run(mode, func(t *testing.T) {
			g, s := readyCatchupGroup(t)
			if err := g.AddLearner(2); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected checkpoint failure")
			if mode == "build" {
				s.buildErr = injected
			} else {
				g.proposalGuard = func() error { return injected }
			}
			if built, err := g.snapshotForCatchup(8); built || !errors.Is(err, injected) {
				t.Fatal("failed image reported success", built, err)
			}
			if first, err := g.storage.FirstIndex(); err != nil || first != 1 {
				t.Fatal("failed image discarded history", first, err)
			}
			if err := g.Submit([]byte("still-writable")); mode == "build" && err != nil {
				t.Fatal("snapshot build error poisoned Raft", err)
			}
			s.buildErr, g.proposalGuard = nil, nil
			if built, err := g.snapshotForCatchup(8); !built || err != nil {
				t.Fatal("snapshot did not retry after recovery", built, err)
			}
		})
	}
}

func TestCatchupSnapshotDoesNotRebuildBeforeCoveredMemberAdvances(t *testing.T) {
	g, s := readyCatchupGroup(t)
	if err := g.AddLearner(2); err != nil {
		t.Fatal(err)
	}
	if built, err := g.snapshotForCatchup(8); !built || err != nil {
		t.Fatal(built, err)
	}
	for i := 0; i < 20; i++ {
		if err := g.Submit([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if built, err := g.snapshotForCatchup(8); built || err != nil || s.released.Load() != 1 {
		t.Fatal("unchanged offline participant accumulated retained images", built, err, s.released.Load())
	}
	// A different, freshly admitted identity must not inherit the old
	// participant's suppression: it needs the current state, not a large tail.
	if err := g.AddLearner(3); err != nil {
		t.Fatal(err)
	}
	if built, err := g.snapshotForCatchup(8); !built || err != nil || s.released.Load() != 2 {
		t.Fatal("new identity did not refresh the old image", built, err, s.released.Load())
	}
}

func TestCatchupSnapshotRefreshesHistoricalImageForNewMemberWithShortTail(t *testing.T) {
	for _, groupID := range []string{RootGroupID, "regional"} {
		for _, inFlight := range []bool{false, true} {
			name := groupID + "/probe"
			if inFlight {
				name = groupID + "/old-snapshot-in-flight"
			}
			t.Run(name, func(t *testing.T) {
				transport := &queuedTransport{groups: make(map[uint64]*Group), blocked: make(map[uint64]bool), groupID: groupID}
				states := make(map[uint64]*preparedTestState)
				for _, id := range []uint64{71, 72} {
					root := t.TempDir()
					state := &preparedTestState{testState: openTestState(t, root), started: make(chan struct{}, 3), release: make(chan struct{})}
					close(state.release)
					options := GroupOptions{StoreRoot: root, GroupID: groupID, RaftID: id, State: state, Transport: transport, Joining: id == 72}
					if id == 71 {
						options.BootstrapPeers = []uint64{71}
					}
					g, err := OpenGroup(options)
					if err != nil {
						t.Fatal(err)
					}
					transport.groups[id], states[id] = g, state
					t.Cleanup(func() { _ = g.Close() })
				}
				leader, learner := transport.groups[71], transport.groups[72]
				if err := leader.Campaign(); err != nil {
					t.Fatal(err)
				}
				transport.deliver(t)
				for i := 0; i < 40; i++ {
					if err := leader.Submit([]byte{byte(i)}); err != nil {
						t.Fatal(err)
					}
				}
				// The old durable image predates admission. Only ONE committed
				// membership entry follows it, much less than the refresh limit.
				if _, err := leader.CreateSnapshot(); err != nil {
					t.Fatal(err)
				}
				old, err := leader.storage.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				if err := leader.AddLearner(72); err != nil {
					t.Fatal(err)
				}
				if states[71].AppliedIndex()-old.GetMetadata().GetIndex() != 1 {
					t.Fatal("fixture does not exercise the short membership tail")
				}
				var pending *raftpb.Message
				if inFlight {
					// Deliver the initial append rejection until Raft sends the
					// historical image, but leave its actual RPC completion pending.
					for turn := 0; turn < 100 && len(transport.queue) != 0; turn++ {
						item := transport.queue[0]
						transport.queue = transport.queue[1:]
						if item.message.GetType() == raftpb.MessageType_MsgSnap {
							pending = item.message
							break
						}
						if err := transport.groups[item.message.GetTo()].Step(item.message); err != nil {
							t.Fatal(err)
						}
					}
					if pending == nil || leader.raw.Status().Progress[72].State != tracker.StateSnapshot {
						t.Fatal("fixture did not enter an old snapshot transfer")
					}
					if pending.GetSnapshot().GetMetadata().GetIndex() != old.GetMetadata().GetIndex() {
						t.Fatal("pending transfer is not the historical image")
					}
				}
				if built, err := leader.snapshotForCatchup(catchupSnapshotEntries); !built || err != nil {
					t.Fatal("historical image was not refreshed for admitted member", built, err)
				}
				fresh, err := leader.storage.Snapshot()
				if err != nil || fresh.GetMetadata().GetIndex() != states[71].AppliedIndex() ||
					!slices.Contains(fresh.GetMetadata().GetConfState().GetLearners(), uint64(72)) {
					t.Fatal("new image does not contain exact committed membership", err)
				}
				if built, err := leader.snapshotForCatchup(catchupSnapshotEntries); built || err != nil || states[71].released.Load() != 2 {
					t.Fatal("new image was rebuilt before participant progress", built, err)
				}
				if pending != nil {
					// Receiving the old image must STILL fail membership validation;
					// the normal failed-send report then retries the new image.
					if _, err := learner.InstallSnapshot(pending); err == nil || states[72].AppliedIndex() != 0 {
						t.Fatal("old image bypassed new member validation", err)
					}
					if err := leader.ReportSnapshot(72, false); err != nil {
						t.Fatal(err)
					}
				}
				for turn := 0; turn < 40 && states[72].AppliedIndex() != states[71].AppliedIndex(); turn++ {
					if err := leader.Tick(); err != nil {
						t.Fatal(err)
					}
					transport.deliver(t)
				}
				installed, err := learner.storage.Snapshot()
				if err != nil || installed.GetMetadata().GetIndex() != fresh.GetMetadata().GetIndex() ||
					states[72].AppliedIndex() != states[71].AppliedIndex() {
					t.Fatal("learner failed to install the fresh consistent image", err)
				}
				if err := leader.Submit([]byte("tail-after-new-image")); err != nil {
					t.Fatal(err)
				}
				transport.deliver(t)
				left, right := states[71].disk, states[72].disk
				if left.Applied != right.Applied || len(left.Commands) != len(right.Commands) {
					t.Fatal("new participant lost state or log tail")
				}
				for i := range left.Commands {
					if !bytes.Equal(left.Commands[i], right.Commands[i]) {
						t.Fatal("new image changed command bytes", i)
					}
				}
			})
		}
	}
}

func TestCatchupSnapshotLearnerRestoresImageAndOnlyReplaysTail(t *testing.T) {
	for _, groupID := range []string{RootGroupID, "regional"} {
		t.Run(groupID, func(t *testing.T) {
			transport := &queuedTransport{groups: make(map[uint64]*Group), blocked: make(map[uint64]bool), groupID: groupID}
			states := make(map[uint64]*preparedTestState)
			for _, id := range []uint64{71, 72} {
				root := t.TempDir()
				state := &preparedTestState{testState: openTestState(t, root), started: make(chan struct{}, 2), release: make(chan struct{})}
				close(state.release)
				options := GroupOptions{StoreRoot: root, GroupID: groupID, RaftID: id, State: state, Transport: transport, Joining: id == 72}
				if id == 71 {
					options.BootstrapPeers = []uint64{71}
				}
				group, err := OpenGroup(options)
				if err != nil {
					t.Fatal(err)
				}
				states[id], transport.groups[id] = state, group
				t.Cleanup(func() { _ = group.Close() })
			}
			leader, learner := transport.groups[71], transport.groups[72]
			if err := leader.Campaign(); err != nil {
				t.Fatal(err)
			}
			transport.deliver(t)
			for i := 0; i < 40; i++ {
				if err := leader.Submit([]byte{byte(i)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := leader.AddLearner(72); err != nil {
				t.Fatal(err)
			}
			if built, err := leader.snapshotForCatchup(8); !built || err != nil {
				t.Fatal("leader did not create catchup image", built, err)
			}
			image, _ := leader.storage.Snapshot()
			if err := leader.Submit([]byte("tail")); err != nil {
				t.Fatal(err)
			}
			transport.deliver(t)
			installed, err := learner.storage.Snapshot()
			if err != nil || installed.GetMetadata().GetIndex() != image.GetMetadata().GetIndex() {
				t.Fatal("learner replayed historical log instead of installing image", installed, err)
			}
			left, right := states[71].disk, states[72].disk
			if left.Applied != right.Applied || len(left.Commands) != len(right.Commands) {
				t.Fatal("learner failed to apply current state and tail")
			}
			for i := range left.Commands {
				if !bytes.Equal(left.Commands[i], right.Commands[i]) {
					t.Fatal("snapshot changed command bytes", i)
				}
			}
			if status, _ := learner.Status(); status.Role != etcdraft.StateFollower {
				t.Fatal("catchup changed learner role", status)
			}
			if built, err := learner.snapshotForCatchup(1); built || err != nil {
				t.Fatal("follower built a serving checkpoint", built, err)
			}
			transport.blocked[72] = true
			for i := 0; i < 20; i++ {
				if err := leader.Submit([]byte{byte(i)}); err != nil {
					t.Fatal(err)
				}
			}
			if built, err := leader.snapshotForCatchup(8); !built || err != nil {
				t.Fatal("participant acknowledged previous image but could not refresh a large remaining tail", built, err)
			}
		})
	}
}

func TestHostCatchupSnapshotSerialAdmissionAndShutdownCancellation(t *testing.T) {
	root := t.TempDir()
	id, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	h, err := OpenHost(HostOptions{StoreRoot: root, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	s := &preparedTestState{testState: hostTestState(t, root, "state"), started: make(chan struct{}, 2), release: make(chan struct{})}
	if err := h.AddGroup(RootGroupID, []uint64{id.RaftID}, false, s); err != nil {
		t.Fatal(err)
	}
	if err := h.Campaign(RootGroupID); err != nil {
		t.Fatal(err)
	}
	g, _ := h.group(RootGroupID)
	peer := id.RaftID ^ 1
	if peer == 0 {
		peer = 2
	}
	if err := g.AddLearner(peer); err != nil {
		t.Fatal(err)
	}
	h.scheduleCatchupSnapshots(1)
	waitPrepared(t, s.started)
	for i := 0; i < 8; i++ {
		h.scheduleCatchupSnapshots(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := h.ReadIndex(ctx, RootGroupID); err != nil {
		t.Fatal("build blocked ordinary Root read", err)
	}
	if err := h.Submit(RootGroupID, []byte("during-build")); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- h.Close() }()
	if err := waitPreparedResult(t, result); err != nil {
		t.Fatal("Close failed to cancel/join build", err)
	}
	if s.released.Load() != 1 || len(s.started) != 0 {
		t.Fatal("overlapping or unjoined snapshot builds", s.released.Load(), len(s.started))
	}
}

func TestHostCatchupSnapshotSkipsBusyGroupAndRotates(t *testing.T) {
	root := t.TempDir()
	id, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	h, err := OpenHost(HostOptions{StoreRoot: root, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	groups := make(map[string]*Group)
	for _, name := range []string{"a", "b"} {
		s := &preparedTestState{testState: hostTestState(t, root, name), started: make(chan struct{}, 2), release: make(chan struct{})}
		close(s.release)
		if err := h.AddGroup(name, []uint64{id.RaftID}, false, s); err != nil {
			t.Fatal(err)
		}
		g, _ := h.group(name)
		groups[name] = g
		if err := g.Campaign(); err != nil {
			t.Fatal(err)
		}
		peer := id.RaftID ^ 1
		if peer == 0 {
			peer = 2
		}
		if err := g.AddLearner(peer); err != nil {
			t.Fatal(err)
		}
	}
	groups["a"].mu.Lock()
	unlock := sync.OnceFunc(groups["a"].mu.Unlock)
	defer unlock()
	h.scheduleCatchupSnapshots(1)
	h.catchupWG.Wait()
	image, err := groups["b"].storage.Snapshot()
	if err != nil || image.GetMetadata().GetIndex() == 0 {
		t.Fatal("busy Group blocked another checkpoint", err)
	}
	unlock()
	h.scheduleCatchupSnapshots(1)
	h.catchupWG.Wait()
	image, err = groups["a"].storage.Snapshot()
	if err != nil || image.GetMetadata().GetIndex() == 0 {
		t.Fatal("ready Group was starved after rotation", err)
	}
}
