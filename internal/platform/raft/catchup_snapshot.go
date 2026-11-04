package raft

import (
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

const (
	catchupSnapshotInterval = 5 * time.Second
	catchupSnapshotEntries  = uint64(10000)
)

// Build at most one file-backed image per Host at a time, only when a leader
// has a substantially lagging participant. Idle/healthy Groups do not create
// periodic images. A busy Group cannot block tick scheduling or other Groups.
func (h *Host) scheduleCatchupSnapshots(minimum uint64) {
	h.mu.Lock()
	if h.closed || h.catchupBusy {
		h.mu.Unlock()
		return
	}
	ids := make([]string, 0, len(h.groups))
	groups := make(map[string]*Group, len(h.groups))
	for id, group := range h.groups {
		ids = append(ids, id)
		groups[id] = group
	}
	slices.Sort(ids)
	start := 0
	for start < len(ids) && ids[start] <= h.catchupCursor {
		start++
	}
	ids = append(ids[start:], ids[:start]...)
	h.catchupBusy = true
	h.catchupWG.Add(1)
	h.mu.Unlock()
	go func() {
		defer h.catchupWG.Done()
		defer func() { h.mu.Lock(); h.catchupBusy = false; h.mu.Unlock() }()
		for _, id := range ids {
			h.mu.RLock()
			closed := h.closed
			h.mu.RUnlock()
			if closed {
				return
			}
			built, err := groups[id].snapshotForCatchup(minimum)
			h.mu.Lock()
			previous := h.catchupErrors[id]
			if err == nil {
				delete(h.catchupErrors, id)
			} else if !h.closed {
				h.catchupErrors[id] = err
			}
			if built || err != nil {
				h.catchupCursor = id
			}
			closed = h.closed
			h.mu.Unlock()
			if err != nil && !closed && (previous == nil || previous.Error() != err.Error()) {
				log.Printf("dubi: Raft catchup snapshot Group %q: %v", id, err)
			}
			if built || err != nil {
				return
			}
		}
	}()
}

func (g *Group) snapshotForCatchup(minimum uint64) (bool, error) {
	if minimum == 0 {
		return false, errors.New("catchup snapshot requires a positive log gap")
	}
	if !g.mu.TryLock() {
		return false, nil
	}
	defer g.mu.Unlock()
	if err := g.checkLocked(); err != nil {
		return false, err
	}
	// Only the existing streaming preparation path may run automatically.
	// Inline adapters may hold the Group lock while materializing large images.
	preparer, ok := g.state.(SnapshotPreparer)
	if !ok || g.snapshotting {
		return false, nil
	}
	status := g.raw.Status()
	if status.RaftState != etcdraft.StateLeader {
		return false, nil
	}
	applied := g.state.AppliedIndex()
	current, err := g.storage.Snapshot()
	if err != nil {
		return false, err
	}
	index := current.GetMetadata().GetIndex()
	if applied <= index {
		return false, nil
	}
	needed := false
	for id, progress := range status.Progress {
		if id == g.id || applied <= progress.Match {
			continue
		}
		covered := snapshotHasParticipant(current.GetMetadata().GetConfState(), id)
		// A compacted image that excludes an admitted participant cannot be
		// installed there. Even one committed membership entry after that cut
		// requires a new, exact image. An old transfer already in flight does
		// not make that old ConfState usable; its normal failure/retry will use
		// the newly persisted image. Never rewrite historical image metadata.
		if index != 0 && progress.Match < index && !covered && snapshotHasParticipant(g.state.ConfState(), id) {
			needed = true
			break
		}
		if applied-index >= minimum && progress.State != tracker.StateSnapshot && applied-progress.Match >= minimum {
			// An existing image already covers this unchanged participant.
			// Do not accumulate new retained images while it remains offline
			// or has not acknowledged the previous image. A newly admitted ID
			// absent from that image, or actual catchup beyond its cut, may
			// trigger a fresh checkpoint instead of replaying a large tail.
			if covered && progress.Match < index {
				continue
			}
			needed = true
			break
		}
	}
	if !needed {
		return false, nil
	}
	if err := g.admitProposal(); err != nil {
		return false, err
	}
	if _, err := g.createPreparedSnapshotLocked(preparer); err != nil {
		return false, fmt.Errorf("prepare lagging participant checkpoint: %w", err)
	}
	return true, nil
}

func snapshotHasParticipant(conf *raftpb.ConfState, id uint64) bool {
	return slices.Contains(conf.GetVoters(), id) || slices.Contains(conf.GetLearners(), id) ||
		slices.Contains(conf.GetVotersOutgoing(), id) || slices.Contains(conf.GetLearnersNext(), id)
}
