package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/storage/diskpressure"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
	"go.etcd.io/raft/v3/raftpb"
)

var ErrHostClosed = errors.New("Raft Host is closed")

type HostOptions struct {
	StoreRoot    string
	TickInterval time.Duration
	Transport    Transport
	// ProposalGuard supplements the automatic Store disk guard. Internal only;
	// it cannot turn off disk admission, and must not re-enter Host or Group.
	ProposalGuard func() error
}

// Host owns the Raft Groups on one Node and holds the Store's exclusive
// process lock. A network listener may route inbound messages to Step.
type Host struct {
	mu            sync.RWMutex
	root          string
	identity      identity.Identity
	transport     Transport
	lock          *storelock.Lock
	groups        map[string]*Group
	errors        map[string]error
	stop          chan struct{}
	done          chan struct{}
	closed        bool
	disk          *diskpressure.Guard
	proposalGuard func() error
	opening       map[string]bool
	ticking       map[string]bool
	openWG        sync.WaitGroup
	tickWG        sync.WaitGroup
	closeDone     chan struct{}
	closeErr      error
}

func OpenHost(options HostOptions) (*Host, error) {
	if options.TickInterval < 0 {
		return nil, errors.New("Raft tick interval must not be negative")
	}
	if options.TickInterval == 0 {
		options.TickInterval = 100 * time.Millisecond
	}
	lock, err := storelock.Acquire(options.StoreRoot)
	if err != nil {
		return nil, err
	}
	id, err := identity.Load(options.StoreRoot)
	if err != nil {
		return nil, errors.Join(err, lock.Release())
	}
	if id.ClusterID == "" {
		return nil, errors.Join(errors.New("Node is not bound to a Cluster"), lock.Release())
	}
	disk, err := diskpressure.Open(options.StoreRoot)
	if err != nil {
		return nil, errors.Join(err, lock.Release())
	}
	host := &Host{root: options.StoreRoot, identity: id, transport: options.Transport,
		lock: lock, groups: make(map[string]*Group), errors: make(map[string]error),
		stop: make(chan struct{}), done: make(chan struct{}), disk: disk,
		opening: make(map[string]bool), ticking: make(map[string]bool), closeDone: make(chan struct{})}
	host.proposalGuard = func() error {
		if err := disk.Check(); err != nil {
			return err
		}
		if options.ProposalGuard != nil {
			return options.ProposalGuard()
		}
		return nil
	}
	go host.run(options.TickInterval)
	return host, nil
}

func (h *Host) Identity() identity.Identity { return h.identity }

// DiskStatus reports actual Store capacity and automatic admission recovery.
// It is not a sum of external Volume capacities or a durability certificate.
func (h *Host) DiskStatus() diskpressure.Status { return h.disk.Status() }

// AddGroup takes ownership of state only if it begins opening the Group.
// Callers retain ownership when the Host is closed or the Group ID is already
// open/opening. Disk recovery runs outside the Host lock; existing Groups must
// remain routable while another Group reconciles a large snapshot.
func (h *Host) AddGroup(groupID string, peers []uint64, joining bool, state StateMachine) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrHostClosed
	}
	if h.groups[groupID] != nil || h.opening[groupID] {
		h.mu.Unlock()
		return fmt.Errorf("Raft Group %q is already open or opening", groupID)
	}
	h.opening[groupID] = true
	h.openWG.Add(1)
	h.mu.Unlock()
	defer h.openWG.Done()
	group, err := OpenGroup(GroupOptions{StoreRoot: h.root, GroupID: groupID,
		RaftID: h.identity.RaftID, BootstrapPeers: peers, Joining: joining,
		State: state, Transport: h.transport, ProposalGuard: h.proposalGuard})
	h.mu.Lock()
	delete(h.opening, groupID)
	if err != nil {
		h.mu.Unlock()
		return err
	}
	if h.closed {
		h.mu.Unlock()
		return errors.Join(ErrHostClosed, group.Close())
	}
	h.groups[groupID] = group
	h.mu.Unlock()
	return nil
}

func (h *Host) group(groupID string) (*Group, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return nil, ErrHostClosed
	}
	group := h.groups[groupID]
	if group == nil {
		return nil, fmt.Errorf("Raft Group %q is not open", groupID)
	}
	return group, nil
}

func (h *Host) HasGroup(groupID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return !h.closed && h.groups[groupID] != nil
}

func (h *Host) Step(groupID string, message *raftpb.Message) error {
	group, err := h.group(groupID)
	if err != nil {
		return err
	}
	return group.Step(message)
}

func (h *Host) ReportUnreachable(groupID string, raftID uint64) error {
	group, err := h.group(groupID)
	if err != nil {
		return err
	}
	return group.ReportUnreachable(raftID)
}

func (h *Host) ReportSnapshot(groupID string, raftID uint64, success bool) error {
	group, err := h.group(groupID)
	if err != nil {
		return err
	}
	return group.ReportSnapshot(raftID, success)
}

func (h *Host) InstallSnapshot(groupID string, message *raftpb.Message) (uint64, error) {
	group, err := h.group(groupID)
	if err != nil {
		return 0, err
	}
	return group.InstallSnapshot(message)
}

func (h *Host) CreateSnapshot(groupID string) (uint64, error) {
	group, err := h.group(groupID)
	if err != nil {
		return 0, err
	}
	return group.CreateSnapshot()
}

func (h *Host) Campaign(groupID string) error {
	group, err := h.group(groupID)
	if err != nil {
		return err
	}
	return group.Campaign()
}

// Submit only proposes a command; it does not wait for durable commitment.
func (h *Host) Submit(groupID string, command []byte) error {
	group, err := h.group(groupID)
	if err != nil {
		return err
	}
	return group.Submit(command)
}

func (h *Host) ReadIndex(ctx context.Context, groupID string) (uint64, error) {
	group, err := h.group(groupID)
	if err != nil {
		return 0, err
	}
	return group.ReadIndex(ctx)
}

func (h *Host) Status(groupID string) (GroupStatus, error) {
	group, err := h.group(groupID)
	if err != nil {
		return GroupStatus{}, err
	}
	return group.Status()
}

func (h *Host) Membership(groupID string) (*raftpb.ConfState, error) {
	group, err := h.group(groupID)
	if err != nil {
		return nil, err
	}
	return group.Membership()
}

func (h *Host) AddLearner(groupID string, raftID uint64) error {
	group, err := h.group(groupID)
	if err != nil {
		return err
	}
	return group.AddLearner(raftID)
}

func (h *Host) PromoteLearner(groupID string, raftID uint64) error {
	group, err := h.group(groupID)
	if err != nil {
		return err
	}
	return group.PromoteLearner(raftID)
}

func (h *Host) RemoveMember(groupID string, raftID uint64) error {
	group, err := h.group(groupID)
	if err != nil {
		return err
	}
	return group.RemoveMember(raftID)
}

func (h *Host) TransferLeadership(ctx context.Context, groupID string, target uint64) (GroupStatus, error) {
	group, err := h.group(groupID)
	if err != nil {
		return GroupStatus{}, err
	}
	return group.TransferLeadership(ctx, target)
}

// TickAll advances every open Group once. It also records errors from
// background ticks so callers can observe a Group that failed closed.
func (h *Host) TickAll() error {
	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return ErrHostClosed
	}
	groups := make(map[string]*Group, len(h.groups))
	for id, group := range h.groups {
		groups[id] = group
	}
	h.mu.RUnlock()
	var failures []error
	for id, group := range groups {
		if err := group.Tick(); err != nil {
			h.mu.Lock()
			h.errors[id] = err
			h.mu.Unlock()
			failures = append(failures, fmt.Errorf("Raft Group %q tick: %w", id, err))
		}
	}
	return errors.Join(failures...)
}

// GroupError returns the latest background tick error for a Group, if any.
func (h *Host) GroupError(groupID string) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.errors[groupID]
}

func (h *Host) run(interval time.Duration) {
	defer close(h.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.scheduleTicks()
		case <-h.stop:
			return
		}
	}
}

// At most one background tick worker exists per Group. Slow disk/apply on
// one Group neither blocks the shared clock nor queues unlimited ticks. A
// busy Group skips elapsed ticks; it does not burst elections after recovery.
// TickAll remains the explicit synchronous API for deterministic callers.
func (h *Host) scheduleTicks() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	for id, group := range h.groups {
		if h.ticking[id] {
			continue
		}
		h.ticking[id] = true
		h.tickWG.Add(1)
		go func(id string, group *Group) {
			defer h.tickWG.Done()
			err := group.Tick()
			h.mu.Lock()
			delete(h.ticking, id)
			if err != nil {
				h.errors[id] = err
			}
			h.mu.Unlock()
		}(id, group)
	}
}

// Close stops the ticker before closing Groups, then releases the Store lock.
func (h *Host) Close() error {
	h.mu.Lock()
	if h.closed {
		done := h.closeDone
		h.mu.Unlock()
		<-done
		return h.closeErr
	}
	h.closed = true
	close(h.stop)
	h.mu.Unlock()
	<-h.done
	// All Add calls and tick scheduling register under h.mu before checking
	// closed, so no new WaitGroup Add can race these shutdown barriers.
	h.openWG.Wait()
	h.tickWG.Wait()
	h.mu.Lock()
	groups := h.groups
	h.groups = make(map[string]*Group)
	h.mu.Unlock()
	var failures []error
	for id, group := range groups {
		if err := group.Close(); err != nil {
			failures = append(failures, fmt.Errorf("close Raft Group %q: %w", id, err))
		}
	}
	failures = append(failures, h.disk.Close(), h.lock.Release())
	h.mu.Lock()
	h.closeErr = errors.Join(failures...)
	close(h.closeDone)
	h.mu.Unlock()
	return h.closeErr
}
