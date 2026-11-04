package records

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

const BalancePrepared = "prepared"
const BalanceDropped = "dropped"
const BalanceCanceling = "canceling"

// NodeBalanceTask is local restart progress, never placement authority. Before
// DropBalanceHolding the controller must retain a fixed current Region
// majority on other Nodes and revalidate its Root plan. Pins prevent concurrent
// sources from each deleting the very holdings used by the other's proof.
type NodeBalanceTask struct {
	Sequence   uint64           `json:"sequence"`
	Descriptor Descriptor       `json:"descriptor"`
	Scope      string           `json:"scope"`
	Keepers    []rootstate.Node `json:"keepers"`
	Phase      string           `json:"phase"`
}

type nodeBalanceState struct {
	Sequence         uint64           `json:"sequence"`
	Task             *NodeBalanceTask `json:"task,omitempty"`
	Cursor           string           `json:"cursor,omitempty"`
	ReclaimSequence  uint64           `json:"reclaim_sequence,omitempty"`
	ReclaimedThrough uint64           `json:"reclaimed_through,omitempty"`
}

var nodeBalanceKey = []byte("maintenance/node-balance")

func (s *Store) nodeBalanceLocked() (nodeBalanceState, error) {
	encoded, closer, err := s.db.Get(nodeBalanceKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return nodeBalanceState{}, nil
	}
	if err != nil {
		return nodeBalanceState{}, classifyIndexRead(err)
	}
	defer closer.Close()
	var state nodeBalanceState
	if len(encoded) > 1<<20 || strictJSON(encoded, &state) != nil || len(state.Cursor) > 8192 || state.ReclaimSequence > state.Sequence || state.ReclaimedThrough > state.ReclaimSequence {
		return state, ErrCorrupt
	}
	if task := state.Task; task != nil {
		if task.Sequence == 0 || task.Sequence != state.Sequence || task.Descriptor.Validate() != nil || len(task.Scope) != 64 || len(task.Keepers) == 0 || len(task.Keepers) > 4096 ||
			(task.Phase != BalancePrepared && task.Phase != BalanceDropped && task.Phase != BalanceCanceling) {
			return state, ErrCorrupt
		}
	}
	return state, nil
}

func (s *Store) saveNodeBalanceLocked(batch *pebble.Batch, state nodeBalanceState) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return batch.Set(nodeBalanceKey, encoded, nil)
}

func (s *Store) NodeBalanceTask() (*NodeBalanceTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	state, err := s.nodeBalanceLocked()
	return state.Task, err
}

// BalanceCandidates advances only discovery, never a retention proof. A task
// must be durably finished before starting another record's pin sequence.
func (s *Store) BalanceCandidates(ctx context.Context) ([]Descriptor, error) {
	s.mu.Lock()
	if err := s.usable(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	state, err := s.nodeBalanceLocked()
	s.mu.Unlock()
	if err != nil || state.Task != nil {
		return nil, err
	}
	page, err := s.rewritePage(ctx, state.Cursor)
	if err != nil {
		return nil, err
	}
	var descriptors []Descriptor
	for _, item := range page {
		descriptors = append(descriptors, item.current.Descriptor.clone())
		state.Cursor = string(indexKey(item.id))
	}
	if len(page) == 0 {
		state.Cursor = ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	current, err := s.nodeBalanceLocked()
	if err != nil {
		return nil, err
	}
	if current.Task != nil || current.Sequence != state.Sequence {
		return nil, errors.New("balance discovery raced an active handoff")
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.saveNodeBalanceLocked(batch, state); err != nil {
		return nil, err
	}
	return descriptors, s.commitMaintenanceLocked(batch)
}

func (s *Store) BeginNodeBalance(ctx context.Context, descriptor Descriptor, scope string, keepers []rootstate.Node) (*NodeBalanceTask, error) {
	if ctx == nil || descriptor.Validate() != nil || len(scope) != 64 || len(keepers) == 0 || len(keepers) > 4096 {
		return nil, errors.New("invalid Node balance handoff")
	}
	seen := map[uint64]bool{}
	for _, node := range keepers {
		if node.RaftID == 0 || node.NodeID == "" || node.Address == "" || seen[node.RaftID] {
			return nil, errors.New("invalid or repeated balance keeper")
		}
		seen[node.RaftID] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	state, err := s.nodeBalanceLocked()
	if err != nil {
		return nil, err
	}
	if state.Task != nil || state.Sequence == math.MaxUint64 {
		return nil, errors.New("Node balance is busy or its sequence is exhausted")
	}
	id, _ := descriptor.ID()
	current, found, err := s.lookup(id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotHeld
	}
	if _, _, err := s.read(id, descriptor, current); err != nil {
		return nil, err
	}
	if retained, err := s.retainedLocked(id); err != nil {
		return nil, err
	} else if retained {
		return nil, ErrRetained
	}
	state.Sequence++
	state.Task = &NodeBalanceTask{Sequence: state.Sequence, Descriptor: descriptor.clone(), Scope: scope,
		Keepers: slices.Clone(keepers), Phase: BalancePrepared}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.saveNodeBalanceLocked(batch, state); err != nil {
		return nil, err
	}
	return state.Task, s.commitMaintenanceLocked(batch)
}

func (s *Store) DropBalanceHolding(ctx context.Context, sequence uint64) error {
	return s.updateNodeBalance(ctx, sequence, func(batch *pebble.Batch, state *nodeBalanceState) error {
		if state.Task.Phase == BalanceDropped {
			return nil
		}
		if state.Task.Phase != BalancePrepared {
			return errors.New("canceled handoff cannot delete a holding")
		}
		id, _ := state.Task.Descriptor.ID()
		if retained, err := s.retainedLocked(id); err != nil {
			return err
		} else if retained {
			return ErrRetained
		}
		if _, held, err := s.lookup(id); err != nil {
			return err
		} else if !held {
			return ErrCorrupt
		}
		if err := batch.Delete(indexKey(id), nil); err != nil {
			return err
		}
		if err := batch.Set([]byte("retention/offloaded/"+id), []byte(state.Task.Scope), nil); err != nil {
			return err
		}
		state.Task.Phase = BalanceDropped
		state.ReclaimSequence = state.Task.Sequence
		return nil
	})
}

// Reclaim progress is independent of the last RPC task: crash or a GC cadence
// delay must not lose the old sealed Packs waiting to be reclaimed.
func (s *Store) NodeBalanceReclaim() (uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return 0, false, err
	}
	state, err := s.nodeBalanceLocked()
	return state.ReclaimSequence, state.ReclaimSequence > state.ReclaimedThrough, err
}

func (s *Store) CompleteNodeBalanceReclaim(ctx context.Context, through uint64) error {
	if ctx == nil {
		return errors.New("nil reclaim context")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := s.nodeBalanceLocked()
	if err != nil {
		return err
	}
	if through > state.ReclaimSequence {
		return errors.New("reclaim acknowledgement is ahead of the deletion ledger")
	}
	state.ReclaimedThrough = max(state.ReclaimedThrough, through)
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := s.saveNodeBalanceLocked(batch, state); err != nil {
		return err
	}
	return s.commitMaintenanceLocked(batch)
}

func (s *Store) CancelNodeBalance(ctx context.Context, sequence uint64) error {
	return s.updateNodeBalance(ctx, sequence, func(_ *pebble.Batch, state *nodeBalanceState) error {
		if state.Task.Phase == BalancePrepared {
			state.Task.Phase = BalanceCanceling
		}
		return nil
	})
}

// FinishNodeBalance is allowed only after the controller has obtained all
// keeper release ACKs. Until then the task survives restart and blocks reuse.
func (s *Store) FinishNodeBalance(ctx context.Context, sequence uint64) error {
	return s.updateNodeBalance(ctx, sequence, func(_ *pebble.Batch, state *nodeBalanceState) error {
		if state.Task.Phase == BalancePrepared {
			return errors.New("unfinished balance task cannot discard retention progress")
		}
		state.Task = nil
		return nil
	})
}

func (s *Store) updateNodeBalance(ctx context.Context, sequence uint64, update func(*pebble.Batch, *nodeBalanceState) error) error {
	if ctx == nil || sequence == 0 {
		return errors.New("balance update requires context and sequence")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := s.nodeBalanceLocked()
	if err != nil {
		return err
	}
	if state.Task == nil || state.Task.Sequence != sequence {
		return errors.New("stale balance sequence")
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := update(batch, &state); err != nil {
		return err
	}
	if err := s.saveNodeBalanceLocked(batch, state); err != nil {
		return err
	}
	return s.commitMaintenanceLocked(batch)
}

func (s *Store) CapacityOffload(descriptor Descriptor) (string, bool, error) {
	id, err := descriptor.ID()
	if err != nil {
		return "", false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return "", false, err
	}
	return s.capacityOffloadLocked(id)
}

func (s *Store) capacityOffloadLocked(id string) (string, bool, error) {
	encoded, closer, err := s.db.Get([]byte("retention/offloaded/" + id))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, classifyIndexRead(err)
	}
	defer closer.Close()
	if len(encoded) != 64 {
		return "", false, ErrCorrupt
	}
	return string(encoded), true, nil
}

func (s *Store) ClearCapacityOffload(ctx context.Context, descriptor Descriptor) error {
	id, err := descriptor.ID()
	if ctx == nil || err != nil {
		return errors.New("invalid capacity offload descriptor or context")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Delete([]byte("retention/offloaded/"+id), nil); err != nil {
		return err
	}
	return s.commitMaintenanceLocked(batch)
}
