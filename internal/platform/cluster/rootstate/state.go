package rootstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var _ raft.StateMachine = (*State)(nil)
var _ raft.Snapshotter = (*State)(nil)

var ErrRequestIDReused = errors.New("Root RequestID was reused for a different command")

var appliedKey = []byte("meta/applied")
var confKey = []byte("meta/conf")

type State struct {
	mu      sync.RWMutex
	db      *pebble.DB
	applied uint64
	conf    *raftpb.ConfState
	closed  bool
}

func Open(root string) (*State, error) {
	if root == "" {
		return nil, errors.New("Root state directory is empty")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	db, err := pebbleutil.Open(filepath.Join(root, "dubi-root-state"), &pebble.Options{Logger: pebbleutil.QuietLogger{}})
	if err != nil {
		return nil, err
	}
	state := &State{db: db, conf: &raftpb.ConfState{}}
	if encoded, found, err := state.get(appliedKey); err != nil {
		_ = db.Close()
		return nil, err
	} else if found {
		if len(encoded) != 8 {
			_ = db.Close()
			return nil, errors.New("invalid Root applied index")
		}
		state.applied = binary.BigEndian.Uint64(encoded)
	}
	if encoded, found, err := state.get(confKey); err != nil {
		_ = db.Close()
		return nil, err
	} else if found {
		if err := proto.Unmarshal(encoded, state.conf); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("decode Root membership: %w", err)
		}
	}
	return state, nil
}

func (s *State) AppliedIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

func (s *State) ConfState() *raftpb.ConfState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return proto.Clone(s.conf).(*raftpb.ConfState)
}

func (s *State) Apply(index uint64, encoded []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkNext(index); err != nil {
		return err
	}
	var command Command
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&command)
	if decodeErr == nil {
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			decodeErr = errors.New("trailing Root command data")
		}
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if decodeErr == nil && validID(command.RequestID) {
		digest := sha256.Sum256(encoded)
		hash := hex.EncodeToString(digest[:])
		key := resultKey(command.RequestID)
		previous, found, err := s.get(key)
		if err != nil {
			return err
		}
		if !found {
			result := Result{RequestID: command.RequestID, Hash: hash, Index: index}
			if err := command.validate(); err != nil {
				result.Error = err.Error()
			} else if rejection, err := s.applyMutation(batch, command); err != nil {
				return err
			} else {
				result.Error = rejection
			}
			if err := setJSON(batch, key, result); err != nil {
				return err
			}
		} else {
			var result Result
			if err := json.Unmarshal(previous, &result); err != nil {
				return fmt.Errorf("decode prior Root request result: %w", err)
			}
			// Both identical retries and RequestID reuse leave the original
			// result and descriptors unchanged; the Raft index still advances.
		}
	}
	return s.commitIndex(batch, index)
}

func (s *State) Advance(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkNext(index); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	return s.commitIndex(batch, index)
}

func (s *State) ApplyMembership(index uint64, conf *raftpb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkNext(index); err != nil {
		return err
	}
	if conf == nil {
		return errors.New("Root membership is nil")
	}
	encoded, err := proto.Marshal(conf)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(confKey, encoded, nil); err != nil {
		return err
	}
	if err := s.commitIndex(batch, index); err != nil {
		return err
	}
	s.conf = proto.Clone(conf).(*raftpb.ConfState)
	return nil
}

func (s *State) checkNext(index uint64) error {
	if s.closed {
		return errors.New("Root state is closed")
	}
	if index != s.applied+1 {
		return fmt.Errorf("Root applied index gap: current %d, next %d", s.applied, index)
	}
	return nil
}

func (s *State) commitIndex(batch *pebble.Batch, index uint64) error {
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], index)
	if err := batch.Set(appliedKey, value[:], nil); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit Root state: %w", err)
	}
	s.applied = index
	return nil
}

func (s *State) applyMutation(batch *pebble.Batch, command Command) (string, error) {
	switch command.Type {
	case RegisterNode:
		value := command.Node
		if _, found, err := s.get(nodeKey(value.RaftID)); err != nil {
			return "", err
		} else if found {
			return "RaftID is already registered", nil
		}
		if _, found, err := s.get(nodeIDKey(value.NodeID)); err != nil {
			return "", err
		} else if found {
			return "NodeID is already registered", nil
		}
		if err := setJSON(batch, nodeKey(value.RaftID), value); err != nil {
			return "", err
		}
		return "", batch.Set(nodeIDKey(value.NodeID), []byte(fmt.Sprintf("%d", value.RaftID)), nil)
	case RegisterGroup:
		value := command.Group
		if _, found, err := s.get(groupKey(value.GroupID)); err != nil {
			return "", err
		} else if found {
			return "GroupID is already registered", nil
		}
		return "", setJSON(batch, groupKey(value.GroupID),
			Group{GroupID: value.GroupID, StateKind: value.StateKind, Epoch: 1})
	case PublishGroupMembers:
		membership := command.Membership
		encoded, found, err := s.get(groupKey(membership.GroupID))
		if err != nil {
			return "", err
		}
		if !found {
			return "Group is not registered", nil
		}
		var descriptor Group
		if err := json.Unmarshal(encoded, &descriptor); err != nil {
			return "", fmt.Errorf("decode Group for membership publication: %w", err)
		}
		if descriptor.Epoch != membership.ExpectedEpoch {
			return "Group membership Epoch does not match", nil
		}
		if _, found, err := s.get(activeGroupChangeKey(membership.GroupID)); err != nil {
			return "", err
		} else if found {
			return "Group has an active membership change", nil
		}
		for _, member := range membership.Members {
			if _, found, err := s.get(nodeKey(member.RaftID)); err != nil {
				return "", err
			} else if !found {
				return "Group member Node is not registered", nil
			}
		}
		descriptor.Epoch++
		descriptor.Members = append([]GroupMember(nil), membership.Members...)
		return "", setJSON(batch, groupKey(membership.GroupID), descriptor)
	case PlanGroupMembers:
		plan := command.GroupPlan
		encoded, found, err := s.get(groupKey(plan.GroupID))
		if err != nil {
			return "", err
		}
		if !found {
			return "Group is not registered", nil
		}
		var descriptor Group
		if err := json.Unmarshal(encoded, &descriptor); err != nil {
			return "", fmt.Errorf("decode Group for membership plan: %w", err)
		}
		if descriptor.Epoch != plan.ExpectedEpoch {
			return "Group membership Epoch does not match", nil
		}
		currentRoles := make(map[uint64]string, len(descriptor.Members))
		for _, member := range descriptor.Members {
			currentRoles[member.RaftID] = member.Role
		}
		for _, member := range plan.Members {
			if currentRoles[member.RaftID] == GroupMemberVoter && member.Role == GroupMemberLearner {
				return "Voter to Learner demotion requires separate remove and add changes", nil
			}
		}
		if _, found, err := s.get(groupChangeKey(plan.ChangeID)); err != nil {
			return "", err
		} else if found {
			return "Group membership ChangeID is already registered", nil
		}
		if _, found, err := s.get(activeGroupChangeKey(plan.GroupID)); err != nil {
			return "", err
		} else if found {
			return "Group already has an active membership change", nil
		}
		for _, member := range plan.Members {
			if _, found, err := s.get(nodeKey(member.RaftID)); err != nil {
				return "", err
			} else if !found {
				return "Group member Node is not registered", nil
			}
		}
		change := GroupChange{ChangeID: plan.ChangeID, GroupID: plan.GroupID,
			ExpectedEpoch: plan.ExpectedEpoch, Members: append([]GroupMember(nil), plan.Members...),
			State: GroupChangePending}
		if err := setJSON(batch, groupChangeKey(plan.ChangeID), change); err != nil {
			return "", err
		}
		return "", batch.Set(activeGroupChangeKey(plan.GroupID), []byte(plan.ChangeID), nil)
	case CompleteGroupMembers:
		completion := command.GroupCompletion
		encoded, found, err := s.get(groupChangeKey(completion.ChangeID))
		if err != nil {
			return "", err
		}
		if !found {
			return "Group membership change is not registered", nil
		}
		var change GroupChange
		if err := json.Unmarshal(encoded, &change); err != nil {
			return "", fmt.Errorf("decode Group membership change: %w", err)
		}
		if change.GroupID != completion.GroupID {
			return "Group membership change belongs to another Group", nil
		}
		if change.State == GroupChangeCompleted {
			return "", nil
		}
		active, found, err := s.get(activeGroupChangeKey(change.GroupID))
		if err != nil {
			return "", err
		}
		if !found || string(active) != change.ChangeID {
			return "Group membership change is not active", nil
		}
		encoded, found, err = s.get(groupKey(change.GroupID))
		if err != nil {
			return "", err
		}
		if !found {
			return "Group is not registered", nil
		}
		var descriptor Group
		if err := json.Unmarshal(encoded, &descriptor); err != nil {
			return "", fmt.Errorf("decode Group for membership completion: %w", err)
		}
		if descriptor.Epoch != change.ExpectedEpoch {
			return "Group membership Epoch does not match", nil
		}
		descriptor.Epoch++
		descriptor.Members = append([]GroupMember(nil), change.Members...)
		change.State = GroupChangeCompleted
		change.CompletedEpoch = descriptor.Epoch
		if err := setJSON(batch, groupKey(change.GroupID), descriptor); err != nil {
			return "", err
		}
		if err := setJSON(batch, groupChangeKey(change.ChangeID), change); err != nil {
			return "", err
		}
		return "", batch.Delete(activeGroupChangeKey(change.GroupID), nil)
	case RegisterTenant:
		value := command.Tenant
		if _, found, err := s.get(tenantKey(value.TenantID)); err != nil {
			return "", err
		} else if found {
			return "TenantID is already registered", nil
		}
		return "", setJSON(batch, tenantKey(value.TenantID), value)
	case CreateDataset:
		value := command.Dataset
		if _, found, err := s.get(tenantKey(value.TenantID)); err != nil {
			return "", err
		} else if !found {
			return "Tenant is not registered", nil
		}
		if _, found, err := s.get(groupKey(value.OwnerGroupID)); err != nil {
			return "", err
		} else if !found {
			return "owner Group is not registered", nil
		}
		if _, found, err := s.get(datasetKey(*value)); err != nil {
			return "", err
		} else if found {
			return "Dataset is already registered", nil
		}
		if _, found, err := s.get(rangeIDKey(value.RangeID)); err != nil {
			return "", err
		} else if found {
			return "RangeID is already registered", nil
		}
		initial := Range{RangeID: value.RangeID, TenantID: value.TenantID,
			CapabilityID: value.CapabilityID, DatasetID: value.DatasetID,
			OwnerGroupID: value.OwnerGroupID, Epoch: 1, State: RangeStatePending}
		if err := setJSON(batch, datasetKey(*value), value); err != nil {
			return "", err
		}
		if err := setJSON(batch, rangeKey(*value), initial); err != nil {
			return "", err
		}
		return "", batch.Set(rangeIDKey(value.RangeID), rangeKey(*value), nil)
	case ActivateRange:
		activation := command.Activation
		rangeKey, found, err := s.get(rangeIDKey(activation.RangeID))
		if err != nil {
			return "", err
		}
		if !found {
			return "Range is not registered", nil
		}
		encoded, found, err := s.get(rangeKey)
		if err != nil {
			return "", err
		}
		if !found {
			return "Range directory is inconsistent", nil
		}
		var descriptor Range
		if err := json.Unmarshal(encoded, &descriptor); err != nil {
			return "", fmt.Errorf("decode Range for activation: %w", err)
		}
		if descriptor.OwnerGroupID != activation.OwnerGroupID || descriptor.Epoch != activation.Epoch {
			return "Range activation does not match pending Owner or Epoch", nil
		}
		if descriptor.State == RangeStateActive {
			return "", nil
		}
		if descriptor.State != RangeStatePending {
			return "Range is not pending activation", nil
		}
		descriptor.State = RangeStateActive
		descriptor.OwnerFenceRequestID = activation.OwnerRequestID
		descriptor.OwnerFenceIndex = activation.OwnerAppliedIndex
		return "", setJSON(batch, rangeKey, descriptor)
	}
	return "unsupported Root command", nil
}

func (s *State) get(key []byte) ([]byte, bool, error) {
	value, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	copy := bytes.Clone(value)
	return copy, true, closer.Close()
}

func setJSON(batch *pebble.Batch, key []byte, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return batch.Set(key, encoded, nil)
}

func nodeKey(id uint64) []byte                   { return []byte(fmt.Sprintf("data/node/%020d", id)) }
func nodeIDKey(id string) []byte                 { return []byte("data/node-id/" + id) }
func groupKey(id string) []byte                  { return []byte("data/group/" + id) }
func groupChangeKey(id string) []byte            { return []byte("data/group-change/" + id) }
func activeGroupChangeKey(groupID string) []byte { return []byte("data/group-active/" + groupID) }
func tenantKey(id string) []byte                 { return []byte("data/tenant/" + id) }
func (value Keyspace) path() string {
	return value.TenantID + "/" + value.CapabilityID + "/" + value.DatasetID
}
func datasetKey(value Dataset) []byte { return []byte("data/dataset/" + value.Keyspace().path()) }
func rangeKey(value Dataset) []byte {
	return keyspaceRangeKey(value.Keyspace())
}
func keyspaceRangeKey(value Keyspace) []byte { return []byte("data/range/" + value.path()) }
func rangeIDKey(id string) []byte            { return []byte("data/range-id/" + id) }
func resultKey(id string) []byte             { return []byte("result/" + id) }

func (s *State) Result(requestID string) (Result, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || !validID(requestID) {
		return Result{}, false, errors.New("Root state is closed or RequestID is invalid")
	}
	encoded, found, err := s.get(resultKey(requestID))
	if !found || err != nil {
		return Result{}, found, err
	}
	var result Result
	err = json.Unmarshal(encoded, &result)
	return result, err == nil, err
}

// ResultForCommand compares the command hash as well as RequestID, so a
// different command cannot inherit a previous successful result.
func (s *State) ResultForCommand(encoded []byte) (Result, bool, error) {
	var command Command
	if err := json.Unmarshal(encoded, &command); err != nil || !validID(command.RequestID) {
		return Result{}, false, errors.New("invalid Root command or RequestID")
	}
	result, found, err := s.Result(command.RequestID)
	if err != nil || !found {
		return result, found, err
	}
	digest := sha256.Sum256(encoded)
	if result.Hash != hex.EncodeToString(digest[:]) {
		return Result{}, true, ErrRequestIDReused
	}
	return result, true, nil
}

func (s *State) Node(raftID uint64) (Node, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return Node{}, false, errors.New("Root state is closed")
	}
	encoded, found, err := s.get(nodeKey(raftID))
	if !found || err != nil {
		return Node{}, found, err
	}
	var value Node
	err = json.Unmarshal(encoded, &value)
	return value, err == nil, err
}

func (s *State) Group(groupID string) (Group, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || raft.ValidateGroupID(groupID) != nil {
		return Group{}, false, errors.New("Root state is closed or GroupID is invalid")
	}
	encoded, found, err := s.get(groupKey(groupID))
	if !found || err != nil {
		return Group{}, found, err
	}
	var value Group
	err = json.Unmarshal(encoded, &value)
	return value, err == nil, err
}

func (s *State) Groups() ([]Group, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("Root state is closed")
	}
	iterator, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("data/group/"), UpperBound: []byte("data/group0"),
	})
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	var groups []Group
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var value Group
		if err := json.Unmarshal(iterator.Value(), &value); err != nil {
			return nil, err
		}
		groups = append(groups, value)
	}
	return groups, iterator.Error()
}

func (s *State) GroupChange(changeID string) (GroupChange, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || !validID(changeID) {
		return GroupChange{}, false, errors.New("Root state is closed or ChangeID is invalid")
	}
	encoded, found, err := s.get(groupChangeKey(changeID))
	if !found || err != nil {
		return GroupChange{}, found, err
	}
	var value GroupChange
	err = json.Unmarshal(encoded, &value)
	return value, err == nil, err
}

func (s *State) ActiveGroupChange(groupID string) (GroupChange, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || raft.ValidateGroupID(groupID) != nil {
		return GroupChange{}, false, errors.New("Root state is closed or GroupID is invalid")
	}
	changeID, found, err := s.get(activeGroupChangeKey(groupID))
	if !found || err != nil {
		return GroupChange{}, found, err
	}
	encoded, found, err := s.get(groupChangeKey(string(changeID)))
	if !found || err != nil {
		return GroupChange{}, found, err
	}
	var value GroupChange
	err = json.Unmarshal(encoded, &value)
	return value, err == nil, err
}

func (s *State) PendingGroupChanges() ([]GroupChange, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("Root state is closed")
	}
	iterator, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("data/group-change/"), UpperBound: []byte("data/group-change0"),
	})
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	var changes []GroupChange
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var value GroupChange
		if err := json.Unmarshal(iterator.Value(), &value); err != nil {
			return nil, err
		}
		if value.State == GroupChangePending {
			changes = append(changes, value)
		}
	}
	return changes, iterator.Error()
}

func (s *State) Tenant(tenantID string) (Tenant, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || !validID(tenantID) {
		return Tenant{}, false, errors.New("Root state is closed or TenantID is invalid")
	}
	encoded, found, err := s.get(tenantKey(tenantID))
	if !found || err != nil {
		return Tenant{}, found, err
	}
	var value Tenant
	err = json.Unmarshal(encoded, &value)
	return value, err == nil, err
}

func (s *State) Dataset(tenantID, capabilityID, datasetID string) (Dataset, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || !validID(tenantID) || !validID(capabilityID) || !validID(datasetID) {
		return Dataset{}, false, errors.New("Root state is closed or Dataset identity is invalid")
	}
	encoded, found, err := s.get(datasetKey(Dataset{TenantID: tenantID, CapabilityID: capabilityID, DatasetID: datasetID}))
	if !found || err != nil {
		return Dataset{}, found, err
	}
	var value Dataset
	err = json.Unmarshal(encoded, &value)
	return value, err == nil, err
}

func (s *State) Route(tenantID, capabilityID, datasetID string, routingKey []byte) (Range, bool, error) {
	return s.RouteKeyspace(Keyspace{TenantID: tenantID, CapabilityID: capabilityID, DatasetID: datasetID}, routingKey)
}

// RouteKeyspace currently resolves the initial complete Range. Split and move
// commands will be added only with migration and fencing orchestration.
func (s *State) RouteKeyspace(keyspace Keyspace, _ []byte) (Range, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || keyspace.Validate() != nil {
		return Range{}, false, errors.New("Root state is closed or Keyspace identity is invalid")
	}
	encoded, found, err := s.get(keyspaceRangeKey(keyspace))
	if !found || err != nil {
		return Range{}, found, err
	}
	var value Range
	if err = json.Unmarshal(encoded, &value); err != nil {
		return Range{}, false, err
	}
	if value.State != RangeStateActive {
		return Range{}, false, nil
	}
	return value, true, nil
}

func (s *State) Range(rangeID string) (Range, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || ValidateRangeID(rangeID) != nil {
		return Range{}, false, errors.New("Root state is closed or RangeID is invalid")
	}
	key, found, err := s.get(rangeIDKey(rangeID))
	if !found || err != nil {
		return Range{}, found, err
	}
	encoded, found, err := s.get(key)
	if !found || err != nil {
		return Range{}, found, err
	}
	var value Range
	err = json.Unmarshal(encoded, &value)
	return value, err == nil, err
}

func (s *State) PendingRanges() ([]Range, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("Root state is closed")
	}
	iterator, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("data/range/"), UpperBound: []byte("data/range0"),
	})
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	var ranges []Range
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var value Range
		if err := json.Unmarshal(iterator.Value(), &value); err != nil {
			return nil, err
		}
		if value.State == RangeStatePending {
			ranges = append(ranges, value)
		}
	}
	return ranges, iterator.Error()
}

func (s *State) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}
