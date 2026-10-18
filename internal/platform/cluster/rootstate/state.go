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
	repository "github.com/dubi-project/dubi/internal/platform/recovery/repository"
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
	mu          sync.RWMutex
	db          *pebble.DB
	applied     uint64
	conf        *raftpb.ConfState
	closed      bool
	failed      error
	commit      func(*pebble.Batch) error // Test-only uncertainty injection; nil uses Sync.
	root        string
	snapshotWG  sync.WaitGroup
	closeDone   chan struct{}
	closeErr    error
	restoring   *restoreFence
	restoreHook func(string) error // Test-only crash/uncertainty boundaries.
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
	state := &State{db: db, conf: &raftpb.ConfState{}, root: root, closeDone: make(chan struct{})}
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
	if err := state.loadRestoreFence(); err != nil {
		return nil, errors.Join(err, db.Close())
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

// RootMembership is the fallible directory projection of the applied Root
// ConfState. ConfState itself is the Raft StateMachine persistence contract;
// routing callers must not use it to bypass uncertainty/restore fencing.
// Callers needing a linearizable view must first obtain a Root ReadIndex.
func (s *State) RootMembership() (*raftpb.ConfState, error) {
	_, conf, err := s.RootPosition()
	return conf, err
}

// RootPosition atomically samples membership with its applied position. In
// particular, recovery fences must not pair old membership with a newer index.
func (s *State) RootPosition() (uint64, *raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return 0, nil, err
	}
	return s.applied, proto.Clone(s.conf).(*raftpb.ConfState), nil
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
			} else if rejection, err := s.applyMutation(batch, command, hash); err != nil {
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
	if err := s.usable(); err != nil {
		return err
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
	if err := s.commitBatch(batch); err != nil {
		return err
	}
	s.applied = index
	return nil
}

func (s *State) usable() error {
	if s.closed {
		return errors.New("Root state is closed")
	}
	if s.failed != nil {
		return fmt.Errorf("Root state needs reopen after uncertain commit: %w", s.failed)
	}
	if s.restoring != nil {
		return errors.New("Root state requires completion of its durable snapshot restore")
	}
	return nil
}

func (s *State) commitBatch(batch *pebble.Batch) error {
	var err error
	if s.commit != nil {
		err = s.commit(batch)
	} else {
		err = batch.Commit(pebble.Sync)
	}
	if err != nil {
		s.failed = err
		return fmt.Errorf("commit Root state (outcome uncertain): %w", err)
	}
	return nil
}

func (s *State) applyMutation(batch *pebble.Batch, command Command, hash string) (string, error) {
	switch command.Type {
	case FenceNode:
		return s.applyNodeFence(batch, *command.NodeFence)
	case AuthorizeRegionGrowth:
		return s.applyRegionGrowth(batch, command, hash)
	case PlanRegionCohort, ConfirmRegionCohort:
		return s.applyRegionCohort(batch, command)
	case PublishEnrollmentAuthority:
		return s.applyEnrollmentAuthority(batch, *command.EnrollmentAuthority)
	case SetRangePlacement:
		return s.applyRangePlacement(batch, *command.RangePlacement)
	case RetireRegionGroup:
		return s.applyRegionGroupRetirement(batch, *command.GroupRetirement)
	case PlanRegionGroupRebuild:
		return s.applyRegionGroupRebuild(batch, *command.GroupRebuild)
	case MarkNodeReady:
		return s.applyNodeReady(batch, *command.NodeReady)
	case CreateRepository:
		value := command.Repository
		if _, found, err := s.get(repositoryKey(value.Name)); err != nil {
			return "", err
		} else if found {
			return "Repository name is already registered", nil
		}
		return "", setJSON(batch, repositoryKey(value.Name), value)
	case UpdateRepository:
		value := command.Repository
		encoded, found, err := s.get(repositoryKey(value.Name))
		if err != nil {
			return "", err
		}
		if !found {
			return "Repository is not registered", nil
		}
		// CLI updates intentionally do not accept plaintext or ciphertext
		// credentials. Preserve the existing encrypted envelope when a caller
		// changes only the non-secret destination settings.
		var previous repository.Repository
		if err := decodeStreamJSON(encoded, &previous); err != nil {
			return "", err
		}
		if len(value.EncryptedCredentials) == 0 {
			value.EncryptedCredentials = append([]byte(nil), previous.EncryptedCredentials...)
			value.CredentialsNonce = append([]byte(nil), previous.CredentialsNonce...)
			value.CredentialsAlgorithm = previous.CredentialsAlgorithm
			value.CredentialsKeyVersion = previous.CredentialsKeyVersion
		}
		return "", setJSON(batch, repositoryKey(value.Name), value)
	case DeleteRepository:
		name := command.RepositoryDelete.Name
		if _, found, err := s.get(repositoryKey(name)); err != nil {
			return "", err
		} else if !found {
			return "Repository is not registered", nil
		}
		if current, found, err := s.get(defaultRepositoryKey); err != nil {
			return "", err
		} else if found && string(current) == name {
			if err := batch.Delete(defaultRepositoryKey, nil); err != nil {
				return "", err
			}
		}
		return "", batch.Delete(repositoryKey(name), nil)
	case SetDefaultRepository:
		name := command.RepositoryDefault.Name
		if _, found, err := s.get(repositoryKey(name)); err != nil {
			return "", err
		} else if !found {
			return "Repository is not registered", nil
		}
		return "", batch.Set(defaultRepositoryKey, []byte(name), nil)
	case CreateBackupJob:
		return s.applyCreateBackupJob(batch, *command.BackupJob)
	case ClaimBackupJob:
		return s.applyClaimBackupJob(batch, *command.BackupJobClaim)
	case CompleteBackupJob:
		return s.applyCompleteBackupJob(batch, *command.BackupJobCompletion)
	case FailBackupJob:
		return s.applyFailBackupJob(batch, *command.BackupJobFailure)
	case FinalizeBackupJob:
		return s.applyFinalizeBackupJob(batch, *command.BackupJobReference)
	case BeginRootJoin, CompleteRootJoin:
		return s.applyRootJoin(batch, command)
	case CreateJoinGrant, ConsumeJoinGrant, RevokeJoinGrant:
		return s.applyJoinGrant(batch, command)
	case CreateTenantCredential, RevokeTenantCredential:
		return s.applyTenantCredential(batch, command)
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
		if value.Name != "" {
			conflict, err := s.nodeNameConflict(value.Name, 0)
			if err != nil {
				return "", err
			}
			if conflict {
				return "Node name is already in use", nil
			}
		}
		if err := setJSON(batch, nodeKey(value.RaftID), value); err != nil {
			return "", err
		}
		return "", batch.Set(nodeIDKey(value.NodeID), []byte(fmt.Sprintf("%d", value.RaftID)), nil)
	case RenameNode:
		value := command.NodeRename
		encoded, found, err := s.get(nodeKey(value.RaftID))
		if err != nil {
			return "", err
		}
		if !found {
			return "Node is not registered", nil
		}
		var node Node
		if err := json.Unmarshal(encoded, &node); err != nil {
			return "", fmt.Errorf("decode Node for rename: %w", err)
		}
		if node.NodeID != value.NodeID {
			return "Node identity does not match", nil
		}
		if _, fenced, err := s.nodeFence(value.RaftID); err != nil {
			return "", err
		} else if fenced {
			return "Node is fenced", nil
		}
		if value.Name != "" {
			conflict, err := s.nodeNameConflict(value.Name, value.RaftID)
			if err != nil {
				return "", err
			}
			if conflict {
				return "Node name is already in use", nil
			}
		}
		node.Name = value.Name
		return "", setJSON(batch, nodeKey(value.RaftID), &node)
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
		retired, retirementErr := s.regionGroupRetired(command.Membership.GroupID)
		if retirementErr != nil {
			return "", retirementErr
		}
		if _, confirmed, err := s.confirmedRegion(command.Membership.GroupID); err != nil {
			return "", err
		} else if confirmed && !retired {
			return "confirmed regional membership requires authorized growth or maintenance", nil
		}
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
		if descriptor.Epoch == 1 {
			region, found, err := s.get(regionCohortGroupKey(membership.GroupID))
			if err != nil {
				return "", err
			}
			if found {
				cohortGeneration := uint64(1)
				if retired {
					head, headFound, headErr := s.regionCohortHead(string(region))
					if headErr != nil {
						return "", headErr
					}
					if !headFound || head.PendingGeneration == 0 {
						return "same-name Region formation has no pending cohort", nil
					}
					cohortGeneration = head.PendingGeneration
				}
				cohort, exists, err := s.regionCohort(string(region), cohortGeneration)
				if err != nil {
					return "", err
				}
				if !exists {
					return "", errors.New("Group formation cohort is missing")
				}
				if cohort.Formation && !formationMatchesMembers(cohort, membership.Members) {
					return "initial Group membership differs from immutable regional formation", nil
				}
				if cohort.Formation {
					for _, id := range cohort.Nodes {
						if _, member, err := s.rootMember(id); err != nil {
							return "", err
						} else if !member {
							return "initial regional publication requires current Root members", nil
						}
					}
				}
			}
		}
		if _, found, err := s.get(activeGroupChangeKey(membership.GroupID)); err != nil {
			return "", err
		} else if found {
			return "Group has an active membership change", nil
		}
		for _, member := range membership.Members {
			if _, fenced, err := s.nodeFence(member.RaftID); err != nil {
				return "", err
			} else if fenced {
				return "Group member Node identity is fenced", nil
			}
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
		_, confirmed, err := s.confirmedRegion(command.GroupPlan.GroupID)
		if err != nil {
			return "", err
		} else if confirmed && !command.GroupPlan.Maintenance {
			return "confirmed regional membership requires an authorized sealed growth task", nil
		}
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
		if plan.Maintenance {
			if err := validateMaintenanceRemoval(descriptor, plan.Members); err != nil {
				return err.Error(), nil
			}
		}
		if descriptor.Epoch != plan.ExpectedEpoch {
			return "Group membership Epoch does not match", nil
		}
		if descriptor.Epoch == 1 {
			region, found, err := s.get(regionCohortGroupKey(plan.GroupID))
			if err != nil {
				return "", err
			}
			if found {
				cohort, exists, err := s.regionCohort(string(region), 1)
				if err != nil {
					return "", err
				}
				if !exists {
					return "", errors.New("Group formation cohort is missing")
				}
				if cohort.Formation {
					return "regional Group initial formation must be published before membership changes", nil
				}
			}
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
			if _, fenced, err := s.nodeFence(member.RaftID); err != nil {
				return "", err
			} else if fenced {
				return "Group member Node identity is fenced", nil
			}
			if _, found, err := s.get(nodeKey(member.RaftID)); err != nil {
				return "", err
			} else if !found {
				return "Group member Node is not registered", nil
			}
		}
		change := GroupChange{ChangeID: plan.ChangeID, GroupID: plan.GroupID,
			ExpectedEpoch: plan.ExpectedEpoch, Members: append([]GroupMember(nil), plan.Members...),
			Maintenance: plan.Maintenance, State: GroupChangePending}
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
		var authorizeErr error
		if change.Maintenance {
			authorizeErr = s.authorizeMaintenanceTask(change)
		} else {
			authorizeErr = s.authorizeGrowthTask(change)
		}
		if err := authorizeErr; err != nil {
			if errors.Is(err, ErrGroupMutationUnauthorized) {
				return "regional growth task is no longer authorized", nil
			}
			return "", err
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
			EngineID: value.EngineID, DatasetID: value.DatasetID,
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
	if err := s.usable(); err != nil {
		return nil, false, err
	}
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
func repositoryKey(name string) []byte           { return []byte("data/repository/" + name) }

var defaultRepositoryKey = []byte("data/repository-default")

func (value Keyspace) path() string {
	return value.TenantID + "/" + value.EngineID + "/" + value.DatasetID
}
func datasetKey(value Dataset) []byte { return []byte("data/dataset/" + value.Keyspace().path()) }
func rangeKey(value Dataset) []byte {
	return keyspaceRangeKey(value.Keyspace())
}
func keyspaceRangeKey(value Keyspace) []byte { return []byte("data/range/" + value.path()) }
func rangeIDKey(id string) []byte            { return []byte("data/range-id/" + id) }
func resultKey(id string) []byte             { return []byte("result/" + id) }

// nodeNameConflict checks the durable Node catalog while ignoring explicitly
// fenced identities. A removed Node's label can therefore be reused by the
// newly enrolled Node that replaces it, while every current or pending Node
// keeps a unique operator-facing name.
func (s *State) nodeNameConflict(name string, exceptRaftID uint64) (bool, error) {
	if name == "" {
		return false, nil
	}
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/node/"), UpperBound: []byte("data/node0")})
	if err != nil {
		return false, err
	}
	defer iterator.Close()
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var node Node
		if err := decodeStreamJSON(iterator.Value(), &node); err != nil {
			return false, err
		}
		if node.RaftID == exceptRaftID || node.Name != name {
			continue
		}
		if _, fenced, err := s.nodeFence(node.RaftID); err != nil {
			return false, err
		} else if !fenced {
			return true, nil
		}
	}
	return false, iterator.Error()
}

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

// NodeByName resolves one active, non-fenced Node by its unique operator
// label. Names are unique among all non-fenced registrations, including a
// pending join, so this lookup cannot silently select an arbitrary Node.
func (s *State) NodeByName(name string) (Node, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return Node{}, false, err
	}
	if err := ValidateNodeName(name); err != nil || name == "" {
		return Node{}, false, errors.New("invalid Node name")
	}
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/node/"), UpperBound: []byte("data/node0")})
	if err != nil {
		return Node{}, false, err
	}
	defer iterator.Close()
	var match Node
	found := false
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var node Node
		if err := decodeStreamJSON(iterator.Value(), &node); err != nil {
			return Node{}, false, err
		}
		if node.Name != name {
			continue
		}
		if _, fenced, err := s.nodeFence(node.RaftID); err != nil {
			return Node{}, false, err
		} else if !fenced {
			if found {
				return Node{}, false, errors.New("Node name is ambiguous")
			}
			match, found = node, true
		}
	}
	if err := iterator.Error(); err != nil {
		return Node{}, false, err
	}
	return match, found, nil
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
	if err := s.usable(); err != nil {
		return nil, err
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
	if err := s.usable(); err != nil {
		return nil, err
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

func (s *State) Repository(name string) (repository.Repository, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return repository.Repository{}, false, err
	}
	if err := repository.ValidateName(name); err != nil {
		return repository.Repository{}, false, err
	}
	encoded, found, err := s.get(repositoryKey(name))
	if !found || err != nil {
		return repository.Repository{}, found, err
	}
	var value repository.Repository
	if err := decodeStreamJSON(encoded, &value); err != nil {
		return repository.Repository{}, false, err
	}
	return value, true, value.Validate()
}

func (s *State) Repositories() ([]repository.Repository, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/repository/"), UpperBound: []byte("data/repository0")})
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	values := make([]repository.Repository, 0)
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var value repository.Repository
		if err := decodeStreamJSON(iterator.Value(), &value); err != nil {
			return nil, err
		}
		if err := value.Validate(); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, iterator.Error()
}

func (s *State) DefaultRepository() (repository.Repository, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return repository.Repository{}, false, err
	}
	encoded, found, err := s.get(defaultRepositoryKey)
	if err != nil || !found {
		return repository.Repository{}, found, err
	}
	repositoryValue, found, err := s.get(repositoryKey(string(encoded)))
	if err != nil || !found {
		return repository.Repository{}, false, errors.New("default Repository is missing")
	}
	var value repository.Repository
	if err := decodeStreamJSON(repositoryValue, &value); err != nil {
		return repository.Repository{}, false, err
	}
	if err := value.Validate(); err != nil {
		return repository.Repository{}, false, err
	}
	return value, true, nil
}

func (s *State) Dataset(tenantID, engineID, datasetID string) (Dataset, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || !validID(tenantID) || !validID(engineID) || !validID(datasetID) {
		return Dataset{}, false, errors.New("Root state is closed or Dataset identity is invalid")
	}
	encoded, found, err := s.get(datasetKey(Dataset{TenantID: tenantID, EngineID: engineID, DatasetID: datasetID}))
	if !found || err != nil {
		return Dataset{}, found, err
	}
	var value Dataset
	err = json.Unmarshal(encoded, &value)
	return value, err == nil, err
}

func (s *State) Route(tenantID, engineID, datasetID string, routingKey []byte) (Range, bool, error) {
	return s.RouteKeyspace(Keyspace{TenantID: tenantID, EngineID: engineID, DatasetID: datasetID}, routingKey)
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
	if err := s.usable(); err != nil {
		return nil, err
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

func (s *State) ActiveRanges() ([]Range, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/range/"), UpperBound: []byte("data/range0")})
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	var ranges []Range
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var value Range
		if err := decodeStreamJSON(iterator.Value(), &value); err != nil {
			return nil, err
		}
		if value.State == RangeStateActive {
			ranges = append(ranges, value)
		}
	}
	return ranges, iterator.Error()
}

func (s *State) Close() error {
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return s.closeErr
	}
	s.closed = true
	s.mu.Unlock()
	s.snapshotWG.Wait()
	err := s.db.Close()
	s.mu.Lock()
	s.closeErr = err
	close(s.closeDone)
	s.mu.Unlock()
	return err
}
