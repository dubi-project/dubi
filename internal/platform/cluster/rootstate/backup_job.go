package rootstate

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/identity"
	repository "github.com/dubi-project/dubi/internal/platform/recovery/repository"
)

const (
	BackupJobPending   = "pending"
	BackupJobRunning   = "running"
	BackupJobCompleted = "completed"
	BackupJobFailed    = "failed"

	BackupWorkerPending   = "pending"
	BackupWorkerRunning   = "running"
	BackupWorkerCompleted = "completed"
	BackupWorkerFailed    = "failed"
)

const (
	maxBackupLeaseSeconds  = 7 * 24 * 60 * 60
	maxBackupJobErrorBytes = 4096
)

// BackupJob is the Root-owned control record for one cluster backup. It is
// deliberately independent of Engine and Module data formats. Workers are a
// membership cut captured at creation time; a later node join does not change
// an in-flight job.
type BackupJob struct {
	JobID      string `json:"job_id"`
	Repository string `json:"repository"`
	ClusterID  string `json:"cluster_id"`
	// Epoch is the immutable Recovery Point namespace allocated at creation.
	// Worker lease/state changes must not rewrite it; manifests use this value
	// as their collision-free repository key.
	Epoch          uint64         `json:"epoch"`
	State          string         `json:"state"`
	CreatedIndex   uint64         `json:"created_index"`
	CompletedIndex uint64         `json:"completed_index,omitempty"`
	LastError      string         `json:"last_error,omitempty"`
	Workers        []BackupWorker `json:"workers"`
}

// BackupWorker records one node's lease and progress for a BackupJob. A
// lease epoch invalidates a previous worker after an administrator or another
// node takes over an expired lease.
type BackupWorker struct {
	RaftID           uint64 `json:"raft_id"`
	NodeID           string `json:"node_id"`
	State            string `json:"state"`
	LeaseEpoch       uint64 `json:"lease_epoch"`
	LeaseExpiresUnix int64  `json:"lease_expires_unix,omitempty"`
	LeaseRaftID      uint64 `json:"lease_raft_id,omitempty"`
	LeaseNodeID      string `json:"lease_node_id,omitempty"`
	Bytes            int64  `json:"bytes,omitempty"`
	Objects          uint64 `json:"objects,omitempty"`
	Error            string `json:"error,omitempty"`
}

type BackupJobClaim struct {
	JobID            string `json:"job_id"`
	RaftID           uint64 `json:"raft_id"`
	NodeID           string `json:"node_id"`
	ClaimantRaftID   uint64 `json:"claimant_raft_id,omitempty"`
	ClaimantNodeID   string `json:"claimant_node_id,omitempty"`
	ObservedUnix     int64  `json:"observed_unix"`
	LeaseExpiresUnix int64  `json:"lease_expires_unix"`
}

type BackupJobCompletion struct {
	JobID          string `json:"job_id"`
	RaftID         uint64 `json:"raft_id"`
	NodeID         string `json:"node_id"`
	LeaseEpoch     uint64 `json:"lease_epoch"`
	ClaimantRaftID uint64 `json:"claimant_raft_id,omitempty"`
	ClaimantNodeID string `json:"claimant_node_id,omitempty"`
	Bytes          int64  `json:"bytes"`
	Objects        uint64 `json:"objects"`
}

type BackupJobFailure struct {
	JobID          string `json:"job_id"`
	RaftID         uint64 `json:"raft_id"`
	NodeID         string `json:"node_id"`
	LeaseEpoch     uint64 `json:"lease_epoch"`
	ClaimantRaftID uint64 `json:"claimant_raft_id,omitempty"`
	ClaimantNodeID string `json:"claimant_node_id,omitempty"`
	Error          string `json:"error"`
}

type BackupJobReference struct {
	JobID string `json:"job_id"`
}

func (value BackupJob) ValidateForCreate() error {
	if !validID(value.JobID) || repositoryNameValid(value.Repository) != nil || identity.ValidateClusterID(value.ClusterID) != nil {
		return errors.New("invalid Backup job identity")
	}
	if value.Epoch != 0 || value.State != "" || value.CreatedIndex != 0 || value.CompletedIndex != 0 || value.LastError != "" || len(value.Workers) != 0 {
		return errors.New("new Backup job must not contain Root state")
	}
	return nil
}

// ValidateStored validates the self-contained shape of a persisted job. An
// index of zero skips the applied-index upper-bound checks for live reads.
func (value BackupJob) ValidateStored(index uint64) error {
	if !validID(value.JobID) || repositoryNameValid(value.Repository) != nil || identity.ValidateClusterID(value.ClusterID) != nil || value.Epoch == 0 || value.CreatedIndex == 0 || (index != 0 && value.CreatedIndex > index) {
		return errors.New("invalid Backup job identity or epoch")
	}
	switch value.State {
	case BackupJobPending, BackupJobRunning:
		if value.CompletedIndex != 0 {
			return errors.New("unfinished Backup job has a completion index")
		}
	case BackupJobCompleted:
		if value.CompletedIndex == 0 || (index != 0 && value.CompletedIndex > index) {
			return errors.New("completed Backup job has an invalid completion index")
		}
	case BackupJobFailed:
		if value.LastError == "" {
			return errors.New("failed Backup job has no error")
		}
	default:
		return errors.New("invalid Backup job state")
	}
	if len(value.LastError) > maxBackupJobErrorBytes {
		return errors.New("Backup job error is too large")
	}
	if len(value.Workers) == 0 {
		return errors.New("Backup job has no workers")
	}
	seen := make(map[uint64]struct{}, len(value.Workers))
	for _, worker := range value.Workers {
		if worker.RaftID == 0 || !nodeIdentity.MatchString(worker.NodeID) {
			return errors.New("Backup job contains an invalid worker identity")
		}
		if _, ok := seen[worker.RaftID]; ok {
			return errors.New("Backup job contains duplicate workers")
		}
		seen[worker.RaftID] = struct{}{}
		switch worker.State {
		case BackupWorkerPending:
			if worker.LeaseExpiresUnix != 0 || worker.LeaseRaftID != 0 || worker.LeaseNodeID != "" || worker.Error != "" {
				return errors.New("pending Backup worker has lease or error state")
			}
		case BackupWorkerRunning:
			if worker.LeaseEpoch == 0 || worker.LeaseExpiresUnix <= 0 || worker.LeaseRaftID == 0 || !nodeIdentity.MatchString(worker.LeaseNodeID) || worker.Error != "" {
				return errors.New("running Backup worker has invalid lease")
			}
		case BackupWorkerCompleted:
			if worker.LeaseEpoch == 0 || worker.LeaseExpiresUnix != 0 || worker.LeaseRaftID != 0 || worker.LeaseNodeID != "" || worker.Error != "" || worker.Bytes < 0 {
				return errors.New("completed Backup worker has invalid progress")
			}
		case BackupWorkerFailed:
			if worker.LeaseEpoch == 0 || worker.LeaseExpiresUnix != 0 || worker.LeaseRaftID != 0 || worker.LeaseNodeID != "" || strings.TrimSpace(worker.Error) == "" || len(worker.Error) > maxBackupJobErrorBytes {
				return errors.New("failed Backup worker has invalid error")
			}
		default:
			return errors.New("invalid Backup worker state")
		}
	}
	return nil
}

func (value BackupJobClaim) Validate() error {
	if !validID(value.JobID) || !nodeIdentity.MatchString(value.NodeID) || value.RaftID == 0 || value.ObservedUnix <= 0 || value.LeaseExpiresUnix <= value.ObservedUnix || value.LeaseExpiresUnix-value.ObservedUnix > maxBackupLeaseSeconds {
		return errors.New("invalid Backup job lease claim")
	}
	if (value.ClaimantRaftID == 0) != (value.ClaimantNodeID == "") {
		return errors.New("invalid Backup lease claimant")
	}
	if value.ClaimantRaftID != 0 && !nodeIdentity.MatchString(value.ClaimantNodeID) {
		return errors.New("invalid Backup lease claimant")
	}
	return nil
}

func (value BackupJobCompletion) Validate() error {
	if !validID(value.JobID) || !nodeIdentity.MatchString(value.NodeID) || value.RaftID == 0 || value.LeaseEpoch == 0 || value.Bytes < 0 {
		return errors.New("invalid Backup job completion")
	}
	if (value.ClaimantRaftID == 0) != (value.ClaimantNodeID == "") {
		return errors.New("invalid Backup completion claimant")
	}
	if value.ClaimantRaftID != 0 && !nodeIdentity.MatchString(value.ClaimantNodeID) {
		return errors.New("invalid Backup completion claimant")
	}
	return nil
}

func (value BackupJobFailure) Validate() error {
	if !validID(value.JobID) || !nodeIdentity.MatchString(value.NodeID) || value.RaftID == 0 || value.LeaseEpoch == 0 || strings.TrimSpace(value.Error) == "" || len(value.Error) > maxBackupJobErrorBytes {
		return errors.New("invalid Backup job failure")
	}
	if (value.ClaimantRaftID == 0) != (value.ClaimantNodeID == "") {
		return errors.New("invalid Backup failure claimant")
	}
	if value.ClaimantRaftID != 0 && !nodeIdentity.MatchString(value.ClaimantNodeID) {
		return errors.New("invalid Backup failure claimant")
	}
	return nil
}

func (value BackupJobReference) Validate() error {
	if !validID(value.JobID) {
		return errors.New("invalid Backup job ID")
	}
	return nil
}

// repositoryNameValid is kept here to avoid exposing repository validation
// through another Root model type while retaining one canonical check.
func repositoryNameValid(name string) error {
	return repository.ValidateName(name)
}

func backupJobKey(id string) []byte { return []byte("data/backup-job/" + id) }

func (s *State) applyCreateBackupJob(batch *pebble.Batch, value BackupJob) (string, error) {
	if _, found, err := s.get(backupJobKey(value.JobID)); err != nil {
		return "", err
	} else if found {
		return "Backup job ID is already registered", nil
	}
	if _, found, err := s.get(repositoryKey(value.Repository)); err != nil {
		return "", err
	} else if !found {
		return "Backup Repository is not registered", nil
	}
	nodes, err := s.backupJobMembers()
	if err != nil {
		return "", err
	}
	if len(nodes) == 0 {
		return "Root has no current members for Backup", nil
	}
	value.State = BackupJobPending
	value.CreatedIndex = s.applied + 1
	value.Epoch = value.CreatedIndex
	value.Workers = make([]BackupWorker, 0, len(nodes))
	for _, node := range nodes {
		value.Workers = append(value.Workers, BackupWorker{RaftID: node.RaftID, NodeID: node.NodeID, State: BackupWorkerPending})
	}
	return "", setJSON(batch, backupJobKey(value.JobID), value)
}

func (s *State) applyClaimBackupJob(batch *pebble.Batch, claim BackupJobClaim) (string, error) {
	job, found, err := s.loadBackupJob(claim.JobID)
	if err != nil {
		return "", err
	}
	if !found {
		return "Backup job is not registered", nil
	}
	claimantRaftID, claimantNodeID := claim.ClaimantRaftID, claim.ClaimantNodeID
	if claimantRaftID == 0 {
		claimantRaftID, claimantNodeID = claim.RaftID, claim.NodeID
	}
	claimant, err := s.rootMemberNode(claimantRaftID)
	if err != nil {
		return "", err
	}
	if claimant.NodeID != claimantNodeID {
		return "Backup lease claimant identity is not a current Root member", nil
	}
	if job.State == BackupJobCompleted || job.State == BackupJobFailed {
		return "Backup job is terminal", nil
	}
	worker, ok := backupWorker(&job, claim.RaftID)
	if !ok || worker.NodeID != claim.NodeID {
		return "Backup worker is not assigned to this Node identity", nil
	}
	if worker.State == BackupWorkerCompleted || worker.State == BackupWorkerFailed {
		return "Backup worker is terminal", nil
	}
	if worker.State == BackupWorkerRunning && claim.ObservedUnix < worker.LeaseExpiresUnix {
		return "Backup worker lease is still active", nil
	}
	if worker.LeaseEpoch == math.MaxUint64 {
		return "Backup worker lease epoch is exhausted", nil
	}
	worker.LeaseEpoch++
	worker.State = BackupWorkerRunning
	worker.LeaseExpiresUnix = claim.LeaseExpiresUnix
	worker.LeaseRaftID = claimantRaftID
	worker.LeaseNodeID = claimantNodeID
	worker.Error = ""
	job.State = BackupJobRunning
	return "", setJSON(batch, backupJobKey(job.JobID), job)
}

func (s *State) applyCompleteBackupJob(batch *pebble.Batch, completion BackupJobCompletion) (string, error) {
	job, found, err := s.loadBackupJob(completion.JobID)
	if err != nil {
		return "", err
	}
	if !found {
		return "Backup job is not registered", nil
	}
	claimantRaftID, claimantNodeID := completion.ClaimantRaftID, completion.ClaimantNodeID
	if claimantRaftID == 0 {
		claimantRaftID, claimantNodeID = completion.RaftID, completion.NodeID
	}
	worker, ok := backupWorker(&job, completion.RaftID)
	if !ok || worker.NodeID != completion.NodeID || worker.State != BackupWorkerRunning || worker.LeaseEpoch != completion.LeaseEpoch || worker.LeaseRaftID != claimantRaftID || worker.LeaseNodeID != claimantNodeID {
		return "Backup worker lease is no longer current", nil
	}
	worker.State = BackupWorkerCompleted
	worker.LeaseExpiresUnix = 0
	worker.LeaseRaftID = 0
	worker.LeaseNodeID = ""
	worker.Bytes = completion.Bytes
	worker.Objects = completion.Objects
	worker.Error = ""
	return "", setJSON(batch, backupJobKey(job.JobID), job)
}

func (s *State) applyFailBackupJob(batch *pebble.Batch, failure BackupJobFailure) (string, error) {
	job, found, err := s.loadBackupJob(failure.JobID)
	if err != nil {
		return "", err
	}
	if !found {
		return "Backup job is not registered", nil
	}
	claimantRaftID, claimantNodeID := failure.ClaimantRaftID, failure.ClaimantNodeID
	if claimantRaftID == 0 {
		claimantRaftID, claimantNodeID = failure.RaftID, failure.NodeID
	}
	worker, ok := backupWorker(&job, failure.RaftID)
	if !ok || worker.NodeID != failure.NodeID || worker.State != BackupWorkerRunning || worker.LeaseEpoch != failure.LeaseEpoch || worker.LeaseRaftID != claimantRaftID || worker.LeaseNodeID != claimantNodeID {
		return "Backup worker lease is no longer current", nil
	}
	worker.State = BackupWorkerFailed
	worker.LeaseExpiresUnix = 0
	worker.LeaseRaftID = 0
	worker.LeaseNodeID = ""
	worker.Error = strings.TrimSpace(failure.Error)
	job.State = BackupJobFailed
	job.LastError = worker.Error
	return "", setJSON(batch, backupJobKey(job.JobID), job)
}

func (s *State) applyFinalizeBackupJob(batch *pebble.Batch, reference BackupJobReference) (string, error) {
	job, found, err := s.loadBackupJob(reference.JobID)
	if err != nil {
		return "", err
	}
	if !found {
		return "Backup job is not registered", nil
	}
	if job.State == BackupJobCompleted {
		return "", nil
	}
	if job.State == BackupJobFailed {
		return "Backup job has failed", nil
	}
	for _, worker := range job.Workers {
		if worker.State != BackupWorkerCompleted {
			return "Backup job still has unfinished workers", nil
		}
	}
	job.State = BackupJobCompleted
	job.CompletedIndex = s.applied + 1
	return "", setJSON(batch, backupJobKey(job.JobID), job)
}

func backupWorker(job *BackupJob, raftID uint64) (*BackupWorker, bool) {
	for index := range job.Workers {
		if job.Workers[index].RaftID == raftID {
			return &job.Workers[index], true
		}
	}
	return nil, false
}

// backupJobMembers must be called while s.mu is held by the Root apply path.
func (s *State) backupJobMembers() ([]Node, error) {
	ids := make(map[uint64]struct{})
	for _, members := range [][]uint64{s.conf.GetVoters(), s.conf.GetVotersOutgoing(), s.conf.GetLearners(), s.conf.GetLearnersNext()} {
		for _, id := range members {
			if id == 0 {
				return nil, errors.New("Root membership contains zero identity")
			}
			ids[id] = struct{}{}
		}
	}
	ordered := make([]uint64, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	result := make([]Node, 0, len(ordered))
	for _, id := range ordered {
		node, err := s.rootMemberNode(id)
		if err != nil {
			return nil, err
		}
		result = append(result, node)
	}
	return result, nil
}

func (s *State) loadBackupJob(jobID string) (BackupJob, bool, error) {
	if !validID(jobID) {
		return BackupJob{}, false, errors.New("invalid Backup job ID")
	}
	encoded, found, err := s.get(backupJobKey(jobID))
	if err != nil || !found {
		return BackupJob{}, found, err
	}
	var job BackupJob
	if err := decodeStreamJSON(encoded, &job); err != nil {
		return BackupJob{}, false, err
	}
	if err := job.ValidateStored(0); err != nil {
		return BackupJob{}, false, err
	}
	return job, true, nil
}

func (s *State) BackupJob(jobID string) (BackupJob, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return BackupJob{}, false, err
	}
	return s.loadBackupJob(jobID)
}

func (s *State) BackupJobs() ([]BackupJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/backup-job/"), UpperBound: []byte("data/backup-job0")})
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	jobs := make([]BackupJob, 0)
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var job BackupJob
		if err := decodeStreamJSON(iterator.Value(), &job); err != nil {
			return nil, err
		}
		if err := job.ValidateStored(0); err != nil {
			return nil, fmt.Errorf("invalid Backup job %q: %w", job.JobID, err)
		}
		jobs = append(jobs, job)
	}
	return jobs, iterator.Error()
}
