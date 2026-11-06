package server

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/dubi-project/dubi/internal/engine/record"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginedata"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
	repository "github.com/dubi-project/dubi/internal/platform/recovery/repository"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// dataService is a Node-authenticated development entry point. Tenant client
// authorization belongs to a later public API and is deliberately not implied.
type dataService struct {
	datawire.UnimplementedDataServiceServer
	runtime       *node.Runtime
	client        *raftrpc.GroupClient
	routes        *raftrpc.RootDirectory
	slots         chan struct{}
	volumeSlots   chan struct{}
	maintenance   *resource.ByteBudget
	io            *resource.Maintenance
	requestBudget *resource.ByteBudget
	geoRetry      *recordwrite.GeoMetadataRetryStore
	geoRecords    *recordrepair.GeoRecordRepairStore
	repair        *liveRecordRepair
}

const maxDemoRecordBytes = 1 << 20
const maxTenantTokenTTLSeconds = 366 * 24 * 60 * 60

func repositoryFromWire(value *datawire.RepositoryConfig) (repository.Repository, error) {
	if value == nil {
		return repository.Repository{}, errors.New("Repository configuration is required")
	}
	config := repository.Repository{
		Name: value.GetName(), Type: repository.Type(value.GetType()), Path: value.GetPath(),
		Bucket: value.GetBucket(), Prefix: value.GetPrefix(), Endpoint: value.GetEndpoint(),
		Region: value.GetRegion(), PathStyle: value.GetPathStyle(),
		EncryptedCredentials: append([]byte(nil), value.GetEncryptedCredentials()...),
		CredentialsNonce:     append([]byte(nil), value.GetCredentialsNonce()...),
		CredentialsAlgorithm: value.GetCredentialsAlgorithm(), CredentialsKeyVersion: value.GetCredentialsKeyVersion(),
	}
	if value.GetCredentialsConfigured() && len(config.EncryptedCredentials) == 0 {
		return repository.Repository{}, errors.New("credentials_configured is server-generated")
	}
	if err := config.Validate(); err != nil {
		return repository.Repository{}, err
	}
	return config, nil
}

// repositoryToWire deliberately redacts the encrypted envelope. Although the
// bytes are not plaintext, returning them from list/show would make an
// operator-facing command copy reusable ciphertext unnecessarily. Mutations
// accept an envelope only from an internal recovery caller in a later phase.
func repositoryToWire(value repository.Repository) *datawire.RepositoryConfig {
	return &datawire.RepositoryConfig{Name: value.Name, Type: string(value.Type), Path: value.Path,
		Bucket: value.Bucket, Prefix: value.Prefix, Endpoint: value.Endpoint, Region: value.Region,
		PathStyle: value.PathStyle, CredentialsAlgorithm: value.CredentialsAlgorithm,
		CredentialsKeyVersion: value.CredentialsKeyVersion, CredentialsConfigured: len(value.EncryptedCredentials) != 0}
}

func (s *dataService) repositoryMutation(ctx context.Context, cluster string, raftID uint64, size int, requestID string, command rootstate.Command, value repository.Repository) (*datawire.RepositoryMutationResponse, error) {
	if rootstate.ValidateRequestID(requestID) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Repository request ID")
	}
	ctx, release, err := s.begin(ctx, cluster, raftID, size)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.rootCommand(ctx, command); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "commit Repository configuration: %v", err)
	}
	result, found, err := s.runtime.Root.Result(requestID)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Repository result: %v", err)
	}
	if !found {
		return nil, status.Error(codes.Unavailable, "Repository command result is missing")
	}
	if result.Error != "" {
		return nil, status.Error(codes.FailedPrecondition, result.Error)
	}
	return &datawire.RepositoryMutationResponse{Repository: repositoryToWire(value), RootIndex: result.Index}, nil
}

func (s *dataService) CreateRepository(ctx context.Context, request *datawire.CreateRepositoryRequest) (*datawire.RepositoryMutationResponse, error) {
	if request == nil || proto.Size(request) > 128<<10 {
		return nil, status.Error(codes.InvalidArgument, "invalid Repository request")
	}
	value, err := repositoryFromWire(request.GetRepository())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid Repository configuration: %v", err)
	}
	return s.repositoryMutation(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request), request.GetRequestId(),
		rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.GetRequestId(), Type: rootstate.CreateRepository, Repository: &value}, value)
}

func (s *dataService) UpdateRepository(ctx context.Context, request *datawire.UpdateRepositoryRequest) (*datawire.RepositoryMutationResponse, error) {
	if request == nil || proto.Size(request) > 128<<10 {
		return nil, status.Error(codes.InvalidArgument, "invalid Repository request")
	}
	value, err := repositoryFromWire(request.GetRepository())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid Repository configuration: %v", err)
	}
	return s.repositoryMutation(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request), request.GetRequestId(),
		rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.GetRequestId(), Type: rootstate.UpdateRepository, Repository: &value}, value)
}

func (s *dataService) DeleteRepository(ctx context.Context, request *datawire.DeleteRepositoryRequest) (*datawire.RepositoryMutationResponse, error) {
	if request == nil || proto.Size(request) > 32<<10 || repository.ValidateName(request.GetName()) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Repository name")
	}
	value, found, err := s.runtime.Root.Repository(request.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Repository before deletion: %v", err)
	}
	if !found {
		return nil, status.Error(codes.NotFound, "Repository is not registered")
	}
	return s.repositoryMutation(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request), request.GetRequestId(),
		rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.GetRequestId(), Type: rootstate.DeleteRepository, RepositoryDelete: &rootstate.RepositoryDelete{Name: request.GetName()}}, value)
}

func (s *dataService) SetDefaultRepository(ctx context.Context, request *datawire.SetDefaultRepositoryRequest) (*datawire.RepositoryMutationResponse, error) {
	if request == nil || proto.Size(request) > 32<<10 || repository.ValidateName(request.GetName()) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid default Repository name")
	}
	value, found, err := s.runtime.Root.Repository(request.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Repository before selecting default: %v", err)
	}
	if !found {
		return nil, status.Error(codes.NotFound, "Repository is not registered")
	}
	return s.repositoryMutation(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request), request.GetRequestId(),
		rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.GetRequestId(), Type: rootstate.SetDefaultRepository, RepositoryDefault: &rootstate.RepositoryDefault{Name: request.GetName()}}, value)
}

func (s *dataService) ListRepositories(ctx context.Context, request *datawire.ListRepositoriesRequest) (*datawire.ListRepositoriesResponse, error) {
	if request == nil || proto.Size(request) > 16<<10 {
		return nil, status.Error(codes.InvalidArgument, "invalid Repository list request")
	}
	_, release, err := s.begin(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	values, err := s.runtime.Root.Repositories()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "list Repositories: %v", err)
	}
	response := &datawire.ListRepositoriesResponse{RootIndex: s.runtime.Root.AppliedIndex()}
	for _, value := range values {
		response.Repositories = append(response.Repositories, repositoryToWire(value))
	}
	if value, found, err := s.runtime.Root.DefaultRepository(); err != nil {
		return nil, status.Errorf(codes.Unavailable, "read default Repository: %v", err)
	} else if found {
		response.DefaultName = value.Name
	}
	return response, nil
}

func backupJobWorkerToWire(value rootstate.BackupWorker) *datawire.BackupJobWorker {
	return &datawire.BackupJobWorker{RaftId: value.RaftID, NodeId: value.NodeID, State: value.State,
		LeaseEpoch: value.LeaseEpoch, LeaseExpiresUnix: value.LeaseExpiresUnix, Bytes: value.Bytes,
		Objects: value.Objects, Error: value.Error, LeaseRaftId: value.LeaseRaftID, LeaseNodeId: value.LeaseNodeID}
}

func backupJobToWire(value rootstate.BackupJob) *datawire.BackupJob {
	result := &datawire.BackupJob{JobId: value.JobID, Repository: value.Repository, ClusterId: value.ClusterID,
		Epoch: value.Epoch, State: value.State, CreatedIndex: value.CreatedIndex,
		CompletedIndex: value.CompletedIndex, LastError: value.LastError}
	for _, worker := range value.Workers {
		result.Workers = append(result.Workers, backupJobWorkerToWire(worker))
	}
	return result
}

func (s *dataService) readBackupJob(jobID string) (*datawire.BackupJobResponse, error) {
	job, found, err := s.runtime.Root.BackupJob(jobID)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Backup job: %v", err)
	}
	if !found {
		return nil, status.Error(codes.NotFound, "Backup job is not registered")
	}
	return &datawire.BackupJobResponse{Job: backupJobToWire(job), RootIndex: s.runtime.Root.AppliedIndex()}, nil
}

func (s *dataService) backupWorkerNode(jobID string, raftID uint64, nodeID string) (string, error) {
	if raftID == s.runtime.Identity.RaftID && nodeID == "" {
		return s.runtime.Identity.NodeID, nil
	}
	job, found, err := s.runtime.Root.BackupJob(jobID)
	if err != nil {
		return "", status.Errorf(codes.Unavailable, "read Backup job worker: %v", err)
	}
	if !found {
		return "", status.Error(codes.NotFound, "Backup job is not registered")
	}
	for _, worker := range job.Workers {
		if worker.RaftID == raftID {
			if nodeID != "" && nodeID != worker.NodeID {
				return "", status.Error(codes.InvalidArgument, "Backup worker identity does not match the job")
			}
			return worker.NodeID, nil
		}
	}
	return "", status.Error(codes.NotFound, "Backup worker is not assigned to the job")
}

func (s *dataService) backupJobMutation(ctx context.Context, cluster string, raftID uint64, size int, requestID string, command rootstate.Command, jobID string) (*datawire.BackupJobResponse, error) {
	if rootstate.ValidateRequestID(requestID) != nil || rootstate.ValidateRequestID(jobID) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Backup job request ID")
	}
	ctx, release, err := s.begin(ctx, cluster, raftID, size)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.rootCommand(ctx, command); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "commit Backup job: %v", err)
	}
	return s.readBackupJob(jobID)
}

func (s *dataService) CreateBackupJob(ctx context.Context, request *datawire.CreateBackupJobRequest) (*datawire.BackupJobResponse, error) {
	if request == nil || proto.Size(request) > 32<<10 || rootstate.ValidateRequestID(request.GetRequestId()) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Backup job request")
	}
	jobID := request.GetJobId()
	if jobID == "" {
		jobID = request.GetRequestId()
	}
	if rootstate.ValidateRequestID(jobID) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Backup job ID")
	}
	repositoryName := request.GetRepository()
	if repositoryName == "" {
		value, found, err := s.runtime.Root.DefaultRepository()
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read default Backup Repository: %v", err)
		}
		if !found {
			return nil, status.Error(codes.FailedPrecondition, "no default Backup Repository is configured")
		}
		repositoryName = value.Name
	}
	if err := repository.ValidateName(repositoryName); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid Backup Repository: %v", err)
	}
	value, found, err := s.runtime.Root.Repository(repositoryName)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Backup Repository: %v", err)
	}
	if !found {
		return nil, status.Error(codes.NotFound, "Backup Repository is not registered")
	}
	job := rootstate.BackupJob{JobID: jobID, Repository: value.Name, ClusterID: request.GetClusterId()}
	return s.backupJobMutation(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request), request.GetRequestId(),
		rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.GetRequestId(), Type: rootstate.CreateBackupJob, BackupJob: &job}, jobID)
}

func (s *dataService) GetBackupJob(ctx context.Context, request *datawire.GetBackupJobRequest) (*datawire.BackupJobResponse, error) {
	if request == nil || proto.Size(request) > 16<<10 || rootstate.ValidateRequestID(request.GetJobId()) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Backup job ID")
	}
	_, release, err := s.begin(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	return s.readBackupJob(request.GetJobId())
}

func (s *dataService) ListBackupJobs(ctx context.Context, request *datawire.ListBackupJobsRequest) (*datawire.ListBackupJobsResponse, error) {
	if request == nil || proto.Size(request) > 16<<10 {
		return nil, status.Error(codes.InvalidArgument, "invalid Backup job list request")
	}
	_, release, err := s.begin(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	jobs, err := s.runtime.Root.BackupJobs()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "list Backup jobs: %v", err)
	}
	response := &datawire.ListBackupJobsResponse{RootIndex: s.runtime.Root.AppliedIndex()}
	for _, job := range jobs {
		response.Jobs = append(response.Jobs, backupJobToWire(job))
	}
	return response, nil
}

func (s *dataService) ClaimBackupJob(ctx context.Context, request *datawire.ClaimBackupJobRequest) (*datawire.BackupJobResponse, error) {
	if request == nil || proto.Size(request) > 16<<10 || rootstate.ValidateRequestID(request.GetRequestId()) != nil || rootstate.ValidateRequestID(request.GetJobId()) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Backup lease claim")
	}
	now := request.GetObservedUnix()
	if now == 0 {
		now = time.Now().Unix()
	}
	expires := request.GetLeaseExpiresUnix()
	if expires == 0 {
		expires = now + 10*60
	}
	workerRaftID := request.GetWorkerRaftId()
	if workerRaftID == 0 {
		workerRaftID = request.GetFromRaftId()
	}
	workerNodeID, err := s.backupWorkerNode(request.GetJobId(), workerRaftID, request.GetWorkerNodeId())
	if err != nil {
		return nil, err
	}
	claim := rootstate.BackupJobClaim{JobID: request.GetJobId(), RaftID: workerRaftID, NodeID: workerNodeID,
		ClaimantRaftID: request.GetFromRaftId(), ClaimantNodeID: s.runtime.Identity.NodeID,
		ObservedUnix: now, LeaseExpiresUnix: expires}
	return s.backupJobMutation(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request), request.GetRequestId(),
		rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.GetRequestId(), Type: rootstate.ClaimBackupJob, BackupJobClaim: &claim}, claim.JobID)
}

func (s *dataService) CompleteBackupJob(ctx context.Context, request *datawire.CompleteBackupJobRequest) (*datawire.BackupJobResponse, error) {
	if request == nil || proto.Size(request) > 16<<10 || rootstate.ValidateRequestID(request.GetRequestId()) != nil || rootstate.ValidateRequestID(request.GetJobId()) != nil || request.GetLeaseEpoch() == 0 || request.GetBytes() < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid Backup completion")
	}
	workerRaftID := request.GetWorkerRaftId()
	if workerRaftID == 0 {
		workerRaftID = request.GetFromRaftId()
	}
	workerNodeID, err := s.backupWorkerNode(request.GetJobId(), workerRaftID, request.GetWorkerNodeId())
	if err != nil {
		return nil, err
	}
	completion := rootstate.BackupJobCompletion{JobID: request.GetJobId(), RaftID: workerRaftID, NodeID: workerNodeID,
		ClaimantRaftID: request.GetFromRaftId(), ClaimantNodeID: s.runtime.Identity.NodeID,
		LeaseEpoch: request.GetLeaseEpoch(), Bytes: request.GetBytes(), Objects: request.GetObjects()}
	return s.backupJobMutation(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request), request.GetRequestId(),
		rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.GetRequestId(), Type: rootstate.CompleteBackupJob, BackupJobCompletion: &completion}, completion.JobID)
}

func (s *dataService) FailBackupJob(ctx context.Context, request *datawire.FailBackupJobRequest) (*datawire.BackupJobResponse, error) {
	if request == nil || proto.Size(request) > 16<<10 || rootstate.ValidateRequestID(request.GetRequestId()) != nil || rootstate.ValidateRequestID(request.GetJobId()) != nil || request.GetLeaseEpoch() == 0 || len(request.GetError()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid Backup failure")
	}
	workerRaftID := request.GetWorkerRaftId()
	if workerRaftID == 0 {
		workerRaftID = request.GetFromRaftId()
	}
	workerNodeID, err := s.backupWorkerNode(request.GetJobId(), workerRaftID, request.GetWorkerNodeId())
	if err != nil {
		return nil, err
	}
	failure := rootstate.BackupJobFailure{JobID: request.GetJobId(), RaftID: workerRaftID, NodeID: workerNodeID,
		ClaimantRaftID: request.GetFromRaftId(), ClaimantNodeID: s.runtime.Identity.NodeID,
		LeaseEpoch: request.GetLeaseEpoch(), Error: request.GetError()}
	return s.backupJobMutation(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request), request.GetRequestId(),
		rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.GetRequestId(), Type: rootstate.FailBackupJob, BackupJobFailure: &failure}, failure.JobID)
}

func (s *dataService) FinalizeBackupJob(ctx context.Context, request *datawire.FinalizeBackupJobRequest) (*datawire.BackupJobResponse, error) {
	if request == nil || proto.Size(request) > 16<<10 || rootstate.ValidateRequestID(request.GetRequestId()) != nil || rootstate.ValidateRequestID(request.GetJobId()) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Backup finalization")
	}
	return s.backupJobMutation(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request), request.GetRequestId(),
		rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.GetRequestId(), Type: rootstate.FinalizeBackupJob, BackupJobReference: &rootstate.BackupJobReference{JobID: request.GetJobId()}}, request.GetJobId())
}

// AddVolume is deliberately local to the Store-owning Node. Unlike data and
// Tenant administration it does not need a Root quorum: a disk can be added
// while the Cluster is degraded. The caller's installed Node certificate and
// the endpoint certificate are pinned to this same local Store identity.
func (s *dataService) AddVolume(ctx context.Context, request *datawire.AddVolumeRequest) (*datawire.AddVolumeResponse, error) {
	if request == nil || proto.Size(request) > 8<<10 || !filepath.IsAbs(request.GetPath()) ||
		request.GetPath() == "" || len(request.GetPath()) > 4096 {
		return nil, status.Error(codes.InvalidArgument, "absolute Volume directory required")
	}
	select {
	case s.volumeSlots <- struct{}{}:
		defer func() { <-s.volumeSlots }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "Node administration is busy")
	}
	local := s.runtime.Identity
	if request.GetClusterId() != local.ClusterID || request.GetFromRaftId() != local.RaftID {
		return nil, status.Error(codes.PermissionDenied, "Volume request differs from local Node")
	}
	leaf, err := identity.VerifiedPeerCertificate(ctx, time.Now())
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, "verified local Node certificate required")
	}
	caller, err := identity.ParseNodeCertificate(leaf)
	if err != nil || caller.ClusterID != local.ClusterID || caller.NodeID != local.NodeID || caller.RaftID != local.RaftID {
		return nil, status.Error(codes.PermissionDenied, "Volume administration requires this Node's certificate")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, "Volume request canceled before registration")
	}
	record, err := s.runtime.Volumes.Add(request.GetPath())
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "register local Volume (outcome may require retry or restart): %v", err)
	}
	return &datawire.AddVolumeResponse{VolumeId: record.ID, Path: record.Path, State: string(record.State)}, nil
}

func (s *dataService) authorizeVolumeAdmin(ctx context.Context, cluster string, raftID uint64) error {
	local := s.runtime.Identity
	if cluster != local.ClusterID || raftID != local.RaftID {
		return status.Error(codes.PermissionDenied, "Volume request differs from local Node")
	}
	leaf, err := identity.VerifiedPeerCertificate(ctx, time.Now())
	if err != nil {
		return status.Error(codes.PermissionDenied, "verified local Node certificate required")
	}
	caller, err := identity.ParseNodeCertificate(leaf)
	if err != nil || caller.ClusterID != local.ClusterID || caller.NodeID != local.NodeID || caller.RaftID != local.RaftID {
		return status.Error(codes.PermissionDenied, "Volume administration requires this Node's certificate")
	}
	return nil
}

func (s *dataService) volumeResponse(id string, response func(volume.Record) proto.Message) (proto.Message, error) {
	report, err := s.runtime.Volumes.Status()
	if err != nil {
		return nil, err
	}
	for _, item := range report.Volumes {
		if item.ID == id {
			return response(item.Record), nil
		}
	}
	return nil, errors.New("Volume disappeared after administration")
}

func (s *dataService) DrainVolume(ctx context.Context, request *datawire.DrainVolumeRequest) (*datawire.DrainVolumeResponse, error) {
	if request == nil || proto.Size(request) > 8<<10 || request.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID is required")
	}
	select {
	case s.volumeSlots <- struct{}{}:
		defer func() { <-s.volumeSlots }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "Node administration is busy")
	}
	if err := s.authorizeVolumeAdmin(ctx, request.GetClusterId(), request.GetFromRaftId()); err != nil {
		return nil, err
	}
	if s.runtime.Packs == nil {
		return nil, status.Error(codes.FailedPrecondition, "Pack pool is not available")
	}
	if err := s.runtime.Packs.DrainVolume(ctx, request.GetVolumeId()); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Drain Volume: %v", err)
	}
	if s.runtime.Records != nil {
		if err := s.runtime.Records.BeginVolumeMigration(request.GetVolumeId()); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "record Volume migration: %v", err)
		}
	}
	value, err := s.volumeResponse(request.GetVolumeId(), func(record volume.Record) proto.Message {
		return &datawire.DrainVolumeResponse{VolumeId: record.ID, Path: record.Path, State: string(record.State)}
	})
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return value.(*datawire.DrainVolumeResponse), nil
}

func (s *dataService) RemoveVolume(ctx context.Context, request *datawire.RemoveVolumeRequest) (*datawire.RemoveVolumeResponse, error) {
	if request == nil || proto.Size(request) > 8<<10 || request.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID is required")
	}
	select {
	case s.volumeSlots <- struct{}{}:
		defer func() { <-s.volumeSlots }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "Node administration is busy")
	}
	if err := s.authorizeVolumeAdmin(ctx, request.GetClusterId(), request.GetFromRaftId()); err != nil {
		return nil, err
	}
	if s.runtime.Packs == nil {
		return nil, status.Error(codes.FailedPrecondition, "Pack pool is not available")
	}
	if s.maintenance != nil {
		release, err := s.maintenance.TryAcquire(64 << 20)
		if err != nil {
			return nil, status.Errorf(codes.ResourceExhausted, "maintenance budget is busy: %v", err)
		}
		defer release()
	}
	// A readable drained Volume is migrated before its metadata is removed.
	// Missing or invalid media is an explicit operator decision: the local
	// Store cannot read it, so cross-Node/Region repair remains responsible for
	// restoring its records after removal.
	if s.runtime.Records != nil {
		report, reportErr := s.runtime.Volumes.Status()
		if reportErr != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "inspect Volume before removal: %v", reportErr)
		}
		for _, item := range report.Volumes {
			if item.ID != request.GetVolumeId() {
				continue
			}
			if item.Health == "missing" || item.Health == "invalid" {
				// A failed or missing Volume may be removed only after the
				// administrator has explicitly chosen it. Transition it through
				// the same durable draining state as a readable Volume so the
				// final RemoveDrained gate cannot observe an active Volume. Do
				// this through the registry: opening a Pack collection would
				// require the failed path to exist.
				if _, err := s.runtime.Volumes.BeginDrain(item.ID); err != nil {
					return nil, status.Errorf(codes.FailedPrecondition, "drain lost Volume before removal: %v", err)
				}
				if err := s.runtime.Records.AbandonVolumeMigration(item.ID); err != nil {
					return nil, status.Errorf(codes.FailedPrecondition, "record lost Volume decision: %v", err)
				}
			} else if item.State != volume.Removed {
				maintenanceCtx, finish, admissionErr := s.io.TryStart(ctx, "volume-drain")
				if admissionErr != nil {
					return nil, status.Error(codes.ResourceExhausted, admissionErr.Error())
				}
				defer finish()
				ctx = maintenanceCtx

				if _, err := s.runtime.Records.MigrateVolume(ctx, item.ID); err != nil {
					return nil, status.Errorf(codes.FailedPrecondition, "migrate Volume data before removal: %v", err)
				}
				if _, err := s.runtime.Records.Compact(ctx); err != nil {
					return nil, status.Errorf(codes.FailedPrecondition, "reclaim migrated Volume data: %v", err)
				}
			}
			break
		}
	}
	if err := s.runtime.Packs.RemoveDrained(ctx, request.GetVolumeId()); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Remove Volume: %v", err)
	}
	value, err := s.volumeResponse(request.GetVolumeId(), func(record volume.Record) proto.Message {
		return &datawire.RemoveVolumeResponse{VolumeId: record.ID, Path: record.Path, State: string(record.State)}
	})
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return value.(*datawire.RemoveVolumeResponse), nil
}

// CompactStore is explicit maintenance, never an implicit write-path action.
// The conservative mode removes only wholly unreachable Packs; rewrite mode
// relocates live records first so mixed Packs can be reclaimed safely.
func (s *dataService) CompactStore(ctx context.Context, request *datawire.CompactStoreRequest) (*datawire.CompactStoreResponse, error) {
	if request == nil || proto.Size(request) > 8<<10 {
		return nil, status.Error(codes.InvalidArgument, "invalid compaction request")
	}
	select {
	case s.volumeSlots <- struct{}{}:
		defer func() { <-s.volumeSlots }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "Node maintenance is busy")
	}
	if err := s.authorizeVolumeAdmin(ctx, request.GetClusterId(), request.GetFromRaftId()); err != nil {
		return nil, err
	}
	if s.runtime.Records == nil {
		return nil, status.Error(codes.FailedPrecondition, "immutable record Store is not available")
	}
	if s.maintenance != nil {
		release, err := s.maintenance.TryAcquire(64 << 20)
		if err != nil {
			return nil, status.Errorf(codes.ResourceExhausted, "maintenance budget is busy: %v", err)
		}
		defer release()
	}
	maintenanceCtx, finish, admissionErr := s.io.TryStart(ctx, "compaction")
	if admissionErr != nil {
		return nil, status.Error(codes.ResourceExhausted, admissionErr.Error())
	}
	defer finish()
	ctx = maintenanceCtx
	var report records.CompactReport
	var err error
	if request.GetRewrite() {
		report, err = s.runtime.Records.RewriteCompaction(ctx)
	} else {
		report, err = s.runtime.Records.Compact(ctx)
	}
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "compact Store: %v", err)
	}
	return &datawire.CompactStoreResponse{ScannedPacks: uint64(report.ScannedPacks),
		RemovedPacks: uint64(report.RemovedPacks), ReclaimedBytes: report.ReclaimedBytes,
		Rewritten: request.GetRewrite()}, nil
}

// LocalRepairStatus is a bounded diagnostic of this process's most recent
// repair observations. Empty issues never assert full data coverage.
func (s *dataService) LocalRepairStatus(ctx context.Context, request *datawire.LocalRepairStatusRequest) (*datawire.LocalRepairStatusResponse, error) {
	if request == nil || proto.Size(request) > 4096 {
		return nil, status.Error(codes.InvalidArgument, "invalid local repair status request")
	}
	local := s.runtime.Identity
	if request.GetClusterId() != local.ClusterID || request.GetFromRaftId() != local.RaftID {
		return nil, status.Error(codes.PermissionDenied, "repair status request differs from local Node")
	}
	leaf, err := identity.VerifiedPeerCertificate(ctx, time.Now())
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, "verified local Node certificate required")
	}
	caller, err := identity.ParseNodeCertificate(leaf)
	if err != nil || caller.ClusterID != local.ClusterID || caller.NodeID != local.NodeID || caller.RaftID != local.RaftID {
		return nil, status.Error(codes.PermissionDenied, "repair status requires this Node's certificate")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return s.localRepairStatusResponse()
}

// ClusterRepairStatus is the peer-facing diagnostic path. A current Root
// member may ask this Node for its own bounded state; the serving Node's
// certificate and response identity remain authoritative for this report.
//
// This is deliberately a best-effort observation path. It validates the
// caller against this Node's latest applied Root membership, but does not
// perform a linearizable Root read. Status must remain usable while a quorum
// is degraded and must never create a fan-out of nested ReadIndex calls that
// competes with the actual data and membership paths.
func (s *dataService) ClusterRepairStatus(ctx context.Context, request *datawire.LocalRepairStatusRequest) (*datawire.LocalRepairStatusResponse, error) {
	if request == nil || proto.Size(request) > 4096 || request.GetFromRaftId() == 0 || request.GetClusterId() != s.runtime.Identity.ClusterID {
		return nil, status.Error(codes.InvalidArgument, "invalid cluster repair status request")
	}
	leaf, err := identity.VerifiedPeerCertificate(ctx, time.Now())
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, "verified Node certificate required")
	}
	caller, err := identity.ParseNodeCertificate(leaf)
	if err != nil || caller.ClusterID != s.runtime.Identity.ClusterID || caller.RaftID != request.GetFromRaftId() {
		return nil, status.Error(codes.PermissionDenied, "cluster repair status caller identity mismatch")
	}
	member, found, err := s.runtime.Root.RootMember(caller.RaftID)
	if err != nil || !found || member.NodeID != caller.NodeID {
		return nil, status.Error(codes.PermissionDenied, "cluster repair status caller is not a current Root member")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return s.localRepairStatusResponse()
}

func (s *dataService) localRepairStatusResponse() (*datawire.LocalRepairStatusResponse, error) {
	local := s.runtime.Identity
	var response *datawire.LocalRepairStatusResponse
	if s.repair == nil {
		response = &datawire.LocalRepairStatusResponse{}
	} else {
		response = encodeRepairStatus(s.repair.snapshot())
	}
	if s.runtime.Records != nil {
		migration, err := s.runtime.Records.VolumeMigration()
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read Volume migration status: %v", err)
		}
		response.VolumeMigration = &datawire.VolumeMigrationStatus{Active: migration.Active,
			SourceVolumeId: migration.SourceVolumeID, Cursor: migration.Cursor,
			Migrated: migration.Migrated, LastError: migration.LastError, TargetVolumeId: migration.TargetVolumeID, SourcePackId: migration.SourcePackID}
		if !migration.StartedAt.IsZero() {
			response.VolumeMigration.StartedUnix = migration.StartedAt.Unix()
		}
		for volumeID, message := range s.runtime.Records.WriteErrors() {
			if len(response.VolumeWriteErrors) >= 64 {
				break
			}
			response.VolumeWriteErrors = append(response.VolumeWriteErrors, &datawire.VolumeWriteIssue{VolumeId: volumeID, Error: message})
		}
	}
	if s.runtime.Volumes != nil {
		report, err := s.runtime.Volumes.Status()
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read Volume health: %v", err)
		}
		response.CapacitySupported = true
		var blocked map[string]string
		if s.runtime.Packs != nil {
			blocked = s.runtime.Packs.WriteErrors()
		}
		response.WritableBytes = volume.WritableCapacity(report, blocked)
		capacity := volume.WritableDiskCapacity(report, blocked)
		response.CapacityTotalBytes, response.CapacityAvailableBytes = capacity.TotalBytes, capacity.AvailableBytes
		for _, item := range report.Volumes {
			if len(response.Volumes) >= 64 {
				break
			}
			response.Volumes = append(response.Volumes, &datawire.VolumeHealthStatus{VolumeId: item.ID,
				State: string(item.State), Health: item.Health, Error: item.Error})
		}
	}
	if s.io != nil {
		ioStatus := s.io.Status()
		response.MaintenanceIo = &datawire.MaintenanceIOStatus{Active: ioStatus.Active, Foreground: ioStatus.Foreground,
			BytesPerSecond: ioStatus.BytesPerSecond, AccountedBytes: ioStatus.AccountedBytes, Deferred: ioStatus.Deferred}
	}
	if s.maintenance != nil {
		budget := s.maintenance.Status()
		response.MaintenanceBudget = &datawire.ResourceBudgetStatus{LimitBytes: budget.LimitBytes,
			UsedBytes: budget.UsedBytes, PeakBytes: budget.PeakBytes, Rejected: budget.Rejected}
	}
	if s.requestBudget != nil {
		budget := s.requestBudget.Status()
		response.RequestBudget = &datawire.ResourceBudgetStatus{LimitBytes: budget.LimitBytes,
			UsedBytes: budget.UsedBytes, PeakBytes: budget.PeakBytes, Rejected: budget.Rejected}
	}
	if s.geoRetry != nil {
		count, err := s.geoRetry.Count()
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read Geo metadata debt: %v", err)
		}
		response.GeoMetadataDebt = uint64(count)
	}
	if s.geoRecords != nil {
		count, err := s.geoRecords.Count()
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read Geo record debt: %v", err)
		}
		response.GeoRecordDebt = uint64(count)
	}
	response.NodeId = local.NodeID
	response.RaftId = local.RaftID
	return response, nil
}

func encodeRepairStatus(observation repairObservation) *datawire.LocalRepairStatusResponse {
	response := &datawire.LocalRepairStatusResponse{WorkerEnabled: true, ScanProgressSupported: true,
		TotalScanGroups: uint32(len(observation.Progress))}
	response.LastCheckUnix = observation.LastCheck.Unix()
	if observation.LastCheck.IsZero() {
		response.LastCheckUnix = 0
	}
	response.TotalIssues = uint32(len(observation.Issues))
	for _, issue := range observation.Issues[:min(len(observation.Issues), 64)] {
		message := []rune(issue.Error)
		if len(message) > 1024 {
			message = message[:1024]
		}
		response.Issues = append(response.Issues, &datawire.LocalRepairIssue{GroupId: issue.GroupID, Error: string(message)})
	}
	for _, progress := range observation.Progress[:min(len(observation.Progress), 64)] {
		response.ScanProgress = append(response.ScanProgress, &datawire.RepairScanProgress{
			GroupId: progress.GroupID, Phase: progress.Phase, Scanned: progress.Scanned,
			Unresolved: progress.Unresolved, NeedsCleanSweep: progress.NeedsCleanSweep,
			StartedUnix: progress.StartedUnix, LastProgressUnix: progress.LastProgressUnix, LastCleanUnix: progress.LastCleanUnix})
	}
	return response
}

func newDataService(runtime *node.Runtime, client *raftrpc.GroupClient, routes *raftrpc.RootDirectory) *dataService {
	return &dataService{runtime: runtime, client: client, routes: routes,
		slots: make(chan struct{}, 8), volumeSlots: make(chan struct{}, 1),
		maintenance: resource.NewByteBudget(128 << 20), io: resource.NewMaintenance(0), requestBudget: resource.NewByteBudget(128 << 20)}
}

func (s *dataService) begin(ctx context.Context, cluster string, raftID uint64, size int) (context.Context, func(), error) {
	if size > maxDemoRecordBytes+(32<<10) {
		return nil, nil, status.Error(codes.ResourceExhausted, "data request exceeds limit")
	}
	queueStarted := time.Now()
	select {
	case s.slots <- struct{}{}:
		recordwrite.Trace(ctx, "rpc.begin_slot_wait", queueStarted)
		ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
		budgetRelease := func() {}
		if s.requestBudget != nil {
			var err error
			budgetRelease, err = s.requestBudget.TryAcquire(uint64(max(size, 1)))
			if err != nil {
				cancel()
				<-s.slots
				return nil, nil, status.Error(codes.ResourceExhausted, "data request buffers are busy")
			}
		}
		foregroundDone := s.io.Foreground()
		release := func() { foregroundDone(); budgetRelease(); cancel(); <-s.slots }
		if cluster != s.runtime.Identity.ClusterID || raftID == 0 {
			release()
			return nil, nil, status.Error(codes.PermissionDenied, "data request belongs to a different Cluster")
		}
		leaf, err := identity.VerifiedPeerCertificate(ctx, time.Now())
		if err != nil {
			release()
			return nil, nil, status.Error(codes.PermissionDenied, "verified Node certificate required")
		}
		parsed, err := identity.ParseNodeCertificate(leaf)
		if err != nil || parsed.ClusterID != cluster || parsed.RaftID != raftID {
			release()
			return nil, nil, status.Error(codes.PermissionDenied, "matching Node certificate required")
		}
		phaseStarted := time.Now()
		rootIndex, err := s.runtime.ReadRootIndex(ctx)
		if err != nil {
			recordwrite.Trace(ctx, "rpc.begin_root_membership_read_index", phaseStarted)
			release()
			return nil, nil, status.Errorf(codes.Unavailable, "confirm Root membership: %v", err)
		}
		recordwrite.Trace(ctx, "rpc.begin_root_membership_read_index", phaseStarted)
		if _, err := identity.VerifiedPeerCertificate(ctx, time.Now()); err != nil {
			release()
			return nil, nil, status.Error(codes.PermissionDenied, "Node certificate expired during Root confirmation")
		}
		member, found, err := s.runtime.Root.RootMember(raftID)
		if err != nil || !found || member.NodeID != parsed.NodeID {
			release()
			return nil, nil, status.Error(codes.PermissionDenied, "Node is not a current Root member")
		}
		return router.WithConfirmedRootIndex(ctx, rootIndex), release, nil
	default:
		return nil, nil, status.Error(codes.ResourceExhausted, "data entry is busy")
	}
}

// CreateTenant is Node-authenticated administration, not a storage-container API.
func (s *dataService) CreateTenant(ctx context.Context, request *datawire.CreateTenantRequest) (*datawire.CreateTenantResponse, error) {
	if request == nil || proto.Size(request) > 32<<10 || len(request.ProtoReflect().GetUnknown()) != 0 || rootstate.ValidateRequestID(request.GetRequestId()) != nil || rootstate.ValidateTenantID(request.GetTenantId()) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Tenant request")
	}
	ctx, release, err := s.begin(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.rootCommand(ctx, rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.RequestId, Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: request.TenantId}}); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "create Tenant: %v", err)
	}
	return &datawire.CreateTenantResponse{RootIndex: s.runtime.Root.AppliedIndex()}, nil
}

// CreateTenantToken is a Node-authenticated administrative action. The
// plaintext is generated after Root admission and returned exactly once; an
// uncertain/lost response cannot be reconstructed from the hash in Root.
func (s *dataService) CreateTenantToken(ctx context.Context, request *datawire.CreateTenantTokenRequest) (*datawire.CreateTenantTokenResponse, error) {
	if request == nil || proto.Size(request) > 32<<10 || rootstate.ValidateRequestID(request.RequestId) != nil ||
		rootstate.ValidateTenantID(request.TenantId) != nil || request.TtlSeconds == 0 ||
		request.TtlSeconds > maxTenantTokenTTLSeconds {
		return nil, status.Error(codes.InvalidArgument, "invalid Tenant Token request")
	}
	ctx, release, err := s.begin(ctx, request.ClusterId, request.FromRaftId, proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	if _, found, err := s.runtime.Root.Result(request.RequestId); err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Token issuance result: %v", err)
	} else if found {
		return nil, status.Error(codes.FailedPrecondition, "Token issuance request already has a Root result; plaintext cannot be recovered")
	}
	if _, found, err := s.runtime.Root.Tenant(request.TenantId); err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Tenant: %v", err)
	} else if !found {
		return nil, status.Error(codes.NotFound, "Tenant is not registered")
	}
	token, credential, err := auth.Generate(request.TenantId, time.Now().UTC(), time.Duration(request.TtlSeconds)*time.Second)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate Tenant Token: %v", err)
	}
	command := rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.RequestId,
		Type: rootstate.CreateTenantCredential, TenantCredential: &credential}
	if err := s.rootCommand(ctx, command); err != nil {
		return nil, status.Errorf(codes.Unavailable, "commit Tenant Token hash (issuance outcome may be uncertain): %v", err)
	}
	return &datawire.CreateTenantTokenResponse{CredentialId: credential.CredentialID,
		Token: token, ExpiresUnix: credential.ExpiresUnix}, nil
}

func (s *dataService) RevokeTenantToken(ctx context.Context, request *datawire.RevokeTenantTokenRequest) (*datawire.RevokeTenantTokenResponse, error) {
	if request == nil || proto.Size(request) > 32<<10 || rootstate.ValidateRequestID(request.RequestId) != nil ||
		rootstate.ValidateTenantCredentialID(request.CredentialId) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Tenant Token revocation")
	}
	ctx, release, err := s.begin(ctx, request.ClusterId, request.FromRaftId, proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	command := rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.RequestId,
		Type:             rootstate.RevokeTenantCredential,
		TenantRevocation: &rootstate.TenantCredentialRevocation{CredentialID: request.CredentialId}}
	if err := s.rootCommand(ctx, command); err != nil {
		return nil, status.Errorf(codes.Unavailable, "revoke Tenant Token: %v", err)
	}
	result, found, err := s.runtime.Root.Result(request.RequestId)
	if err != nil || !found {
		return nil, status.Errorf(codes.Unavailable, "read revocation result: %v", err)
	}
	return &datawire.RevokeTenantTokenResponse{RootIndex: result.Index}, nil
}

func (s *dataService) rootCommand(ctx context.Context, command rootstate.Command) error {
	return s.rootCommandWithSubmit(ctx, command, s.runtime.Host.Submit)
}

func (s *dataService) rootCommandWithSubmit(ctx context.Context, command rootstate.Command, submit func(string, []byte) error) error {
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		return err
	}
	if err := submit(raft.RootGroupID, encoded); err != nil {
		return err
	}
	// A ReadIndex begun immediately after a forwarded proposal may precede the
	// proposal in the leader's log. Wait for this exact durable RequestID/hash.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := s.runtime.ReadRootIndex(ctx); err != nil {
			return err
		}
		result, found, err := s.runtime.Root.ResultForCommand(encoded)
		if err != nil {
			return err
		}
		if found {
			if result.Error != "" {
				return errors.New(result.Error)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *dataService) PutRecord(ctx context.Context, request *datawire.PutRecordRequest) (*datawire.PutRecordResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || rootstate.ValidateRequestID(request.GetRequestId()) != nil || len(request.GetValue()) > maxDemoRecordBytes || proto.Size(request) > maxDemoRecordBytes+(32<<10) {
		return nil, status.Error(codes.InvalidArgument, "invalid Record mutation")
	}
	started := time.Now()
	ctx = withDataTrace(ctx, "put", request.GetRequestId())
	defer recordwrite.Trace(ctx, "rpc.put.total", started)
	phaseStarted := time.Now()
	ctx, release, err := s.begin(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request))
	recordwrite.Trace(ctx, "rpc.put.begin", phaseStarted)
	if err != nil {
		return nil, err
	}
	defer release()
	entry, err := enginedata.NewWithGeoRetry(s.runtime, s.client, s.geoRetry)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "prepare immutable Record entry: %v", err)
	}
	if s.geoRecords != nil {
		entry.SetGeoRetryStores(s.geoRetry, s.geoRecords)
	}
	binding, err := entry.Bind(record.ModuleID, record.EngineID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	bound, err := binding.ForTenant(request.TenantId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	phaseStarted = time.Now()
	result, err := bound.Write(ctx, enginedata.BoundWrite{RequestID: request.RequestId,
		Key: request.Key, Body: request.Value, Publish: func(descriptor records.Descriptor) ([]byte, error) {
			return record.ReferencePutPayload(descriptor)
		}})
	recordwrite.Trace(ctx, "rpc.put.engine_write", phaseStarted)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "write Record with physical majority: %v", err)
	}
	return &datawire.PutRecordResponse{OwnerGroupId: result.Local.Reference.OwnerGroupID, RangeId: result.Local.Reference.RangeID, AppliedIndex: result.Local.Reference.Index, State: string(result.State),
		ConfirmationIndex: result.Local.ConfirmationIndex, RegionGeneration: result.Local.RegionGeneration}, nil
}

func (s *dataService) GetRecord(ctx context.Context, request *datawire.GetRecordRequest) (*datawire.GetRecordResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 || proto.Size(request) > 32<<10 {
		return nil, status.Error(codes.InvalidArgument, "invalid Record read")
	}
	started := time.Now()
	ctx = withDataTrace(ctx, "get", "")
	defer recordwrite.Trace(ctx, "rpc.get.total", started)
	phaseStarted := time.Now()
	ctx, release, err := s.begin(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request))
	recordwrite.Trace(ctx, "rpc.get.begin", phaseStarted)
	if err != nil {
		return nil, err
	}
	defer release()
	entry, err := enginedata.NewWithGeoRetry(s.runtime, s.client, s.geoRetry)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "prepare immutable Record entry: %v", err)
	}
	binding, err := entry.Bind(record.ModuleID, record.EngineID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	bound, err := binding.ForTenant(request.TenantId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	phaseStarted = time.Now()
	read, err := bound.Read(ctx, request.Key)
	recordwrite.Trace(ctx, "rpc.get.engine_read", phaseStarted)
	if err != nil {
		if errors.Is(err, router.ErrNoRoute) {
			return &datawire.GetRecordResponse{State: "not_found"}, nil
		}
		return nil, status.Errorf(codes.Unavailable, "read Record with physical majority: %v", err)
	}
	state := read.State
	if state == "" {
		state = "local_ready"
	}
	return &datawire.GetRecordResponse{Found: true, Value: read.Body, ConfirmedIndex: read.Reference.Index,
		State: state, RegionGeneration: read.RegionGeneration}, nil
}
