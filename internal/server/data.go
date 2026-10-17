package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"time"

	"github.com/dubi-project/dubi/internal/engine/record"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginedata"
	"github.com/dubi-project/dubi/internal/platform/cluster/rangebootstrap"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
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
	requestBudget *resource.ByteBudget
	geoRetry      *recordwrite.GeoMetadataRetryStore
	geoRecords    *recordrepair.GeoRecordRepairStore
	repair        *liveRecordRepair
}

const maxDemoRecordBytes = 1 << 20
const maxTenantTokenTTLSeconds = 366 * 24 * 60 * 60

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
				if err := s.runtime.Records.AbandonVolumeMigration(item.ID); err != nil {
					return nil, status.Errorf(codes.FailedPrecondition, "record lost Volume decision: %v", err)
				}
			} else if item.State != volume.Removed {
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
			Migrated: migration.Migrated, LastError: migration.LastError}
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
		for _, item := range report.Volumes {
			if len(response.Volumes) >= 64 {
				break
			}
			response.Volumes = append(response.Volumes, &datawire.VolumeHealthStatus{VolumeId: item.ID,
				State: string(item.State), Health: item.Health, Error: item.Error})
		}
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
		entries, err := s.geoRetry.List()
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read Geo metadata debt: %v", err)
		}
		response.GeoMetadataDebt = uint64(len(entries))
	}
	if s.geoRecords != nil {
		entries, err := s.geoRecords.List()
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read Geo record debt: %v", err)
		}
		response.GeoRecordDebt = uint64(len(entries))
	}
	response.NodeId = local.NodeID
	response.RaftId = local.RaftID
	return response, nil
}

func encodeRepairStatus(observation repairObservation) *datawire.LocalRepairStatusResponse {
	response := &datawire.LocalRepairStatusResponse{WorkerEnabled: true}
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
	return response
}

func newDataService(runtime *node.Runtime, client *raftrpc.GroupClient, routes *raftrpc.RootDirectory) *dataService {
	return &dataService{runtime: runtime, client: client, routes: routes,
		slots: make(chan struct{}, 8), volumeSlots: make(chan struct{}, 1),
		maintenance: resource.NewByteBudget(128 << 20), requestBudget: resource.NewByteBudget(128 << 20)}
}

func (s *dataService) begin(ctx context.Context, cluster string, raftID uint64, size int) (context.Context, func(), error) {
	if size > maxDemoRecordBytes+(32<<10) {
		return nil, nil, status.Error(codes.ResourceExhausted, "data request exceeds limit")
	}
	select {
	case s.slots <- struct{}{}:
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
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
		release := func() { budgetRelease(); cancel(); <-s.slots }
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
		if _, err := s.runtime.ReadRootIndex(ctx); err != nil {
			release()
			return nil, nil, status.Errorf(codes.Unavailable, "confirm Root membership: %v", err)
		}
		if _, err := identity.VerifiedPeerCertificate(ctx, time.Now()); err != nil {
			release()
			return nil, nil, status.Error(codes.PermissionDenied, "Node certificate expired during Root confirmation")
		}
		member, found, err := s.runtime.Root.RootMember(raftID)
		if err != nil || !found || member.NodeID != parsed.NodeID {
			release()
			return nil, nil, status.Error(codes.PermissionDenied, "Node is not a current Root member")
		}
		return ctx, release, nil
	default:
		return nil, nil, status.Error(codes.ResourceExhausted, "data entry is busy")
	}
}

func (s *dataService) CreateDataset(ctx context.Context, request *datawire.CreateDatasetRequest) (*datawire.CreateDatasetResponse, error) {
	if request == nil || proto.Size(request) > 32<<10 || rootstate.ValidateRequestID(request.GetRequestId()) != nil || len(request.GetRequestId()) > 100 ||
		rootstate.ValidateTenantID(request.GetTenantId()) != nil ||
		(rootstate.Keyspace{TenantID: request.GetTenantId(), EngineID: record.EngineID, DatasetID: request.GetDatasetId()}).Validate() != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Dataset request")
	}
	ctx, release, err := s.begin(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	if existing, found, err := s.runtime.Root.Dataset(request.TenantId, record.EngineID, request.DatasetId); err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Dataset: %v", err)
	} else if found {
		return s.activateDataset(ctx, existing)
	}
	local, member, err := s.runtime.Root.RootMember(s.runtime.Identity.RaftID)
	if err != nil || !member {
		return nil, status.Error(codes.FailedPrecondition, "local Node is not a Root member")
	}
	ownerGroup := regiongroups.GroupID(local.RegionID)
	progress, ready, err := s.runtime.Root.RegionProgress(ownerGroup)
	if err != nil || !ready || !progress.Eligible || progress.Pending != nil {
		return nil, status.Error(codes.FailedPrecondition, "local Region needs a confirmed data Group")
	}
	if _, found, err := s.runtime.Root.Tenant(request.TenantId); err != nil {
		return nil, status.Errorf(codes.Unavailable, "read Tenant: %v", err)
	} else if !found {
		command := rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.RequestId + "-tenant",
			Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: request.TenantId}}
		if err := s.rootCommand(ctx, command); err != nil {
			// A concurrent creator may have registered the same Tenant.
			if _, found, readErr := s.runtime.Root.Tenant(request.TenantId); readErr != nil || !found {
				return nil, err
			}
		}
	}
	digest := sha256.Sum256([]byte(request.TenantId + "\x00" + request.DatasetId))
	dataset := rootstate.Dataset{TenantID: request.TenantId, EngineID: record.EngineID,
		DatasetID: request.DatasetId, RangeID: "range-" + hex.EncodeToString(digest[:16]), OwnerGroupID: ownerGroup}
	command := rootstate.Command{Version: rootstate.FormatVersion, RequestID: request.RequestId + "-dataset",
		Type: rootstate.CreateDataset, Dataset: &dataset}
	if err := s.rootCommand(ctx, command); err != nil {
		if existing, found, readErr := s.runtime.Root.Dataset(request.TenantId, record.EngineID, request.DatasetId); readErr != nil || !found {
			return nil, err
		} else {
			dataset = existing
		}
	}
	return s.activateDataset(ctx, dataset)
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
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		return err
	}
	if err := s.runtime.Host.Submit(raft.RootGroupID, encoded); err != nil {
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

func (s *dataService) activateDataset(ctx context.Context, dataset rootstate.Dataset) (*datawire.CreateDatasetResponse, error) {
	bootstrap, err := rangebootstrap.New(s.runtime.Host, s.runtime.Root, func(groupID string) (rangebootstrap.OwnerControl, bool) {
		if state, found := s.runtime.Engines.Resolve(groupID); found {
			owner, err := rangebootstrap.NewLocalOwner(s.runtime.Host, state)
			return owner, err == nil
		}
		for _, candidate := range s.routes.Candidates(groupID) {
			if candidate == s.runtime.Identity.RaftID {
				continue
			}
			owner, err := s.client.Owner(candidate, groupID)
			if err == nil {
				return owner, true
			}
		}
		return nil, false
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "prepare Range bootstrap: %v", err)
	}
	if err := bootstrap.Reconcile(ctx, dataset.RangeID); err != nil {
		return nil, status.Errorf(codes.Unavailable, "activate initial Range (retry create): %v", err)
	}
	return &datawire.CreateDatasetResponse{RangeId: dataset.RangeID,
		OwnerGroupId: dataset.OwnerGroupID, RootIndex: s.runtime.Root.AppliedIndex()}, nil
}

func (s *dataService) target(ctx context.Context, tenant, dataset string, key []byte) (router.Target, error) {
	space := rootstate.Keyspace{TenantID: tenant, EngineID: record.EngineID, DatasetID: dataset}
	if space.Validate() != nil || len(key) == 0 || len(key) > 4096 {
		return router.Target{}, status.Error(codes.InvalidArgument, "invalid Record identity or key")
	}
	resolver, err := router.New(s.runtime.Host, s.runtime.Root)
	if err != nil {
		return router.Target{}, status.Errorf(codes.Internal, "prepare route: %v", err)
	}
	target, err := resolver.ResolveTarget(ctx, space, key)
	if err != nil {
		return router.Target{}, status.Errorf(codes.FailedPrecondition, "resolve Record Range: %v", err)
	}
	progress, ready, err := s.runtime.Root.RegionProgress(target.OwnerGroupID)
	if err != nil || !ready || !progress.Eligible || progress.Pending != nil {
		return router.Target{}, status.Error(codes.FailedPrecondition, "data Region is not confirmed")
	}
	return target, nil
}

func (s *dataService) PutRecord(ctx context.Context, request *datawire.PutRecordRequest) (*datawire.PutRecordResponse, error) {
	if request == nil || rootstate.ValidateRequestID(request.GetRequestId()) != nil || len(request.GetValue()) > maxDemoRecordBytes || proto.Size(request) > maxDemoRecordBytes+(32<<10) {
		return nil, status.Error(codes.InvalidArgument, "invalid Record mutation")
	}
	ctx, release, err := s.begin(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	entry, err := enginedata.NewWithGeoRetry(s.runtime, s.client, s.geoRetry)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "prepare immutable Record entry: %v", err)
	}
	keyspace := rootstate.Keyspace{TenantID: request.TenantId, EngineID: record.EngineID, DatasetID: request.DatasetId}
	result, err := entry.Write(ctx, enginedata.WriteRequest{RequestID: request.RequestId, Keyspace: keyspace,
		Key: request.Key, Body: request.Value, Publish: func(descriptor records.Descriptor) ([]byte, error) {
			return record.ReferencePutPayload(descriptor)
		}})
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "write Record with physical majority: %v", err)
	}
	return &datawire.PutRecordResponse{AppliedIndex: result.Local.Reference.Index, State: string(result.State),
		ConfirmationIndex: result.Local.ConfirmationIndex, RegionGeneration: result.Local.RegionGeneration}, nil
}

func (s *dataService) GetRecord(ctx context.Context, request *datawire.GetRecordRequest) (*datawire.GetRecordResponse, error) {
	if request == nil || proto.Size(request) > 32<<10 {
		return nil, status.Error(codes.InvalidArgument, "invalid Record read")
	}
	ctx, release, err := s.begin(ctx, request.GetClusterId(), request.GetFromRaftId(), proto.Size(request))
	if err != nil {
		return nil, err
	}
	defer release()
	entry, err := enginedata.NewWithGeoRetry(s.runtime, s.client, s.geoRetry)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "prepare immutable Record entry: %v", err)
	}
	keyspace := rootstate.Keyspace{TenantID: request.TenantId, EngineID: record.EngineID, DatasetID: request.DatasetId}
	read, err := entry.Read(ctx, keyspace, request.Key)
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
