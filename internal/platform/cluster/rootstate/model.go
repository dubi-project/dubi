// Package rootstate holds DUBI's engine-neutral Root Group directory.
package rootstate

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	repository "github.com/dubi-project/dubi/internal/platform/recovery/repository"
)

const FormatVersion = 1

const (
	RegisterNode               = "register_node"
	RenameNode                 = "rename_node"
	FenceNode                  = "fence_node"
	RegisterGroup              = "register_group"
	RegisterTenant             = "register_tenant"
	CreateTenantCredential     = "create_tenant_credential"
	RevokeTenantCredential     = "revoke_tenant_credential"
	CreateDataset              = "create_dataset"
	ActivateRange              = "activate_range"
	PublishGroupMembers        = "publish_group_members"
	PlanGroupMembers           = "plan_group_members"
	CompleteGroupMembers       = "complete_group_members"
	PublishEnrollmentAuthority = "publish_enrollment_authority"
	SetRangePlacement          = "set_range_placement"
	RetireRegionGroup          = "retire_region_group"
	PlanRegionGroupRebuild     = "plan_region_group_rebuild"
	MarkNodeReady              = "mark_node_ready"
	CreateRepository           = "create_repository"
	UpdateRepository           = "update_repository"
	DeleteRepository           = "delete_repository"
	SetDefaultRepository       = "set_default_repository"
	CreateBackupJob            = "create_backup_job"
	ClaimBackupJob             = "claim_backup_job"
	CompleteBackupJob          = "complete_backup_job"
	FailBackupJob              = "fail_backup_job"
	FinalizeBackupJob          = "finalize_backup_job"
)

const (
	RangeStatePending      = "pending"
	RangeStateActive       = "active"
	GroupChangePending     = "pending"
	GroupChangeCompleted   = "completed"
	AuthorityStateActive   = "active"
	AuthorityStateDegraded = "degraded"
)

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var nodeIdentity = regexp.MustCompile(`^node_[0-9a-f]{32}$`)

type Node struct {
	NodeID string `json:"node_id"`
	RaftID uint64 `json:"raft_id"`
	// Name is an optional operator-facing label. It is never used as Node
	// identity, a routing key, or an authorization principal.
	Name     string `json:"name,omitempty"`
	RegionID string `json:"region_id"`
	Address  string `json:"address"`
}

type NodeRename struct {
	NodeID string `json:"node_id"`
	RaftID uint64 `json:"raft_id"`
	Name   string `json:"name,omitempty"`
}

// NodeReadyRecord is the durable hand-off from local Node recovery to Root
// topology. Admission only proves that a Node is a Root learner; this record
// proves that the Node has replayed its local Root state and completed the
// identity-bound activation fence before it can be promoted to a voter.
type NodeReadyRecord struct {
	NodeID         string `json:"node_id"`
	RaftID         uint64 `json:"raft_id"`
	ActivatedIndex uint64 `json:"activated_index"`
}

type Group struct {
	GroupID   string        `json:"group_id"`
	StateKind string        `json:"state_kind"`
	Epoch     uint64        `json:"epoch"`
	Members   []GroupMember `json:"members,omitempty"`
}

const (
	GroupMemberVoter   = "voter"
	GroupMemberLearner = "learner"
	GroupStateEngine   = "engine"
)

type GroupMember struct {
	RaftID uint64 `json:"raft_id"`
	Role   string `json:"role"`
}

type GroupMembership struct {
	GroupID       string        `json:"group_id"`
	ExpectedEpoch uint64        `json:"expected_epoch"`
	Members       []GroupMember `json:"members"`
}

// RegionGroupRetirement is the immutable authority cut recorded before a
// future empty-Group reconstruction. It does not change Raft membership.
type RegionGroupRetirement struct {
	GroupID    string   `json:"group_id"`
	RegionID   string   `json:"region_id"`
	Generation uint64   `json:"generation"`
	GroupEpoch uint64   `json:"group_epoch"`
	FenceIndex uint64   `json:"fence_index"`
	Nodes      []uint64 `json:"nodes"`
	RetiredAt  uint64   `json:"retired_at"`
}

// RegionGroupRebuild is a Root-persisted, module-neutral target cut.
type RegionGroupRebuild struct {
	GroupID            string   `json:"group_id"`
	RegionID           string   `json:"region_id"`
	PreviousGeneration uint64   `json:"previous_generation"`
	PreviousGroupEpoch uint64   `json:"previous_group_epoch"`
	PreviousFenceIndex uint64   `json:"previous_fence_index"`
	PreviousNodes      []uint64 `json:"previous_nodes"`
	TargetGeneration   uint64   `json:"target_generation"`
	TargetNodes        []uint64 `json:"target_nodes"`
	PlannedAt          uint64   `json:"planned_at"`
}

// GroupChange is a persistent control task. Members is the desired final
// ConfState projection, not a user-selected replica count.
type GroupChange struct {
	ChangeID      string        `json:"change_id"`
	GroupID       string        `json:"group_id"`
	ExpectedEpoch uint64        `json:"expected_epoch"`
	Members       []GroupMember `json:"members"`
	State         string        `json:"state"`
	// Maintenance is an explicit operator/member-removal change. It is not a
	// replacement task and never authorizes adding a new member.
	Maintenance      bool   `json:"maintenance,omitempty"`
	CompletedEpoch   uint64 `json:"completed_epoch,omitempty"`
	RegionID         string `json:"region_id,omitempty"`
	GrowthGeneration uint64 `json:"growth_generation,omitempty"`
}

type GroupChangePlan struct {
	ChangeID      string        `json:"change_id"`
	GroupID       string        `json:"group_id"`
	ExpectedEpoch uint64        `json:"expected_epoch"`
	Members       []GroupMember `json:"members"`
	Maintenance   bool          `json:"maintenance,omitempty"`
}

// EnrollmentAuthorityPlan is the Root-authorized signer delegation cut. The
// private signing material never enters Root; Root stores only the epoch and
// the current majority of eligible authority Nodes.
type EnrollmentAuthorityPlan struct {
	ExpectedEpoch uint64   `json:"expected_epoch"`
	Epoch         uint64   `json:"epoch"`
	Members       []uint64 `json:"members"`
	State         string   `json:"state"`
	ReadyMembers  []uint64 `json:"ready_members,omitempty"`
}

// EnrollmentAuthority is the durable Root projection consumed by nodes when
// deciding which current identities may issue a sealed signer transfer.
type EnrollmentAuthority struct {
	Epoch          uint64   `json:"epoch"`
	Members        []uint64 `json:"members"`
	State          string   `json:"state"`
	ConfirmedIndex uint64   `json:"confirmed_index"`
	ReadyMembers   []uint64 `json:"ready_members,omitempty"`
}

type GroupChangeCompletion struct {
	ChangeID string `json:"change_id"`
	GroupID  string `json:"group_id"`
}

type Tenant struct {
	TenantID string `json:"tenant_id"`
}

// TenantCredential stores only a digest of a random application Token. The
// plaintext Token is never a Root command, state value, snapshot or result.
type TenantCredential struct {
	CredentialID string `json:"credential_id"`
	TenantID     string `json:"tenant_id"`
	TokenSHA256  string `json:"token_sha256"`
	CreatedUnix  int64  `json:"created_unix"`
	ExpiresUnix  int64  `json:"expires_unix"`
	RevokedIndex uint64 `json:"revoked_index,omitempty"`
}

type TenantCredentialRevocation struct {
	CredentialID string `json:"credential_id"`
}

// Keyspace is an internal routing domain, not a user-facing resource or a
// separately allocated persistent ID. A Tenant may own many Keyspaces.
type Keyspace struct {
	TenantID  string
	EngineID  string
	DatasetID string
}

type Dataset struct {
	TenantID     string `json:"tenant_id"`
	EngineID     string `json:"engine_id"`
	DatasetID    string `json:"dataset_id"`
	RangeID      string `json:"range_id"`
	OwnerGroupID string `json:"owner_group_id"`
}

// RepositoryDelete and RepositoryDefault are the small Root command payloads
// for lifecycle operations on a cluster-level backup/recovery Repository.
// Repository itself is the complete structured configuration; it intentionally
// has no URI field and carries only encrypted credential bytes.
type RepositoryDelete struct {
	Name string `json:"name"`
}

type RepositoryDefault struct {
	Name string `json:"name"`
}

func (value Dataset) Keyspace() Keyspace {
	return Keyspace{TenantID: value.TenantID, EngineID: value.EngineID, DatasetID: value.DatasetID}
}

type Range struct {
	RangeID             string `json:"range_id"`
	TenantID            string `json:"tenant_id"`
	EngineID            string `json:"engine_id"`
	DatasetID           string `json:"dataset_id"`
	Start               []byte `json:"start"`
	End                 []byte `json:"end"`
	OwnerGroupID        string `json:"owner_group_id"`
	Epoch               uint64 `json:"epoch"`
	State               string `json:"state"`
	OwnerFenceRequestID string `json:"owner_fence_request_id,omitempty"`
	OwnerFenceIndex     uint64 `json:"owner_fence_index,omitempty"`
}

type RangeActivation struct {
	RangeID           string `json:"range_id"`
	OwnerGroupID      string `json:"owner_group_id"`
	Epoch             uint64 `json:"epoch"`
	OwnerRequestID    string `json:"owner_request_id"`
	OwnerAppliedIndex uint64 `json:"owner_applied_index"`
}

// RangeRegionPlacement binds one logical Range to the Region Group cohorts
// that may hold its data. OwnerGroupID remains the current routing Owner; the
// Region list is the durable cross-Region placement set used by Geo writes.
type RangeRegionPlacement struct {
	RegionID   string `json:"region_id"`
	GroupID    string `json:"group_id"`
	Generation uint64 `json:"generation"`
}

type RangePlacement struct {
	RangeID      string                 `json:"range_id"`
	RangeEpoch   uint64                 `json:"range_epoch"`
	OwnerGroupID string                 `json:"owner_group_id"`
	Regions      []RangeRegionPlacement `json:"regions"`
}

func (value Range) Keyspace() Keyspace {
	return Keyspace{TenantID: value.TenantID, EngineID: value.EngineID, DatasetID: value.DatasetID}
}

func (value Keyspace) Validate() error {
	if !validID(value.TenantID) || !validID(value.EngineID) || !validID(value.DatasetID) {
		return errors.New("invalid internal Keyspace identity")
	}
	return nil
}

func ValidateRangeID(value string) error {
	if !validID(value) {
		return errors.New("invalid RangeID")
	}
	return nil
}

func ValidateEngineID(value string) error {
	if !validID(value) {
		return errors.New("invalid EngineID")
	}
	return nil
}

func ValidateTenantID(value string) error {
	if !validID(value) {
		return errors.New("invalid TenantID")
	}
	return nil
}

func ValidateRequestID(value string) error {
	if !validID(value) {
		return errors.New("invalid RequestID")
	}
	return nil
}

// ValidateNodeName accepts an empty label so unattended startup can leave the
// operator-facing name unset. Non-empty labels use the same portable alphabet
// and 128-byte bound as other DUBI identifiers.
func ValidateNodeName(value string) error {
	if value == "" {
		return nil
	}
	if !validID(value) {
		return errors.New("invalid Node name")
	}
	return nil
}

func (value Range) Validate() error {
	if err := value.Keyspace().Validate(); err != nil {
		return err
	}
	if ValidateRangeID(value.RangeID) != nil || raft.ValidateGroupID(value.OwnerGroupID) != nil ||
		value.OwnerGroupID == raft.RootGroupID || value.Epoch == 0 ||
		(value.State != RangeStatePending && value.State != RangeStateActive) ||
		(len(value.Start) != 0 && len(value.End) != 0 && strings.Compare(string(value.Start), string(value.End)) >= 0) {
		return errors.New("invalid Range descriptor")
	}
	return nil
}

// Command is an internal Root Group command, not a public engine API.
// Exactly one payload matching Type must be present.
type Command struct {
	Version             int                         `json:"version"`
	RequestID           string                      `json:"request_id"`
	Type                string                      `json:"type"`
	Node                *Node                       `json:"node,omitempty"`
	NodeRename          *NodeRename                 `json:"node_rename,omitempty"`
	NodeFence           *NodeFenceRequest           `json:"node_fence,omitempty"`
	Group               *Group                      `json:"group,omitempty"`
	Tenant              *Tenant                     `json:"tenant,omitempty"`
	TenantCredential    *TenantCredential           `json:"tenant_credential,omitempty"`
	TenantRevocation    *TenantCredentialRevocation `json:"tenant_revocation,omitempty"`
	Dataset             *Dataset                    `json:"dataset,omitempty"`
	Activation          *RangeActivation            `json:"activation,omitempty"`
	Membership          *GroupMembership            `json:"membership,omitempty"`
	GroupPlan           *GroupChangePlan            `json:"group_plan,omitempty"`
	GroupCompletion     *GroupChangeCompletion      `json:"group_completion,omitempty"`
	JoinGrant           *JoinGrant                  `json:"join_grant,omitempty"`
	JoinConsumption     *JoinGrantConsumption       `json:"join_consumption,omitempty"`
	JoinRevocation      *JoinGrantRevocation        `json:"join_revocation,omitempty"`
	RootJoinStart       *RootJoinStart              `json:"root_join_start,omitempty"`
	RootJoinFinish      *RootJoinFinish             `json:"root_join_finish,omitempty"`
	RegionPlan          *RegionCohortPlan           `json:"region_plan,omitempty"`
	RegionConfirmation  *RegionCohortConfirmation   `json:"region_confirmation,omitempty"`
	RegionGrowth        *RegionGrowthSeal           `json:"region_growth,omitempty"`
	EnrollmentAuthority *EnrollmentAuthorityPlan    `json:"enrollment_authority,omitempty"`
	RangePlacement      *RangePlacement             `json:"range_placement,omitempty"`
	GroupRetirement     *RegionGroupRetirement      `json:"group_retirement,omitempty"`
	GroupRebuild        *RegionGroupRebuild         `json:"group_rebuild,omitempty"`
	NodeReady           *NodeReadyRecord            `json:"node_ready,omitempty"`
	Repository          *repository.Repository      `json:"repository,omitempty"`
	RepositoryDelete    *RepositoryDelete           `json:"repository_delete,omitempty"`
	RepositoryDefault   *RepositoryDefault          `json:"repository_default,omitempty"`
	BackupJob           *BackupJob                  `json:"backup_job,omitempty"`
	BackupJobClaim      *BackupJobClaim             `json:"backup_job_claim,omitempty"`
	BackupJobCompletion *BackupJobCompletion        `json:"backup_job_completion,omitempty"`
	BackupJobFailure    *BackupJobFailure           `json:"backup_job_failure,omitempty"`
	BackupJobReference  *BackupJobReference         `json:"backup_job_reference,omitempty"`
}

type Result struct {
	RequestID string `json:"request_id"`
	Hash      string `json:"hash"`
	Index     uint64 `json:"index"`
	Error     string `json:"error,omitempty"`
}

func validID(value string) bool { return identifier.MatchString(value) }

func EncodeCommand(command Command) ([]byte, error) {
	if err := command.validate(); err != nil {
		return nil, err
	}
	return json.Marshal(command)
}

func (command Command) validate() error {
	if command.Version != FormatVersion || !validID(command.RequestID) {
		return errors.New("invalid Root command version or RequestID")
	}
	payloads := 0
	for _, present := range []bool{command.Node != nil, command.NodeRename != nil, command.NodeFence != nil, command.Group != nil, command.Tenant != nil,
		command.TenantCredential != nil, command.TenantRevocation != nil, command.Dataset != nil,
		command.Activation != nil, command.Membership != nil, command.GroupPlan != nil, command.GroupCompletion != nil,
		command.JoinGrant != nil, command.JoinConsumption != nil, command.JoinRevocation != nil,
		command.RootJoinStart != nil, command.RootJoinFinish != nil, command.RegionPlan != nil, command.RegionConfirmation != nil, command.RegionGrowth != nil, command.EnrollmentAuthority != nil, command.RangePlacement != nil, command.GroupRetirement != nil, command.GroupRebuild != nil, command.NodeReady != nil, command.Repository != nil, command.RepositoryDelete != nil, command.RepositoryDefault != nil, command.BackupJob != nil, command.BackupJobClaim != nil, command.BackupJobCompletion != nil, command.BackupJobFailure != nil, command.BackupJobReference != nil} {
		if present {
			payloads++
		}
	}
	if payloads != 1 {
		return errors.New("Root command requires exactly one payload")
	}
	switch command.Type {
	case FenceNode:
		if command.NodeFence == nil || !nodeIdentity.MatchString(command.NodeFence.NodeID) || command.NodeFence.RaftID == 0 {
			return errors.New("invalid Node fence request")
		}
		return nil
	case CreateTenantCredential:
		if command.TenantCredential == nil {
			return errors.New("create_tenant_credential requires credential payload")
		}
		if command.TenantCredential.RevokedIndex != 0 {
			return errors.New("new Tenant credential cannot be revoked")
		}
		return command.TenantCredential.Validate()
	case RevokeTenantCredential:
		if command.TenantRevocation == nil || !validTenantCredentialID(command.TenantRevocation.CredentialID) {
			return errors.New("invalid Tenant credential revocation")
		}
		return nil
	case AuthorizeRegionGrowth:
		if command.RegionGrowth == nil {
			return errors.New("authorize_region_growth requires a seal")
		}
		return command.RegionGrowth.Validate()
	case PlanRegionCohort:
		if command.RegionPlan == nil {
			return errors.New("plan_region_cohort requires Region plan")
		}
		return command.RegionPlan.validate()
	case ConfirmRegionCohort:
		if command.RegionConfirmation == nil {
			return errors.New("confirm_region_cohort requires Region confirmation")
		}
		return command.RegionConfirmation.validate()
	case BeginRootJoin:
		if command.RootJoinStart == nil {
			return errors.New("begin_root_join requires enrollment request")
		}
		if err := command.RootJoinStart.validate(); err != nil {
			return err
		}
	case CompleteRootJoin:
		if command.RootJoinFinish == nil || identity.ValidateJoinGrantID(command.RootJoinFinish.GrantID) != nil {
			return errors.New("complete_root_join requires Grant identity")
		}
	case CreateJoinGrant:
		if command.JoinGrant == nil || command.JoinGrant.Validate() != nil || command.JoinGrant.State != JoinGrantActive {
			return errors.New("create_join_grant requires a valid active Join Grant")
		}
	case ConsumeJoinGrant:
		value := command.JoinConsumption
		if value == nil || identity.ValidateJoinGrantID(value.GrantID) != nil || !validDigest(value.RequestSHA256) || value.AuthorizedUnix <= 0 {
			return errors.New("invalid Join Grant consumption")
		}
	case RevokeJoinGrant:
		value := command.JoinRevocation
		if value == nil || identity.ValidateJoinGrantID(value.GrantID) != nil || value.RevokedUnix <= 0 {
			return errors.New("invalid Join Grant revocation")
		}
	case RegisterNode:
		if command.Node == nil || command.Group != nil || command.Tenant != nil || command.Dataset != nil || command.Activation != nil || command.Membership != nil || command.GroupPlan != nil || command.GroupCompletion != nil {
			return errors.New("register_node requires only Node payload")
		}
		value := command.Node
		if !nodeIdentity.MatchString(value.NodeID) || value.RaftID == 0 || !validID(value.RegionID) || ValidateNodeName(value.Name) != nil || strings.TrimSpace(value.Address) == "" || len(value.Address) > 1024 {
			return errors.New("invalid Node descriptor")
		}
	case RenameNode:
		if command.NodeRename == nil || !nodeIdentity.MatchString(command.NodeRename.NodeID) || command.NodeRename.RaftID == 0 || ValidateNodeName(command.NodeRename.Name) != nil {
			return errors.New("invalid Node rename")
		}
	case RegisterGroup:
		if command.Group == nil || command.Node != nil || command.Tenant != nil || command.Dataset != nil || command.Activation != nil || command.Membership != nil || command.GroupPlan != nil || command.GroupCompletion != nil {
			return errors.New("register_group requires only Group payload")
		}
		if err := raft.ValidateGroupID(command.Group.GroupID); err != nil {
			return err
		}
		if !validID(command.Group.StateKind) || command.Group.Epoch != 0 || len(command.Group.Members) != 0 {
			return errors.New("new Group cannot supply membership state")
		}
	case RegisterTenant:
		if command.Tenant == nil || command.Node != nil || command.Group != nil || command.Dataset != nil || command.Activation != nil || command.Membership != nil || command.GroupPlan != nil || command.GroupCompletion != nil || !validID(command.Tenant.TenantID) {
			return errors.New("invalid Tenant registration")
		}
	case CreateDataset:
		if command.Dataset == nil || command.Node != nil || command.Group != nil || command.Tenant != nil || command.Activation != nil || command.Membership != nil || command.GroupPlan != nil || command.GroupCompletion != nil {
			return errors.New("create_dataset requires only Dataset payload")
		}
		value := command.Dataset
		if !validID(value.TenantID) || !validID(value.EngineID) || !validID(value.DatasetID) || !validID(value.RangeID) {
			return errors.New("invalid Dataset or initial Range ID")
		}
		if err := raft.ValidateGroupID(value.OwnerGroupID); err != nil {
			return fmt.Errorf("invalid Dataset owner Group: %w", err)
		}
		if value.OwnerGroupID == raft.RootGroupID {
			return errors.New("Dataset owner must be a non-Root Group")
		}
	case ActivateRange:
		if command.Activation == nil || command.Node != nil || command.Group != nil || command.Tenant != nil || command.Dataset != nil || command.Membership != nil || command.GroupPlan != nil || command.GroupCompletion != nil {
			return errors.New("activate_range requires only RangeActivation payload")
		}
		value := command.Activation
		if ValidateRangeID(value.RangeID) != nil || raft.ValidateGroupID(value.OwnerGroupID) != nil ||
			value.OwnerGroupID == raft.RootGroupID || value.Epoch == 0 ||
			ValidateRequestID(value.OwnerRequestID) != nil || value.OwnerAppliedIndex == 0 {
			return errors.New("invalid Range activation")
		}
	case PublishGroupMembers:
		if command.Membership == nil || command.Node != nil || command.Group != nil || command.Tenant != nil ||
			command.Dataset != nil || command.Activation != nil || command.GroupPlan != nil || command.GroupCompletion != nil {
			return errors.New("publish_group_members requires only GroupMembership payload")
		}
		value := command.Membership
		if raft.ValidateGroupID(value.GroupID) != nil || value.GroupID == raft.RootGroupID ||
			value.ExpectedEpoch == 0 || validateGroupMembers(value.Members) != nil {
			return errors.New("invalid Group membership")
		}
	case PlanGroupMembers:
		if command.GroupPlan == nil || command.Node != nil || command.Group != nil || command.Tenant != nil ||
			command.Dataset != nil || command.Activation != nil || command.Membership != nil || command.GroupCompletion != nil {
			return errors.New("plan_group_members requires only GroupChangePlan payload")
		}
		value := command.GroupPlan
		if !validID(value.ChangeID) || raft.ValidateGroupID(value.GroupID) != nil || value.GroupID == raft.RootGroupID ||
			value.ExpectedEpoch == 0 || validateGroupMembers(value.Members) != nil {
			return errors.New("invalid Group membership plan")
		}
	case CompleteGroupMembers:
		if command.GroupCompletion == nil || command.Node != nil || command.Group != nil || command.Tenant != nil ||
			command.Dataset != nil || command.Activation != nil || command.Membership != nil || command.GroupPlan != nil {
			return errors.New("complete_group_members requires only GroupChangeCompletion payload")
		}
		value := command.GroupCompletion
		if !validID(value.ChangeID) || raft.ValidateGroupID(value.GroupID) != nil || value.GroupID == raft.RootGroupID {
			return errors.New("invalid Group membership completion")
		}
	case PublishEnrollmentAuthority:
		if command.EnrollmentAuthority == nil {
			return errors.New("publish_enrollment_authority requires authority payload")
		}
		return command.EnrollmentAuthority.validate()
	case SetRangePlacement:
		if command.RangePlacement == nil {
			return errors.New("set_range_placement requires placement payload")
		}
		return command.RangePlacement.Validate()
	case RetireRegionGroup:
		if command.GroupRetirement == nil {
			return errors.New("retire_region_group requires retirement payload")
		}
		return command.GroupRetirement.validateInput()
	case PlanRegionGroupRebuild:
		if command.GroupRebuild == nil {
			return errors.New("plan_region_group_rebuild requires rebuild payload")
		}
		return command.GroupRebuild.validateInput()
	case MarkNodeReady:
		value := command.NodeReady
		if value == nil || !nodeIdentity.MatchString(value.NodeID) || value.RaftID == 0 || value.ActivatedIndex == 0 {
			return errors.New("invalid Node readiness record")
		}
	case CreateRepository, UpdateRepository:
		if command.Repository == nil {
			return fmt.Errorf("%s requires Repository payload", command.Type)
		}
		return command.Repository.Validate()
	case DeleteRepository:
		if command.RepositoryDelete == nil || repository.ValidateName(command.RepositoryDelete.Name) != nil {
			return errors.New("invalid Repository deletion")
		}
	case SetDefaultRepository:
		if command.RepositoryDefault == nil || repository.ValidateName(command.RepositoryDefault.Name) != nil {
			return errors.New("invalid default Repository")
		}
	case CreateBackupJob:
		if command.BackupJob == nil {
			return errors.New("create_backup_job requires job payload")
		}
		return command.BackupJob.ValidateForCreate()
	case ClaimBackupJob:
		if command.BackupJobClaim == nil {
			return errors.New("claim_backup_job requires claim payload")
		}
		return command.BackupJobClaim.Validate()
	case CompleteBackupJob:
		if command.BackupJobCompletion == nil {
			return errors.New("complete_backup_job requires completion payload")
		}
		return command.BackupJobCompletion.Validate()
	case FailBackupJob:
		if command.BackupJobFailure == nil {
			return errors.New("fail_backup_job requires failure payload")
		}
		return command.BackupJobFailure.Validate()
	case FinalizeBackupJob:
		if command.BackupJobReference == nil {
			return errors.New("finalize_backup_job requires job reference")
		}
		return command.BackupJobReference.Validate()
	default:
		return fmt.Errorf("unsupported Root command %q", command.Type)
	}
	return nil
}

func validateGroupMembers(members []GroupMember) error {
	if len(members) == 0 {
		return errors.New("Group membership is empty")
	}
	seen := make(map[uint64]bool, len(members))
	voters := 0
	for _, member := range members {
		if member.RaftID == 0 || seen[member.RaftID] ||
			(member.Role != GroupMemberVoter && member.Role != GroupMemberLearner) {
			return errors.New("invalid or duplicate Group member")
		}
		seen[member.RaftID] = true
		if member.Role == GroupMemberVoter {
			voters++
		}
	}
	if voters == 0 {
		return errors.New("Group membership has no Voter")
	}
	return nil
}
