// Package rootstate holds DUBI's capability-neutral Root Group directory.
package rootstate

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/raft"
)

const FormatVersion = 1

const (
	RegisterNode         = "register_node"
	RegisterGroup        = "register_group"
	RegisterTenant       = "register_tenant"
	CreateDataset        = "create_dataset"
	ActivateRange        = "activate_range"
	PublishGroupMembers  = "publish_group_members"
	PlanGroupMembers     = "plan_group_members"
	CompleteGroupMembers = "complete_group_members"
)

const (
	RangeStatePending    = "pending"
	RangeStateActive     = "active"
	GroupChangePending   = "pending"
	GroupChangeCompleted = "completed"
)

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var nodeIdentity = regexp.MustCompile(`^node_[0-9a-f]{32}$`)

type Node struct {
	NodeID   string `json:"node_id"`
	RaftID   uint64 `json:"raft_id"`
	RegionID string `json:"region_id"`
	Address  string `json:"address"`
}

type Group struct {
	GroupID   string        `json:"group_id"`
	StateKind string        `json:"state_kind"`
	Epoch     uint64        `json:"epoch"`
	Members   []GroupMember `json:"members,omitempty"`
}

const (
	GroupMemberVoter     = "voter"
	GroupMemberLearner   = "learner"
	GroupStateCapability = "capability"
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

// GroupChange is a persistent control task. Members is the desired final
// ConfState projection, not a user-selected replica count.
type GroupChange struct {
	ChangeID       string        `json:"change_id"`
	GroupID        string        `json:"group_id"`
	ExpectedEpoch  uint64        `json:"expected_epoch"`
	Members        []GroupMember `json:"members"`
	State          string        `json:"state"`
	CompletedEpoch uint64        `json:"completed_epoch,omitempty"`
}

type GroupChangePlan struct {
	ChangeID      string        `json:"change_id"`
	GroupID       string        `json:"group_id"`
	ExpectedEpoch uint64        `json:"expected_epoch"`
	Members       []GroupMember `json:"members"`
}

type GroupChangeCompletion struct {
	ChangeID string `json:"change_id"`
	GroupID  string `json:"group_id"`
}

type Tenant struct {
	TenantID string `json:"tenant_id"`
}

// Keyspace is an internal routing domain, not a user-facing resource or a
// separately allocated persistent ID. A Tenant may own many Keyspaces.
type Keyspace struct {
	TenantID     string
	CapabilityID string
	DatasetID    string
}

type Dataset struct {
	TenantID     string `json:"tenant_id"`
	CapabilityID string `json:"capability_id"`
	DatasetID    string `json:"dataset_id"`
	RangeID      string `json:"range_id"`
	OwnerGroupID string `json:"owner_group_id"`
}

func (value Dataset) Keyspace() Keyspace {
	return Keyspace{TenantID: value.TenantID, CapabilityID: value.CapabilityID, DatasetID: value.DatasetID}
}

type Range struct {
	RangeID             string `json:"range_id"`
	TenantID            string `json:"tenant_id"`
	CapabilityID        string `json:"capability_id"`
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

func (value Range) Keyspace() Keyspace {
	return Keyspace{TenantID: value.TenantID, CapabilityID: value.CapabilityID, DatasetID: value.DatasetID}
}

func (value Keyspace) Validate() error {
	if !validID(value.TenantID) || !validID(value.CapabilityID) || !validID(value.DatasetID) {
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

func ValidateCapabilityID(value string) error {
	if !validID(value) {
		return errors.New("invalid CapabilityID")
	}
	return nil
}

func ValidateRequestID(value string) error {
	if !validID(value) {
		return errors.New("invalid RequestID")
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

// Command is an internal Root Group command, not a public capability API.
// Exactly one payload matching Type must be present.
type Command struct {
	Version         int                    `json:"version"`
	RequestID       string                 `json:"request_id"`
	Type            string                 `json:"type"`
	Node            *Node                  `json:"node,omitempty"`
	Group           *Group                 `json:"group,omitempty"`
	Tenant          *Tenant                `json:"tenant,omitempty"`
	Dataset         *Dataset               `json:"dataset,omitempty"`
	Activation      *RangeActivation       `json:"activation,omitempty"`
	Membership      *GroupMembership       `json:"membership,omitempty"`
	GroupPlan       *GroupChangePlan       `json:"group_plan,omitempty"`
	GroupCompletion *GroupChangeCompletion `json:"group_completion,omitempty"`
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
	switch command.Type {
	case RegisterNode:
		if command.Node == nil || command.Group != nil || command.Tenant != nil || command.Dataset != nil || command.Activation != nil || command.Membership != nil || command.GroupPlan != nil || command.GroupCompletion != nil {
			return errors.New("register_node requires only Node payload")
		}
		value := command.Node
		if !nodeIdentity.MatchString(value.NodeID) || value.RaftID == 0 || !validID(value.RegionID) || strings.TrimSpace(value.Address) == "" || len(value.Address) > 1024 {
			return errors.New("invalid Node descriptor")
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
		if !validID(value.TenantID) || !validID(value.CapabilityID) || !validID(value.DatasetID) || !validID(value.RangeID) {
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
