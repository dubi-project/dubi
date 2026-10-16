package rootstate

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

const AuthorizeRegionGrowth = "authorize_region_growth"

// RegionGrowthSeal is an INTERNAL authenticated coordinator assertion, not a
// remote disk proof. Before proposing it, the coordinator must observe the
// exact durable seal on the old participants. Data indexes never compare to
// Root indexes. Root atomically binds this assertion to ONE growth task.
type RegionGrowthSeal struct {
	RegionID                string `json:"region_id"`
	GroupID                 string `json:"group_id"`
	Generation              uint64 `json:"generation"`
	PlannedIndex            uint64 `json:"planned_index"`
	ExpectedEpoch           uint64 `json:"expected_epoch"`
	PreviousFenceRequestID  string `json:"previous_fence_request_id"`
	PreviousFenceIndex      uint64 `json:"previous_fence_index"`
	PreviousMembershipIndex uint64 `json:"previous_membership_index"`
	// Recovery is set only when an explicit maintenance removal left the live
	// Group as a stable subset of the previous confirmed cohort. In that case
	// the old fence index may precede the later membership cut.
	Recovery      bool          `json:"recovery,omitempty"`
	SealRequestID string        `json:"seal_request_id"`
	SealIndex     uint64        `json:"seal_index"`
	ChangeID      string        `json:"change_id"`
	Members       []GroupMember `json:"members"`
}

func (p RegionGrowthSeal) Validate() error {
	if !validID(p.RegionID) || raft.ValidateGroupID(p.GroupID) != nil || p.GroupID == raft.RootGroupID || p.Generation < 2 || p.PlannedIndex == 0 || p.ExpectedEpoch < 2 || p.ExpectedEpoch == math.MaxUint64 || !validID(p.PreviousFenceRequestID) || p.PreviousMembershipIndex == 0 || (!p.Recovery && p.PreviousFenceIndex <= p.PreviousMembershipIndex) || p.PreviousFenceIndex == 0 || !validID(p.SealRequestID) || p.SealRequestID == p.PreviousFenceRequestID || p.SealIndex <= p.PreviousFenceIndex || !validID(p.ChangeID) || len(p.Members) > MaxRegionCohortNodes || validateGroupMembers(p.Members) != nil {
		return errors.New("invalid regional growth seal")
	}
	for j := 1; j < len(p.Members); j++ {
		if p.Members[j-1].RaftID >= p.Members[j].RaftID {
			return errors.New("growth members must be sorted")
		}
	}
	return nil
}

type RegionGrowth struct {
	Seal            RegionGrowthSeal `json:"seal"`
	BaseMembers     []GroupMember    `json:"base_members"`
	AuthorizedIndex uint64           `json:"authorized_index"`
	RequestID       string           `json:"request_id"`
	ResultSHA256    string           `json:"result_sha256"`
}

func (g RegionGrowth) Validate() error {
	if g.Seal.Validate() != nil || g.AuthorizedIndex <= g.Seal.PlannedIndex || !validID(g.RequestID) || !validDigest(g.ResultSHA256) || validateGroupMembers(g.BaseMembers) != nil || len(g.BaseMembers) >= len(g.Seal.Members) {
		return errors.New("invalid regional growth record")
	}
	roles := make(map[uint64]string, len(g.Seal.Members))
	for _, m := range g.Seal.Members {
		roles[m.RaftID] = m.Role
	}
	for _, m := range g.BaseMembers {
		if roles[m.RaftID] != m.Role {
			return errors.New("regional growth changed an existing role")
		}
	}
	return nil
}

func regionGrowthKey(region string, generation uint64) []byte {
	return []byte(fmt.Sprintf("data/region-growth/%s/%020d", region, generation))
}

func (s *State) RegionGrowth(region string, generation uint64) (RegionGrowth, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return RegionGrowth{}, false, err
	}
	if !validID(region) || generation < 2 {
		return RegionGrowth{}, false, errors.New("invalid growth lookup")
	}
	return s.regionGrowth(region, generation)
}
func (s *State) regionGrowth(region string, generation uint64) (RegionGrowth, bool, error) {
	var record RegionGrowth
	encoded, found, err := s.get(regionGrowthKey(region, generation))
	if err != nil || !found {
		return record, found, err
	}
	if len(encoded) > 1<<20 || decodeStreamJSON(encoded, &record) != nil || record.Validate() != nil || record.Seal.RegionID != region || record.Seal.Generation != generation || record.AuthorizedIndex > s.applied {
		return record, false, errors.New("invalid stored regional growth")
	}
	return record, true, nil
}

// confirmedRegion distinguishes initial/unassigned Groups from a denominator
// that has already been published. Direct membership publication and generic
// tasks cannot mutate a confirmed regional Group.
func (s *State) confirmedRegion(group string) (RegionCohortHead, bool, error) {
	region, found, err := s.get(regionCohortGroupKey(group))
	if err != nil || !found {
		return RegionCohortHead{}, false, err
	}
	head, found, err := s.regionCohortHead(string(region))
	if err == nil && !found {
		err = errors.New("regional Group has no cohort head")
	}
	return head, found && head.CurrentGeneration != 0, err
}

func growthMatchesTask(g RegionGrowth, task GroupChange) bool {
	return task.RegionID == g.Seal.RegionID && task.GrowthGeneration == g.Seal.Generation && task.GroupID == g.Seal.GroupID && task.ChangeID == g.Seal.ChangeID && task.ExpectedEpoch == g.Seal.ExpectedEpoch && slices.Equal(task.Members, g.Seal.Members)
}

func (s *State) authorizeGrowthTask(task GroupChange) error {
	head, regional, err := s.confirmedRegion(task.GroupID)
	if err != nil {
		return err
	}
	if !regional {
		if task.RegionID != "" || task.GrowthGeneration != 0 {
			return ErrGroupMutationUnauthorized
		}
		return nil
	}
	if task.RegionID != head.RegionID || task.GrowthGeneration != head.PendingGeneration || task.GrowthGeneration < 2 || head.CurrentGeneration != task.GrowthGeneration-1 {
		return ErrGroupMutationUnauthorized
	}
	g, found, err := s.regionGrowth(task.RegionID, task.GrowthGeneration)
	if err != nil {
		return err
	}
	if !found || !growthMatchesTask(g, task) {
		return ErrGroupMutationUnauthorized
	}
	cohort, found, err := s.regionCohort(task.RegionID, task.GrowthGeneration)
	if err != nil {
		return err
	}
	if !found || cohort.State != RegionCohortPlanned || cohort.PlannedIndex != g.Seal.PlannedIndex || !cohortMatchesMembers(cohort, task.Members) {
		return ErrGroupMutationUnauthorized
	}
	ready, err := s.cohortCurrentMembers(cohort)
	if err != nil {
		return err
	}
	if !ready {
		return ErrGroupMutationUnauthorized
	}
	return nil
}

// authorizeMaintenanceTask authorizes an explicit member-removal change. It
// does not add a member, alter the Region cohort, or infer anything from
// liveness. A later ordinary growth/join can add a fresh Node and repair its
// data independently.
func (s *State) authorizeMaintenanceTask(task GroupChange) error {
	if task.RegionID != "" || task.GrowthGeneration != 0 || raft.ValidateGroupID(task.GroupID) != nil || task.GroupID == raft.RootGroupID {
		return ErrGroupMutationUnauthorized
	}
	encoded, found, err := s.get(groupKey(task.GroupID))
	if err != nil {
		return err
	}
	if !found {
		return ErrGroupMutationUnauthorized
	}
	var group Group
	if err := decodeStreamJSON(encoded, &group); err != nil {
		return err
	}
	if group.Epoch != task.ExpectedEpoch || group.StateKind != GroupStateEngine {
		return ErrGroupMutationUnauthorized
	}
	return validateMaintenanceRemoval(group, task.Members)
}

func (s *State) applyRegionGrowth(batch *pebble.Batch, command Command, hash string) (string, error) {
	p := command.RegionGrowth
	if existing, found, err := s.regionGrowth(p.RegionID, p.Generation); err != nil {
		return "", err
	} else if found {
		if reflect.DeepEqual(existing.Seal, *p) {
			return "", nil
		} // Historical retry, not live execution authority.
		return "regional growth authorization is immutable", nil
	}
	head, found, err := s.regionCohortHead(p.RegionID)
	if err != nil {
		return "", err
	}
	if !found || head.CurrentGeneration != p.Generation-1 || head.PendingGeneration != p.Generation {
		return "regional growth differs from current pending generation", nil
	}
	previous, found, err := s.regionCohort(p.RegionID, p.Generation-1)
	if err != nil {
		return "", err
	}
	if !found || previous.State != RegionCohortConfirmed || previous.GroupID != p.GroupID || previous.GroupEpoch > p.ExpectedEpoch || previous.GroupFenceRequestID != p.PreviousFenceRequestID || previous.GroupFenceIndex != p.PreviousFenceIndex {
		return "regional growth seal differs from confirmed old fence", nil
	}
	next, found, err := s.regionCohort(p.RegionID, p.Generation)
	if err != nil {
		return "", err
	}
	if !found || next.State != RegionCohortPlanned || next.GroupID != p.GroupID || next.PlannedIndex != p.PlannedIndex || !cohortMatchesMembers(next, p.Members) {
		return "regional growth differs from original plan", nil
	}
	ready, err := s.cohortCurrentMembers(next)
	if err != nil {
		return "", err
	}
	if !ready {
		return "regional growth requires stable current Root participants", nil
	}
	encoded, found, err := s.get(groupKey(p.GroupID))
	if err != nil {
		return "", err
	}
	var group Group
	if !found || decodeStreamJSON(encoded, &group) != nil {
		return "", errors.New("regional growth Group missing or corrupt")
	}
	recovery := group.Epoch > previous.GroupEpoch && cohortContainsMembers(previous, group.Members) && len(group.Members) < len(previous.Nodes)
	if group.StateKind != GroupStateEngine || group.Epoch != p.ExpectedEpoch || p.Recovery != recovery || !cohortExtends(previous, next, group.Members) || !cohortContainsMembers(previous, group.Members) {
		return "regional growth old Group changed", nil
	}
	for _, key := range [][]byte{groupChangeKey(p.ChangeID), activeGroupChangeKey(p.GroupID)} {
		if _, found, err := s.get(key); err != nil {
			return "", err
		} else if found {
			return "regional growth conflicts with existing task", nil
		}
	}
	g := RegionGrowth{Seal: *p, BaseMembers: slices.Clone(group.Members), AuthorizedIndex: s.applied + 1, RequestID: command.RequestID, ResultSHA256: hash}
	if g.Validate() != nil {
		return "regional growth may only add participants without changing existing roles", nil
	}
	task := GroupChange{ChangeID: p.ChangeID, GroupID: p.GroupID, ExpectedEpoch: p.ExpectedEpoch, Members: slices.Clone(p.Members), State: GroupChangePending, RegionID: p.RegionID, GrowthGeneration: p.Generation}
	if err := setJSON(batch, regionGrowthKey(p.RegionID, p.Generation), g); err != nil {
		return "", err
	}
	if err := setJSON(batch, groupChangeKey(p.ChangeID), task); err != nil {
		return "", err
	}
	return "", batch.Set(activeGroupChangeKey(p.GroupID), []byte(p.ChangeID), nil)
}

func (s *State) completedGrowth(cohort RegionCohort, group Group) (RegionGrowth, bool, error) {
	g, found, err := s.regionGrowth(cohort.RegionID, cohort.Generation)
	if err != nil || !found {
		return g, false, err
	}
	encoded, found, err := s.get(groupChangeKey(g.Seal.ChangeID))
	if err != nil {
		return g, false, err
	}
	var task GroupChange
	if !found || decodeStreamJSON(encoded, &task) != nil {
		return g, false, errors.New("regional growth task missing or corrupt")
	}
	return g, g.Seal.PlannedIndex == cohort.PlannedIndex && growthMatchesTask(g, task) && task.State == GroupChangeCompleted && task.CompletedEpoch == g.Seal.ExpectedEpoch+1 && group.Epoch == task.CompletedEpoch && slices.Equal(group.Members, task.Members), nil
}
