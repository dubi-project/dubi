package rootstate

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

const (
	PlanRegionCohort      = "plan_region_cohort"
	ConfirmRegionCohort   = "confirm_region_cohort"
	RegionCohortPlanned   = "planned"
	RegionCohortConfirmed = "confirmed"
	// This is a bounded internal record limit, not a configurable replica count.
	MaxRegionCohortNodes = 4096
)

// RegionCohort is an immutable participant generation after confirmation.
// Nodes are data failure domains, independent of Root voting seats and Volumes.
// Membership alone is NOT evidence that any particular payload is durable.
type RegionCohort struct {
	RegionID   string   `json:"region_id"`
	Generation uint64   `json:"generation"`
	GroupID    string   `json:"group_id"`
	Nodes      []uint64 `json:"nodes"`
	State      string   `json:"state"`
	// Formation is assigned by Root when the first Group is formed.
	Formation bool `json:"formation,omitempty"`
	// Reformation is the internal equivalent for a same-name Region after its
	// previous cohort was explicitly retired. It is never user supplied.
	Reformation         bool   `json:"reformation,omitempty"`
	PlannedIndex        uint64 `json:"planned_index"`
	ConfirmedIndex      uint64 `json:"confirmed_index,omitempty"`
	GroupEpoch          uint64 `json:"group_epoch,omitempty"`
	GroupFenceRequestID string `json:"group_fence_request_id,omitempty"`
	GroupFenceIndex     uint64 `json:"group_fence_index,omitempty"`
}

// RegionCohortHead separates desired participants from the confirmed denominator.
// A pending generation never changes CurrentGeneration. Old records are retained.
type RegionCohortHead struct {
	RegionID          string `json:"region_id"`
	CurrentGeneration uint64 `json:"current_generation"`
	PendingGeneration uint64 `json:"pending_generation,omitempty"`
}

type RegionCohortPlan struct {
	RegionID           string   `json:"region_id"`
	ExpectedGeneration uint64   `json:"expected_generation"`
	GroupID            string   `json:"group_id"`
	Nodes              []uint64 `json:"nodes"`
}

// RegionCohortConfirmation is an INTERNAL coordinator assertion, not a public
// durability receipt. Before proposing it, the coordinator must confirm stable
// actual Group membership, persist a generation-bound fence in that Group, and
// observe every planned participant applied through that fence. Root can check
// catalog consistency but cannot verify remote disk/application state. The initial
// formation confirmer and growth coordinator implement their respective phases.
// This does not certify user data or grant perpetual write authority.
type RegionCohortConfirmation struct {
	RegionID            string `json:"region_id"`
	Generation          uint64 `json:"generation"`
	GroupID             string `json:"group_id"`
	GroupEpoch          uint64 `json:"group_epoch"`
	GroupFenceRequestID string `json:"group_fence_request_id"`
	GroupFenceIndex     uint64 `json:"group_fence_index"`
}

func validCohortNodes(nodes []uint64) bool {
	if len(nodes) == 0 || len(nodes) > MaxRegionCohortNodes {
		return false
	}
	for i, id := range nodes {
		if id == 0 || (i > 0 && nodes[i-1] >= id) {
			return false
		}
	}
	return true
}

func (p RegionCohortPlan) validate() error {
	if !validID(p.RegionID) || p.ExpectedGeneration == math.MaxUint64 || raft.ValidateGroupID(p.GroupID) != nil || p.GroupID == raft.RootGroupID || !validCohortNodes(p.Nodes) {
		return errors.New("invalid Region cohort plan: requires sorted unique local Nodes and non-Root Group")
	}
	return nil
}

func (c RegionCohortConfirmation) validate() error {
	if !validID(c.RegionID) || c.Generation == 0 || raft.ValidateGroupID(c.GroupID) != nil || c.GroupID == raft.RootGroupID || c.GroupEpoch < 2 || !validID(c.GroupFenceRequestID) || c.GroupFenceIndex == 0 {
		return errors.New("invalid Region cohort confirmation")
	}
	return nil
}

func (c RegionCohort) Validate() error {
	if c.Formation && c.Generation != 1 {
		return errors.New("only the first cohort can authorize initial formation")
	}
	if c.Reformation && c.Generation < 2 {
		return errors.New("Region reformation requires a later generation")
	}
	if c.Generation == 0 || c.PlannedIndex == 0 || (RegionCohortPlan{RegionID: c.RegionID, GroupID: c.GroupID, Nodes: c.Nodes}).validate() != nil {
		return errors.New("invalid Region cohort")
	}
	if c.State == RegionCohortPlanned && c.ConfirmedIndex == 0 && c.GroupEpoch == 0 && c.GroupFenceRequestID == "" && c.GroupFenceIndex == 0 {
		return nil
	}
	if c.State == RegionCohortConfirmed && c.ConfirmedIndex > c.PlannedIndex && c.confirmation().validate() == nil {
		return nil
	}
	return errors.New("invalid Region cohort state or fence")
}

func (c RegionCohort) confirmation() RegionCohortConfirmation {
	return RegionCohortConfirmation{RegionID: c.RegionID, Generation: c.Generation, GroupID: c.GroupID, GroupEpoch: c.GroupEpoch, GroupFenceRequestID: c.GroupFenceRequestID, GroupFenceIndex: c.GroupFenceIndex}
}

func (h RegionCohortHead) validate() error {
	if !validID(h.RegionID) || (h.CurrentGeneration == 0 && h.PendingGeneration != 1) ||
		(h.PendingGeneration != 0 && (h.CurrentGeneration == math.MaxUint64 || h.PendingGeneration != h.CurrentGeneration+1)) {
		return errors.New("invalid Region cohort head")
	}
	return nil
}

func regionCohortKey(region string, generation uint64) []byte {
	return []byte(fmt.Sprintf("data/region-cohort/%s/%020d", region, generation))
}
func regionCohortHeadKey(region string) []byte { return []byte("data/region-cohort-head/" + region) }
func regionCohortGroupKey(group string) []byte { return []byte("data/region-cohort-group/" + group) }

// GroupInitialCohort returns the immutable initial formation, not a current
// placement/health view. The reverse binding and generation share one read lock.
func (s *State) GroupInitialCohort(groupID string) (RegionCohort, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if raft.ValidateGroupID(groupID) != nil || groupID == raft.RootGroupID {
		return RegionCohort{}, false, errors.New("invalid regional Group lookup")
	}
	region, found, err := s.get(regionCohortGroupKey(groupID))
	if err != nil || !found {
		return RegionCohort{}, found, err
	}
	if !validID(string(region)) {
		return RegionCohort{}, false, errors.New("invalid Group Region binding")
	}
	c, found, err := s.regionCohort(string(region), 1)
	if err != nil {
		return RegionCohort{}, false, err
	}
	if !found || c.GroupID != groupID {
		return RegionCohort{}, false, errors.New("Group has no matching initial cohort")
	}
	return c, true, nil
}

// PendingRegionFormation returns the current fresh-Group formation intent.
// Unlike GroupInitialCohort it also covers a same-name Region after the old
// cohort was explicitly retired. It is an internal recovery view, not a user
// supplied membership override.
func (s *State) PendingRegionFormation(groupID string) (RegionCohort, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if raft.ValidateGroupID(groupID) != nil || groupID == raft.RootGroupID {
		return RegionCohort{}, false, errors.New("invalid regional formation Group lookup")
	}
	region, found, err := s.get(regionCohortGroupKey(groupID))
	if err != nil || !found {
		return RegionCohort{}, found, err
	}
	head, found, err := s.regionCohortHead(string(region))
	if err != nil || !found || head.PendingGeneration == 0 {
		return RegionCohort{}, false, err
	}
	cohort, found, err := s.regionCohort(string(region), head.PendingGeneration)
	if err != nil || !found || cohort.GroupID != groupID || (!cohort.Formation && !cohort.Reformation) || cohort.State != RegionCohortPlanned {
		return RegionCohort{}, false, err
	}
	return cohort, true, nil
}

func (s *State) RegionCohort(region string, generation uint64) (RegionCohort, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !validID(region) || generation == 0 {
		return RegionCohort{}, false, errors.New("invalid Region cohort lookup")
	}
	return s.regionCohort(region, generation)
}

func (s *State) regionCohort(region string, generation uint64) (RegionCohort, bool, error) {
	encoded, found, err := s.get(regionCohortKey(region, generation))
	if err != nil || !found {
		return RegionCohort{}, found, err
	}
	var value RegionCohort
	if err := decodeStreamJSON(encoded, &value); err != nil {
		return RegionCohort{}, false, err
	}
	if err := value.Validate(); err != nil {
		return RegionCohort{}, false, err
	}
	if value.RegionID != region || value.Generation != generation {
		return RegionCohort{}, false, errors.New("Region cohort key differs from record")
	}
	return value, true, nil
}

func (s *State) RegionCohortHead(region string) (RegionCohortHead, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !validID(region) {
		return RegionCohortHead{}, false, errors.New("invalid Region cohort head lookup")
	}
	return s.regionCohortHead(region)
}

func (s *State) regionCohortHead(region string) (RegionCohortHead, bool, error) {
	encoded, found, err := s.get(regionCohortHeadKey(region))
	if err != nil || !found {
		return RegionCohortHead{}, found, err
	}
	var value RegionCohortHead
	if err := decodeStreamJSON(encoded, &value); err != nil {
		return RegionCohortHead{}, false, err
	}
	if err := value.validate(); err != nil {
		return RegionCohortHead{}, false, err
	}
	if value.RegionID != region {
		return RegionCohortHead{}, false, errors.New("Region cohort head key differs from record")
	}
	return value, true, nil
}

// RegionConfirmationView is a single Root applied cut. It is not remote Group
// evidence; callers must obtain Root ReadIndex before using it and revalidate
// after collecting participant observations. A pending but unready plan returns
// found=false, never a partial authorization assembled from separate reads.
type RegionConfirmationView struct {
	AppliedIndex uint64
	Cohort       RegionCohort
	Group        Group
}

func (s *State) PendingRegionConfirmation(groupID string) (RegionConfirmationView, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return RegionConfirmationView{}, false, err
	}
	if raft.ValidateGroupID(groupID) != nil || groupID == raft.RootGroupID {
		return RegionConfirmationView{}, false, errors.New("invalid regional confirmation Group")
	}
	region, found, err := s.get(regionCohortGroupKey(groupID))
	if err != nil || !found {
		return RegionConfirmationView{}, false, err
	}
	head, found, err := s.regionCohortHead(string(region))
	if err != nil {
		return RegionConfirmationView{}, false, err
	}
	if !found {
		return RegionConfirmationView{}, false, errors.New("regional Group has no cohort head")
	}
	if head.PendingGeneration == 0 {
		return RegionConfirmationView{}, false, nil
	}
	cohort, found, err := s.regionCohort(string(region), head.PendingGeneration)
	if err != nil {
		return RegionConfirmationView{}, false, err
	}
	if !found || cohort.GroupID != groupID || cohort.State != RegionCohortPlanned {
		return RegionConfirmationView{}, false, errors.New("regional pending cohort linkage is invalid")
	}
	if _, active, err := s.get(activeGroupChangeKey(groupID)); err != nil || active {
		return RegionConfirmationView{}, false, err
	}
	encoded, found, err := s.get(groupKey(groupID))
	if err != nil {
		return RegionConfirmationView{}, false, err
	}
	if !found {
		return RegionConfirmationView{}, false, errors.New("regional Group is missing")
	}
	var group Group
	if err := decodeStreamJSON(encoded, &group); err != nil {
		return RegionConfirmationView{}, false, err
	}
	if group.GroupID != groupID || group.StateKind != GroupStateEngine {
		return RegionConfirmationView{}, false, errors.New("regional Group binding is invalid")
	}
	if group.Epoch < 2 || !cohortMatchesMembers(cohort, group.Members) {
		return RegionConfirmationView{}, false, nil
	}
	if cohort.Generation > 1 && !cohort.Reformation {
		if _, ready, err := s.completedGrowth(cohort, group); err != nil || !ready {
			return RegionConfirmationView{}, false, err
		}
	}
	ready, err := s.cohortCurrentMembers(cohort)
	if err != nil || !ready {
		return RegionConfirmationView{}, false, err
	}
	return RegionConfirmationView{AppliedIndex: s.applied, Cohort: cohort, Group: group}, true, nil
}

// Caller holds s.mu. Health is deliberately absent: current membership, not
// recent reachability or historical Node registration, grants participation.
func (s *State) cohortCurrentMembers(cohort RegionCohort) (bool, error) {
	if len(s.conf.GetVotersOutgoing()) != 0 || len(s.conf.GetLearnersNext()) != 0 || s.conf.GetAutoLeave() {
		return false, nil
	}
	for _, id := range cohort.Nodes {
		node, member, err := s.rootMember(id)
		if err != nil || !member {
			return false, err
		}
		if node.RegionID != cohort.RegionID {
			return false, nil
		}
	}
	return true, nil
}

// cohortCurrentMembersForGroup is used during the short recovery window after
// an explicit maintenance removal. The old cohort may still name the fenced
// Node, while the live Group contains the surviving Root members.
func (s *State) cohortCurrentMembersForGroup(group Group) (bool, error) {
	if len(s.conf.GetVotersOutgoing()) != 0 || len(s.conf.GetLearnersNext()) != 0 || s.conf.GetAutoLeave() {
		return false, nil
	}
	for _, member := range group.Members {
		node, rootMember, err := s.rootMember(member.RaftID)
		if err != nil || !rootMember || node.RegionID == "" {
			return false, err
		}
	}
	return true, nil
}

// cohortContainsMembers verifies that a currently published Group is a stable
// subset of a cohort. A maintenance removal may temporarily shrink the live
// Group while the immutable cohort still names the old participants.
func cohortContainsMembers(cohort RegionCohort, members []GroupMember) bool {
	if validateGroupMembers(members) != nil {
		return false
	}
	for _, member := range members {
		if _, found := slices.BinarySearch(cohort.Nodes, member.RaftID); !found {
			return false
		}
	}
	return true
}

func cohortExistingMembers(cohort RegionCohort, members []GroupMember) []GroupMember {
	filtered := make([]GroupMember, 0, len(members))
	for _, member := range members {
		if _, found := slices.BinarySearch(cohort.Nodes, member.RaftID); found {
			filtered = append(filtered, member)
		}
	}
	return filtered
}

// cohortExtends accepts ordinary additions and the recovery step after an
// explicit maintenance removal. The next cohort must keep every currently
// published Group member and add at least one new participant.
func cohortExtends(previous, next RegionCohort, current []GroupMember) bool {
	if previous.RegionID != next.RegionID || previous.GroupID != next.GroupID || len(next.Nodes) == 0 ||
		!cohortContainsMembers(previous, current) || !cohortContainsMembers(next, current) {
		return false
	}
	added := false
	for _, id := range next.Nodes {
		if _, found := slices.BinarySearch(previous.Nodes, id); !found {
			added = true
			break
		}
	}
	if !added {
		return false
	}
	if len(next.Nodes) > len(previous.Nodes) {
		for _, id := range previous.Nodes {
			if _, found := slices.BinarySearch(next.Nodes, id); !found {
				return false
			}
		}
		return true
	}
	// Same-size recovery is valid only when the live Group is smaller and the
	// new cohort adds a fresh participant after explicit maintenance removal.
	return len(current) < len(next.Nodes)
}

func (s *State) applyRegionCohort(batch *pebble.Batch, command Command) (string, error) {
	if command.Type == PlanRegionCohort {
		p := command.RegionPlan
		head, found, err := s.regionCohortHead(p.RegionID)
		if err != nil {
			return "", err
		}
		if !found {
			head = RegionCohortHead{RegionID: p.RegionID}
		}
		if head.CurrentGeneration != p.ExpectedGeneration || head.PendingGeneration != 0 {
			return "Region cohort generation changed or a plan is pending", nil
		}
		next := RegionCohort{RegionID: p.RegionID, Generation: p.ExpectedGeneration + 1, GroupID: p.GroupID, Nodes: slices.Clone(p.Nodes), State: RegionCohortPlanned, PlannedIndex: s.applied + 1}
		retired, err := s.regionGroupRetired(p.GroupID)
		if err != nil {
			return "", err
		}
		freshFormation := head.CurrentGeneration != 0 && retired
		if head.CurrentGeneration != 0 && !freshFormation {
			previous, exists, err := s.regionCohort(p.RegionID, head.CurrentGeneration)
			if err != nil {
				return "", err
			}
			if !exists || previous.State != RegionCohortConfirmed {
				return "", errors.New("Region cohort head has no confirmed generation")
			}
		}
		if _, exists, err := s.get(regionCohortKey(p.RegionID, next.Generation)); err != nil {
			return "", err
		} else if exists {
			return "", errors.New("Region cohort generation already exists without pending head")
		}
		encoded, exists, err := s.get(groupKey(p.GroupID))
		if err != nil {
			return "", err
		}
		if !exists {
			return "Region cohort Group is not registered", nil
		}
		var group Group
		if err := decodeStreamJSON(encoded, &group); err != nil {
			return "", err
		}
		if group.GroupID != p.GroupID || group.StateKind != GroupStateEngine {
			return "Region cohort requires engine-neutral data Group", nil
		}
		if head.CurrentGeneration != 0 && !freshFormation {
			previous, _, err := s.regionCohort(p.RegionID, head.CurrentGeneration)
			if err != nil {
				return "", err
			}
			if !cohortExtends(previous, next, group.Members) {
				return "Region cohort must retain the current Group and add a fresh participant", nil
			}
		}
		if head.CurrentGeneration != 0 && !freshFormation {
			previous, _, err := s.regionCohort(p.RegionID, head.CurrentGeneration)
			if err != nil {
				return "", err
			}
			maintenanceRecovery := group.Epoch > previous.GroupEpoch &&
				cohortContainsMembers(previous, group.Members) && len(group.Members) < len(previous.Nodes)
			if (group.Epoch != previous.GroupEpoch || !cohortMatchesMembers(previous, group.Members)) && !maintenanceRecovery {
				return "regional growth requires unchanged confirmed Group or an explicit maintenance recovery subset", nil
			}
			if _, active, err := s.get(activeGroupChangeKey(p.GroupID)); err != nil {
				return "", err
			} else if active {
				return "regional growth conflicts with active member task", nil
			}
			if ready, err := s.cohortCurrentMembers(next); err != nil {
				return "", err
			} else if !ready {
				return "regional growth plan requires stable current Root participants", nil
			}
		}
		next.Formation = next.Generation == 1 && group.Epoch == 1 && len(group.Members) == 0
		next.Reformation = freshFormation && group.Epoch == 1 && len(group.Members) == 0
		if freshFormation {
			planned, found, err := s.get(regionGroupRebuildKey(p.GroupID))
			if err != nil {
				return "", err
			}
			if !found {
				return "same-name Region formation requires a fresh Group intent", nil
			}
			var rebuild RegionGroupRebuild
			if err := decodeStreamJSON(planned, &rebuild); err != nil {
				return "", err
			}
			if rebuild.RegionID != p.RegionID || !slices.Equal(rebuild.TargetNodes, p.Nodes) {
				return "same-name Region formation does not match the fresh Node set", nil
			}
		}
		if next.Formation || next.Reformation {
			if _, active, err := s.get(activeGroupChangeKey(p.GroupID)); err != nil {
				return "", err
			} else if active {
				return "initial regional formation conflicts with active Group membership task", nil
			}
		}
		owner, assigned, err := s.get(regionCohortGroupKey(p.GroupID))
		if err != nil {
			return "", err
		}
		if assigned && string(owner) != p.RegionID {
			return "Region cohort Group belongs to another Region", nil
		}
		if found && !assigned {
			return "", errors.New("Region cohort Group binding is missing")
		}
		for _, id := range p.Nodes {
			if next.Formation || next.Reformation {
				_, member, err := s.rootMember(id)
				if err != nil {
					return "", err
				}
				if !member {
					return "initial regional formation requires current Root members", nil
				}
			}
			encoded, exists, err := s.get(nodeKey(id))
			if err != nil {
				return "", err
			}
			if !exists {
				return "Region cohort Node is not registered", nil
			}
			var node Node
			if err := decodeStreamJSON(encoded, &node); err != nil {
				return "", err
			}
			if node.RaftID != id || node.RegionID != p.RegionID {
				return "Region cohort Node belongs to another Region", nil
			}
		}
		head.PendingGeneration = next.Generation
		if err := batch.Set(regionCohortGroupKey(p.GroupID), []byte(p.RegionID), nil); err != nil {
			return "", err
		}
		if err := setJSON(batch, regionCohortKey(p.RegionID, next.Generation), next); err != nil {
			return "", err
		}
		return "", setJSON(batch, regionCohortHeadKey(p.RegionID), head)
	}
	c := command.RegionConfirmation
	cohort, found, err := s.regionCohort(c.RegionID, c.Generation)
	if err != nil {
		return "", err
	}
	if !found {
		return "Region cohort is not planned", nil
	}
	if cohort.State == RegionCohortConfirmed {
		if cohort.confirmation() == *c {
			return "", nil
		}
		return "Region cohort confirmation is immutable", nil
	}
	head, found, err := s.regionCohortHead(c.RegionID)
	if err != nil {
		return "", err
	}
	if !found || head.PendingGeneration != c.Generation || cohort.GroupID != c.GroupID {
		return "Region cohort confirmation does not match pending plan", nil
	}
	if _, active, err := s.get(activeGroupChangeKey(c.GroupID)); err != nil {
		return "", err
	} else if active {
		return "Region cohort Group membership is changing", nil
	}
	encoded, found, err := s.get(groupKey(c.GroupID))
	if err != nil {
		return "", err
	}
	if !found {
		return "Region cohort Group is missing", nil
	}
	var group Group
	if err := decodeStreamJSON(encoded, &group); err != nil {
		return "", err
	}
	if group.GroupID != c.GroupID || group.Epoch != c.GroupEpoch || group.StateKind != GroupStateEngine || !cohortMatchesMembers(cohort, group.Members) {
		return "Region cohort confirmation differs from published Group membership", nil
	}
	if ready, err := s.cohortCurrentMembers(cohort); err != nil {
		return "", err
	} else if !ready {
		return "Region cohort confirmation requires stable current Root members in its Region", nil
	}
	if head.CurrentGeneration != 0 && !cohort.Formation && !cohort.Reformation {
		growth, ready, err := s.completedGrowth(cohort, group)
		if err != nil {
			return "", err
		}
		if !ready || c.GroupFenceIndex <= growth.Seal.SealIndex || c.GroupFenceRequestID == growth.Seal.SealRequestID {
			return "regional confirmation requires completed sealed growth and a later candidate fence", nil
		}
		previous, found, err := s.regionCohort(c.RegionID, head.CurrentGeneration)
		if err != nil {
			return "", err
		}
		if !found || previous.State != RegionCohortConfirmed {
			return "", errors.New("Region cohort previous confirmation is missing")
		}
		if c.GroupEpoch <= previous.GroupEpoch || c.GroupFenceIndex <= previous.GroupFenceIndex || c.GroupFenceRequestID == previous.GroupFenceRequestID {
			return "Region cohort confirmation did not advance Group fence", nil
		}
	}
	cohort.State, cohort.ConfirmedIndex = RegionCohortConfirmed, s.applied+1
	cohort.GroupEpoch, cohort.GroupFenceRequestID, cohort.GroupFenceIndex = c.GroupEpoch, c.GroupFenceRequestID, c.GroupFenceIndex
	head.CurrentGeneration, head.PendingGeneration = cohort.Generation, 0
	if err := setJSON(batch, regionCohortKey(c.RegionID, c.Generation), cohort); err != nil {
		return "", err
	}
	return "", setJSON(batch, regionCohortHeadKey(c.RegionID), head)
}

// Data participants include both voting and non-voting Group members. A
// confirmed cohort needs three voters, or all participants as voters when
// smaller than three. Local-only foreground confirmation remains separately
// restricted to three-or-more Nodes.
func cohortMatchesMembers(c RegionCohort, members []GroupMember) bool {
	if len(members) != len(c.Nodes) || validateGroupMembers(members) != nil {
		return false
	}
	voters := 0
	for _, member := range members {
		if _, found := slices.BinarySearch(c.Nodes, member.RaftID); !found {
			return false
		}
		if member.Role == GroupMemberVoter {
			voters++
		}
	}
	return voters >= min(3, len(c.Nodes))
}

func formationMatchesMembers(c RegionCohort, members []GroupMember) bool {
	if !cohortMatchesMembers(c, members) {
		return false
	}
	for _, member := range members {
		if member.Role != GroupMemberVoter {
			return false
		}
	}
	return true
}
