package rootstate

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

func regionGroupRetirementKey(groupID string) []byte {
	return []byte("data/region-group-retirement/" + groupID)
}

func regionGroupRebuildKey(groupID string) []byte {
	return []byte("data/region-group-rebuild/" + groupID)
}

// regionGroupRetired is an internal Root-only marker. Once set by an
// explicit Node removal against an unreachable Group, the old Group is no
// longer a live membership responsibility. Its historical cohort remains
// readable for recovery and audit, while a future same-name Region may form
// with fresh identities.
func (s *State) regionGroupRetired(groupID string) (bool, error) {
	encoded, found, err := s.get(regionGroupRetirementKey(groupID))
	if err != nil || !found {
		return found, err
	}
	var value RegionGroupRetirement
	if err := decodeStreamJSON(encoded, &value); err != nil {
		return false, err
	}
	if err := value.validateStored(); err != nil {
		return false, err
	}
	// Once the fresh target membership has been published, the same named
	// Group is live again. The retirement record remains as history, but it
	// must no longer hide current Node responsibilities.
	rebuildEncoded, rebuildFound, err := s.get(regionGroupRebuildKey(groupID))
	if err != nil || !rebuildFound {
		return true, err
	}
	var rebuild RegionGroupRebuild
	if err := decodeStreamJSON(rebuildEncoded, &rebuild); err != nil {
		return false, err
	}
	groupEncoded, groupFound, err := s.get(groupKey(groupID))
	if err != nil || !groupFound {
		return true, err
	}
	var group Group
	if err := decodeStreamJSON(groupEncoded, &group); err != nil {
		return false, err
	}
	if len(group.Members) == 0 {
		return true, nil
	}
	for _, member := range group.Members {
		if !slices.Contains(rebuild.TargetNodes, member.RaftID) {
			return true, nil
		}
	}
	return false, nil
}

func validateNodeCut(nodes []uint64) error {
	if !validCohortNodes(nodes) {
		return errors.New("Node cut must be non-empty, sorted, and unique")
	}
	return nil
}

func (value RegionGroupRetirement) validateInput() error {
	if err := validateRegionGroupIdentity(value.GroupID, value.RegionID); err != nil ||
		value.Generation == 0 || value.GroupEpoch < 2 || value.FenceIndex == 0 || value.RetiredAt != 0 {
		return errors.New("invalid Region Group retirement")
	}
	return validateNodeCut(value.Nodes)
}

func (value RegionGroupRetirement) validateStored() error {
	if err := value.validateInput(); err != nil && value.RetiredAt == 0 {
		return err
	}
	if err := validateRegionGroupIdentity(value.GroupID, value.RegionID); err != nil ||
		value.Generation == 0 || value.GroupEpoch < 2 || value.FenceIndex == 0 || value.RetiredAt == 0 {
		return errors.New("invalid stored Region Group retirement")
	}
	return validateNodeCut(value.Nodes)
}

func (value RegionGroupRebuild) validateInput() error {
	if err := validateRegionGroupIdentity(value.GroupID, value.RegionID); err != nil ||
		value.PreviousGeneration == 0 || value.PreviousGeneration == math.MaxUint64 || value.PreviousGroupEpoch < 2 || value.PreviousFenceIndex == 0 ||
		value.TargetGeneration != value.PreviousGeneration+1 || value.PlannedAt != 0 {
		return errors.New("invalid Region Group rebuild")
	}
	if err := validateNodeCut(value.PreviousNodes); err != nil {
		return err
	}
	if err := validateNodeCut(value.TargetNodes); err != nil {
		return err
	}
	for _, id := range value.TargetNodes {
		if slices.Contains(value.PreviousNodes, id) {
			return errors.New("Region Group rebuild target overlaps previous Nodes")
		}
	}
	return nil
}

func (value RegionGroupRebuild) validateStored() error {
	if err := value.validateInput(); err != nil && value.PlannedAt == 0 {
		return err
	}
	if err := validateRegionGroupIdentity(value.GroupID, value.RegionID); err != nil ||
		value.PreviousGeneration == 0 || value.PreviousGeneration == math.MaxUint64 || value.PreviousGroupEpoch < 2 || value.PreviousFenceIndex == 0 ||
		value.TargetGeneration != value.PreviousGeneration+1 || value.PlannedAt == 0 {
		return errors.New("invalid stored Region Group rebuild")
	}
	if err := validateNodeCut(value.PreviousNodes); err != nil {
		return err
	}
	if err := validateNodeCut(value.TargetNodes); err != nil {
		return err
	}
	for _, id := range value.TargetNodes {
		if slices.Contains(value.PreviousNodes, id) {
			return errors.New("Region Group rebuild target overlaps previous Nodes")
		}
	}
	return nil
}

func validateRegionGroupIdentity(groupID, regionID string) error {
	if raft.ValidateGroupID(groupID) != nil || groupID == raft.RootGroupID || !validID(regionID) {
		return errors.New("invalid Region Group identity")
	}
	return nil
}

func (s *State) RegionGroupRetirement(groupID string) (RegionGroupRetirement, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return RegionGroupRetirement{}, false, err
	}
	encoded, found, err := s.get(regionGroupRetirementKey(groupID))
	if err != nil || !found {
		return RegionGroupRetirement{}, found, err
	}
	var value RegionGroupRetirement
	if err := decodeStreamJSON(encoded, &value); err != nil {
		return RegionGroupRetirement{}, false, err
	}
	return value, true, value.validateStored()
}

// RegionGroupIsRetired reports whether the old Region Group is still out of
// service. A retirement record remains forever as history, so callers that
// need the live routing state must use this view instead of only checking for
// the record's existence.
func (s *State) RegionGroupIsRetired(groupID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.regionGroupRetired(groupID)
}

func (s *State) RegionGroupRebuild(groupID string) (RegionGroupRebuild, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return RegionGroupRebuild{}, false, err
	}
	encoded, found, err := s.get(regionGroupRebuildKey(groupID))
	if err != nil || !found {
		return RegionGroupRebuild{}, found, err
	}
	var value RegionGroupRebuild
	if err := decodeStreamJSON(encoded, &value); err != nil {
		return RegionGroupRebuild{}, false, err
	}
	return value, true, value.validateStored()
}

func (s *State) applyRegionGroupRetirement(batch *pebble.Batch, value RegionGroupRetirement) (string, error) {
	if err := value.validateInput(); err != nil {
		return "", err
	}
	encoded, found, err := s.get(regionGroupRetirementKey(value.GroupID))
	if err != nil {
		return "", err
	}
	if found {
		var existing RegionGroupRetirement
		if err := decodeStreamJSON(encoded, &existing); err != nil {
			return "", err
		}
		if existing.GroupID == value.GroupID && existing.RegionID == value.RegionID && existing.Generation == value.Generation && existing.GroupEpoch == value.GroupEpoch && existing.FenceIndex == value.FenceIndex && slices.Equal(existing.Nodes, value.Nodes) {
			return "", nil
		}
		return "conflicting Region Group retirement already exists", nil
	}
	progress, confirmed, err := s.regionProgress(value.GroupID)
	if err != nil {
		return "", err
	}
	if !confirmed || progress.Current.RegionID != value.RegionID || progress.Current.Generation != value.Generation || progress.Current.GroupEpoch != value.GroupEpoch || progress.Current.GroupFenceIndex != value.FenceIndex || !slices.Equal(progress.Current.Nodes, value.Nodes) {
		return "retirement does not match the confirmed Region Group cut", nil
	}
	if _, active, err := s.get(activeGroupChangeKey(value.GroupID)); err != nil {
		return "", err
	} else if active {
		return "Region Group has an active membership task", nil
	}
	value.RetiredAt = s.applied + 1
	return "", setJSON(batch, regionGroupRetirementKey(value.GroupID), value)
}

func (s *State) applyRegionGroupRebuild(batch *pebble.Batch, value RegionGroupRebuild) (string, error) {
	if err := value.validateInput(); err != nil {
		return "", err
	}
	retirementEncoded, found, err := s.get(regionGroupRetirementKey(value.GroupID))
	if err != nil {
		return "", err
	}
	if !found {
		return "Region Group retirement is required before rebuild planning", nil
	}
	var retirement RegionGroupRetirement
	if err := decodeStreamJSON(retirementEncoded, &retirement); err != nil {
		return "", err
	}
	if retirement.validateStored() != nil || retirement.RegionID != value.RegionID || retirement.Generation != value.PreviousGeneration || retirement.GroupEpoch != value.PreviousGroupEpoch || retirement.FenceIndex != value.PreviousFenceIndex || !slices.Equal(retirement.Nodes, value.PreviousNodes) {
		return "rebuild does not match the retired Region Group cut", nil
	}
	for _, id := range retirement.Nodes {
		if _, member, err := s.rootMember(id); err != nil {
			return "", err
		} else if member {
			return "old Region Node is still a Root member", nil
		}
	}
	existingEncoded, found, err := s.get(regionGroupRebuildKey(value.GroupID))
	if err != nil {
		return "", err
	}
	if found {
		var existing RegionGroupRebuild
		if err := decodeStreamJSON(existingEncoded, &existing); err != nil {
			return "", err
		}
		if existing.RegionID == value.RegionID && existing.PreviousGeneration == value.PreviousGeneration && existing.PreviousGroupEpoch == value.PreviousGroupEpoch && existing.PreviousFenceIndex == value.PreviousFenceIndex && slices.Equal(existing.PreviousNodes, value.PreviousNodes) && existing.TargetGeneration == value.TargetGeneration && slices.Equal(existing.TargetNodes, value.TargetNodes) {
			return "", nil
		}
		return "conflicting Region Group rebuild already exists", nil
	}
	for _, id := range value.TargetNodes {
		node, member, err := s.rootMember(id)
		if err != nil {
			return "", err
		}
		if !member {
			return fmt.Sprintf("target Node %d is not a Root member", id), nil
		}
		if node.RegionID != value.RegionID {
			return fmt.Sprintf("target Node %d is in the wrong Region", id), nil
		}
	}
	value.PlannedAt = s.applied + 1
	groupEncoded, found, err := s.get(groupKey(value.GroupID))
	if err != nil {
		return "", err
	}
	if !found {
		return "Region Group descriptor is missing", nil
	}
	var group Group
	if err := decodeStreamJSON(groupEncoded, &group); err != nil {
		return "", err
	}
	if group.GroupID != value.GroupID || group.StateKind != GroupStateCapability {
		return "Region Group descriptor is invalid", nil
	}
	// The old Group is no longer routable after retirement. Reset only the
	// Root projection; fresh Nodes will create their own empty local Raft Group
	// and the normal formation observer will publish the new membership.
	group.Epoch = 1
	group.Members = nil
	if err := setJSON(batch, groupKey(value.GroupID), group); err != nil {
		return "", err
	}
	return "", setJSON(batch, regionGroupRebuildKey(value.GroupID), value)
}
