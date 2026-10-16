package rootstate

import (
	"errors"

	"github.com/dubi-project/dubi/internal/platform/raft"
)

// RegionProgress is one owned applied-state cut. Obtain ReadIndex before using
// it and revalidate after remote observations. Eligible never filters health.
type RegionProgress struct {
	AppliedIndex  uint64
	Eligible      bool
	Head          RegionCohortHead
	Current       RegionCohort
	Pending       *RegionCohort
	Group         Group
	CurrentGrowth *RegionGrowth
	PendingGrowth *RegionGrowth
	PendingTask   *GroupChange
}

func (s *State) RegionProgress(groupID string) (RegionProgress, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.regionProgress(groupID)
}

// Caller holds s.mu. Shared with multi-Region placement reads so they cannot
// combine cohorts from different applied Root cuts.
func (s *State) regionProgress(groupID string) (RegionProgress, bool, error) {
	var view RegionProgress
	if err := s.usable(); err != nil {
		return view, false, err
	}
	if raft.ValidateGroupID(groupID) != nil || groupID == raft.RootGroupID {
		return view, false, errors.New("invalid regional progress Group")
	}
	head, confirmed, err := s.confirmedRegion(groupID)
	if err != nil || !confirmed {
		return view, false, err
	}
	view.AppliedIndex, view.Head = s.applied, head
	current, found, err := s.regionCohort(head.RegionID, head.CurrentGeneration)
	if err != nil {
		return view, false, err
	}
	if !found || current.State != RegionCohortConfirmed || current.GroupID != groupID {
		return view, false, errors.New("invalid current regional cohort")
	}
	view.Current = current
	encoded, found, err := s.get(groupKey(groupID))
	if err != nil {
		return view, false, err
	}
	if !found || decodeStreamJSON(encoded, &view.Group) != nil || view.Group.GroupID != groupID || view.Group.StateKind != GroupStateEngine {
		return view, false, errors.New("invalid regional progress Group")
	}
	loadGrowth := func(generation uint64) (*RegionGrowth, error) {
		key := regionGrowthKey(head.RegionID, generation)
		encoded, found, err := s.get(key)
		if err != nil || !found {
			return nil, err
		}
		if err := validateRegionGrowthSnapshot(s.db, s.applied, key, encoded); err != nil {
			return nil, err
		}
		var growth RegionGrowth
		if err := decodeStreamJSON(encoded, &growth); err != nil {
			return nil, err
		}
		return &growth, nil
	}
	if current.Generation > 1 && !current.Formation && !current.Reformation {
		view.CurrentGrowth, err = loadGrowth(current.Generation)
		if err != nil {
			return view, false, err
		}
		if view.CurrentGrowth == nil {
			return view, false, errors.New("confirmed regional growth has no authorization")
		}
	}
	if head.PendingGeneration != 0 {
		pending, found, err := s.regionCohort(head.RegionID, head.PendingGeneration)
		if err != nil {
			return view, false, err
		}
		if !found || pending.State != RegionCohortPlanned || ((!pending.Formation && !pending.Reformation) && !cohortExtends(current, pending, cohortExistingMembers(current, view.Group.Members))) {
			return view, false, errors.New("invalid pending regional progress")
		}
		view.Pending = &pending
		view.PendingGrowth, err = loadGrowth(pending.Generation)
		if err != nil {
			return view, false, err
		}
		if view.PendingGrowth != nil {
			encoded, found, err := s.get(groupChangeKey(view.PendingGrowth.Seal.ChangeID))
			if err != nil {
				return view, false, err
			}
			var task GroupChange
			if !found || decodeStreamJSON(encoded, &task) != nil || !growthMatchesTask(*view.PendingGrowth, task) {
				return view, false, errors.New("regional progress task linkage is invalid")
			}
			view.PendingTask = &task
		}
	}
	// Check current descriptor and task linkage even before an authorization
	// exists. A generic task cannot be adopted as a regional growth task.
	active, hasActive, err := s.get(activeGroupChangeKey(groupID))
	if err != nil {
		return view, false, err
	}
	if hasActive != (view.PendingTask != nil && view.PendingTask.State == GroupChangePending) || hasActive && string(active) != view.PendingTask.ChangeID {
		return view, false, errors.New("regional progress active task is invalid")
	}
	if view.PendingTask == nil || view.PendingTask.State == GroupChangePending {
		if view.Pending != nil && (view.Pending.Formation || view.Pending.Reformation) {
			// The retired Group was reset to an empty Epoch-1 descriptor. The
			// fresh formation is validated by its pending cohort and Root
			// membership, not against the retired Group's old members.
		} else if view.Group.Epoch == current.GroupEpoch {
			if !cohortMatchesMembers(current, view.Group.Members) {
				return view, false, errors.New("regional descriptor changed without completed growth")
			}
		} else if view.Group.Epoch < current.GroupEpoch || !cohortContainsMembers(current, view.Group.Members) {
			return view, false, errors.New("regional descriptor lost a confirmed participant")
		}
	}
	if view.Group.Epoch > current.GroupEpoch {
		view.Eligible, err = s.cohortCurrentMembersForGroup(view.Group)
	} else {
		view.Eligible, err = s.cohortCurrentMembers(current)
	}
	if err == nil && view.Eligible && view.Pending != nil {
		view.Eligible, err = s.cohortCurrentMembers(*view.Pending)
	}
	return view, err == nil, err
}
