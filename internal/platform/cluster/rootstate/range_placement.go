package rootstate

import (
	"errors"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

func (p RangePlacement) Validate() error {
	if ValidateRangeID(p.RangeID) != nil || p.RangeEpoch == 0 || raft.ValidateGroupID(p.OwnerGroupID) != nil ||
		p.OwnerGroupID == raft.RootGroupID || len(p.Regions) == 0 {
		return errors.New("invalid Range Region placement")
	}
	seen := make(map[string]bool, len(p.Regions))
	ownerFound := false
	for i, region := range p.Regions {
		if !validID(region.RegionID) || raft.ValidateGroupID(region.GroupID) != nil || region.GroupID == raft.RootGroupID ||
			region.Generation == 0 || seen[region.RegionID] || (i > 0 && p.Regions[i-1].RegionID >= region.RegionID) {
			return errors.New("Range Region placement must be sorted and unique")
		}
		seen[region.RegionID] = true
		if region.GroupID == p.OwnerGroupID {
			ownerFound = true
		}
	}
	if !ownerFound {
		return errors.New("Range placement lacks its routing Owner Group")
	}
	return nil
}

func rangePlacementKey(rangeID string) []byte { return []byte("data/range-placement/" + rangeID) }

func (s *State) RangePlacement(rangeID string) (RangePlacement, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return RangePlacement{}, false, err
	}
	if ValidateRangeID(rangeID) != nil {
		return RangePlacement{}, false, errors.New("invalid Range placement lookup")
	}
	encoded, found, err := s.get(rangePlacementKey(rangeID))
	if err != nil || !found {
		return RangePlacement{}, found, err
	}
	var placement RangePlacement
	if err := decodeStreamJSON(encoded, &placement); err != nil || placement.Validate() != nil || placement.RangeID != rangeID {
		return RangePlacement{}, false, errors.New("invalid Range placement record")
	}
	return placement, true, nil
}

// RangeRegionPlacementView joins the durable Range placement mapping with one
// Root applied cut of its Region cohorts and registered participants.
func (s *State) RangeRegionPlacementView(rangeID string, maxRegions, maxNodes int) (RangePlacement, RegionPlacementView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return RangePlacement{}, RegionPlacementView{}, err
	}
	if ValidateRangeID(rangeID) != nil {
		return RangePlacement{}, RegionPlacementView{}, errors.New("invalid Range placement lookup")
	}
	encoded, found, err := s.get(rangePlacementKey(rangeID))
	if err != nil {
		return RangePlacement{}, RegionPlacementView{}, err
	}
	if !found {
		return RangePlacement{}, RegionPlacementView{}, errors.New("Range has no Region placement")
	}
	var placement RangePlacement
	if err := decodeStreamJSON(encoded, &placement); err != nil || placement.Validate() != nil || placement.RangeID != rangeID {
		return RangePlacement{}, RegionPlacementView{}, errors.New("invalid Range placement record")
	}
	groups := make([]string, len(placement.Regions))
	for i, region := range placement.Regions {
		groups[i] = region.GroupID
	}
	view, err := s.regionPlacementView(groups, maxRegions, maxNodes)
	if err != nil {
		return RangePlacement{}, RegionPlacementView{}, err
	}
	for i, region := range placement.Regions {
		if view.Regions[i].Progress.Current.RegionID != region.RegionID ||
			view.Regions[i].Progress.Current.Generation != region.Generation {
			return RangePlacement{}, RegionPlacementView{}, errors.New("Range placement Region generation is not current")
		}
	}
	return placement, view, nil
}

func (s *State) applyRangePlacement(batch *pebble.Batch, placement RangePlacement) (string, error) {
	if err := placement.Validate(); err != nil {
		return "invalid Range Region placement", nil
	}
	rangePath, found, err := s.get(rangeIDKey(placement.RangeID))
	if err != nil || !found {
		return "Range is not registered", nil
	}
	if !strings.HasPrefix(string(rangePath), "data/range/") {
		return "Range directory is invalid", nil
	}
	encoded, found, err := s.get(rangePath)
	if err != nil || !found {
		return "Range directory is invalid", nil
	}
	var descriptor Range
	if err := decodeStreamJSON(encoded, &descriptor); err != nil || descriptor.Validate() != nil || descriptor.RangeID != placement.RangeID {
		return "Range directory is invalid", nil
	}
	if descriptor.State != RangeStateActive || descriptor.Epoch != placement.RangeEpoch || descriptor.OwnerGroupID != placement.OwnerGroupID {
		return "Range placement does not match the active Range", nil
	}
	for _, region := range placement.Regions {
		groupEncoded, found, err := s.get(groupKey(region.GroupID))
		if err != nil || !found {
			return "Range placement Group is not registered", nil
		}
		var group Group
		if err := decodeStreamJSON(groupEncoded, &group); err != nil || group.StateKind != GroupStateEngine {
			return "Range placement Group is invalid", nil
		}
		cohort, found, err := s.regionCohort(region.RegionID, region.Generation)
		head, headFound, headErr := s.regionCohortHead(region.RegionID)
		if err != nil || headErr != nil || !found || !headFound || cohort.GroupID != region.GroupID || cohort.State != RegionCohortConfirmed || head.CurrentGeneration != region.Generation {
			return "Range placement Region cohort is not confirmed", nil
		}
	}
	if current, found, err := s.get(rangePlacementKey(placement.RangeID)); err != nil {
		return "", err
	} else if found {
		var prior RangePlacement
		if err := decodeStreamJSON(current, &prior); err != nil {
			return "", err
		}
		if prior.RangeEpoch != placement.RangeEpoch || prior.OwnerGroupID != placement.OwnerGroupID || !sameRangeRegionSet(prior.Regions, placement.Regions) {
			return "Range placement is already fixed for this Range epoch", nil
		}
		if slices.Equal(prior.Regions, placement.Regions) {
			return "", nil
		}
		// A confirmed Region cohort may advance generation after an explicit
		// retirement and same-name reformation. Refresh only those generation
		// fences; changing the Range epoch, Owner, or Region set requires a
		// separate Range-level placement transition.
		return "", setJSON(batch, rangePlacementKey(placement.RangeID), placement)
	}
	return "", setJSON(batch, rangePlacementKey(placement.RangeID), placement)
}

func sameRangeRegionSet(a, b []RangeRegionPlacement) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].RegionID != b[i].RegionID || a[i].GroupID != b[i].GroupID {
			return false
		}
	}
	return true
}
