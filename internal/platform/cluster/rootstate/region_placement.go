package rootstate

import "errors"

// RegionPlacementView captures explicitly selected regional Groups and their
// registered participants under one Root lock. Selection must come from a
// durable policy, never an online-node filter. It is not an Active Region
// policy and does not prove physical data or recoverable metadata coverage.
type RegionPlacementView struct {
	AppliedIndex uint64
	Regions      []RegionPlacement
}

type RegionPlacement struct {
	Progress RegionProgress
	Nodes    []Node
}

// The caller supplies allocation bounds, not a deployment replica count.
// Missing, uncertain, unconfirmed, or nonmember participants fail closed.
func (s *State) RegionPlacementView(groups []string, maxRegions, maxNodes int) (RegionPlacementView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return RegionPlacementView{}, err
	}
	return s.regionPlacementView(groups, maxRegions, maxNodes)
}

// Caller holds s.mu and has checked the state fence.
func (s *State) regionPlacementView(groups []string, maxRegions, maxNodes int) (RegionPlacementView, error) {
	if len(groups) == 0 || maxRegions <= 0 || len(groups) > maxRegions || maxNodes <= 0 {
		return RegionPlacementView{}, errors.New("invalid regional placement read bounds")
	}
	view := RegionPlacementView{AppliedIndex: s.applied}
	seen := make(map[string]bool)
	total := 0
	for _, group := range groups {
		progress, found, err := s.regionProgress(group)
		if err != nil {
			return RegionPlacementView{}, err
		}
		if !found || seen[progress.Current.RegionID] {
			return RegionPlacementView{}, errors.New("regional placement requires distinct confirmed Regions")
		}
		seen[progress.Current.RegionID] = true
		if len(progress.Current.Nodes) > maxNodes-total {
			return RegionPlacementView{}, errors.New("regional placement exceeds participant bound")
		}
		total += len(progress.Current.Nodes)
		region := RegionPlacement{Progress: progress}
		for _, id := range progress.Current.Nodes {
			node, member, err := s.rootMember(id)
			if err != nil {
				return RegionPlacementView{}, err
			}
			if !member {
				return RegionPlacementView{}, errors.New("regional placement participant is not a current Root member")
			}
			region.Nodes = append(region.Nodes, node)
		}
		view.Regions = append(view.Regions, region)
	}
	return view, nil
}
