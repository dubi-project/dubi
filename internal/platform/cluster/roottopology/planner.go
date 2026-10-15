// Package roottopology automatically distributes Root control-plane votes.
// It does not select data placements, define data durability, or remove Nodes.
package roottopology

import (
	"cmp"
	"errors"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

// An internal control-plane bound, never an operator replica-count setting.
// Additional Nodes remain learners and independent data-placement candidates.
const MaxRootVoters = 31

var ErrJoint = errors.New("Root topology is already in joint consensus")

// DesiredVoters derives its denominator ONLY from durable admitted membership
// and immutable Region registration. It never consumes health observations.
// Retain existing voters first to avoid churn when lower random IDs join.
func DesiredVoters(view rootjoin.View) ([]uint64, error) {
	if err := view.Validate(view.ClusterID); err != nil {
		return nil, err
	}
	if len(view.VotersOutgoing) != 0 || len(view.LearnersNext) != 0 || view.AutoLeave {
		return nil, ErrJoint
	}
	byRegion := make(map[string][]rootstate.Node)
	for _, node := range view.Peers {
		byRegion[node.RegionID] = append(byRegion[node.RegionID], node)
	}
	regions := make([]string, 0, len(byRegion))
	for region := range byRegion {
		regions = append(regions, region)
		slices.SortFunc(byRegion[region], func(a, b rootstate.Node) int {
			av, bv := slices.Contains(view.Voters, a.RaftID), slices.Contains(view.Voters, b.RaftID)
			if av != bv {
				if av {
					return -1
				}
				return 1
			}
			return cmp.Compare(a.RaftID, b.RaftID)
		})
	}
	slices.Sort(regions)
	if len(regions) > MaxRootVoters {
		return nil, errors.New("Root topology exceeds supported Region count; cannot silently omit a Region")
	}
	var desired []uint64
	if len(regions) >= 3 {
		for _, region := range regions {
			desired = append(desired, byRegion[region][0].RaftID)
		}
	} else {
		count := min(len(view.Peers), MaxRootVoters)
		if count > 2 && count%2 == 0 {
			count--
		}
		// Two Regions cannot both survive arbitrary loss of the other Region
		// under one majority Raft group. Distribute votes as evenly as possible;
		// do not invent a witness, weaken quorum, or promise symmetric failover.
		for offset := 0; len(desired) < count; offset++ {
			for _, region := range regions {
				if offset < len(byRegion[region]) {
					desired = append(desired, byRegion[region][offset].RaftID)
					if len(desired) == count {
						break
					}
				}
			}
		}
	}
	slices.Sort(desired)
	return desired, nil
}
