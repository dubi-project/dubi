package placement

import "errors"

type WriteMode string

const (
	// LocalFirst returns after the local Region data majority. Cross-Region
	// repair continues separately; this mode has an explicit LocalReady window.
	WriteLocalFirst WriteMode = "local_first"
	// GeoSynchronous requires a Region majority before the write may return.
	WriteGeoSynchronous WriteMode = "geo_synchronous"
)

// WritePolicy is derived from durable confirmed topology, never from current
// online counts. It is a policy decision only; the selected Writer still needs
// a concrete GeoPlan and fresh physical evidence.
type WritePolicy struct {
	Mode            WriteMode
	LocalNodes      int
	ActiveRegions   int
	RequiredRegions int
}

// SelectWritePolicy implements the foundation rule:
//   - at least three confirmed Nodes in the local Region: local majority first;
//   - fewer than three: synchronous majority across Active Regions;
//   - no cross-Region majority available: fail closed.
func SelectWritePolicy(localNodes, activeRegions int) (WritePolicy, error) {
	if localNodes < 0 || activeRegions < 0 {
		return WritePolicy{}, errors.New("write policy topology counts cannot be negative")
	}
	if localNodes >= 3 {
		return WritePolicy{Mode: WriteLocalFirst, LocalNodes: localNodes, ActiveRegions: activeRegions}, nil
	}
	if activeRegions < 2 {
		return WritePolicy{}, errors.New("small Region requires at least two confirmed Active Regions")
	}
	return WritePolicy{Mode: WriteGeoSynchronous, LocalNodes: localNodes, ActiveRegions: activeRegions,
		RequiredRegions: activeRegions/2 + 1}, nil
}

func (p WritePolicy) AllowsLocalReady() bool { return p.Mode == WriteLocalFirst }

func (p WritePolicy) RequiresGeoReady() bool { return p.Mode == WriteGeoSynchronous }
