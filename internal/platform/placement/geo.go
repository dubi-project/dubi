package placement

import (
	"encoding/hex"
	"errors"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

// GeoPlan fixes an explicitly selected set of confirmed Regions, including
// one- and two-Node Regions. Small Regions are never treated as local-ready;
// the write policy must require the resulting Region majority to be combined
// with a cross-Region majority before GeoReady is returned.
type GeoPlan struct {
	regions []LocalPlan
}

func PlanGeo(view rootstate.RegionPlacementView) (GeoPlan, error) {
	if view.AppliedIndex == 0 || len(view.Regions) == 0 {
		return GeoPlan{}, errors.New("geo placement requires an applied Root cut and Regions")
	}
	plan := GeoPlan{}
	seenRegions, seenGroups := make(map[string]bool), make(map[string]bool)
	seenNodes, seenRaft := make(map[string]bool), make(map[uint64]bool)
	for _, region := range view.Regions {
		if region.Progress.AppliedIndex != view.AppliedIndex {
			return GeoPlan{}, errors.New("geo placement mixes Root applied cuts")
		}
		local, err := planRegion(region.Progress, region.Nodes)
		if err != nil {
			return GeoPlan{}, err
		}
		if seenRegions[local.RegionID] || seenGroups[local.GroupID] {
			return GeoPlan{}, errors.New("geo placement repeats a Region or Group")
		}
		seenRegions[local.RegionID], seenGroups[local.GroupID] = true, true
		for _, node := range local.nodes {
			if seenNodes[node.NodeID] || seenRaft[node.RaftID] {
				return GeoPlan{}, errors.New("geo placement repeats a Node across Regions")
			}
			seenNodes[node.NodeID], seenRaft[node.RaftID] = true, true
		}
		plan.regions = append(plan.regions, local)
	}
	slices.SortFunc(plan.regions, func(a, b LocalPlan) int {
		if a.RegionID < b.RegionID {
			return -1
		}
		if a.RegionID > b.RegionID {
			return 1
		}
		return 0
	})
	return plan, nil
}

func (p GeoPlan) Regions() []LocalPlan {
	regions := slices.Clone(p.regions)
	for i := range regions {
		regions[i].nodes = slices.Clone(regions[i].nodes)
	}
	return regions
}

func (p GeoPlan) RequiredRegions() int { return len(p.regions)/2 + 1 }

func (p GeoPlan) Matches(other GeoPlan) bool {
	if len(p.regions) == 0 || len(p.regions) != len(other.regions) {
		return false
	}
	for i := range p.regions {
		if !p.regions[i].Matches(other.regions[i]) {
			return false
		}
	}
	return true
}

// RegionHoldings binds authenticated physical observations to the exact Region
// generation, Group epoch, and fence; neither a Volume nor a Region gets an
// extra vote by submitting repeated evidence.
type RegionHoldings struct {
	RegionID   string
	Generation uint64
	GroupEpoch uint64
	FenceIndex uint64
	Nodes      []uint64
	Holdings   []Holding
}

// GeoDataCoverage is a physical observation, NOT GeoReady or a durable receipt.
// Majority alone neither proves the writer's local threshold nor recoverable
// authority metadata. Missing Regions remain in RequiredRegions.
type GeoDataCoverage struct {
	RecordID             string
	ReferenceRequestID   string
	ReferenceIndex       uint64
	ReferenceCommandHash string
	RangeID              string
	RangeEpoch           uint64
	RoutingKey           []byte
	RequiredRegions      int
	CoveredRegions       []string
}

// PublicationCut is the minimum authoritative metadata needed to bind a
// physical cross-Region observation. It intentionally carries no Store or
// Volume location; those are evidence, not logical metadata.
type PublicationCut struct {
	RecordID    string
	RequestID   string
	Index       uint64
	CommandHash string
	RangeID     string
	RangeEpoch  uint64
	RoutingKey  []byte
}

func (p PublicationCut) Validate() error {
	if !validRecordID(p.RecordID) || p.RequestID == "" || p.Index == 0 ||
		len(p.CommandHash) != 64 || p.RangeEpoch == 0 || len(p.RoutingKey) > 4096 ||
		rootstate.ValidateRequestID(p.RequestID) != nil || rootstate.ValidateRangeID(p.RangeID) != nil {
		return errors.New("invalid geo publication cut")
	}
	decoded, err := hex.DecodeString(p.CommandHash)
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != p.CommandHash {
		return errors.New("invalid geo publication command hash")
	}
	return nil
}

func validRecordID(value string) bool {
	const prefix = "record_"
	if len(value) != len(prefix)+64 || value[:len(prefix)] != prefix {
		return false
	}
	decoded, err := hex.DecodeString(value[len(prefix):])
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value[len(prefix):]
}

func (c GeoDataCoverage) HasMajority() bool {
	return c.RecordID != "" && c.RequiredRegions > 0 && len(c.CoveredRegions) >= c.RequiredRegions
}

// HasBoundPublication is the stricter form required before a future durable
// GeoReady receipt may be published. Physical coverage without this binding is
// only an observation and cannot survive reference replacement safely.
func (c GeoDataCoverage) HasBoundPublication() bool {
	return c.HasMajority() && c.ReferenceRequestID != "" && c.ReferenceIndex != 0 &&
		c.ReferenceCommandHash != "" && c.RangeID != "" && c.RangeEpoch != 0
}

func (p GeoPlan) Assess(recordID string, evidence []RegionHoldings) (GeoDataCoverage, error) {
	return p.assess(recordID, PublicationCut{}, evidence)
}

// AssessPublication binds the physical Region evidence to one exact current
// logical publication. It still returns an observation, not a persisted
// GeoReady receipt; the Owner Group must later atomically record and recheck it.
func (p GeoPlan) AssessPublication(publication PublicationCut, evidence []RegionHoldings) (GeoDataCoverage, error) {
	if err := publication.Validate(); err != nil {
		return GeoDataCoverage{}, err
	}
	return p.assess(publication.RecordID, publication, evidence)
}

func (p GeoPlan) assess(recordID string, publication PublicationCut, evidence []RegionHoldings) (GeoDataCoverage, error) {
	if recordID == "" || len(p.regions) == 0 {
		return GeoDataCoverage{}, errors.New("invalid geo placement or RecordID")
	}
	known := make(map[string]LocalPlan, len(p.regions))
	for _, region := range p.regions {
		known[region.RegionID] = region
	}
	seen, stores := make(map[string]bool), make(map[string]bool)
	coverage := GeoDataCoverage{RecordID: recordID, RequiredRegions: p.RequiredRegions(),
		ReferenceRequestID: publication.RequestID, ReferenceIndex: publication.Index,
		ReferenceCommandHash: publication.CommandHash, RangeID: publication.RangeID,
		RangeEpoch: publication.RangeEpoch, RoutingKey: slices.Clone(publication.RoutingKey)}
	for _, observed := range evidence {
		region, found := known[observed.RegionID]
		if !found || seen[observed.RegionID] || observed.Generation != region.Generation ||
			observed.GroupEpoch != region.GroupEpoch || observed.FenceIndex != region.FenceIndex ||
			!slices.Equal(observed.Nodes, nodeIDs(region.nodes)) {
			return GeoDataCoverage{}, errors.New("geo evidence differs from confirmed Region cut")
		}
		seen[observed.RegionID] = true
		count, err := region.Count(recordID, observed.Holdings)
		if err != nil {
			return GeoDataCoverage{}, err
		}
		for _, holding := range observed.Holdings {
			if stores[holding.StoreID] {
				return GeoDataCoverage{}, errors.New("geo evidence repeats a Store across Regions")
			}
			stores[holding.StoreID] = true
		}
		if count >= region.Required {
			coverage.CoveredRegions = append(coverage.CoveredRegions, region.RegionID)
		}
	}
	slices.Sort(coverage.CoveredRegions)
	return coverage, nil
}

func nodeIDs(nodes []rootstate.Node) []uint64 {
	ids := make([]uint64, len(nodes))
	for i, node := range nodes {
		ids[i] = node.RaftID
	}
	return ids
}
