package routedstate

import (
	"encoding/hex"
	"errors"
	"slices"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
)

// GeoConfirmation is an Owner Group claim that one exact publication was
// physically verified in a strict majority of explicitly selected Regions.
// Region observations are evidence, not transferable signatures; callers must
// revalidate the Root plan and storage before exposing a live GeoReady state.
type GeoConfirmation struct {
	Target               router.Target              `json:"target"`
	Key                  []byte                     `json:"key"`
	ReferenceRequestID   string                     `json:"reference_request_id"`
	ReferenceIndex       uint64                     `json:"reference_index"`
	ReferenceCommandHash string                     `json:"reference_command_hash"`
	RecordID             string                     `json:"record_id"`
	Regions              []placement.RegionHoldings `json:"regions"`
}

const MaxGeoConfirmationBytes = 4 << 20

func (c GeoConfirmation) Validate() error {
	if c.Target.Validate() != nil || len(c.Key) == 0 || len(c.Key) > 4096 ||
		rootstate.ValidateRequestID(c.ReferenceRequestID) != nil || c.ReferenceIndex == 0 ||
		!validRecordID(c.RecordID) || len(c.ReferenceCommandHash) != 64 || len(c.Regions) < 2 {
		return errors.New("invalid geo confirmation identity or Region majority")
	}
	decoded, err := hex.DecodeString(c.ReferenceCommandHash)
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != c.ReferenceCommandHash {
		return errors.New("invalid geo confirmation reference hash")
	}
	seenRegions := make(map[string]bool, len(c.Regions))
	seenNodes := make(map[uint64]bool)
	seenStores := make(map[string]bool)
	lastRegion := ""
	for _, region := range c.Regions {
		if rootstate.ValidateRangeID(region.RegionID) != nil || region.Generation == 0 || region.GroupEpoch < 2 || region.FenceIndex == 0 ||
			seenRegions[region.RegionID] || len(region.Nodes) == 0 || len(region.Holdings) < len(region.Nodes)/2+1 || len(region.Holdings) > len(region.Nodes) {
			return errors.New("invalid geo confirmation Region cut")
		}
		if lastRegion != "" && lastRegion >= region.RegionID {
			return errors.New("geo confirmation Regions must be sorted")
		}
		seenRegions[region.RegionID] = true
		lastRegion = region.RegionID
		for i, id := range region.Nodes {
			if id == 0 || (i > 0 && region.Nodes[i-1] >= id) || seenNodes[id] {
				return errors.New("geo confirmation repeats a Node")
			}
			seenNodes[id] = true
		}
		seen := make(map[uint64]bool, len(region.Holdings))
		stores := make(map[string]bool, len(region.Holdings))
		identities := make(map[string]bool, len(region.Holdings))
		for _, holding := range region.Holdings {
			if !slices.Contains(region.Nodes, holding.RaftID) || seen[holding.RaftID] ||
				!validPhysicalID(holding.NodeID, "node_") || identities[holding.NodeID] ||
				!validPhysicalID(holding.StoreID, "store_") || stores[holding.StoreID] || holding.RecordID != c.RecordID {
				return errors.New("geo confirmation has inconsistent physical holding")
			}
			seen[holding.RaftID], stores[holding.StoreID], identities[holding.NodeID] = true, true, true
			if seenStores[holding.StoreID] {
				return errors.New("geo confirmation repeats a Store across Regions")
			}
			seenStores[holding.StoreID] = true
		}
	}
	return nil
}

func validRecordID(value string) bool {
	const prefix = "record_"
	if len(value) != len(prefix)+64 || !strings.HasPrefix(value, prefix) {
		return false
	}
	decoded, err := hex.DecodeString(value[len(prefix):])
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value[len(prefix):]
}
