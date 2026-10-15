package recordwrite

import (
	"bytes"
	"errors"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
)

// GeoConfirmationClaim converts a completed placement observation into the
// deterministic Owner Group command payload. The caller must have obtained
// coverage from GeoPlan.AssessPublication; this function does not turn a
// partial or unbound observation into a GeoReady claim.
func GeoConfirmationClaim(target router.Target, key []byte, coverage placement.GeoDataCoverage,
	evidence []placement.RegionHoldings) (routedstate.GeoConfirmation, error) {
	if target.Validate() != nil || len(key) == 0 || !coverage.HasBoundPublication() ||
		coverage.RangeID != target.RangeID || coverage.RangeEpoch != target.Epoch ||
		!bytes.Equal(coverage.RoutingKey, target.RoutingKey) {
		return routedstate.GeoConfirmation{}, errors.New("geo coverage differs from Owner route or publication")
	}
	covered := make(map[string]bool, len(coverage.CoveredRegions))
	for _, region := range coverage.CoveredRegions {
		covered[region] = true
	}
	regions := make([]placement.RegionHoldings, 0, len(coverage.CoveredRegions))
	for _, observed := range evidence {
		if covered[observed.RegionID] {
			regions = append(regions, observed)
		}
	}
	slices.SortFunc(regions, func(a, b placement.RegionHoldings) int {
		if a.RegionID < b.RegionID {
			return -1
		}
		if a.RegionID > b.RegionID {
			return 1
		}
		return 0
	})
	if len(regions) != len(covered) || len(regions) < coverage.RequiredRegions {
		return routedstate.GeoConfirmation{}, errors.New("geo coverage evidence is incomplete")
	}
	claim := routedstate.GeoConfirmation{Target: target, Key: slices.Clone(key),
		ReferenceRequestID: coverage.ReferenceRequestID, ReferenceIndex: coverage.ReferenceIndex,
		ReferenceCommandHash: coverage.ReferenceCommandHash, RecordID: coverage.RecordID, Regions: regions}
	if err := claim.Validate(); err != nil {
		return routedstate.GeoConfirmation{}, err
	}
	return claim, nil
}
