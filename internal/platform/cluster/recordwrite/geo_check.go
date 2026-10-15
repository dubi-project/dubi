package recordwrite

import (
	"bytes"
	"context"
	"errors"
	"reflect"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// CheckCommittedGeo is the final internal gate for a persisted cross-Region
// coverage account. The caller supplies a fresh Root-derived GeoPlan and direct
// physical observations; stale evidence or a changed Owner reference fails.
// The returned coverage is still an internal observation, not a public lease.
func CheckCommittedGeo(ctx context.Context, barrier Barrier, owner *capstate.State,
	target router.Target, descriptor records.Descriptor, publication placement.PublicationCut,
	plan placement.GeoPlan, evidence []placement.RegionHoldings,
	confirmationRequestID string, claim routedstate.GeoConfirmation) (placement.GeoDataCoverage, error) {
	if ctx == nil || barrier == nil || owner == nil || target.Validate() != nil || descriptor.Validate() != nil ||
		target.Keyspace != descriptor.Keyspace || !bytes.Equal(target.RoutingKey, publication.RoutingKey) ||
		rootstate.ValidateRequestID(confirmationRequestID) != nil || claim.Validate() != nil ||
		claim.ReferenceRequestID != publication.RequestID || claim.ReferenceIndex != publication.Index {
		return placement.GeoDataCoverage{}, errors.New("invalid committed geo coverage identity")
	}
	recordID, err := descriptor.ID()
	if err != nil || publication.RecordID != recordID || publication.RangeID != target.RangeID || publication.RangeEpoch != target.Epoch {
		return placement.GeoDataCoverage{}, errors.New("geo publication differs from Owner route or descriptor")
	}
	coverage, err := plan.AssessPublication(publication, evidence)
	if err != nil {
		return placement.GeoDataCoverage{}, err
	}
	fresh, err := GeoConfirmationClaim(target, descriptor.Key, coverage, evidence)
	if err != nil || !reflect.DeepEqual(fresh, claim) {
		return placement.GeoDataCoverage{}, errors.New("committed geo coverage is for an obsolete Region plan or publication")
	}
	err = readOwner(ctx, barrier, owner, target, func(view capstate.ReadView) error {
		ref, found, readErr := view.RecordReference(descriptor.Key)
		if readErr != nil || !found || ref.RequestID != publication.RequestID || ref.Index != publication.Index ||
			ref.CommandHash != publication.CommandHash || ref.RangeID != publication.RangeID || ref.RangeEpoch != publication.RangeEpoch ||
			!bytes.Equal(ref.RoutingKey, publication.RoutingKey) {
			return errors.New("exact authoritative record publication is not current")
		}
		receipt, found, readErr := view.GeoCoverage(descriptor.Key)
		if readErr != nil {
			return readErr
		}
		if !found || receipt.RequestID != confirmationRequestID || !reflect.DeepEqual(receipt.Claim, claim) {
			return errors.New("exact geo coverage is not current in Owner Group")
		}
		return nil
	})
	if err != nil {
		return placement.GeoDataCoverage{}, err
	}
	return coverage, nil
}
