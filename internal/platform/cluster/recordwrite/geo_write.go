package recordwrite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// GeoWriteRequest opts into synchronous cross-Region staging. Ordinary
// WriteLocal remains local-majority-first; callers must explicitly provide a
// fresh GeoPlan when they need a GeoCoverage account.
type GeoWriteRequest struct {
	RequestID  string
	Target     router.Target
	Descriptor records.Descriptor
	Body       []byte
	Payload    []byte
	Plan       placement.GeoPlan
}

type GeoWrite struct {
	Reference         enginestate.RecordReference
	ConfirmationIndex uint64
	RegionGeneration  uint64
	RequiredRegions   int
}

type rangeGeoRoot interface {
	Root
	placement.RangePlacementReader
}

// WriteGeoFromRange obtains the durable Range-to-Region mapping at a Root
// read cut and then executes the explicit synchronous Geo write. It fails when
// the mapping has not been published; it never guesses sibling Groups from
// the current online process set.
func (w Writer) WriteGeoFromRange(ctx context.Context, request WriteRequest, maxRegions, maxNodes int) (GeoWrite, error) {
	root, ok := w.Root.(rangeGeoRoot)
	if !ok {
		return GeoWrite{}, errors.New("Root does not expose durable Range Region placement")
	}
	if request.Target.RangeID == "" {
		return GeoWrite{}, errors.New("geo write requires a Range")
	}
	_, plan, err := placement.ReadRangeGeoPlan(ctx, w.Barrier, root, request.Target.RangeID, maxRegions, maxNodes)
	if err != nil {
		return GeoWrite{}, err
	}
	return w.WriteGeo(ctx, GeoWriteRequest{RequestID: request.RequestID, Target: request.Target,
		Descriptor: request.Descriptor, Body: request.Body, Payload: request.Payload, Plan: plan})
}

// WriteGeo stages physical bytes in every planned Region before publishing the
// logical reference, then persists and rechecks a GeoCoverage account. It is
// intentionally opt-in so the default local-first write semantics are not
// silently changed while cross-Region Repair/metadata checkpointing is still
// being completed.
func (w Writer) WriteGeo(ctx context.Context, request GeoWriteRequest) (GeoWrite, error) {
	if ctx == nil || w.Barrier == nil || w.Owner == nil || w.Store == nil || w.Sender == nil || w.Submit == nil || w.MirrorGeo == nil ||
		rootstate.ValidateRequestID(request.RequestID) != nil || request.Target.Validate() != nil ||
		request.Descriptor.Validate() != nil || request.Target.Keyspace != request.Descriptor.Keyspace ||
		w.Owner.GroupID() != request.Target.OwnerGroupID {
		return GeoWrite{}, errors.New("geo write requires an Owner, metadata mirror transport, stable RequestID and complete geo request")
	}
	mutation, err := routedstate.EncodeMutation(request.RequestID, request.Target, request.Payload)
	if err != nil {
		return GeoWrite{}, err
	}
	publication, found, err := w.resultAtCut(ctx, request.Target.OwnerGroupID, func() (enginestate.Result, bool, error) {
		return w.Owner.ResultForMutation(request.RequestID, request.Target, request.Payload)
	})
	if err != nil {
		return GeoWrite{}, err
	}
	if found && (!publication.Applied || publication.Error != "") {
		return GeoWrite{}, fmt.Errorf("geo immutable reference publication rejected: %s", publication.Error)
	}
	staged, err := StageGeo(ctx, request.Plan, w.LocalID, w.LocalNodeID, w.Store, w.Sender, request.Descriptor, request.Body)
	if err != nil {
		return GeoWrite{}, err
	}
	if !found {
		if err := w.Submit(ctx, request.Target.OwnerGroupID, mutation); err != nil {
			return GeoWrite{}, fmt.Errorf("submit geo immutable reference publication: %w", err)
		}
		publication, err = w.awaitResult(ctx, request.Target.OwnerGroupID, func() (enginestate.Result, bool, error) {
			return w.Owner.ResultForMutation(request.RequestID, request.Target, request.Payload)
		})
		if err != nil {
			return GeoWrite{}, err
		}
	}
	if !publication.Applied || publication.Error != "" || publication.Index == 0 {
		return GeoWrite{}, fmt.Errorf("geo immutable reference publication rejected: %s", publication.Error)
	}
	recordID, err := request.Descriptor.ID()
	if err != nil {
		return GeoWrite{}, err
	}
	ref, err := publishedReference(ctx, w.Barrier, w.Owner, request.Target, request.Descriptor.Key,
		recordID, request.RequestID, publication.Index)
	if err != nil {
		return GeoWrite{}, err
	}
	cut := placement.PublicationCut{RecordID: recordID, RequestID: ref.RequestID, Index: ref.Index,
		CommandHash: ref.CommandHash, RangeID: ref.RangeID, RangeEpoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
	coverage, err := request.Plan.AssessPublication(cut, staged.Evidence)
	if err != nil {
		return GeoWrite{}, err
	}
	mirrors, err := GeoMetadataMirrors(request.Target, request.Descriptor, cut, request.Plan, staged.Evidence)
	if err != nil {
		return GeoWrite{}, err
	}
	if _, err := ReplicateGeoMetadataWithRetryStore(ctx, w.MirrorGeo, request.Plan, mirrors, w.MirrorRetry); err != nil {
		return GeoWrite{}, fmt.Errorf("replicate geo metadata mirrors: %w", err)
	}
	claim, err := GeoConfirmationClaim(request.Target, request.Descriptor.Key, coverage, staged.Evidence)
	if err != nil {
		return GeoWrite{}, err
	}
	var prior enginestate.GeoCoverageRecord
	var hasPrior bool
	if err := readOwner(ctx, w.Barrier, w.Owner, request.Target, func(view enginestate.ReadView) error {
		var readErr error
		prior, hasPrior, readErr = view.GeoCoverage(request.Descriptor.Key)
		return readErr
	}); err != nil {
		return GeoWrite{}, err
	}
	if hasPrior && prior.Claim.ReferenceRequestID == claim.ReferenceRequestID && prior.Claim.ReferenceIndex == claim.ReferenceIndex {
		if current, err := CheckCommittedGeo(ctx, w.Barrier, w.Owner, request.Target, request.Descriptor, cut,
			request.Plan, staged.Evidence, prior.RequestID, prior.Claim); err == nil {
			return GeoWrite{Reference: ref, ConfirmationIndex: prior.Index, RegionGeneration: localRegionGeneration(request.Plan, w.LocalID), RequiredRegions: current.RequiredRegions}, nil
		}
	}
	confirmID, err := randomGeoConfirmationID()
	if err != nil {
		return GeoWrite{}, err
	}
	command, err := routedstate.EncodeGeoConfirmation(confirmID, claim)
	if err != nil {
		return GeoWrite{}, err
	}
	if err := w.Submit(ctx, request.Target.OwnerGroupID, command); err != nil {
		return GeoWrite{}, fmt.Errorf("submit geo coverage confirmation: %w", err)
	}
	confirmation, err := w.awaitResult(ctx, request.Target.OwnerGroupID, func() (enginestate.Result, bool, error) {
		return w.Owner.ResultForGeoConfirmation(confirmID, claim)
	})
	if err != nil {
		return GeoWrite{}, err
	}
	if !confirmation.Applied || confirmation.Error != "" {
		return GeoWrite{}, fmt.Errorf("geo coverage confirmation rejected: %s", confirmation.Error)
	}
	current, err := CheckCommittedGeo(ctx, w.Barrier, w.Owner, request.Target, request.Descriptor, cut,
		request.Plan, staged.Evidence, confirmID, claim)
	if err != nil {
		return GeoWrite{}, err
	}
	return GeoWrite{Reference: ref, ConfirmationIndex: confirmation.Index, RegionGeneration: localRegionGeneration(request.Plan, w.LocalID), RequiredRegions: current.RequiredRegions}, nil
}

func localRegionGeneration(plan placement.GeoPlan, localID uint64) uint64 {
	for _, region := range plan.Regions() {
		for _, node := range region.Nodes() {
			if node.RaftID == localID {
				return region.Generation
			}
		}
	}
	return 0
}

func randomGeoConfirmationID() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return "geo-confirm-" + hex.EncodeToString(entropy[:]), nil
}
