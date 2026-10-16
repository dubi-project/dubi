package recordwrite

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// ConfirmationClaim converts a fresh post-publication physical observation
// into a deterministic Owner Group command. It does not submit or claim that
// the command was applied; the caller must check the persisted result and
// re-read Root before exposing a completed durability level.
func ConfirmationClaim(target router.Target, observed PublishedObservation) (routedstate.LocalConfirmation, error) {
	ref := observed.Reference
	plan := observed.Plan
	id, err := ref.Descriptor.ID()
	if err != nil || target.Validate() != nil || target.Keyspace != ref.Descriptor.Keyspace ||
		target.OwnerGroupID != ref.OwnerGroupID || target.RangeID != ref.RangeID || target.Epoch != ref.RangeEpoch ||
		!bytes.Equal(target.RoutingKey, ref.RoutingKey) ||
		plan.GroupID != target.OwnerGroupID || plan.RegionID == "" || plan.Generation == 0 ||
		plan.GroupEpoch < 2 || plan.FenceIndex == 0 {
		return routedstate.LocalConfirmation{}, errors.New("observed publication differs from its Owner route or Region plan")
	}
	count, err := plan.Count(id, observed.Holdings)
	if err != nil || count < plan.Required {
		return routedstate.LocalConfirmation{}, errors.New("observed publication lacks an independent stable majority")
	}
	nodes := plan.Nodes()
	target.RoutingKey = bytes.Clone(target.RoutingKey)
	claim := routedstate.LocalConfirmation{Target: target, Key: bytes.Clone(ref.Descriptor.Key),
		ReferenceRequestID: ref.RequestID, ReferenceIndex: ref.Index, RecordID: id,
		RegionID: plan.RegionID, Generation: plan.Generation, GroupEpoch: plan.GroupEpoch,
		FenceIndex: plan.FenceIndex, Holdings: slices.Clone(observed.Holdings)}
	for _, node := range nodes {
		claim.Nodes = append(claim.Nodes, node.RaftID)
	}
	if err := claim.Validate(); err != nil {
		return routedstate.LocalConfirmation{}, err
	}
	return claim, nil
}

// CheckCommittedLocal is the final internal durability gate after a local
// confirmation command has committed. It rechecks physical majority, the
// exact current reference, the persisted confirmation Result, and the Root
// cohort. The result describes this check, not a lease against future disk
// loss or a public Tenant read/write endpoint.
func CheckCommittedLocal(ctx context.Context, barrier Barrier, root Root, owner *enginestate.State,
	localID uint64, localNodeID string, store Verifier, observer Observer, target router.Target,
	descriptor records.Descriptor, referenceRequestID string, referenceIndex uint64,
	confirmationRequestID string, claim routedstate.LocalConfirmation) (PublishedObservation, error) {
	if claim.Validate() != nil || rootstate.ValidateRequestID(confirmationRequestID) != nil ||
		claim.ReferenceRequestID != referenceRequestID || claim.ReferenceIndex != referenceIndex {
		return PublishedObservation{}, errors.New("invalid committed local confirmation identity")
	}
	observed, err := ObservePublishedLocal(ctx, barrier, root, owner, localID, localNodeID,
		store, observer, target, descriptor, referenceRequestID, referenceIndex)
	if err != nil {
		return PublishedObservation{}, err
	}
	fresh, err := ConfirmationClaim(target, observed)
	if err != nil {
		return PublishedObservation{}, err
	}
	if claim.RegionID != fresh.RegionID || claim.Generation != fresh.Generation ||
		claim.GroupEpoch != fresh.GroupEpoch || claim.FenceIndex != fresh.FenceIndex ||
		!slices.Equal(claim.Nodes, fresh.Nodes) || claim.RecordID != fresh.RecordID {
		return PublishedObservation{}, errors.New("committed local confirmation is for an obsolete Region plan or publication")
	}
	err = readOwner(ctx, barrier, owner, target, func(view enginestate.ReadView) error {
		receipt, found, readErr := view.LocalConfirmation(descriptor.Key)
		if readErr != nil {
			return readErr
		}
		if !found || receipt.RequestID != confirmationRequestID || !reflect.DeepEqual(receipt.Claim, claim) {
			return errors.New("exact local confirmation is not current in Owner Group")
		}
		return nil
	})
	if err != nil {
		return PublishedObservation{}, err
	}
	latest, err := placement.ReadLocalPlan(ctx, barrier, root, target.OwnerGroupID)
	if err != nil {
		return PublishedObservation{}, err
	}
	if !observed.Plan.Matches(latest) {
		return PublishedObservation{}, errors.New("Root Region plan changed after local confirmation read")
	}
	return observed, nil
}
