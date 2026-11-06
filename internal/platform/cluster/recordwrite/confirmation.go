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

// errStaleLocalCohort is refreshable only after a fresh physical majority has
// proved the same publication under the current cohort. Other read failures
// must not initiate confirmation work.
var errStaleLocalCohort = errors.New("committed local confirmation is for an obsolete Region plan or publication")

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

// CheckCommittedLocal revalidates a previously committed local confirmation
// before a retry reuses it. It checks physical majority, the exact current
// reference, the persisted confirmation Result, and the Root cohort. A new
// foreground write returns from its just-committed Owner confirmation without
// repeating this cross-node observation. Neither result is a lease against
// future disk loss or a public Tenant read/write endpoint.
func CheckCommittedLocal(ctx context.Context, barrier Barrier, root Root, owner *enginestate.State,
	localID uint64, localNodeID string, store Verifier, observer Observer, target router.Target,
	descriptor records.Descriptor, referenceRequestID string, referenceIndex uint64,
	confirmationRequestID string, claim routedstate.LocalConfirmation) (PublishedObservation, error) {
	observed, err := observeCommittedLocal(ctx, barrier, root, owner, localID, localNodeID,
		store, observer, target, descriptor, referenceRequestID, referenceIndex, confirmationRequestID, claim)
	if err != nil {
		return PublishedObservation{}, err
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

// observeCommittedLocal establishes a fresh physical majority and verifies
// that the persisted claim still describes that exact publication and plan.
// Durability retries use the full Root check here. Reader instead defers that
// check to CheckReadCurrent after reading bytes and verifying physical holdings.
func observeCommittedLocal(ctx context.Context, barrier Barrier, root Root, owner *enginestate.State,
	localID uint64, localNodeID string, store Verifier, observer Observer, target router.Target,
	descriptor records.Descriptor, referenceRequestID string, referenceIndex uint64,
	confirmationRequestID string, claim routedstate.LocalConfirmation) (PublishedObservation, error) {
	return observeCommitted(ctx, barrier, root, owner, localID, localNodeID, store, observer, target,
		descriptor, referenceRequestID, referenceIndex, confirmationRequestID, claim, false)
}

func observeCommitted(ctx context.Context, barrier Barrier, root Root, owner *enginestate.State,
	localID uint64, localNodeID string, store Verifier, observer Observer, target router.Target,
	descriptor records.Descriptor, referenceRequestID string, referenceIndex uint64,
	confirmationRequestID string, claim routedstate.LocalConfirmation, deferRootCheck bool) (PublishedObservation, error) {
	if claim.Validate() != nil || rootstate.ValidateRequestID(confirmationRequestID) != nil ||
		claim.ReferenceRequestID != referenceRequestID || claim.ReferenceIndex != referenceIndex {
		return PublishedObservation{}, errors.New("invalid committed local confirmation identity")
	}
	observed, err := observePublishedLocal(ctx, barrier, root, owner, localID, localNodeID,
		store, observer, target, descriptor, referenceRequestID, referenceIndex, deferRootCheck)
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
		if claim.RegionID == fresh.RegionID && claim.RecordID == fresh.RecordID &&
			reflect.DeepEqual(claim.Target, fresh.Target) && bytes.Equal(claim.Key, fresh.Key) {
			if deferRootCheck {
				// Refresh is a rare side effect. Before allowing it, obtain the
				// full fresh observation just as the durability retry path does.
				return observeCommittedLocal(ctx, barrier, root, owner, localID, localNodeID,
					store, observer, target, descriptor, referenceRequestID, referenceIndex,
					confirmationRequestID, claim)
			}
			return PublishedObservation{}, errStaleLocalCohort
		}
		return PublishedObservation{}, errors.New("committed local confirmation is for an obsolete Region plan or publication")
	}
	return observed, nil
}

// CheckReadCurrent rechecks the logical publication and its Root cohort after
// the immutable bytes have been read and verified. The earlier physical
// majority check establishes the expected plan; this final check prevents
// returning bytes after a concurrent Owner overwrite or Region transition
// without repeating the same remote holding scan.
func CheckReadCurrent(ctx context.Context, barrier Barrier, root Root, owner *enginestate.State,
	target router.Target, expected enginestate.RecordReference, confirmationRequestID string,
	claim routedstate.LocalConfirmation, expectedPlan placement.LocalPlan) (placement.LocalPlan, error) {
	recordID, descriptorErr := expected.Descriptor.ID()
	if ctx == nil || barrier == nil || root == nil || owner == nil || target.Validate() != nil ||
		owner.GroupID() != target.OwnerGroupID || claim.Validate() != nil ||
		rootstate.ValidateRequestID(confirmationRequestID) != nil || rootstate.ValidateRequestID(expected.RequestID) != nil ||
		expected.Index == 0 || descriptorErr != nil || expected.Descriptor.Validate() != nil ||
		expected.OwnerGroupID != target.OwnerGroupID || expected.RangeID != target.RangeID || expected.RangeEpoch != target.Epoch ||
		claim.Target.Keyspace != target.Keyspace || claim.Target.RangeID != target.RangeID ||
		claim.Target.OwnerGroupID != target.OwnerGroupID || claim.Target.Epoch != target.Epoch ||
		!bytes.Equal(claim.Target.RoutingKey, target.RoutingKey) || !bytes.Equal(claim.Key, expected.Descriptor.Key) ||
		claim.ReferenceRequestID != expected.RequestID || claim.ReferenceIndex != expected.Index || claim.RecordID != recordID ||
		expectedPlan.GroupID != target.OwnerGroupID || claim.RegionID != expectedPlan.RegionID ||
		claim.Generation != expectedPlan.Generation || claim.GroupEpoch != expectedPlan.GroupEpoch ||
		claim.FenceIndex != expectedPlan.FenceIndex {
		return placement.LocalPlan{}, errors.New("final read check requires a valid Owner publication and Root cohort")
	}
	err := readOwner(ctx, barrier, owner, target, func(view enginestate.ReadView) error {
		ref, found, readErr := view.RecordReference(expected.Descriptor.Key)
		if readErr != nil {
			return readErr
		}
		if !found || ref.Index != expected.Index || ref.RequestID != expected.RequestID ||
			ref.CommandHash != expected.CommandHash || ref.OwnerGroupID != expected.OwnerGroupID ||
			ref.RangeID != expected.RangeID || ref.RangeEpoch != expected.RangeEpoch ||
			!bytes.Equal(ref.Descriptor.Key, expected.Descriptor.Key) {
			return errors.New("authoritative record publication changed during read")
		}
		receipt, found, readErr := view.LocalConfirmation(expected.Descriptor.Key)
		if readErr != nil {
			return readErr
		}
		if !found || receipt.RequestID != confirmationRequestID || !reflect.DeepEqual(receipt.Claim, claim) {
			return errors.New("exact local confirmation changed during read")
		}
		return nil
	})
	if err != nil {
		return placement.LocalPlan{}, err
	}
	latest, err := placement.ReadLocalPlan(ctx, barrier, root, target.OwnerGroupID)
	if err != nil {
		return placement.LocalPlan{}, err
	}
	if !expectedPlan.Matches(latest) {
		return placement.LocalPlan{}, errors.New("Root Region plan changed during final read check")
	}
	return latest, nil
}
