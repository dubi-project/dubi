package recordwrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
)

// EnsureCurrentLocalConfirmation refreshes a published reference after cohort
// growth or delayed physical repair. It does not mutate module payloads. A
// current receipt is left untouched; reads still recheck live physical data.
// Managed Nodes call this only from the current Owner leader to avoid every
// member emitting an independent confirmation command for the same cut.
func (w Writer) EnsureCurrentLocalConfirmation(ctx context.Context, ref capstate.RecordReference) (bool, error) {
	if ctx == nil || w.Barrier == nil || w.Root == nil || w.Owner == nil || w.Store == nil ||
		w.Observer == nil || w.Submit == nil || w.LocalID == 0 || w.LocalNodeID == "" ||
		ref.OwnerGroupID != w.Owner.GroupID() ||
		ref.Descriptor.Validate() != nil || ref.Index == 0 || ref.RequestID == "" {
		return false, errors.New("local reconfirmation requires a current Owner reference and complete Node runtime")
	}
	target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID,
		OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: bytes.Clone(ref.RoutingKey)}
	if err := target.Validate(); err != nil {
		return false, err
	}
	plan, err := placement.ReadLocalPlan(ctx, w.Barrier, w.Root, ref.OwnerGroupID)
	if err != nil {
		return false, err
	}
	id, err := ref.Descriptor.ID()
	if err != nil {
		return false, err
	}
	var receipt capstate.LocalConfirmationRecord
	var found bool
	err = readOwner(ctx, w.Barrier, w.Owner, target, func(view capstate.ReadView) error {
		current, exists, readErr := view.RecordReference(ref.Descriptor.Key)
		if readErr != nil {
			return readErr
		}
		if !exists || current.Index != ref.Index || current.RequestID != ref.RequestID ||
			current.CommandHash != ref.CommandHash || !reflect.DeepEqual(current.Descriptor, ref.Descriptor) ||
			!bytes.Equal(current.RoutingKey, ref.RoutingKey) {
			return errors.New("record reference changed before local reconfirmation")
		}
		receipt, found, readErr = view.LocalConfirmation(ref.Descriptor.Key)
		return readErr
	})
	if err != nil {
		return false, err
	}
	if found && receipt.Claim.ReferenceRequestID == ref.RequestID &&
		receipt.Claim.ReferenceIndex == ref.Index && receipt.Claim.RecordID == id &&
		receipt.Claim.RegionID == plan.RegionID && receipt.Claim.Generation == plan.Generation &&
		receipt.Claim.GroupEpoch == plan.GroupEpoch && receipt.Claim.FenceIndex == plan.FenceIndex &&
		matchingPlanNodes(plan, receipt.Claim.Nodes) {
		return false, nil
	}
	observed, err := ObservePublishedLocal(ctx, w.Barrier, w.Root, w.Owner, w.LocalID, w.LocalNodeID,
		w.Store, w.Observer, target, ref.Descriptor, ref.RequestID, ref.Index)
	if err != nil {
		return false, err
	}
	if !plan.Matches(observed.Plan) {
		return false, errors.New("Region plan changed before local reconfirmation")
	}
	claim, err := ConfirmationClaim(target, observed)
	if err != nil {
		return false, err
	}
	confirmID, err := randomConfirmationID()
	if err != nil {
		return false, err
	}
	command, err := routedstate.EncodeLocalConfirmation(confirmID, claim)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := w.Submit(ctx, target.OwnerGroupID, command); err != nil {
		return false, fmt.Errorf("submit repaired local confirmation: %w", err)
	}
	result, err := w.awaitResult(ctx, target.OwnerGroupID, func() (capstate.Result, bool, error) {
		return w.Owner.ResultForLocalConfirmation(confirmID, claim)
	})
	if err != nil {
		return false, err
	}
	if !result.Applied || result.Error != "" {
		return false, fmt.Errorf("repaired local confirmation rejected: %s", result.Error)
	}
	_, err = CheckCommittedLocal(ctx, w.Barrier, w.Root, w.Owner, w.LocalID, w.LocalNodeID,
		w.Store, w.Observer, target, ref.Descriptor, ref.RequestID, ref.Index, confirmID, claim)
	return err == nil, err
}

func matchingPlanNodes(plan placement.LocalPlan, ids []uint64) bool {
	nodes := plan.Nodes()
	if len(nodes) != len(ids) {
		return false
	}
	want := make([]uint64, 0, len(nodes))
	for _, node := range nodes {
		want = append(want, node.RaftID)
	}
	return slices.Equal(want, ids)
}
