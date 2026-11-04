package recordwrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
)

// PendingLocalConfirmations checks one bounded repair page with one Owner
// read barrier and two Root placement checks. Current receipts need no new
// confirmation command. This is a metadata check, not a physical majority
// proof: callers must verify local bytes separately, and use the complete
// EnsureCurrentLocalConfirmation protocol for each returned reference.
func (w Writer) PendingLocalConfirmations(ctx context.Context, refs []enginestate.RecordReference) ([]enginestate.RecordReference, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	if ctx == nil || w.Barrier == nil || w.Root == nil || w.Owner == nil || len(refs) > enginestate.MaxLiveReferencePage {
		return nil, errors.New("local confirmation page requires a bounded page, Owner, Root and barrier")
	}
	plan, err := placement.ReadLocalPlan(ctx, w.Barrier, w.Root, w.Owner.GroupID())
	if err != nil {
		return nil, fmt.Errorf("confirm repair page placement: %w", err)
	}
	index, err := w.Barrier.ReadIndex(ctx, w.Owner.GroupID())
	if err != nil {
		return nil, fmt.Errorf("confirm repair page Owner: %w", err)
	}
	if index == 0 {
		return nil, errors.New("repair page Owner returned no confirmed read index")
	}
	cut := ownerPageReadCut{group: w.Owner.GroupID(), index: index}
	var pending []enginestate.RecordReference
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID,
			OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: bytes.Clone(ref.RoutingKey)}
		if ref.Version != 2 || ref.OwnerGroupID != w.Owner.GroupID() || ref.Index == 0 || ref.RequestID == "" ||
			ref.Descriptor.Validate() != nil || target.Validate() != nil {
			return nil, errors.New("repair page contains an invalid or foreign Owner reference")
		}
		var receipt enginestate.LocalConfirmationRecord
		var found bool
		err := readOwner(ctx, cut, w.Owner, target, func(view enginestate.ReadView) error {
			current, exists, err := view.RecordReference(ref.Descriptor.Key)
			if err != nil {
				return err
			}
			if !exists || !sameReference(current, ref) {
				return errReferenceChangedBeforeReconfirmation
			}
			receipt, found, err = view.LocalConfirmation(ref.Descriptor.Key)
			return err
		})
		if errors.Is(err, errReferenceChangedBeforeReconfirmation) {
			continue // The next sweep evaluates the replacement publication.
		}
		if err != nil {
			return nil, fmt.Errorf("check repair reference %s: %w", ref.RequestID, err)
		}
		if !found || !localReceiptMatchesPlan(receipt, ref, plan) {
			pending = append(pending, ref)
		}
	}
	latest, err := placement.ReadLocalPlan(ctx, w.Barrier, w.Root, w.Owner.GroupID())
	if err != nil {
		return nil, fmt.Errorf("recheck repair page placement: %w", err)
	}
	if !plan.Matches(latest) {
		return nil, errors.New("Region plan changed during local confirmation page")
	}
	return pending, nil
}

// This cut is scoped to one call above. Each routed read still checks applied
// state and the exact live Range fence. It is never reused by later repair
// pages, physical reconfirmation, or foreground requests.
type ownerPageReadCut struct {
	group string
	index uint64
}

func (cut ownerPageReadCut) ReadIndex(ctx context.Context, group string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if group != cut.group || cut.index == 0 {
		return 0, errors.New("repair page read cut belongs to another Owner")
	}
	return cut.index, nil
}
