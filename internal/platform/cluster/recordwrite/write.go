package recordwrite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

// SubmitCommand may forward to the current Owner leader. It must not report
// application success merely because the command was queued; WriteLocal
// independently waits for the Owner Group's persisted Result.
type SubmitCommand func(context.Context, string, []byte) error

// Writer is an internal, engine-neutral write coordinator. The caller's
// opaque mutation must publish exactly the supplied immutable Descriptor via
// Transaction.SetRecordReference. Nothing here authorizes a Tenant or defines
// a Engine's schema.
type Writer struct {
	Barrier     Barrier
	Root        Root
	Owner       *enginestate.State
	LocalID     uint64
	LocalNodeID string
	Store       Store
	Sender      Sender
	Observer    Observer
	Submit      SubmitCommand
	// MirrorGeo is required by the explicit cross-Region WriteGeo path. Local
	// callers may leave it nil; WriteGeo refuses to publish a cross-Region
	// coverage result without durable metadata mirrors in every planned Region.
	MirrorGeo GeoMetadataMirrorTransport
	// MirrorRetry persists mirror delivery debt when a destination Region is
	// temporarily unavailable. It is optional for callers that only need the
	// synchronous primitive; production orchestration should provide it.
	MirrorRetry *GeoMetadataRetryStore
}

type WriteRequest struct {
	RequestID  string
	Target     router.Target
	Descriptor records.Descriptor
	Body       []byte
	Payload    []byte // Opaque engine mutation that publishes Descriptor.
}

// LocalWrite is returned only after a committed confirmation and a fresh
// majority/Root/Owner recheck. It is a completed local durability event, not
// GeoReady or an ongoing lease against later storage failures.
type LocalWrite struct {
	Reference         enginestate.RecordReference
	ConfirmationIndex uint64
	RegionGeneration  uint64
}

// WriteLocal is retry-safe with the same RequestID, descriptor, body and
// mutation payload. A changed mutation with the same RequestID conflicts via
// the persisted Owner Result. After a lost confirmation response, the retry
// reads the current receipt and rechecks physical data before returning; a
// stale/missing receipt triggers a fresh confirmation command with an
// automatically generated internal RequestID.
func (w Writer) WriteLocal(ctx context.Context, request WriteRequest) (LocalWrite, error) {
	started := time.Now()
	defer Trace(ctx, "write.local.total", started)
	if ctx == nil || w.Barrier == nil || w.Root == nil || w.Owner == nil || w.Store == nil ||
		w.Sender == nil || w.Observer == nil || w.Submit == nil ||
		rootstate.ValidateRequestID(request.RequestID) != nil || w.Owner.GroupID() != request.Target.OwnerGroupID {
		return LocalWrite{}, errors.New("local write requires an Owner, stable RequestID and complete Node runtime")
	}
	mutation, err := routedstate.EncodeMutation(request.RequestID, request.Target, request.Payload)
	if err != nil {
		return LocalWrite{}, err
	}
	phaseStarted := time.Now()
	publication, found, err := w.resultAtCut(ctx, request.Target.OwnerGroupID, func() (enginestate.Result, bool, error) {
		return w.Owner.ResultForMutation(request.RequestID, request.Target, request.Payload)
	})
	Trace(ctx, "write.initial_owner_read", phaseStarted)
	if err != nil {
		return LocalWrite{}, err
	}
	if found && (!publication.Applied || publication.Error != "") {
		return LocalWrite{}, fmt.Errorf("immutable reference publication rejected: %s", publication.Error)
	}
	if found {
		// A response can be lost after both Raft commands commit. Reuse the
		// receipt only after validating the retry body and rechecking the
		// current reference, physical majority and Region cut. No Pack transfer
		// is needed when the existing confirmation is still good.
		if err := validateLocalPayload(request.Descriptor, request.Body); err != nil {
			return LocalWrite{}, err
		}
		recordID, err := request.Descriptor.ID()
		if err != nil {
			return LocalWrite{}, err
		}
		if _, err := publishedReference(ctx, w.Barrier, w.Owner, request.Target,
			request.Descriptor.Key, recordID, request.RequestID, publication.Index); err != nil {
			return LocalWrite{}, err
		}
		var prior enginestate.LocalConfirmationRecord
		var hasPrior bool
		if err := readOwner(ctx, w.Barrier, w.Owner, request.Target, func(view enginestate.ReadView) error {
			var readErr error
			prior, hasPrior, readErr = view.LocalConfirmation(request.Descriptor.Key)
			return readErr
		}); err != nil {
			return LocalWrite{}, err
		}
		if hasPrior && prior.Claim.ReferenceRequestID == request.RequestID &&
			prior.Claim.ReferenceIndex == publication.Index && prior.Claim.RecordID == recordID {
			if ready, err := w.finish(ctx, request, publication.Index, prior.RequestID, prior.Index, prior.Claim); err == nil {
				return ready, nil
			}
			// A stale receipt or lost physical majority still needs staging.
		}
	}
	phaseStarted = time.Now()
	staged, err := StageLocal(ctx, w.Barrier, w.Root, w.LocalID, w.LocalNodeID,
		w.Store, w.Sender, request.Target, request.Descriptor, request.Body)
	Trace(ctx, "write.stage_local", phaseStarted)
	if err != nil {
		return LocalWrite{}, err
	}
	if !found {
		phaseStarted = time.Now()
		if err := w.Submit(ctx, request.Target.OwnerGroupID, mutation); err != nil {
			Trace(ctx, "write.publish_owner", phaseStarted)
			return LocalWrite{}, fmt.Errorf("submit immutable reference publication: %w", err)
		}
		publication, err = w.awaitResult(ctx, request.Target.OwnerGroupID, func() (enginestate.Result, bool, error) {
			return w.Owner.ResultForMutation(request.RequestID, request.Target, request.Payload)
		})
		Trace(ctx, "write.publish_owner", phaseStarted)
		if err != nil {
			return LocalWrite{}, err
		}
	}
	if !publication.Applied || publication.Error != "" || publication.Index == 0 {
		return LocalWrite{}, fmt.Errorf("immutable reference publication rejected: %s", publication.Error)
	}
	// StageLocal already verified the immutable Pack bytes on a strict
	// majority and rechecked that the exact Root cohort stayed unchanged across
	// those writes. Reuse that evidence to build the confirmation claim instead
	// of repeating the same remote holding scan immediately after publication.
	// finish still performs the full physical-majority scan AFTER the Owner
	// confirmation commits, before this method can report LocalReady.
	phaseStarted = time.Now()
	observed, err := stagedPublication(ctx, w.Barrier, w.Owner, request, staged, publication.Index)
	Trace(ctx, "write.staged_publication", phaseStarted)
	if err != nil {
		return LocalWrite{}, err
	}
	claim, err := ConfirmationClaim(request.Target, observed)
	if err != nil {
		return LocalWrite{}, err
	}
	// Recover an already committed confirmation before creating a new one.
	var prior enginestate.LocalConfirmationRecord
	var hasPrior bool
	phaseStarted = time.Now()
	err = readOwner(ctx, w.Barrier, w.Owner, request.Target, func(view enginestate.ReadView) error {
		var readErr error
		prior, hasPrior, readErr = view.LocalConfirmation(request.Descriptor.Key)
		return readErr
	})
	Trace(ctx, "write.prior_confirmation_read", phaseStarted)
	if err != nil {
		return LocalWrite{}, err
	}
	if hasPrior && prior.Claim.ReferenceRequestID == request.RequestID &&
		prior.Claim.ReferenceIndex == publication.Index && prior.Claim.RecordID == staged.RecordID {
		if ready, err := w.finish(ctx, request, publication.Index, prior.RequestID, prior.Index, prior.Claim); err == nil {
			return ready, nil
		}
		// A historical receipt cannot be reused if its cohort changed. The
		// fresh observation above may be confirmed against the current cut.
	}
	confirmID, err := randomConfirmationID()
	if err != nil {
		return LocalWrite{}, err
	}
	command, err := routedstate.EncodeLocalConfirmation(confirmID, claim)
	if err != nil {
		return LocalWrite{}, err
	}
	phaseStarted = time.Now()
	if err := w.Submit(ctx, request.Target.OwnerGroupID, command); err != nil {
		Trace(ctx, "write.confirm_owner", phaseStarted)
		return LocalWrite{}, fmt.Errorf("submit local majority confirmation: %w", err)
	}
	confirmation, err := w.awaitResult(ctx, request.Target.OwnerGroupID, func() (enginestate.Result, bool, error) {
		return w.Owner.ResultForLocalConfirmation(confirmID, claim)
	})
	Trace(ctx, "write.confirm_owner", phaseStarted)
	if err != nil {
		return LocalWrite{}, err
	}
	if !confirmation.Applied || confirmation.Error != "" {
		return LocalWrite{}, fmt.Errorf("local majority confirmation rejected: %s", confirmation.Error)
	}
	phaseStarted = time.Now()
	result, err := w.finish(ctx, request, publication.Index, confirmID, confirmation.Index, claim)
	Trace(ctx, "write.final_recheck", phaseStarted)
	return result, err
}

func stagedPublication(ctx context.Context, barrier Barrier, owner *enginestate.State, request WriteRequest,
	staged Staged, publicationIndex uint64) (PublishedObservation, error) {
	if ctx == nil || barrier == nil || owner == nil || staged.RecordID == "" ||
		staged.Plan.GroupID != request.Target.OwnerGroupID || staged.Plan.Required == 0 {
		return PublishedObservation{}, errors.New("staged publication requires verified local-majority evidence")
	}
	count, err := staged.Plan.Count(staged.RecordID, staged.Holdings)
	if err != nil || count < staged.Plan.Required {
		return PublishedObservation{}, errors.New("staged publication lacks an independent physical majority")
	}
	reference, err := publishedReference(ctx, barrier, owner, request.Target, request.Descriptor.Key,
		staged.RecordID, request.RequestID, publicationIndex)
	if err != nil {
		return PublishedObservation{}, err
	}
	return PublishedObservation{Plan: staged.Plan, Reference: reference,
		Holdings: append([]placement.Holding(nil), staged.Holdings...)}, nil
}

func (w Writer) finish(ctx context.Context, request WriteRequest, publicationIndex uint64,
	confirmationRequestID string, confirmationIndex uint64, claim routedstate.LocalConfirmation) (LocalWrite, error) {
	confirmed, err := CheckCommittedLocal(ctx, w.Barrier, w.Root, w.Owner, w.LocalID, w.LocalNodeID,
		w.Store, w.Observer, request.Target, request.Descriptor, request.RequestID, publicationIndex,
		confirmationRequestID, claim)
	if err != nil {
		return LocalWrite{}, err
	}
	return LocalWrite{Reference: confirmed.Reference, RegionGeneration: confirmed.Plan.Generation,
		ConfirmationIndex: confirmationIndex}, nil
}

func (w Writer) resultAtCut(ctx context.Context, groupID string, lookup func() (enginestate.Result, bool, error)) (enginestate.Result, bool, error) {
	for {
		index, err := w.Barrier.ReadIndex(ctx, groupID)
		if err != nil {
			return enginestate.Result{}, false, err
		}
		if index == 0 {
			return enginestate.Result{}, false, errors.New("Owner read barrier returned no confirmed index")
		}
		if w.Owner.AppliedIndex() >= index {
			return lookup()
		}
		if err := pauseRead(ctx); err != nil {
			return enginestate.Result{}, false, err
		}
	}
}

func (w Writer) awaitResult(ctx context.Context, groupID string, lookup func() (enginestate.Result, bool, error)) (enginestate.Result, error) {
	for {
		result, found, err := w.resultAtCut(ctx, groupID, lookup)
		if err != nil {
			return enginestate.Result{}, err
		}
		if found {
			return result, nil
		}
		if err := pauseRead(ctx); err != nil {
			return enginestate.Result{}, err
		}
	}
}

func pauseRead(ctx context.Context) error {
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func randomConfirmationID() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return "local-confirm-" + hex.EncodeToString(entropy[:]), nil
}
