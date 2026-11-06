package recordwrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

type Fetcher interface {
	FetchRecord(context.Context, rootstate.Node, records.Descriptor, *records.Store) (raftrpc.RecordFetchResult, error)
}

// Reader is an internal immutable-data read gate, not a Tenant-authorized
// endpoint. It requires a current committed local confirmation and a fresh
// physical majority. A node that lacks bytes first fills its shared Pack from
// an authenticated current member, then freshly proves majority and currentness.
type Reader struct {
	Barrier     Barrier
	Root        Root
	Owner       *enginestate.State
	LocalID     uint64
	LocalNodeID string
	Store       *records.Store
	Observer    Observer
	Fetcher     Fetcher
	// RefreshLocal optionally gives the current Owner leader a bounded chance
	// to refresh a stale cohort receipt. true requests one complete fresh read;
	// callback success alone never grants authority to return bytes.
	RefreshLocal func(context.Context, enginestate.RecordReference) (bool, error)
}

type LocalRead struct {
	Body             []byte
	Reference        enginestate.RecordReference
	RegionGeneration uint64
	State            string
}

func (r Reader) ReadLocal(ctx context.Context, target router.Target, key []byte) (LocalRead, error) {
	started := time.Now()
	defer Trace(ctx, "read.local.total", started)
	if ctx == nil || r.Barrier == nil || r.Root == nil || r.Owner == nil || r.Store == nil ||
		r.Observer == nil || r.Fetcher == nil || r.LocalID == 0 || r.LocalNodeID == "" ||
		target.Validate() != nil || r.Owner.GroupID() != target.OwnerGroupID || len(key) == 0 || len(key) > 4096 {
		return LocalRead{}, errors.New("local read requires a valid Owner, route and Node runtime")
	}
	var ref enginestate.RecordReference
	var receipt enginestate.LocalConfirmationRecord
	var hasRef, hasReceipt bool
	phaseStarted := time.Now()
	err := readOwner(ctx, r.Barrier, r.Owner, target, func(view enginestate.ReadView) error {
		var readErr error
		ref, hasRef, readErr = view.RecordReference(key)
		if readErr != nil {
			return readErr
		}
		receipt, hasReceipt, readErr = view.LocalConfirmation(key)
		return readErr
	})
	Trace(ctx, "read.owner_reference", phaseStarted)
	if err != nil {
		return LocalRead{}, err
	}
	if !hasRef || !hasReceipt || !bytes.Equal(ref.Descriptor.Key, key) ||
		receipt.Claim.ReferenceIndex != ref.Index || receipt.Claim.ReferenceRequestID != ref.RequestID {
		return LocalRead{}, errors.New("record has no current local durability confirmation")
	}
	// Reading or filling the local cache grants no read authority. Do it before
	// the physical-majority probe so a silent failed peer cannot consume the
	// entire deadline while this online Node still lacks a usable copy.
	phaseStarted = time.Now()
	body, _, err := r.Store.Read(ctx, ref.Descriptor)
	if errors.Is(err, records.ErrNotHeld) {
		if _, fetchErr := r.fillMissingLocal(ctx, target, ref.Descriptor); fetchErr != nil {
			return LocalRead{}, fetchErr
		}
		body, _, err = r.Store.Read(ctx, ref.Descriptor)
	}
	if err != nil {
		return LocalRead{}, err
	}
	Trace(ctx, "read.pack_read_or_fetch", phaseStarted)
	phaseStarted = time.Now()
	verified, err := observeCommitted(ctx, r.Barrier, r.Root, r.Owner, r.LocalID, r.LocalNodeID,
		r.Store, r.Observer, target, ref.Descriptor, ref.RequestID, ref.Index,
		receipt.RequestID, receipt.Claim, true)
	Trace(ctx, "read.initial_majority_check", phaseStarted)
	if err != nil {
		if errors.Is(err, errStaleLocalCohort) && r.RefreshLocal != nil {
			refreshed, refreshErr := r.RefreshLocal(ctx, ref)
			if refreshErr != nil {
				return LocalRead{}, errors.Join(err, refreshErr)
			}
			if refreshed {
				// The publication, cohort or physical holdings may have changed
				// during refresh. Discard this attempt and re-run every read gate.
				r.RefreshLocal = nil
				return r.ReadLocal(ctx, target, key)
			}
		}
		return LocalRead{}, err
	}
	// The remote fetch or local Pack read may overlap an Owner overwrite or a
	// Region transition. The preceding observation established physical
	// majority; now check logical currentness and the fresh Root plan together.
	// The provisional observation cannot authorize bytes without this final gate.
	phaseStarted = time.Now()
	finalPlan, err := CheckReadCurrent(ctx, r.Barrier, r.Root, r.Owner, target, ref,
		receipt.RequestID, receipt.Claim, verified.Plan)
	Trace(ctx, "read.final_currentness_check", phaseStarted)
	if err != nil {
		return LocalRead{}, err
	}
	return LocalRead{Body: body, Reference: ref, RegionGeneration: finalPlan.Generation, State: "local_ready"}, nil
}

// fillMissingLocal uses only authenticated members of the current confirmed
// cohort. It cannot turn a cached receipt, partial transfer or one source ACK
// into a read result. The caller must freshly prove majority and currentness.
func (r Reader) fillMissingLocal(ctx context.Context, target router.Target, descriptor records.Descriptor) (bool, error) {
	if _, err := r.Store.Verify(ctx, descriptor); err == nil {
		return false, nil
	} else if !errors.Is(err, records.ErrNotHeld) {
		return false, err
	}
	plan, err := readInitialLocalPlan(ctx, r.Barrier, r.Root, target.OwnerGroupID)
	if err != nil {
		return false, err
	}
	member := false
	for _, node := range plan.Nodes() {
		if node.RaftID == r.LocalID && node.NodeID == r.LocalNodeID {
			member = true
		}
	}
	if !member {
		return false, errors.New("local read repair is outside the confirmed cohort")
	}
	remaining := len(plan.Nodes()) - 1
	for _, node := range plan.Nodes() {
		if node.RaftID == r.LocalID {
			continue
		}
		attempt, done := remoteCandidateContext(ctx, remaining)
		remaining--
		_, err := r.Fetcher.FetchRecord(attempt, node, descriptor, r.Store)
		done()
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			if unavailableHolding(err) {
				continue
			}
			return false, fmt.Errorf("read repair from Node %d: %w", node.RaftID, err)
		}
		if _, err := r.Store.Verify(ctx, descriptor); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}
