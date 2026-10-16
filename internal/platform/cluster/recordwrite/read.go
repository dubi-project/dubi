package recordwrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"

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
// physical majority. A node that lacks the bytes fetches them from a verified
// member and persists them in its own shared Pack before returning data.
type Reader struct {
	Barrier     Barrier
	Root        Root
	Owner       *enginestate.State
	LocalID     uint64
	LocalNodeID string
	Store       *records.Store
	Observer    Observer
	Fetcher     Fetcher
}

type LocalRead struct {
	Body             []byte
	Reference        enginestate.RecordReference
	RegionGeneration uint64
	State            string
}

func (r Reader) ReadLocal(ctx context.Context, target router.Target, key []byte) (LocalRead, error) {
	if ctx == nil || r.Barrier == nil || r.Root == nil || r.Owner == nil || r.Store == nil ||
		r.Observer == nil || r.Fetcher == nil || r.LocalID == 0 || r.LocalNodeID == "" ||
		target.Validate() != nil || r.Owner.GroupID() != target.OwnerGroupID || len(key) == 0 || len(key) > 4096 {
		return LocalRead{}, errors.New("local read requires a valid Owner, route and Node runtime")
	}
	var ref enginestate.RecordReference
	var receipt enginestate.LocalConfirmationRecord
	var hasRef, hasReceipt bool
	err := readOwner(ctx, r.Barrier, r.Owner, target, func(view enginestate.ReadView) error {
		var readErr error
		ref, hasRef, readErr = view.RecordReference(key)
		if readErr != nil {
			return readErr
		}
		receipt, hasReceipt, readErr = view.LocalConfirmation(key)
		return readErr
	})
	if err != nil {
		return LocalRead{}, err
	}
	if !hasRef || !hasReceipt || !bytes.Equal(ref.Descriptor.Key, key) ||
		receipt.Claim.ReferenceIndex != ref.Index || receipt.Claim.ReferenceRequestID != ref.RequestID {
		return LocalRead{}, errors.New("record has no current local durability confirmation")
	}
	verified, err := CheckCommittedLocal(ctx, r.Barrier, r.Root, r.Owner, r.LocalID, r.LocalNodeID,
		r.Store, r.Observer, target, ref.Descriptor, ref.RequestID, ref.Index,
		receipt.RequestID, receipt.Claim)
	if err != nil {
		return LocalRead{}, err
	}
	body, _, err := r.Store.Read(ctx, ref.Descriptor)
	if errors.Is(err, records.ErrNotHeld) {
		var failures []error
		holders := make(map[uint64]bool, len(verified.Holdings))
		for _, h := range verified.Holdings {
			holders[h.RaftID] = true
		}
		for _, node := range verified.Plan.Nodes() {
			if node.RaftID == r.LocalID || !holders[node.RaftID] {
				continue
			}
			if _, fetchErr := r.Fetcher.FetchRecord(ctx, node, ref.Descriptor, r.Store); fetchErr != nil {
				if unavailableHolding(fetchErr) {
					failures = append(failures, fmt.Errorf("Node %d: %w", node.RaftID, fetchErr))
					continue
				}
				return LocalRead{}, fetchErr
			}
			body, _, err = r.Store.Read(ctx, ref.Descriptor)
			if err == nil {
				break
			}
			if !errors.Is(err, records.ErrNotHeld) {
				return LocalRead{}, err
			}
		}
		if err != nil {
			return LocalRead{}, errors.Join(append([]error{errors.New("verified remote majority could not serve immutable bytes")}, failures...)...)
		}
	} else if err != nil {
		return LocalRead{}, err
	}
	// The remote fetch or local Pack read may overlap an Owner overwrite or a
	// Region transition. Never release bytes without a final current cut.
	final, err := CheckCommittedLocal(ctx, r.Barrier, r.Root, r.Owner, r.LocalID, r.LocalNodeID,
		r.Store, r.Observer, target, ref.Descriptor, ref.RequestID, ref.Index,
		receipt.RequestID, receipt.Claim)
	if err != nil {
		return LocalRead{}, err
	}
	return LocalRead{Body: body, Reference: final.Reference, RegionGeneration: final.Plan.Generation, State: "local_ready"}, nil
}
