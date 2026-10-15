package recordwrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Observer interface {
	ObserveRecordHolding(context.Context, uint64, records.Descriptor) (raftrpc.HoldingObservation, error)
}

type Verifier interface {
	Verify(context.Context, records.Descriptor) (records.Holding, error)
}

// PublishedObservation is a fresh check of an exact committed reference and
// physical majority. It is still NOT a durable LocalReady receipt: the caller
// must persist a generation- and publication-bound confirmation before
// reporting a completed data write.
type PublishedObservation struct {
	Plan      placement.LocalPlan
	Reference capstate.RecordReference
	Holdings  []placement.Holding
}

// ObservePublishedLocal does not need the staging attempt's in-memory state.
// A retry after a lost response or process restart can use the original
// RequestID and committed publication index. Missing/corrupt/unavailable
// holdings are retried on other members of the unchanged confirmed cohort;
// the denominator never shrinks. The two Owner reads detect replacement or
// deletion of the exact logical publication during physical IO.
func ObservePublishedLocal(ctx context.Context, barrier Barrier, root Root, owner *capstate.State,
	localID uint64, localNodeID string, store Verifier, observer Observer,
	target router.Target, descriptor records.Descriptor, requestID string, publicationIndex uint64) (PublishedObservation, error) {
	if ctx == nil || barrier == nil || root == nil || owner == nil || localID == 0 || localNodeID == "" ||
		store == nil || observer == nil || target.Validate() != nil || descriptor.Validate() != nil ||
		target.Keyspace != descriptor.Keyspace || requestID == "" || publicationIndex == 0 {
		return PublishedObservation{}, errors.New("published observation requires a valid Owner, route and immutable publication")
	}
	recordID, err := descriptor.ID()
	if err != nil {
		return PublishedObservation{}, err
	}
	plan, err := placement.ReadLocalPlan(ctx, barrier, root, target.OwnerGroupID)
	if err != nil {
		return PublishedObservation{}, err
	}
	localMember := false
	for _, node := range plan.Nodes() {
		if node.RaftID == localID {
			if node.NodeID != localNodeID {
				return PublishedObservation{}, errors.New("local Node identity differs from confirmed cohort")
			}
			localMember = true
			break
		}
	}
	if !localMember {
		return PublishedObservation{}, errors.New("local Node is not in confirmed cohort")
	}
	reference, err := publishedReference(ctx, barrier, owner, target, descriptor.Key, recordID, requestID, publicationIndex)
	if err != nil {
		return PublishedObservation{}, err
	}
	result := PublishedObservation{Plan: plan, Reference: reference}
	var failures []error
	for _, node := range plan.Nodes() {
		if err := ctx.Err(); err != nil {
			return PublishedObservation{}, err
		}
		var holding placement.Holding
		if node.RaftID == localID {
			if node.NodeID != localNodeID {
				return PublishedObservation{}, errors.New("local Node identity differs from confirmed cohort")
			}
			verified, err := store.Verify(ctx, descriptor)
			if err != nil {
				if unavailableHolding(err) {
					failures = append(failures, fmt.Errorf("local Node: %w", err))
					continue
				}
				return PublishedObservation{}, fmt.Errorf("verify local immutable record: %w", err)
			}
			id, err := verified.Descriptor.ID()
			if err != nil || id != recordID {
				return PublishedObservation{}, errors.New("local Store verified a different immutable descriptor")
			}
			holding = placement.Holding{RaftID: localID, NodeID: localNodeID, StoreID: verified.StoreID, RecordID: verified.RecordID}
		} else {
			observed, err := observer.ObserveRecordHolding(ctx, node.RaftID, descriptor)
			if err != nil {
				if unavailableHolding(err) {
					failures = append(failures, fmt.Errorf("Node %d: %w", node.RaftID, err))
					continue
				}
				return PublishedObservation{}, fmt.Errorf("observe immutable record on Node %d: %w", node.RaftID, err)
			}
			id, err := observed.Holding.Descriptor.ID()
			if err != nil || id != recordID || observed.RootConfirmedIndex == 0 {
				return PublishedObservation{}, errors.New("remote Node observed a different immutable descriptor or no Root confirmation")
			}
			holding = placement.Holding{RaftID: observed.RaftID, NodeID: observed.NodeID,
				StoreID: observed.Holding.StoreID, RecordID: observed.Holding.RecordID}
		}
		result.Holdings = append(result.Holdings, holding)
		count, err := plan.Count(recordID, result.Holdings)
		if err != nil {
			return PublishedObservation{}, fmt.Errorf("validate published physical holdings: %w", err)
		}
		if count >= plan.Required {
			break
		}
	}
	if len(result.Holdings) < plan.Required {
		return PublishedObservation{}, errors.Join(append([]error{errors.New("published record lacks a verified stable local majority")}, failures...)...)
	}
	latest, err := placement.ReadLocalPlan(ctx, barrier, root, target.OwnerGroupID)
	if err != nil {
		return PublishedObservation{}, err
	}
	if !plan.Matches(latest) {
		return PublishedObservation{}, errors.New("local Region cohort changed during published record observation")
	}
	last, err := publishedReference(ctx, barrier, owner, target, descriptor.Key, recordID, requestID, publicationIndex)
	if err != nil {
		return PublishedObservation{}, err
	}
	if last.CommandHash != reference.CommandHash || last.Index != reference.Index || !slices.Equal(last.Descriptor.Key, reference.Descriptor.Key) {
		return PublishedObservation{}, errors.New("authoritative record reference changed during physical observation")
	}
	return result, nil
}

func publishedReference(ctx context.Context, barrier Barrier, owner *capstate.State, target router.Target,
	key []byte, recordID, requestID string, publicationIndex uint64) (capstate.RecordReference, error) {
	var ref capstate.RecordReference
	var found bool
	err := readOwner(ctx, barrier, owner, target, func(view capstate.ReadView) error {
		var readErr error
		ref, found, readErr = view.RecordReference(key)
		return readErr
	})
	if err != nil {
		return capstate.RecordReference{}, err
	}
	id, idErr := ref.Descriptor.ID()
	if !found || idErr != nil || id != recordID || ref.RequestID != requestID || ref.Index != publicationIndex ||
		ref.OwnerGroupID != target.OwnerGroupID || ref.RangeID != target.RangeID || ref.RangeEpoch != target.Epoch ||
		!bytes.Equal(ref.RoutingKey, target.RoutingKey) {
		return capstate.RecordReference{}, errors.New("exact authoritative record publication is not current")
	}
	return ref, nil
}

func unavailableHolding(err error) bool {
	if errors.Is(err, records.ErrNotHeld) || errors.Is(err, records.ErrCorrupt) || errors.Is(err, records.ErrUnavailable) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.NotFound, codes.DataLoss:
		return true
	}
	return false
}
