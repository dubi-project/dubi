package recordwrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
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
	Reference enginestate.RecordReference
	Holdings  []placement.Holding
}

// ObservePublishedLocal does not need the staging attempt's in-memory state.
// A retry after a lost response or process restart can use the original
// RequestID and committed publication index. Missing/corrupt/unavailable
// holdings are retried on other members of the unchanged confirmed cohort;
// the denominator never shrinks. The two Owner reads detect replacement or
// deletion of the exact logical publication during physical IO.
func ObservePublishedLocal(ctx context.Context, barrier Barrier, root Root, owner *enginestate.State,
	localID uint64, localNodeID string, store Verifier, observer Observer,
	target router.Target, descriptor records.Descriptor, requestID string, publicationIndex uint64) (PublishedObservation, error) {
	started := time.Now()
	defer Trace(ctx, "observe.total", started)
	if ctx == nil || barrier == nil || root == nil || owner == nil || localID == 0 || localNodeID == "" ||
		store == nil || observer == nil || target.Validate() != nil || descriptor.Validate() != nil ||
		target.Keyspace != descriptor.Keyspace || requestID == "" || publicationIndex == 0 {
		return PublishedObservation{}, errors.New("published observation requires a valid Owner, route and immutable publication")
	}
	recordID, err := descriptor.ID()
	if err != nil {
		return PublishedObservation{}, err
	}
	phaseStarted := time.Now()
	plan, err := placement.ReadLocalPlan(ctx, barrier, root, target.OwnerGroupID)
	Trace(ctx, "observe.root_plan", phaseStarted)
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
	phaseStarted = time.Now()
	reference, err := publishedReference(ctx, barrier, owner, target, descriptor.Key, recordID, requestID, publicationIndex)
	Trace(ctx, "observe.owner_reference", phaseStarted)
	if err != nil {
		return PublishedObservation{}, err
	}
	result := PublishedObservation{Plan: plan, Reference: reference}
	var failures []error
	var remoteNodes []rootstate.Node
	for _, node := range plan.Nodes() {
		if err := ctx.Err(); err != nil {
			return PublishedObservation{}, err
		}
		var holding placement.Holding
		if node.RaftID == localID {
			if node.NodeID != localNodeID {
				return PublishedObservation{}, errors.New("local Node identity differs from confirmed cohort")
			}
			phaseStarted = time.Now()
			verified, err := store.Verify(ctx, descriptor)
			Trace(ctx, "observe.local_pack_verify", phaseStarted)
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
			remoteNodes = append(remoteNodes, node)
			continue
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
	if len(result.Holdings) < plan.Required && len(remoteNodes) != 0 {
		// A three-Node Region needs only one remote holding when the local
		// Store is healthy. Serially dividing a 60s read deadline among both
		// remote Nodes gave the first candidate only 20s; if that was the sole
		// other healthy holder, we could falsely report a missing majority.
		// Probe the unchanged confirmed cohort concurrently and stop as soon as
		// its physical majority is established. Each RPC remains bounded by the
		// caller's context and the server's holding-proof timeout.
		attemptCtx, cancel := context.WithCancel(ctx)
		type holdingResult struct {
			node    rootstate.Node
			observe raftrpc.HoldingObservation
			err     error
		}
		results := make(chan holdingResult, len(remoteNodes))
		for _, node := range remoteNodes {
			node := node
			go func() {
				phaseStarted := time.Now()
				observed, err := observer.ObserveRecordHolding(attemptCtx, node.RaftID, descriptor)
				Trace(ctx, fmt.Sprintf("observe.remote_holding.node_%d", node.RaftID), phaseStarted)
				results <- holdingResult{node: node, observe: observed, err: err}
			}()
		}
		remaining := len(remoteNodes)
		for remaining > 0 && len(result.Holdings) < plan.Required {
			select {
			case <-ctx.Done():
				cancel()
				return PublishedObservation{}, errors.Join(append(append([]error{errors.New("published record lacks a verified stable local majority")}, failures...), ctx.Err())...)
			case item := <-results:
				remaining--
				if item.err != nil {
					if unavailableHolding(item.err) {
						failures = append(failures, fmt.Errorf("Node %d: %w", item.node.RaftID, item.err))
						continue
					}
					cancel()
					return PublishedObservation{}, fmt.Errorf("observe immutable record on Node %d: %w", item.node.RaftID, item.err)
				}
				id, err := item.observe.Holding.Descriptor.ID()
				if err != nil || id != recordID || item.observe.RootConfirmedIndex == 0 {
					cancel()
					return PublishedObservation{}, errors.New("remote Node observed a different immutable descriptor or no Root confirmation")
				}
				result.Holdings = append(result.Holdings, placement.Holding{RaftID: item.observe.RaftID,
					NodeID: item.observe.NodeID, StoreID: item.observe.Holding.StoreID, RecordID: item.observe.Holding.RecordID})
				count, err := plan.Count(recordID, result.Holdings)
				if err != nil {
					cancel()
					return PublishedObservation{}, fmt.Errorf("validate published physical holdings: %w", err)
				}
				if count >= plan.Required {
					break
				}
			}
		}
		cancel()
	}
	if len(result.Holdings) < plan.Required {
		return PublishedObservation{}, errors.Join(append([]error{errors.New("published record lacks a verified stable local majority")}, failures...)...)
	}
	slices.SortFunc(result.Holdings, func(a, b placement.Holding) int {
		if a.RaftID < b.RaftID {
			return -1
		}
		if a.RaftID > b.RaftID {
			return 1
		}
		return 0
	})
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

func publishedReference(ctx context.Context, barrier Barrier, owner *enginestate.State, target router.Target,
	key []byte, recordID, requestID string, publicationIndex uint64) (enginestate.RecordReference, error) {
	var ref enginestate.RecordReference
	var found bool
	err := readOwner(ctx, barrier, owner, target, func(view enginestate.ReadView) error {
		var readErr error
		ref, found, readErr = view.RecordReference(key)
		return readErr
	})
	if err != nil {
		return enginestate.RecordReference{}, err
	}
	id, idErr := ref.Descriptor.ID()
	if !found || idErr != nil || id != recordID || ref.RequestID != requestID || ref.Index != publicationIndex ||
		ref.OwnerGroupID != target.OwnerGroupID || ref.RangeID != target.RangeID || ref.RangeEpoch != target.Epoch ||
		!bytes.Equal(ref.RoutingKey, target.RoutingKey) {
		return enginestate.RecordReference{}, errors.New("exact authoritative record publication is not current")
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
