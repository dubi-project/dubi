// Package recordwrite prepares immutable bytes on a stable local Node
// majority before a engine publishes an authoritative reference.
package recordwrite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Barrier interface {
	ReadIndex(context.Context, string) (uint64, error)
}
type Root interface {
	RegionProgress(string) (rootstate.RegionProgress, bool, error)
	RootMembers(...uint64) ([]rootstate.Node, bool, error)
}
type Store interface {
	Put(context.Context, records.Descriptor, []byte) (records.Holding, error)
	Verify(context.Context, records.Descriptor) (records.Holding, error)
}
type Sender interface {
	PutRecord(context.Context, rootstate.Node, records.Descriptor, []byte) (raftrpc.HoldingObservation, error)
}

const remoteCandidateTimeout = 30 * time.Second

// Staged collects synchronous physical confirmations from a stable local
// majority during one attempt. It is NOT LocalReady: observations are not an
// atomic cut, and no logical reference, generation-bound durable receipt or
// post-commit recheck has been published. Callers may not return a successful
// durable write from this alone.
type Staged struct {
	Plan     placement.LocalPlan
	RecordID string
	Holdings []placement.Holding
}

// StageLocal obtains synchronous confirmations from a strict majority of the
// exact confirmed cohort. Network failures may leave unreferenced bytes;
// retries use the Store's verified deduplication. Root is checked before and
// after IO, and a pending generation invalidates the attempt rather than
// silently changing the denominator. It does not publish metadata.
func StageLocal(ctx context.Context, barrier Barrier, root Root, localID uint64, localNodeID string,
	store Store, sender Sender, target router.Target, d records.Descriptor, body []byte) (Staged, error) {
	if ctx == nil || barrier == nil || root == nil || localID == 0 || localNodeID == "" || store == nil || sender == nil ||
		target.Validate() != nil || target.Keyspace != d.Keyspace || d.Validate() != nil || d.Size != uint64(len(body)) {
		return Staged{}, errors.New("local record staging requires a valid Node, descriptor, Store and sender")
	}
	if err := validateLocalPayload(d, body); err != nil {
		return Staged{}, err
	}
	// Descriptor.GroupID identifies the immutable record's origin, which can
	// differ from the current metadata owner after a Range handoff.
	plan, err := placement.ReadLocalPlan(ctx, barrier, root, target.OwnerGroupID)
	if err != nil {
		return Staged{}, err
	}
	nodes := plan.Nodes()
	localIndex := -1
	for i, node := range nodes {
		if node.RaftID == localID {
			if node.NodeID != localNodeID {
				return Staged{}, errors.New("local Node identity differs from confirmed cohort")
			}
			localIndex = i
		}
	}
	if localIndex < 0 {
		return Staged{}, errors.New("local Node is not in confirmed cohort")
	}
	recordID, err := d.ID()
	if err != nil {
		return Staged{}, err
	}
	// Prefer a local copy, then each distinct old participant. An unavailable
	// Node does not reduce Required; another confirmed Node may satisfy it.
	ordered := append([]rootstate.Node{nodes[localIndex]}, slices.Delete(slices.Clone(nodes), localIndex, localIndex+1)...)
	staged := Staged{Plan: plan, RecordID: recordID}
	var failures []error
	remainingRemote := len(ordered) - 1
	for _, node := range ordered {
		if err := ctx.Err(); err != nil {
			return Staged{}, err
		}
		var holding placement.Holding
		if node.RaftID == localID {
			if _, err := store.Put(ctx, d, body); err != nil {
				return Staged{}, fmt.Errorf("store local immutable record: %w", err)
			}
			verified, err := store.Verify(ctx, d)
			if err != nil {
				return Staged{}, fmt.Errorf("verify local immutable record: %w", err)
			}
			if id, err := verified.Descriptor.ID(); err != nil || id != recordID {
				return Staged{}, errors.New("local Store verified a different immutable descriptor")
			}
			holding = placement.Holding{RaftID: localID, NodeID: localNodeID, StoreID: verified.StoreID, RecordID: verified.RecordID}
		} else {
			attempt, done := remoteCandidateContext(ctx, remainingRemote)
			remainingRemote--
			observed, err := sender.PutRecord(attempt, node, d, body)
			done()
			if err != nil {
				if ctx.Err() != nil {
					return Staged{}, ctx.Err()
				}
				if retryable(err) || errors.Is(err, context.DeadlineExceeded) {
					failures = append(failures, fmt.Errorf("Node %d: %w", node.RaftID, err))
					continue
				}
				return Staged{}, fmt.Errorf("transfer immutable record to Node %d: %w", node.RaftID, err)
			}
			if id, err := observed.Holding.Descriptor.ID(); err != nil || id != recordID || observed.RootConfirmedIndex == 0 {
				return Staged{}, errors.New("remote Node returned a different immutable descriptor or no Root confirmation")
			}
			holding = placement.Holding{RaftID: observed.RaftID, NodeID: observed.NodeID,
				StoreID: observed.Holding.StoreID, RecordID: observed.Holding.RecordID}
		}
		staged.Holdings = append(staged.Holdings, holding)
		count, err := plan.Count(recordID, staged.Holdings)
		if err != nil {
			return Staged{}, fmt.Errorf("validate local physical holdings: %w", err)
		}
		if count >= plan.Required {
			break
		}
	}
	if len(staged.Holdings) < plan.Required {
		return Staged{}, errors.Join(append([]error{errors.New("stable local data majority was not physically verified")}, failures...)...)
	}
	latest, err := placement.ReadLocalPlan(ctx, barrier, root, target.OwnerGroupID)
	if err != nil {
		return Staged{}, err
	}
	if !plan.Matches(latest) {
		return Staged{}, errors.New("local Region cohort changed during immutable data staging")
	}
	return staged, nil
}

// Bound an earlier transfer only when another confirmed cohort member can
// still be tried. The last candidate keeps the caller's original budget.
func remoteCandidateContext(ctx context.Context, remaining int) (context.Context, context.CancelFunc) {
	if remaining <= 1 {
		return ctx, func() {}
	}
	limit := remoteCandidateTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if share := time.Until(deadline) / time.Duration(remaining); share < limit {
			limit = share
		}
	}
	return context.WithTimeout(ctx, limit)
}

func validateLocalPayload(d records.Descriptor, body []byte) error {
	if err := d.Validate(); err != nil || d.Size != uint64(len(body)) {
		return errors.New("local record staging requires a valid descriptor and matching payload size")
	}
	digest := sha256.Sum256(body)
	if hex.EncodeToString(digest[:]) != d.SHA256 {
		return errors.New("local record staging payload differs from descriptor")
	}
	return nil
}

func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	}
	return false
}
