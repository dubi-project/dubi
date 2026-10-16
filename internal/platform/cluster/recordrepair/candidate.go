package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// VerifyPendingCandidateLocal is a fresh, one-shot physical check of EVERY
// authoritative reference in the exact sealed cut. It is not a persistent
// retention lease or a substitute for the Root/Owner identity-fence protocol.
// The caller must still revalidate the candidate fence and Root plan before
// publishing the next generation.
func VerifyPendingCandidateLocal(ctx context.Context, barrier PendingBarrier, root PendingRoot, local uint64,
	seal regionfence.Handoff, artifacts *snapshot.Store, destination *records.Store, client *raftrpc.GroupClient) (Report, error) {
	if artifacts == nil || destination == nil || client == nil {
		return Report{}, errors.New("candidate verification requires artifacts, destination and authenticated client")
	}
	return fillPendingLocal(ctx, barrier, root, local, seal,
		func(ctx context.Context, source rootstate.Node) (enginestate.RecordCut, error) {
			return client.FetchRecordCut(ctx, source, seal, artifacts)
		},
		func(ctx context.Context, cut enginestate.RecordCut, _ []rootstate.Node) (Report, error) {
			candidates, err := pendingCandidateMembers(root, seal)
			if err != nil {
				return Report{}, err
			}
			report, err := verifyCandidateReferences(ctx, cut, candidates, local, destination.Verify,
				client.ObserveRecordHolding, func(visit func(enginestate.RecordReference) error) error {
					return enginestate.WalkVerifiedRecordCut(ctx, artifacts, cut, visit)
				})
			if err != nil {
				return Report{}, err
			}
			after, err := pendingCandidateMembers(root, seal)
			if err != nil || !reflect.DeepEqual(candidates, after) {
				return Report{}, errors.Join(err, errors.New("pending candidate registration changed during physical verification"))
			}
			return report, nil
		})
}

func pendingCandidateMembers(root PendingRoot, seal regionfence.Handoff) ([]rootstate.Node, error) {
	ids := seal.Intent.Next.Nodes
	nodes, all, err := root.RootMembers(ids...)
	if err != nil || !all || len(nodes) != len(ids) {
		return nil, errors.Join(err, errors.New("pending candidate Nodes are not current Root members"))
	}
	seen := make(map[string]bool, len(nodes))
	for i, node := range nodes {
		if node.RaftID != ids[i] || node.NodeID == "" || node.Address == "" ||
			node.RegionID != seal.Intent.Next.RegionID || seen[node.NodeID] {
			return nil, errors.New("pending candidate registration differs from Root cohort")
		}
		seen[node.NodeID] = true
	}
	return nodes, nil
}

type candidateLocalVerify func(context.Context, records.Descriptor) (records.Holding, error)
type candidateRemoteObserve func(context.Context, uint64, records.Descriptor) (raftrpc.HoldingObservation, error)

func verifyCandidateReferences(ctx context.Context, cut enginestate.RecordCut, candidates []rootstate.Node, local uint64,
	verify candidateLocalVerify, observe candidateRemoteObserve, walk cutWalker) (Report, error) {
	var report Report
	if ctx == nil || cut.Validate() != nil || len(candidates) == 0 || local == 0 || verify == nil || observe == nil || walk == nil {
		return report, errors.New("invalid pending candidate physical verification")
	}
	if !slices.Equal(cut.Seal.Intent.Next.Nodes, candidateIDs(candidates)) {
		return report, errors.New("physical verification candidates differ from the sealed next cohort")
	}
	localNode := rootstate.Node{}
	for _, node := range candidates {
		if node.RaftID == local {
			localNode = node
			break
		}
	}
	if localNode.NodeID == "" {
		return report, errors.New("physical verifier is not a pending cohort member")
	}
	required := len(candidates)/2 + 1
	err := walk(func(ref enginestate.RecordReference) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		d := ref.Descriptor
		id, err := d.ID()
		if err != nil {
			return err
		}
		localHolding, err := verify(ctx, d)
		if err != nil {
			return fmt.Errorf("new Node lost a filled candidate record: %w", err)
		}
		if localHolding.RecordID != id || localHolding.StoreID == "" {
			return errors.New("local candidate returned a different record or empty Store")
		}
		if actual, err := localHolding.Descriptor.ID(); err != nil || actual != id {
			return errors.New("local candidate verified a different descriptor")
		}
		stores := map[string]bool{localHolding.StoreID: true}
		confirmed := 1
		for _, node := range candidates {
			if confirmed >= required {
				break
			}
			if node.RaftID == local {
				continue
			}
			o, err := observe(ctx, node.RaftID, d)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if candidateHoldingUnavailable(err) {
					continue
				}
				return fmt.Errorf("observe pending Node %d: %w", node.RaftID, err)
			}
			observedID, err := o.Holding.Descriptor.ID()
			if err != nil || o.RaftID != node.RaftID || o.NodeID != node.NodeID || o.RootConfirmedIndex < cut.Seal.Intent.Next.PlannedIndex ||
				o.Holding.RecordID != id || observedID != id || o.Holding.StoreID == "" || stores[o.Holding.StoreID] {
				return errors.New("candidate holding differs from authenticated Node, descriptor or Store")
			}
			stores[o.Holding.StoreID] = true
			confirmed++
		}
		if confirmed < required {
			return fmt.Errorf("record %s has %d of %d required pending physical holdings", id, confirmed, required)
		}
		if report.ReferencedBytes > ^uint64(0)-d.Size {
			return errors.New("candidate reference byte count overflow")
		}
		report.References++
		report.ReferencedBytes += d.Size
		return nil
	})
	if err != nil {
		return Report{}, err
	}
	if report.References != cut.Count || report.ReferencedBytes != cut.PayloadBytes {
		return Report{}, errors.New("candidate verification differs from sealed reference summary")
	}
	return report, nil
}

func candidateIDs(nodes []rootstate.Node) []uint64 {
	ids := make([]uint64, len(nodes))
	for i, node := range nodes {
		ids[i] = node.RaftID
	}
	return ids
}

func candidateHoldingUnavailable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, records.ErrNotHeld) ||
		errors.Is(err, records.ErrCorrupt) || errors.Is(err, records.ErrUnavailable) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.NotFound, codes.DataLoss:
		return true
	}
	return false
}
