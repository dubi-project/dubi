package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PendingRoot exposes one Root read barrier followed by an applied-state cut.
// The source descriptors are read from current Root membership, never from a
// health-filtered list or a caller-supplied address.
type PendingRoot interface {
	RegionProgress(string) (rootstate.RegionProgress, bool, error)
	RootMembers(...uint64) ([]rootstate.Node, bool, error)
}

type PendingBarrier interface {
	ReadIndex(context.Context, string) (uint64, error)
}

// FillPendingLocal is a one-shot backfill for a newly added Node. It makes no
// durable coverage claim: callers must gate generation publication separately.
// Retrying after a partial transfer is safe because FillLocalCut re-reads Pack.
func FillPendingLocal(ctx context.Context, barrier PendingBarrier, root PendingRoot, local uint64,
	seal regionfence.Handoff, artifacts *snapshot.Store, destination *records.Store, client *raftrpc.GroupClient) (Report, error) {
	if artifacts == nil || destination == nil || client == nil {
		return Report{}, errors.New("pending record fill requires artifacts, destination and client")
	}
	return fillPendingLocal(ctx, barrier, root, local, seal,
		func(ctx context.Context, source rootstate.Node) (capstate.RecordCut, error) {
			return client.FetchRecordCut(ctx, source, seal, artifacts)
		},
		func(ctx context.Context, cut capstate.RecordCut, sources []rootstate.Node) (Report, error) {
			return FillLocalCut(ctx, artifacts, cut, local, sources, destination, client)
		})
}

type pendingCutFetcher func(context.Context, rootstate.Node) (capstate.RecordCut, error)
type pendingCutFiller func(context.Context, capstate.RecordCut, []rootstate.Node) (Report, error)

func fillPendingLocal(ctx context.Context, barrier PendingBarrier, root PendingRoot, local uint64,
	seal regionfence.Handoff, fetch pendingCutFetcher, fill pendingCutFiller) (Report, error) {
	if ctx == nil || barrier == nil || root == nil || local == 0 || fetch == nil || fill == nil ||
		seal.Validate() != nil || seal.Activation != nil ||
		slices.Contains(seal.Intent.Previous.Fence.Nodes(), local) || !slices.Contains(seal.Intent.Next.Nodes, local) {
		return Report{}, errors.New("record fill requires a newly added Node and exact pending seal")
	}
	group := seal.Intent.Previous.Fence.GroupID
	view, sources, err := pendingCut(ctx, barrier, root, local, group, seal)
	if err != nil {
		return Report{}, err
	}
	var cut capstate.RecordCut
	var last error
	for i, source := range sources {
		attempt, cancel := sourceAttemptContext(ctx, len(sources)-i)
		cut, err = fetch(attempt, source)
		cancel()
		if err == nil {
			break
		}
		last = err
		if ctx.Err() != nil {
			return Report{}, ctx.Err()
		}
		if !errors.Is(err, context.DeadlineExceeded) && status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded && status.Code(err) != codes.ResourceExhausted {
			return Report{}, fmt.Errorf("fetch sealed cut from Node %d: %w", source.RaftID, err)
		}
	}
	if err != nil {
		return Report{}, fmt.Errorf("no old participant supplied sealed cut: %w", last)
	}
	if cut.Validate() != nil || !reflect.DeepEqual(cut.Seal, seal) {
		return Report{}, errors.New("fetched cut differs from pending seal")
	}
	if err := recheckPendingCut(ctx, barrier, root, local, group, seal, view, sources); err != nil {
		return Report{}, err
	}
	report, err := fill(ctx, cut, sources)
	if err != nil {
		return Report{}, err
	}
	if err := recheckPendingCut(ctx, barrier, root, local, group, seal, view, sources); err != nil {
		return Report{}, err
	}
	return report, nil
}

func pendingCut(ctx context.Context, barrier PendingBarrier, root PendingRoot, local uint64, group string,
	seal regionfence.Handoff) (rootstate.RegionProgress, []rootstate.Node, error) {
	if _, err := barrier.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return rootstate.RegionProgress{}, nil, err
	}
	view, found, err := root.RegionProgress(group)
	if err != nil {
		return rootstate.RegionProgress{}, nil, err
	}
	p, old := seal.Intent.Previous.Fence, seal.Intent.Previous
	oldIDs := p.Nodes()
	if !found || !view.Eligible || view.Pending == nil || view.PendingGrowth == nil ||
		!reflect.DeepEqual(*view.Pending, seal.Intent.Next) ||
		view.Current.RegionID != p.RegionID || view.Current.GroupID != group ||
		view.Current.Generation != p.Generation || view.Current.PlannedIndex != p.PlannedIndex ||
		view.Current.GroupFenceRequestID != old.RequestID || view.Current.GroupFenceIndex != old.Index ||
		!slices.Equal(view.Current.Nodes, oldIDs) || !slices.Contains(view.Pending.Nodes, local) ||
		view.PendingGrowth.Seal.SealRequestID != seal.SealRequestID ||
		view.PendingGrowth.Seal.SealIndex != seal.SealIndex {
		return rootstate.RegionProgress{}, nil, errors.New("record fill is not the current authorized regional growth")
	}
	sourceIDs := slices.Clone(oldIDs)
	if view.PendingGrowth.Seal.Recovery {
		// After explicit maintenance removal, the retired old identity remains
		// in the immutable seal but is no longer a Root member. Recover from the
		// live Group subset; the old cohort denominator is still enforced by the
		// seal/confirmation gates, not by pretending the retired Node is a source.
		live := make(map[uint64]bool, len(view.Group.Members))
		for _, member := range view.Group.Members {
			if slices.Contains(oldIDs, member.RaftID) {
				live[member.RaftID] = true
			}
		}
		sourceIDs = sourceIDs[:0]
		for _, id := range oldIDs {
			if live[id] {
				sourceIDs = append(sourceIDs, id)
			}
		}
	}
	// A one-node old cohort is valid during the initial small-Region growth
	// path: the Region may have formed before its other Nodes joined. Recovery
	// from an already degraded cohort keeps its separate minimum-source gate in
	// fillMode; this check only verifies that every sealed source is still a
	// current Root member.
	sources, all, err := root.RootMembers(sourceIDs...)
	if err != nil || !all || len(sources) != len(sourceIDs) || len(sourceIDs) == 0 {
		return rootstate.RegionProgress{}, nil, errors.Join(err, errors.New("sealed old participants are not current Root members"))
	}
	for i, source := range sources {
		if source.RaftID != sourceIDs[i] || source.RegionID != p.RegionID || source.NodeID == "" || source.Address == "" {
			return rootstate.RegionProgress{}, nil, errors.New("sealed old participant registration differs from Root")
		}
	}
	return view, sources, nil
}

func recheckPendingCut(ctx context.Context, barrier PendingBarrier, root PendingRoot, local uint64,
	group string, seal regionfence.Handoff, before rootstate.RegionProgress, sources []rootstate.Node) error {
	after, currentSources, err := pendingCut(ctx, barrier, root, local, group, seal)
	if err != nil {
		return err
	}
	before.AppliedIndex, after.AppliedIndex = 0, 0
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(sources, currentSources) {
		return errors.New("regional growth or source registration changed during record fill")
	}
	return ctx.Err()
}
