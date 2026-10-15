package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func candidateFixture(t *testing.T) (capstate.RecordCut, []capstate.RecordReference, []rootstate.Node) {
	t.Helper()
	seal, _, cut := pendingFixture(t)
	refs := testReferences(t)
	cut.Seal = seal
	cut.Count = uint64(len(refs))
	for _, ref := range refs {
		cut.PayloadBytes += ref.Descriptor.Size
	}
	nodes := []rootstate.Node{
		{RaftID: 1, NodeID: "node-1"}, {RaftID: 2, NodeID: "node-2"},
		{RaftID: 3, NodeID: "node-3"}, {RaftID: 4, NodeID: "node-4"},
	}
	return cut, refs, nodes
}

func TestVerifyCandidateReferencesRequiresEveryRecordPhysicalMajority(t *testing.T) {
	cut, refs, nodes := candidateFixture(t)
	for _, tc := range []struct {
		name          string
		missing       map[string]map[uint64]bool
		remoteFailure error
		badNode       bool
		duplicate     bool
		staleRoot     bool
		localFailure  error
		wantPass      bool
	}{
		{name: "one-old-offline", missing: map[string]map[uint64]bool{"one": {1: true}, "two": {1: true}}, wantPass: true},
		{name: "second-reference-short", missing: map[string]map[uint64]bool{"two": {1: true, 2: true}}, wantPass: false},
		{name: "one-remote-index-unavailable", missing: map[string]map[uint64]bool{"one": {1: true}, "two": {1: true}}, remoteFailure: records.ErrUnavailable, wantPass: true},
		{name: "two-remote-index-unavailable", missing: map[string]map[uint64]bool{"two": {1: true, 2: true}}, remoteFailure: records.ErrUnavailable, wantPass: false},
		{name: "spoofed-node", badNode: true, wantPass: false},
		{name: "duplicate-store", duplicate: true, wantPass: false},
		{name: "stale-root", staleRoot: true, wantPass: false},
		{name: "new-node-lost", localFailure: records.ErrNotHeld, wantPass: false},
		{name: "new-node-index-unavailable", localFailure: records.ErrUnavailable, wantPass: false},
		{name: "new-node-index-corrupt", localFailure: records.ErrCorrupt, wantPass: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var observed []uint64
			verify := func(_ context.Context, d records.Descriptor) (records.Holding, error) {
				if tc.localFailure != nil {
					return records.Holding{}, tc.localFailure
				}
				id, _ := d.ID()
				return records.Holding{Descriptor: d, RecordID: id, StoreID: "store-4"}, nil
			}
			observe := func(_ context.Context, target uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
				observed = append(observed, target)
				if tc.missing[string(d.Key)][target] {
					if tc.remoteFailure != nil {
						return raftrpc.HoldingObservation{}, tc.remoteFailure
					}
					return raftrpc.HoldingObservation{}, status.Error(codes.NotFound, "not held")
				}
				id, _ := d.ID()
				nodeID := fmt.Sprintf("node-%d", target)
				if tc.badNode && target == 1 {
					nodeID = "another-node"
				}
				storeID := fmt.Sprintf("store-%d", target)
				if tc.duplicate && target == 2 {
					storeID = "store-1"
				}
				index := cut.Seal.Intent.Next.PlannedIndex
				if tc.staleRoot && target == 1 {
					index--
				}
				return raftrpc.HoldingObservation{RaftID: target, NodeID: nodeID, RootConfirmedIndex: index,
					Holding: records.Holding{Descriptor: d, RecordID: id, StoreID: storeID}}, nil
			}
			report, err := verifyCandidateReferences(context.Background(), cut, nodes, 4, verify, observe, testWalk(refs))
			if (err == nil) != tc.wantPass {
				t.Fatal("candidate majority result", report, err, observed)
			}
			if tc.wantPass && (report.References != cut.Count || report.ReferencedBytes != cut.PayloadBytes) {
				t.Fatal("candidate majority did not cover every sealed reference", report)
			}
		})
	}
}

func TestVerifyCandidateReferencesRejectsWrongCohortAndManifestSummary(t *testing.T) {
	cut, refs, nodes := candidateFixture(t)
	verify := func(_ context.Context, d records.Descriptor) (records.Holding, error) {
		id, _ := d.ID()
		return records.Holding{Descriptor: d, RecordID: id, StoreID: "store-4"}, nil
	}
	observe := func(_ context.Context, target uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
		id, _ := d.ID()
		return raftrpc.HoldingObservation{RaftID: target, NodeID: fmt.Sprintf("node-%d", target),
			RootConfirmedIndex: cut.Seal.Intent.Next.PlannedIndex,
			Holding:            records.Holding{Descriptor: d, RecordID: id, StoreID: fmt.Sprintf("store-%d", target)}}, nil
	}
	nodes[0].RaftID = 9
	if _, err := verifyCandidateReferences(context.Background(), cut, nodes, 4, verify, observe, testWalk(refs)); err == nil {
		t.Fatal("foreign candidate was accepted")
	}
	nodes[0].RaftID = 1
	cut.Count++
	if _, err := verifyCandidateReferences(context.Background(), cut, nodes, 4, verify, observe, testWalk(refs)); err == nil || errors.Is(err, context.Canceled) {
		t.Fatal("incomplete manifest summary was accepted", err)
	}
}
