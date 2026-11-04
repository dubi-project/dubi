package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type retryReferenceHandler struct{ body []byte }

func (h retryReferenceHandler) Apply(tx *enginestate.Transaction, target router.Target, _ []byte) (enginestate.Outcome, error) {
	d, err := records.Describe(target.Keyspace, "origin-group", target.RoutingKey, h.body)
	if err != nil {
		return enginestate.Outcome{}, err
	}
	return enginestate.Outcome{}, tx.SetRecordReference(target.RoutingKey, d)
}

type transientPageSource struct {
	*repairSourceFake
	calls      map[string]int
	mode       string
	order      []string
	onFailure  func()
	onRecovery func()
}

func (f *transientPageSource) ObserveRecordHolding(ctx context.Context, id uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	key := string(d.Key)
	f.calls[key]++
	f.order = append(f.order, key)
	if f.mode == "denied" {
		return raftrpc.HoldingObservation{}, status.Error(codes.PermissionDenied, "revoked")
	}
	if f.mode == "missing" {
		return raftrpc.HoldingObservation{}, status.Error(codes.NotFound, "absent")
	}
	if f.calls[key] <= 2 || f.mode == "persistent" {
		if f.calls[key] == 2 && f.onFailure != nil {
			f.onFailure()
		}
		return raftrpc.HoldingObservation{}, status.Error(codes.Unavailable, "temporary quorum loss")
	}
	if f.mode == "retry-deadline" {
		<-ctx.Done()
		return raftrpc.HoldingObservation{}, ctx.Err()
	}
	if f.onRecovery != nil {
		f.onRecovery()
	}
	return f.repairSourceFake.ObserveRecordHolding(ctx, id, d)
}

func TestLiveRepairRetriesOnlyTransientRowsAtPageTail(t *testing.T) {
	for _, mode := range []string{"recovered", "bounded", "persistent", "denied", "missing", "overwritten", "changed-cohort", "final-quorum-loss", "canceled", "large", "bytes-bound", "retry-deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := []byte("body")
			if mode == "large" {
				body = make([]byte, liveRetryBytes+1)
			}
			if mode == "bytes-bound" {
				body = make([]byte, 2<<20)
			}
			count := 1
			if mode == "bounded" {
				count = 12
			}
			if mode == "bytes-bound" {
				count = 3
			}
			plan := repairTestPlan(t)
			root := &pendingTestRoot{sources: plan.Nodes(), view: rootstate.RegionProgress{AppliedIndex: 5, Eligible: true,
				Head:    rootstate.RegionCohortHead{RegionID: "west", CurrentGeneration: 1},
				Current: rootstate.RegionCohort{RegionID: "west", GroupID: "region-west", Generation: 1, Nodes: []uint64{1, 2, 3}, State: rootstate.RegionCohortConfirmed, PlannedIndex: 1, ConfirmedIndex: 5, GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 4},
				Group:   rootstate.Group{GroupID: "region-west", Epoch: 2, Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}}}
			owner, err := enginestate.Open(t.TempDir(), "region-west", retryReferenceHandler{body})
			if err != nil {
				t.Fatal(err)
			}
			defer owner.Close()
			space := rootstate.Keyspace{TenantID: "tenant", EngineID: "record", DatasetID: "items"}
			if err := owner.ApplyRange(1, "range-activate", rootstate.Range{RangeID: "range-west", TenantID: space.TenantID, EngineID: space.EngineID, DatasetID: space.DatasetID, OwnerGroupID: "region-west", Epoch: 1, State: rootstate.RangeStateActive}); err != nil {
				t.Fatal(err)
			}
			target := router.Target{Keyspace: space, RangeID: "range-west", OwnerGroupID: "region-west", Epoch: 1}
			for i := 0; i < count; i++ {
				target.RoutingKey = []byte(fmt.Sprintf("key-%02d", i))
				if err := owner.ApplyRouted(owner.AppliedIndex()+1, fmt.Sprintf("publish-%d", i), target, []byte("publish")); err != nil {
					t.Fatal(err)
				}
			}
			roots := 0
			barrier := candidateBarrierFunc(func(ctx context.Context, g string) (uint64, error) {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
				if g == "root" {
					roots++
					if mode == "final-quorum-loss" && roots == 2 {
						return 0, errors.New("Root unavailable")
					}
					return 5, nil
				}
				return owner.AppliedIndex(), nil
			})
			client := &transientPageSource{repairSourceFake: &repairSourceFake{nodes: plan.Nodes(), body: body}, calls: make(map[string]int), mode: mode}
			if mode == "overwritten" {
				client.onFailure = func() {
					if err := owner.ApplyRouted(owner.AppliedIndex()+1, "overwrite", target, []byte("overwrite")); err != nil {
						t.Fatal(err)
					}
				}
			}
			if mode == "canceled" {
				client.onFailure = cancel
			}
			if mode == "changed-cohort" {
				client.onRecovery = func() { root.view.Current.GroupEpoch++; root.view.Group.Epoch++ }
			}
			store := repairTestStore(t)
			got, err := RepairCurrentLocalPageWithRetry(ctx, barrier, root, owner, "region-west", 1, plan.Nodes()[0].NodeID, store, client, nil, 64)
			switch mode {
			case "changed-cohort", "final-quorum-loss", "canceled":
				if err == nil {
					t.Fatal("unsafe page advanced", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wantRecovered := uint64(0)
			if mode == "recovered" {
				wantRecovered = 1
			}
			if mode == "bounded" {
				wantRecovered = 8
			}
			if mode == "bytes-bound" {
				wantRecovered = 2
			}
			if got.Fetched != wantRecovered || got.Unrepaired != uint64(count)-wantRecovered || len(got.Failures) != count-int(wantRecovered) || len(got.Unresolved) != len(got.Failures) {
				t.Fatal("retry report lost debt", got)
			}
			for key, calls := range client.calls {
				limit := 4
				if mode == "denied" {
					limit = 1
				}
				if mode == "missing" || mode == "large" || mode == "overwritten" {
					limit = 2
				}
				if calls > limit {
					t.Fatal("hard failure or row retried too often", key, calls, limit)
				}
			}
			if mode == "bounded" {
				for i := 0; i < count*2; i++ {
					if client.order[i] != fmt.Sprintf("key-%02d", i/2) {
						t.Fatal("retry starved later rows", client.order)
					}
				}
			}
			if !got.Page.Done {
				t.Fatal("fixture page not completed")
			}
		})
	}
}

func TestTransientPageRetryNeverClassifiesHardOrRemoteOnlyFailuresAsTransient(t *testing.T) {
	for _, err := range []error{records.ErrCorrupt, records.ErrUnavailable, records.ErrNotHeld, status.Error(codes.PermissionDenied, "denied"), status.Error(codes.Unauthenticated, "denied"), status.Error(codes.DataLoss, "corrupt"), errors.Join(errRemoteRepairCompleted, status.Error(codes.Unavailable, "local failed"))} {
		if transientSourceError(err) {
			t.Fatal("hard or remote-only failure became a local retry", err)
		}
	}
}
