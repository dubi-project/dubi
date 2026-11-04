package recordrepair

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCapacityOffloadIsRecheckedAndRepairRestoresLostMajority(t *testing.T) {
	for _, mode := range []string{"remote-majority", "lost-majority", "changed-plan", "wrong-node"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			plan := repairTestPlan(t)
			store := repairTestStore(t)
			body := []byte("retained on other current Nodes")
			d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "search", DatasetID: "items"}, "origin-group", []byte("offloaded"), body)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Put(ctx, d, body); err != nil {
				t.Fatal(err)
			}
			scope := plan.RetentionScope()
			if mode == "changed-plan" {
				scope = strings.Repeat("f", 64)
			}
			task, err := store.BeginNodeBalance(ctx, d, scope, plan.Nodes()[1:])
			if err != nil {
				t.Fatal(err)
			}
			if err := store.DropBalanceHolding(ctx, task.Sequence); err != nil {
				t.Fatal(err)
			}
			if err := store.FinishNodeBalance(ctx, task.Sequence); err != nil {
				t.Fatal(err)
			}
			client := &repairSourceFake{nodes: plan.Nodes(), body: body, observeErr: map[uint64]error{}, spoofNodeID: map[uint64]string{}}
			if mode == "lost-majority" {
				client.observeErr[3] = status.Error(codes.Unavailable, "failed keeper")
			}
			if mode == "wrong-node" {
				client.spoofNodeID[2] = "other identity"
			}
			var report LivePageReport
			err = processRepairReference(ctx, plan, enginestate.RecordReference{Descriptor: d, RequestID: "offloaded"}, 1, store, client, 8, &report)
			if mode == "wrong-node" {
				if err == nil {
					t.Fatal("spoofed keeper accepted as an offload majority")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "remote-majority" {
				if report.Offloaded != 1 || report.AlreadyHeld != 0 || report.Fetched != 0 || report.Unrepaired != 0 || len(report.References) != 1 {
					t.Fatal(report)
				}
				if _, err := store.Verify(ctx, d); !errors.Is(err, records.ErrNotHeld) {
					t.Fatal("scan unnecessarily recreated the offloaded copy", err)
				}
			} else {
				if report.Offloaded != 0 || report.Fetched != 1 || report.Unrepaired != 0 {
					t.Fatal("lost proof did not trigger normal repair", report)
				}
				if _, err := store.Verify(ctx, d); err != nil {
					t.Fatal(err)
				}
				if _, found, err := store.CapacityOffload(d); err != nil || found {
					t.Fatal("repair left a stale capacity hint", found, err)
				}
			}
		})
	}
}
