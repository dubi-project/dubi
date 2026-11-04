package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

func TestGeoQueueRestartSkipsAttemptedSlowHeadAndRetainsDebt(t *testing.T) {
	root := t.TempDir()
	store, err := OpenGeoRecordRepairStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ref := geoRepairReference(t)
	for i := 0; i < 20; i++ {
		ref.RequestID = fmt.Sprintf("request-%03d", i)
		if err := store.Save(ref); err != nil {
			t.Fatal(err)
		}
	}
	first, _, err := store.NextPage(8)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	resolve := func(ctx context.Context, _ enginestate.RecordReference) (placement.GeoPlan, string, uint64, error) {
		<-ctx.Done()
		return placement.GeoPlan{}, "", 0, ctx.Err()
	}
	observe := func(context.Context, rootstate.Node, records.Descriptor) (raftrpc.HoldingObservation, error) {
		return raftrpc.HoldingObservation{}, errors.New("unexpected observation")
	}
	report, err := RetryPendingGeoRecords(ctx, store, resolve, observe, &geoRetryTransport{})
	cancel()
	if err == nil || report.Attempted != 1 {
		t.Fatal("slow first task did not leave a bounded retry", report, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenGeoRecordRepairStore(root)
	if err != nil {
		t.Fatal(err)
	}
	next, count, err := store.NextPage(8)
	if err != nil || len(next) != 8 || count != 20 || next[0].RequestID == first[0].RequestID {
		t.Fatal("restart retried the same blocking head", count, next, err)
	}
	for _, r := range next {
		if err := store.MarkAttempt(r); err != nil {
			t.Fatal(err)
		}
	}
	third, _, err := store.NextPage(8)
	if err != nil || len(third) != 8 || third[0].RequestID == next[0].RequestID {
		t.Fatal(third, err)
	}
	if count, err := store.Count(); err != nil || count != 20 {
		t.Fatal("attempt advancement retired debt", count, err)
	}
}

func TestRepairCapacityProbesDoNotSerializeUnavailablePeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var started atomic.Int64
	release := make(chan struct{})
	availability, err := repairAvailability(ctx, geoRepairPlan(t), nil, 1024, func(call context.Context, _ rootstate.Node, _ uint64) (bool, error) {
		if started.Add(1) == 2 {
			close(release)
		}
		select {
		case <-release:
			return true, nil
		case <-call.Done():
			return false, call.Err()
		}
	})
	if err != nil || len(availability) != 6 || started.Load() != 6 {
		t.Fatal("capacity probes waited serially before discovering healthy targets", availability, err)
	}
}

func TestGeoQueueLargerThanOldBatchBound(t *testing.T) {
	root := t.TempDir()
	store, err := OpenGeoRecordRepairStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ref := geoRepairReference(t)
	for i := 0; i < geoRepairMaxEntries+1; i++ {
		ref.RequestID = fmt.Sprintf("request-%06d", i)
		if err := store.Save(ref); err != nil {
			t.Fatal(i, err)
		}
	}
	page, count, err := store.NextPage(8)
	if err != nil || len(page) != 8 || count != geoRepairMaxEntries+1 {
		t.Fatal(len(page), count, err)
	}
	if err := store.Delete(page[0]); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(page[0]); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Count(); got != geoRepairMaxEntries {
		t.Fatal("duplicate deletion corrupted count", got)
	}
}

type capacityRetryTransport struct {
	*geoRetryTransport
	full map[uint64]bool
}

func (t *capacityRetryTransport) RepairHasCapacity(_ context.Context, node rootstate.Node, _ uint64) (bool, error) {
	return !t.full[node.RaftID], nil
}

func TestGeoRetryCapacitySkipsFullTargetWithoutShrinkingMajority(t *testing.T) {
	for _, allFull := range []bool{false, true} {
		t.Run(fmt.Sprint(allFull), func(t *testing.T) {
			store, err := OpenGeoRecordRepairStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			ref := geoRepairReference(t)
			if err := store.Save(ref); err != nil {
				t.Fatal(err)
			}
			plan := geoRepairPlan(t)
			transport := &capacityRetryTransport{geoRetryTransport: &geoRetryTransport{held: map[uint64]bool{1: true, 2: true}}, full: map[uint64]bool{4: true}}
			if allFull {
				transport.full[5], transport.full[6] = true, true
			}
			observer := func(_ context.Context, node rootstate.Node, d records.Descriptor) (raftrpc.HoldingObservation, error) {
				if !transport.held[node.RaftID] {
					return raftrpc.HoldingObservation{}, errors.New("not held")
				}
				id, _ := d.ID()
				return raftrpc.HoldingObservation{RaftID: node.RaftID, NodeID: node.NodeID, RootConfirmedIndex: 9,
					Holding: records.Holding{RecordID: id, Descriptor: d, StoreID: "store-" + node.NodeID}}, nil
			}
			report, err := RetryPendingGeoRecords(context.Background(), store, func(context.Context, enginestate.RecordReference) (placement.GeoPlan, string, uint64, error) {
				return plan, "west", 9, nil
			}, observer, transport)
			if transport.held[4] {
				t.Fatal("full target selected")
			}
			if allFull {
				if err == nil || report.Completed != 0 || report.Pending != 1 {
					t.Fatal("capacity failure shrank durability policy", report, err)
				}
			} else {
				if err != nil || report.Completed != 1 || !transport.held[5] || !transport.held[6] {
					t.Fatal("alternative capacity did not reach the original strict majority", report, transport.held, err)
				}
			}
		})
	}
}
