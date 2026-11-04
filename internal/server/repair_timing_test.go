package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

func TestRepairTimingBoundsLabelsAndClosesConcurrentCollector(t *testing.T) {
	var timing repairTiming
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 1000 {
				timing.observe("repair.root", 2*time.Microsecond)
				timing.observe("observe.remote_holding.node_123", time.Hour)
			}
		}()
	}
	workers.Wait()
	timing.observe("repair.root", -time.Second)
	values := timing.finish()
	if len(values) != 1 || values["repair.root"] != (repairTimingValue{Calls: 8000, SumUS: 16000, MaxUS: 2}) {
		t.Fatal("lost concurrent counts or retained an unbounded label", values)
	}
	timing.observe("repair.root", time.Hour)
	if timing.finish() != nil || timing.values[0].Calls != 8000 {
		t.Fatal("late RPC timing reopened the collector")
	}
}

func TestRepairTimingDisabledKeepsOriginalContext(t *testing.T) {
	t.Setenv("DUBI_TRACE_DATA", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got, finish := withRepairTiming(ctx, "local")
	if got != ctx {
		t.Fatal("normal operation installed diagnostic tracing")
	}
	finish()
	finish()
}

func TestRepairTimingEnabledEmitsOneBoundedSummary(t *testing.T) {
	t.Setenv("DUBI_TRACE_DATA", "1")
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(previous)
	ctx, finish := withRepairTiming(context.Background(), "geo")
	recordwrite.Trace(ctx, "repair.root", time.Now().Add(-3*time.Millisecond))
	recordwrite.Trace(ctx, "repair.owner", time.Now().Add(-2*time.Millisecond))
	recordwrite.Trace(ctx, "observe.remote_holding.node_secret", time.Now())
	finish()
	recordwrite.Trace(ctx, "repair.root", time.Now())
	finish()
	line := output.String()
	if strings.Count(line, "dubi_repair_timing lane=geo ") != 1 || strings.Contains(line, "node_secret") {
		t.Fatal("missing, repeated or unbounded diagnostic output", line)
	}
	_, encoded, found := strings.Cut(line, " phases=")
	var values map[string]repairTimingValue
	if !found || json.Unmarshal([]byte(encoded), &values) != nil || len(values) != 2 ||
		values["repair.root"].Calls != 1 || values["repair.root"].SumUS < 3000 || values["repair.owner"].Calls != 1 {
		t.Fatal("enabled worker tracing lost phase timings", line)
	}
	t.Log(strings.TrimSpace(line))
}

type timingBarrierFunc func(context.Context, string) (uint64, error)

func (f timingBarrierFunc) ReadIndex(ctx context.Context, group string) (uint64, error) {
	return f(ctx, group)
}

type timingVerifierFunc func(context.Context, records.Descriptor) (records.Holding, error)

func (f timingVerifierFunc) Verify(ctx context.Context, d records.Descriptor) (records.Holding, error) {
	return f(ctx, d)
}

func (f timingVerifierFunc) Put(ctx context.Context, d records.Descriptor, _ []byte) (records.Holding, error) {
	return f(ctx, d)
}

type timingMirrorFunc func(context.Context, uint64, string, []byte) (enginestate.Result, error)

func (f timingMirrorFunc) MirrorGeo(ctx context.Context, target uint64, group string, command []byte) (enginestate.Result, error) {
	return f(ctx, target, group, command)
}

func TestRepairTimingWrappersPreserveArgumentsErrorsAndBatchInterface(t *testing.T) {
	var timing repairTiming
	ctx := recordwrite.WithTrace(context.Background(), timing.observe)
	want := errors.New("original failure")
	b := repairTimingBarrier{timingBarrierFunc(func(call context.Context, group string) (uint64, error) {
		if call != ctx || group != raft.RootGroupID {
			t.Fatal("changed barrier arguments")
		}
		return 17, want
	})}
	if index, err := b.ReadIndex(ctx, raft.RootGroupID); index != 17 || err != want {
		t.Fatal(index, err)
	}
	v := repairTimingVerifier{timingVerifierFunc(func(call context.Context, d records.Descriptor) (records.Holding, error) {
		if call != ctx || string(d.Key) != "key" {
			t.Fatal("changed verifier arguments")
		}
		return records.Holding{StoreID: "store"}, want
	})}
	if holding, err := v.Verify(ctx, records.Descriptor{Key: []byte("key")}); holding.StoreID != "store" || err != want {
		t.Fatal(holding, err)
	}
	m := repairTimingMirror{timingMirrorFunc(func(call context.Context, target uint64, group string, command []byte) (enginestate.Result, error) {
		if call != ctx || target != 7 || (group != "region-a" && group != "region-b") || string(command) != "opaque" {
			t.Fatal("changed mirror arguments")
		}
		return enginestate.Result{Index: 42}, want
	}), "region-a"}
	for _, group := range []string{"region-a", "region-b"} {
		if result, err := m.MirrorGeo(ctx, 7, group, []byte("opaque")); result.Index != 42 || err != want {
			t.Fatal(result, err)
		}
	}
	var observer interface {
		ObserveRecordHoldings(context.Context, uint64, []records.Descriptor) ([]raftrpc.RecordHoldingResult, error)
	} = repairTimingObserver{}
	if rows, err := observer.ObserveRecordHoldings(ctx, 7, nil); rows != nil || err == nil {
		t.Fatal("diagnostic observer lost batch validation", rows, err)
	}
	values := timing.finish()
	for _, phase := range []string{"repair.root", "repair.pack_verify", "repair.mirror_region", "repair.mirror_remote", "repair.holding_batch"} {
		if values[phase].Calls != 1 {
			t.Fatal("missing or repeated wrapper timing", phase, values)
		}
	}
}
