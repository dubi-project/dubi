package server

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Fixed diagnostic labels retain no keys, payloads, request IDs or peer IDs.
// Timings may overlap across workers and nested phases; their sum is not wall
// time. Normal operation keeps the existing diagnostic switch disabled.
var repairTimingPhases = [...]string{
	"repair.root", "repair.owner", "repair.pack_verify", "repair.holding",
	"repair.holding_batch", "repair.mirror_remote", "repair.mirror_region",
	"repair.owner_submit", "repair.record_save", "repair.mirror_save",
	"repair.mirror_delete", "repair.geo_page", "repair.local_page",
}

type repairTimingValue struct {
	Calls uint64 `json:"calls"`
	SumUS int64  `json:"sum_us"`
	MaxUS int64  `json:"max_us"`
}

type repairTiming struct {
	mu     sync.Mutex
	closed bool
	values [len(repairTimingPhases)]repairTimingValue
}

func (t *repairTiming) observe(phase string, elapsed time.Duration) {
	for i, known := range repairTimingPhases {
		if phase != known || elapsed < 0 {
			continue
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		if !t.closed {
			v := &t.values[i]
			v.Calls++
			v.SumUS += elapsed.Microseconds()
			v.MaxUS = max(v.MaxUS, elapsed.Microseconds())
		}
		return
	}
}

func (t *repairTiming) finish() map[string]repairTimingValue {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	values := make(map[string]repairTimingValue)
	for i, v := range t.values {
		if v.Calls != 0 {
			values[repairTimingPhases[i]] = v
		}
	}
	return values
}

// One bounded summary per worker round, only under the existing opt-in data
// diagnostic switch. A canceled holding RPC may report a late timing; closing
// this collector ignores it instead of extending its lifetime or logging keys.
func withRepairTiming(ctx context.Context, lane string) (context.Context, func()) {
	if os.Getenv("DUBI_TRACE_DATA") != "1" {
		return ctx, func() {}
	}
	t := &repairTiming{}
	started := time.Now()
	return recordwrite.WithTrace(ctx, t.observe), func() {
		values := t.finish()
		if len(values) == 0 {
			return
		}
		encoded, _ := json.Marshal(values)
		log.Printf("dubi_repair_timing lane=%s elapsed_ms=%d phases=%s", lane, time.Since(started).Milliseconds(), encoded)
	}
}

type repairTimingBarrier struct{ recordwrite.Barrier }

func (b repairTimingBarrier) ReadIndex(ctx context.Context, group string) (uint64, error) {
	started := time.Now()
	defer traceRepairBarrier(ctx, group, started)
	return b.Barrier.ReadIndex(ctx, group)
}

func traceRepairBarrier(ctx context.Context, group string, started time.Time) {
	phase := "repair.owner"
	if group == raft.RootGroupID {
		phase = "repair.root"
	}
	recordwrite.Trace(ctx, phase, started)
}

type repairTimingVerifier struct{ recordwrite.Store }

func (v repairTimingVerifier) Verify(ctx context.Context, d records.Descriptor) (records.Holding, error) {
	started := time.Now()
	defer recordwrite.Trace(ctx, "repair.pack_verify", started)
	return v.Store.Verify(ctx, d)
}

// Preserve the optional batch interface, so diagnostic timing cannot silently
// turn the Geo worker back into individual holding requests.
type repairTimingObserver struct{ *raftrpc.GroupClient }

func (o repairTimingObserver) ObserveRecordHolding(ctx context.Context, target uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	started := time.Now()
	defer recordwrite.Trace(ctx, "repair.holding", started)
	return o.GroupClient.ObserveRecordHolding(ctx, target, d)
}

func (o repairTimingObserver) ObserveRecordHoldings(ctx context.Context, target uint64, ds []records.Descriptor) ([]raftrpc.RecordHoldingResult, error) {
	started := time.Now()
	defer recordwrite.Trace(ctx, "repair.holding_batch", started)
	return o.GroupClient.ObserveRecordHoldings(ctx, target, ds)
}

type repairTimingMirror struct {
	recordwrite.GeoMetadataMirrorTransport
	localGroup string
}

func (m repairTimingMirror) MirrorGeoBatch(ctx context.Context, target uint64, group string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
	batch, ok := m.GeoMetadataMirrorTransport.(recordwrite.GeoMetadataMirrorBatchTransport)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "Geo mirror batching unavailable")
	}
	started := time.Now()
	phase := "repair.mirror_remote"
	if group == m.localGroup {
		phase = "repair.mirror_region"
	}
	defer recordwrite.Trace(ctx, phase, started)
	return batch.MirrorGeoBatch(ctx, target, group, commands)
}

func (m repairTimingMirror) MirrorGeo(ctx context.Context, target uint64, group string, command []byte) (enginestate.Result, error) {
	started := time.Now()
	phase := "repair.mirror_remote"
	if group == m.localGroup {
		phase = "repair.mirror_region"
	}
	defer recordwrite.Trace(ctx, phase, started)
	return m.GeoMetadataMirrorTransport.MirrorGeo(ctx, target, group, command)
}
