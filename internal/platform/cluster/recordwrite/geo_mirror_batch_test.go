package recordwrite

import (
	"context"
	"errors"
	"fmt"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
	"testing"
	"time"
)

type batchMirrorFunc struct {
	batch  func(context.Context, uint64, string, [][]byte) ([]raftrpc.GeoMirrorBatchResult, error)
	single func(context.Context, uint64, string, []byte) (enginestate.Result, error)
}

func (m batchMirrorFunc) MirrorGeoBatch(c context.Context, n uint64, g string, b [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
	return m.batch(c, n, g, b)
}
func (m batchMirrorFunc) MirrorGeo(c context.Context, n uint64, g string, b []byte) (enginestate.Result, error) {
	if m.single == nil {
		return enginestate.Result{}, errors.New("unexpected single call")
	}
	return m.single(c, n, g, b)
}
func poolFixture(t *testing.T, transport batchMirrorFunc) (*geoMirrorPagePool, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p := &geoMirrorPagePool{GeoMetadataMirrorTransport: transport, batch: transport, ctx: ctx, cancel: cancel, waits: make(chan *geoMirrorCall, 8), done: make(chan struct{})}
	return p, func() { cancel(); <-p.done }
}
func poolCall(label string, target uint64, group string) *geoMirrorCall {
	return &geoMirrorCall{ctx: context.Background(), target: target, group: group, command: []byte(label), done: make(chan raftrpc.GeoMirrorBatchResult, 1)}
}
func awaitMirror(t *testing.T, w *geoMirrorCall) raftrpc.GeoMirrorBatchResult {
	t.Helper()
	select {
	case r := <-w.done:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("mirror pool did not finish")
		return raftrpc.GeoMirrorBatchResult{}
	}
}
func TestMirrorPoolRequeuesOnlyFailedRowsAfterNewArrivals(t *testing.T) {
	var p *geoMirrorPagePool
	var batches [][]string
	attempts := map[string]int{}
	a, b, c := poolCall("a", 2, "owner"), poolCall("b", 2, "owner"), poolCall("c", 2, "owner")
	m := batchMirrorFunc{batch: func(ctx context.Context, _ uint64, _ string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
		var labels []string
		rows := make([]raftrpc.GeoMirrorBatchResult, len(commands))
		for i, cmd := range commands {
			k := string(cmd)
			labels = append(labels, k)
			attempts[k]++
			if k == "a" && attempts[k] == 1 {
				rows[i].Err = status.Error(codes.Unavailable, "pending")
			} else {
				rows[i].Result = enginestate.Result{RequestID: k, Index: 1, Applied: true}
			}
		}
		batches = append(batches, labels)
		if len(batches) == 1 {
			p.waits <- c
		}
		return rows, nil
	}}
	var closePool func()
	p, closePool = poolFixture(t, m)
	p.waits <- a
	p.waits <- b
	go p.run()
	defer closePool()
	if r := awaitMirror(t, b); r.Err != nil || !r.Result.Applied {
		t.Fatal(r)
	}
	awaitMirror(t, c)
	awaitMirror(t, a)
	if len(batches) != 2 || fmt.Sprint(batches[1]) != "[c a]" || attempts["a"] != 2 || attempts["b"] != 1 {
		t.Fatal(batches, attempts)
	}
}
func TestMirrorPoolBoundsRetriesAndKeepsPermanentResults(t *testing.T) {
	for _, mode := range []string{"temporary", "denied", "corrupt", "rejected", "unsupported"} {
		t.Run(mode, func(t *testing.T) {
			calls, singles := 0, 0
			m := batchMirrorFunc{batch: func(context.Context, uint64, string, [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
				calls++
				switch mode {
				case "denied":
					return nil, status.Error(codes.PermissionDenied, "fenced")
				case "corrupt":
					return nil, nil
				case "rejected":
					return []raftrpc.GeoMirrorBatchResult{{Result: enginestate.Result{RequestID: "a", Index: 1, Error: "stale generation"}}}, nil
				case "unsupported":
					return nil, status.Error(codes.Unimplemented, "old peer")
				}
				return []raftrpc.GeoMirrorBatchResult{{Err: status.Error(codes.Unavailable, "pending")}}, nil
			}, single: func(context.Context, uint64, string, []byte) (enginestate.Result, error) {
				singles++
				return enginestate.Result{RequestID: "a", Index: 2, Applied: true}, nil
			}}
			p, closePool := poolFixture(t, m)
			a := poolCall("a", 2, "owner")
			p.waits <- a
			go p.run()
			r := awaitMirror(t, a)
			closePool()
			want := 1
			if mode == "temporary" {
				want = 3
			}
			if calls != want {
				t.Fatal(calls, want)
			}
			switch mode {
			case "temporary":
				if status.Code(r.Err) != codes.Unavailable {
					t.Fatal(r)
				}
			case "rejected":
				if r.Err != nil || r.Result.Applied || r.Result.Error == "" {
					t.Fatal(r)
				}
			case "unsupported":
				if singles != 1 || !r.Result.Applied {
					t.Fatal(singles, r)
				}
			default:
				if r.Err == nil || singles != 0 {
					t.Fatal(r, singles)
				}
			}
		})
	}
}
func TestMirrorPoolSplitsBytesAndDestinationWithoutWaitingToFill(t *testing.T) {
	var groups []string
	maxBytes := 0
	m := batchMirrorFunc{batch: func(_ context.Context, n uint64, g string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
		total := 0
		for _, cmd := range commands {
			total += len(cmd)
		}
		maxBytes = max(maxBytes, total)
		groups = append(groups, fmt.Sprint(n, "/", g))
		out := make([]raftrpc.GeoMirrorBatchResult, len(commands))
		for i := range out {
			out[i].Result = enginestate.Result{Index: 1, Applied: true}
		}
		return out, nil
	}}
	p, closePool := poolFixture(t, m)
	var waits []*geoMirrorCall
	for i := 0; i < 4; i++ {
		w := poolCall(fmt.Sprint(i), 2, "owner")
		if i < 2 {
			w.command = make([]byte, 160<<10)
			w.command[0] = byte(i)
		}
		if i == 2 {
			w.target = 3
		}
		if i == 3 {
			w.group = "another"
		}
		waits = append(waits, w)
		p.waits <- w
	}
	go p.run()
	for _, w := range waits {
		awaitMirror(t, w)
	}
	closePool()
	if maxBytes > raftrpc.GeoMirrorBatchBytes || len(groups) != 4 {
		t.Fatal(maxBytes, groups)
	}
}
func TestMirrorPoolCancellationStopsInFlightBatch(t *testing.T) {
	started := make(chan struct{})
	exited := make(chan struct{})
	m := batchMirrorFunc{batch: func(ctx context.Context, _ uint64, _ string, _ [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
		close(started)
		<-ctx.Done()
		close(exited)
		return nil, ctx.Err()
	}}
	transport, closePool := newGeoMirrorPagePool(context.Background(), m)
	done := make(chan error, 1)
	go func() { _, e := transport.MirrorGeo(context.Background(), 2, "owner", []byte("a")); done <- e }()
	<-started
	closePool()
	<-exited
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}
func TestGeoMirrorSuccessLeavesOnlyFailedRegionInDurableQueue(t *testing.T) {
	plan, target, d := geoStagePlan(t)
	staged, e := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, d, []byte("body"))
	if e != nil {
		t.Fatal(e)
	}
	id, _ := d.ID()
	pub := placement.PublicationCut{RecordID: id, RequestID: "publication", Index: 11, CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", RangeID: target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, e := GeoMetadataMirrors(target, d, pub, plan, staged.Evidence)
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	store, e := OpenGeoMetadataRetryStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	transport := batchMirrorFunc{single: func(_ context.Context, _ uint64, _ string, data []byte) (enginestate.Result, error) {
		c, e := routedstate.DecodeCommand(data)
		if e != nil {
			return enginestate.Result{}, e
		}
		if c.GeoMetadata.RegionID == mirrors[0].RegionID {
			return enginestate.Result{}, status.Error(codes.Unavailable, "offline")
		}
		return enginestate.Result{RequestID: c.RequestID, Index: 10, Applied: true}, nil
	}}
	if _, e = ReplicateGeoMetadataWithRetryStore(context.Background(), transport, plan, mirrors, store); e == nil {
		t.Fatal("missing region accepted")
	}
	if e = store.Close(); e != nil {
		t.Fatal(e)
	}
	store, e = OpenGeoMetadataRetryStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	remaining, e := store.List()
	if e != nil || len(remaining) != 1 || remaining[0].RegionID != mirrors[0].RegionID {
		t.Fatal(remaining, e)
	}
}

type delayedPageBarrier struct{ Barrier }

func (b delayedPageBarrier) ReadIndex(ctx context.Context, g string) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(2 * time.Millisecond):
		return b.Barrier.ReadIndex(ctx, g)
	}
}
func TestGeoPageBatchesOwnerAndRemoteMirrorsWithoutLosingRowProof(t *testing.T) {
	w, refs, base, _, _, remote := geoBatchFixture(t, 32)
	single := w.MirrorGeo
	var mu sync.Mutex
	calls, maxRows := 0, 0
	w.Barrier = delayedPageBarrier{base}
	w.MirrorGeo = batchMirrorFunc{single: single.MirrorGeo, batch: func(ctx context.Context, n uint64, g string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		calls++
		maxRows = max(maxRows, len(commands))
		mu.Unlock()
		rows := make([]raftrpc.GeoMirrorBatchResult, len(commands))
		for i, b := range commands {
			rows[i].Result, rows[i].Err = single.MirrorGeo(ctx, n, g, b)
		}
		return rows, nil
	}}
	rows, e := w.RefreshGeoConfirmationPage(context.Background(), refs, 3, 3)
	if e != nil {
		t.Fatal(e)
	}
	for i, r := range rows {
		if r.Err != nil || !r.Confirmed {
			t.Fatal(i, r)
		}
	}
	assertLocalGeoMirrors(t, w, refs)
	if len(remote.mirrors) != 32 || base.rootCalls != 2 || base.ownerCalls >= 128 || calls >= 32 || maxRows < 2 {
		t.Fatal("batch did not reduce round trips", base.ownerCalls, base.rootCalls, calls, maxRows, len(remote.mirrors))
	}
	t.Logf("32 records: Owner cuts=%d, remote RPCs=%d, max batch=%d, Root cuts=%d", base.ownerCalls, calls, maxRows, base.rootCalls)
}

func TestDurableMirrorBatchRetryRotatesFailedRowBehindUntouchedWork(t *testing.T) {
	plan, target, d := geoStagePlan(t)
	staged, e := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, d, []byte("body"))
	if e != nil {
		t.Fatal(e)
	}
	id, _ := d.ID()
	pub := placement.PublicationCut{RecordID: id, RequestID: "publication", Index: 11, CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", RangeID: target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, e := GeoMetadataMirrors(target, d, pub, plan, staged.Evidence)
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	store, e := OpenGeoMetadataRetryStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { store.Close() }()
	for i := 0; i < 20; i++ {
		m := mirrors[0]
		m.ReferenceRequestID = fmt.Sprintf("pending-%02d", i)
		if e = store.Save(m); e != nil {
			t.Fatal(e)
		}
	}
	first, _, e := store.NextPage(8)
	if e != nil {
		t.Fatal(e)
	}
	failed := first[0].ReferenceRequestID
	transport := batchMirrorFunc{batch: func(_ context.Context, _ uint64, _ string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
		rows := make([]raftrpc.GeoMirrorBatchResult, len(commands))
		for i, b := range commands {
			c, e := routedstate.DecodeCommand(b)
			if e != nil {
				return nil, e
			}
			if c.GeoMetadata.ReferenceRequestID == failed {
				rows[i].Err = status.Error(codes.Unavailable, "temporarily unavailable")
			} else {
				rows[i].Result = enginestate.Result{RequestID: c.RequestID, Index: 10, Applied: true}
			}
		}
		return rows, nil
	}}
	resolve := func(_ context.Context, m routedstate.GeoMetadataMirror) (placement.LocalPlan, error) {
		for _, r := range plan.Regions() {
			if r.RegionID == m.RegionID {
				return r, nil
			}
		}
		return placement.LocalPlan{}, errors.New("missing region")
	}
	report, e := RetryPendingGeoMetadata(context.Background(), transport, store, resolve)
	if e != nil || report.Attempted != 8 || report.Completed != 7 || report.Pending != 13 {
		t.Fatal(report, e)
	}
	if e = store.Close(); e != nil {
		t.Fatal(e)
	}
	store, e = OpenGeoMetadataRetryStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	next, count, e := store.NextPage(8)
	if e != nil || count != 13 || len(next) != 8 {
		t.Fatal(next, count, e)
	}
	for _, m := range next {
		if m.ReferenceRequestID == failed {
			t.Fatal("failed row displaced untouched work after restart")
		}
	}
}

// A bounded diagnostic of equal artificial WAN/consensus waits. No timing
// threshold is asserted: the real VPS Join remains the performance authority.
func TestGeoBatchRoundTripDiagnostic(t *testing.T) {
	w, refs, base, _, _, _ := geoBatchFixture(t, 32)
	w.Barrier = delayedPageBarrier{base}
	single := w.MirrorGeo
	calls := 0
	var mu sync.Mutex
	delay := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
			return nil
		}
	}
	w.MirrorGeo = batchMirrorFunc{single: func(ctx context.Context, n uint64, g string, b []byte) (enginestate.Result, error) {
		if e := delay(ctx); e != nil {
			return enginestate.Result{}, e
		}
		mu.Lock()
		calls++
		mu.Unlock()
		return single.MirrorGeo(ctx, n, g, b)
	}, batch: func(ctx context.Context, n uint64, g string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
		if e := delay(ctx); e != nil {
			return nil, e
		}
		mu.Lock()
		calls++
		mu.Unlock()
		rows := make([]raftrpc.GeoMirrorBatchResult, len(commands))
		for i, b := range commands {
			rows[i].Result, rows[i].Err = single.MirrorGeo(ctx, n, g, b)
		}
		return rows, nil
	}}
	started := time.Now()
	rows, e := w.RefreshGeoConfirmationPage(context.Background(), refs, 3, 3)
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range rows {
		if r.Err != nil || !r.Confirmed {
			t.Fatal(r)
		}
	}
	t.Logf("diagnostic: elapsed_ms=%d owner_cuts=%d mirror_rpcs=%d", time.Since(started).Milliseconds(), base.ownerCalls, calls)
}

func TestStagedGeoConfirmationRejectsPublicationChangedAfterPhysicalPreparation(t *testing.T) {
	w, refs, _, _, _, _ := geoBatchFixture(t, 8)
	single := w.MirrorGeo
	w.MirrorGeo = batchMirrorFunc{single: single.MirrorGeo, batch: func(ctx context.Context, n uint64, g string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
		rows := make([]raftrpc.GeoMirrorBatchResult, len(commands))
		for i, b := range commands {
			rows[i].Result, rows[i].Err = single.MirrorGeo(ctx, n, g, b)
		}
		return rows, nil
	}}
	submit := w.Submit
	var mu sync.Mutex
	changed := false
	w.Submit = func(ctx context.Context, g string, b []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if !changed {
			changed = true
			ref := refs[len(refs)-1]
			target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID, OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
			if err := w.Owner.ApplyRouted(w.Owner.AppliedIndex()+1, "overwrite-after-prepare", target, []byte("publish")); err != nil {
				return err
			}
		}
		return submit(ctx, g, b)
	}
	rows, e := w.RefreshGeoConfirmationPage(context.Background(), refs, 3, 3)
	if e != nil {
		t.Fatal(e)
	}
	if rows[len(rows)-1].Confirmed || rows[len(rows)-1].Err == nil {
		t.Fatal("old prepared publication cleared debt", rows[len(rows)-1])
	}
	for _, row := range rows[:len(rows)-1] {
		if row.Err != nil || !row.Confirmed {
			t.Fatal("changed row prevented unrelated confirmation", row)
		}
	}
}

func TestStagedGeoConfirmationKeepsFreshFinalRootAndRegionChecks(t *testing.T) {
	for _, mode := range []string{"quorum", "region", "route", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			w, refs, b, root, _, _ := geoBatchFixture(t, 8)
			single := w.MirrorGeo
			w.MirrorGeo = batchMirrorFunc{single: single.MirrorGeo, batch: func(ctx context.Context, n uint64, g string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
				rows := make([]raftrpc.GeoMirrorBatchResult, len(commands))
				for i, x := range commands {
					rows[i].Result, rows[i].Err = single.MirrorGeo(ctx, n, g, x)
				}
				return rows, nil
			}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.final = func() error {
				switch mode {
				case "quorum":
					return errors.New("Root lost quorum")
				case "region":
					root.view.Regions[0].Progress.Current.Generation++
					root.view.Regions[0].Progress.Head.CurrentGeneration++
				case "route":
					root.mapping.RangeEpoch++
				case "cancel":
					cancel()
				}
				return nil
			}
			if rows, e := w.RefreshGeoConfirmationPage(ctx, refs, 3, 3); e == nil || rows != nil {
				t.Fatal("unsafe staged page exposed success", rows, e)
			}
		})
	}
}
