package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pipelineHoldings struct {
	mu   sync.Mutex
	held map[string]bool
}

func (h *pipelineHoldings) verify(d records.Descriptor) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.held[string(d.Key)] {
		return nil
	}
	return records.ErrNotHeld
}
func (h *pipelineHoldings) put(ds []records.Descriptor) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, d := range ds {
		h.held[string(d.Key)] = true
	}
}
func (h *pipelineHoldings) count() int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.held) }
func pipelineReferences(t *testing.T, count int) []enginestate.RecordReference {
	t.Helper()
	var refs []enginestate.RecordReference
	for i := 0; i < count; i++ {
		d, err := records.Describe(rootstate.Keyspace{TenantID: "t", EngineID: "record", DatasetID: "d"}, "region-west", []byte(fmt.Sprintf("key-%04d", i)), []byte("data"))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, enginestate.RecordReference{Descriptor: d})
	}
	return refs
}
func runPipeline(ctx context.Context, refs []enginestate.RecordReference, held *pipelineHoldings, fetch fetcher, batch batchFetcher) (Report, error) {
	return fillModePipelined(ctx, []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, testSources(), false, testWalk(refs), held.verify, fetch, batch)
}

func TestFillPipelineOverlapsOnlyTwoPagesAndJoinsCanceledIO(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelEarly), func(t *testing.T) {
			refs := pipelineReferences(t, 96)
			held := &pipelineHoldings{held: map[string]bool{}}
			entered := make(chan struct{}, 4)
			release := make(chan struct{})
			var active, peak atomic.Int32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			batch := func(ctx context.Context, _ rootstate.Node, ds []records.Descriptor) error {
				n := active.Add(1)
				defer active.Add(-1)
				for {
					old := peak.Load()
					if n <= old || peak.CompareAndSwap(old, n) {
						break
					}
				}
				if len(ds) > 32 {
					return errors.New("page exceeded descriptor bound")
				}
				var size uint64
				for _, d := range ds {
					size += d.Size
				}
				if size > 4<<20 {
					return errors.New("page exceeded payload bound")
				}
				entered <- struct{}{}
				<-release // Retain ownership even after cancellation.
				if err := ctx.Err(); err != nil {
					return err
				}
				held.put(ds)
				return nil
			}
			type outcome struct {
				report Report
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				r, e := runPipeline(ctx, refs, held, func(context.Context, rootstate.Node, records.Descriptor) error {
					return errors.New("unexpected individual fallback")
				}, batch)
				done <- outcome{r, e}
			}()
			released := false
			defer func() {
				if !released {
					cancel()
					close(release)
					select {
					case <-done:
					case <-time.After(5 * time.Second):
					}
				}
			}()
			for i := 0; i < 2; i++ {
				select {
				case <-entered:
				case r := <-done:
					t.Fatal("fill returned before overlapping pages", r)
				case <-time.After(time.Second):
					t.Fatal("small backfill pages remained serial")
				}
			}
			select {
			case <-entered:
				t.Fatal("more than two active pages")
			default:
			}
			if cancelEarly {
				cancel()
				select {
				case r := <-done:
					t.Fatal("fill returned with IO still owned", r)
				case <-time.After(30 * time.Millisecond):
				}
			}
			close(release)
			released = true
			select {
			case r := <-done:
				if active.Load() != 0 || peak.Load() != 2 {
					t.Fatal("unbounded or unjoined page workers", active.Load(), peak.Load())
				}
				if cancelEarly {
					if !errors.Is(r.err, context.Canceled) || r.report != (Report{}) {
						t.Fatal("canceled fill exposed complete report", r)
					}
				} else if r.err != nil || r.report.References != 96 || r.report.Fetched != 96 || held.count() != 96 {
					t.Fatal(r, held.count())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("page workers did not exit")
			}
		})
	}
}

func TestFillPipelineDrainsSmallPagesBeforeLargeRecord(t *testing.T) {
	refs := pipelineReferences(t, 129)
	refs[64].Descriptor.Size = raftrpc.FetchRecordBatchBytes + 1
	held := &pipelineHoldings{held: map[string]bool{}}
	var small, large atomic.Int32
	firstPages := make(chan struct{}, 2)
	releaseSmall := make(chan struct{})
	largeEntered := make(chan struct{}, 1)
	releaseLarge := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batch := func(ctx context.Context, _ rootstate.Node, ds []records.Descriptor) error {
		if large.Load() != 0 {
			return errors.New("small page overlapped oversized record")
		}
		small.Add(1)
		defer small.Add(-1)
		if string(ds[0].Key) < "key-0064" {
			firstPages <- struct{}{}
			select {
			case <-releaseSmall:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		held.put(ds)
		return nil
	}
	fetch := func(ctx context.Context, _ rootstate.Node, d records.Descriptor) error {
		if small.Load() != 0 || d.Size <= raftrpc.FetchRecordBatchBytes {
			return errors.New("large transfer did not drain pages")
		}
		large.Add(1)
		defer large.Add(-1)
		largeEntered <- struct{}{}
		select {
		case <-releaseLarge:
		case <-ctx.Done():
			return ctx.Err()
		}
		held.put([]records.Descriptor{d})
		return nil
	}
	done := make(chan error, 1)
	go func() {
		r, e := runPipeline(ctx, refs, held, fetch, batch)
		if e == nil && (r.References != 129 || r.Fetched != 129) {
			e = errors.New("incomplete mixed-size fill")
		}
		done <- e
	}()
	defer func() { cancel() }()
	for i := 0; i < 2; i++ {
		select {
		case <-firstPages:
		case e := <-done:
			t.Fatal(e)
		case <-time.After(time.Second):
			t.Fatal("small pages did not start")
		}
	}
	select {
	case <-largeEntered:
		t.Fatal("oversized record started before earlier IO drained")
	default:
	}
	close(releaseSmall)
	select {
	case <-largeEntered:
	case e := <-done:
		t.Fatal(e)
	case <-time.After(time.Second):
		t.Fatal("oversized record did not start")
	}
	if small.Load() != 0 {
		t.Fatal("small pages remained active during large transfer")
	}
	close(releaseLarge)
	select {
	case e := <-done:
		if e != nil || held.count() != 129 {
			t.Fatal(e, held.count())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mixed-size fill did not finish")
	}
}

func TestFillPipelineOwnsReusedDescriptorFrames(t *testing.T) {
	refs := pipelineReferences(t, 96)
	held := &pipelineHoldings{held: map[string]bool{}}
	walker := func(visit func(enginestate.RecordReference) error) error {
		buf := make([]byte, len(refs[0].Descriptor.Key))
		for _, r := range refs {
			copy(buf, r.Descriptor.Key)
			r.Descriptor.Key = buf
			if err := visit(r); err != nil {
				return err
			}
		}
		return nil
	}
	batch := func(_ context.Context, _ rootstate.Node, ds []records.Descriptor) error { held.put(ds); return nil }
	r, e := fillModePipelined(context.Background(), []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, testSources(), false, walker, held.verify, func(context.Context, rootstate.Node, records.Descriptor) error {
		return errors.New("unexpected individual fallback")
	}, batch)
	if e != nil || r.References != 96 || held.count() != 96 {
		t.Fatal("walker frame was retained without ownership", r, e, held.count())
	}
}

func TestFillPipelineBoundsAggregatePayloadAcrossPages(t *testing.T) {
	refs := pipelineReferences(t, 7)
	for i := range refs {
		refs[i].Descriptor.Size = 2 << 20
	}
	held := &pipelineHoldings{held: map[string]bool{}}
	var active, peak atomic.Int64
	batch := func(ctx context.Context, _ rootstate.Node, ds []records.Descriptor) error {
		var size uint64
		for _, d := range ds {
			size += d.Size
		}
		if size > 4<<20 {
			return errors.New("payload page exceeded four MiB")
		}
		n := active.Add(int64(size))
		defer active.Add(-int64(size))
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
		held.put(ds)
		return nil
	}
	r, err := runPipeline(context.Background(), refs, held,
		func(context.Context, rootstate.Node, records.Descriptor) error {
			return errors.New("unexpected individual transfer")
		}, batch)
	if err != nil || r.References != 7 || r.ReferencedBytes != 14<<20 || peak.Load() > 8<<20 || active.Load() != 0 {
		t.Fatal("aggregate payload escaped its bound", r, err, peak.Load(), active.Load())
	}
}

func TestFillPipelinePreservesFallbackAndRejectsUnsafeCompletion(t *testing.T) {
	for _, mode := range []string{"other-source", "disjoint", "unverified", "revoked", "corrupt-local", "invalid-source", "walker-error"} {
		t.Run(mode, func(t *testing.T) {
			refs := pipelineReferences(t, 80)
			held := &pipelineHoldings{held: map[string]bool{}}
			var individual atomic.Int32
			verify := func(d records.Descriptor) error {
				if mode == "corrupt-local" && string(d.Key) == "key-0035" {
					return records.ErrCorrupt
				}
				return held.verify(d)
			}
			batch := func(_ context.Context, s rootstate.Node, ds []records.Descriptor) error {
				switch mode {
				case "revoked":
					return status.Error(codes.PermissionDenied, "fenced")
				case "unverified":
					return nil
				case "disjoint":
					return status.Error(codes.NotFound, "no complete source")
				case "other-source":
					if s.RaftID == 1 {
						return status.Error(codes.Unavailable, "offline")
					}
				}
				held.put(ds)
				return nil
			}
			fetch := func(_ context.Context, s rootstate.Node, d records.Descriptor) error {
				individual.Add(1)
				if (d.Key[len(d.Key)-1]%2 == 0) != (s.RaftID == 1) {
					return status.Error(codes.NotFound, "other holder")
				}
				held.put([]records.Descriptor{d})
				return nil
			}
			sources := testSources()
			if mode == "invalid-source" {
				sources[0].RaftID = 9
			}
			walker := testWalk(refs)
			if mode == "walker-error" {
				walker = func(visit func(enginestate.RecordReference) error) error {
					for _, r := range refs[:70] {
						if err := visit(r); err != nil {
							return err
						}
					}
					return errors.New("cut digest rejected")
				}
			}
			r, e := fillModePipelined(context.Background(), []uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, 4, sources, false, walker, verify, fetch, batch)
			if mode == "other-source" || mode == "disjoint" {
				if e != nil || r.References != 80 || r.Fetched != 80 || held.count() != 80 {
					t.Fatal(r, e, held.count())
				}
				if mode == "disjoint" && individual.Load() < 80 {
					t.Fatal("individual fallback was skipped")
				}
			} else if e == nil || r != (Report{}) {
				t.Fatal("unsafe fill exposed complete report", r, e)
			}
			if (mode == "revoked" || mode == "unverified" || mode == "invalid-source") && individual.Load() != 0 {
				t.Fatal("unsafe fallback was permitted")
			}
		})
	}
}
