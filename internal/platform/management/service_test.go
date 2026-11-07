package management

import (
	"context"
	"errors"
	v1 "github.com/dubi-project/dubi/api/v1"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testSource struct {
	directory Directory
	observe   func(context.Context, Member) (Observation, error)
}

func (s testSource) Directory(context.Context) (Directory, error) { return s.directory, nil }
func (s testSource) Observe(ctx context.Context, m Member) (Observation, error) {
	return s.observe(ctx, m)
}
func goodObservation() Observation {
	return Observation{ObservedUnix: time.Now().Unix(), Complete: true, Health: v1.OperationalHealth{Status: "healthy", Reasons: []v1.ObservationError{}}, Volumes: []v1.ManagedVolume{{VolumeID: "v1"}}, Maintenance: []v1.MaintenanceActivity{}}
}
func TestCoalescingCancellationAndStaleFallback(t *testing.T) {
	var calls atomic.Int32
	var fail atomic.Bool
	gate := make(chan struct{})
	source := testSource{observe: func(ctx context.Context, m Member) (Observation, error) {
		calls.Add(1)
		select {
		case <-gate:
		case <-ctx.Done():
			return Observation{}, ctx.Err()
		}
		if fail.Load() {
			return Observation{}, errors.New("offline")
		}
		return goodObservation(), nil
	}}
	service := New(source)
	m := Member{NodeID: "a", RegionID: "r"}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nodes := service.Nodes(context.Background(), []Member{m})
			if !nodes[0].Complete {
				t.Error("incomplete cached observation")
			}
		}()
	}
	close(gate)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("duplicate fanout: %d", calls.Load())
	}
	fail.Store(true)
	service.mu.Lock()
	service.cache["a"].checked = time.Time{}
	service.mu.Unlock()
	n := service.Nodes(context.Background(), []Member{m})[0]
	if n.Complete || !n.Stale || n.Status != "unavailable" || n.VolumeCount == nil {
		t.Fatalf("stale fallback: %+v", n)
	}
	service.mu.Lock()
	service.cache["a"].value.ObservedUnix = time.Now().Add(-2 * time.Minute).Unix()
	service.mu.Unlock()
	n = service.Nodes(context.Background(), []Member{m})[0]
	if n.ObservedUnix != nil || n.Maintenance != nil || n.VolumeCount != nil {
		t.Fatalf("expired cache remains usable: %+v", n)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n = service.Nodes(ctx, []Member{m})[0]
	if n.Status != "unknown" {
		t.Fatalf("request cancellation presented as node failure: %+v", n)
	}
}
func TestUnavailableRegionRetainedAndCapacityNotFabricated(t *testing.T) {
	source := testSource{observe: func(_ context.Context, m Member) (Observation, error) {
		if m.RegionID == "offline" {
			return Observation{}, errors.New("offline")
		}
		return goodObservation(), nil
	}}
	service := New(source)
	nodes := service.Nodes(context.Background(), []Member{{NodeID: "a", RegionID: "offline"}, {NodeID: "b", RegionID: "offline"}, {NodeID: "c", RegionID: "up"}})
	regions := Regions(nodes)
	if len(regions) != 2 || regions[0].NodeCount != 2 || regions[0].Status != "unavailable" || regions[0].Complete || regions[0].Capacity != nil {
		t.Fatalf("region: %+v", regions)
	}
	if nodes[0].ObservedUnix != nil || nodes[0].Capacity != nil || nodes[0].VolumeCount != nil {
		t.Fatalf("unknown turned into zero: %+v", nodes[0])
	}
}
func TestObservationConcurrencyBound(t *testing.T) {
	var active, peak atomic.Int32
	source := testSource{observe: func(ctx context.Context, _ Member) (Observation, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			return Observation{}, ctx.Err()
		}
		return goodObservation(), nil
	}}
	service := New(source)
	members := make([]Member, 40)
	for i := range members {
		members[i].NodeID = string(rune('A' + i))
	}
	service.Nodes(context.Background(), members)
	if peak.Load() > 8 || peak.Load() < 2 {
		t.Fatalf("concurrency=%d", peak.Load())
	}
}

func TestObservationCacheHasBoundedRetention(t *testing.T) {
	service := New(testSource{})
	for i := range 300 {
		id := string(rune(i + 1))
		service.cache[id] = &cached{weight: 100000, checked: time.Now()}
		service.cacheBytes += 100000
	}
	service.trimCache()
	if len(service.cache) > 256 || service.cacheBytes > 16<<20 {
		t.Fatalf("unbounded retention entries=%d bytes=%d", len(service.cache), service.cacheBytes)
	}
}
