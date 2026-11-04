package recordwrite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

type geoBatchRoot struct {
	reconfirmRoot
	mapping rootstate.RangePlacement
}

func (r *geoBatchRoot) RangeRegionPlacementView(string, int, int) (rootstate.RangePlacement, rootstate.RegionPlacementView, error) {
	return r.mapping, r.view, nil
}

type geoBatchObserver struct {
	mu sync.Mutex
	reconfirmCutObserver
	absent bool
}

func (o *geoBatchObserver) ObserveRecordHolding(ctx context.Context, id uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.absent && id == 4 {
		return raftrpc.HoldingObservation{}, errors.New("east has no physical copy")
	}
	return o.reconfirmCutObserver.ObserveRecordHolding(ctx, id, d)
}
func geoBatchFixture(t *testing.T, count int) (Writer, []enginestate.RecordReference, *repairBatchBarrier, *geoBatchRoot, *geoBatchObserver, *geoMirrorTransport) {
	t.Helper()
	w, refs, b, base, _ := repairBatchFixture(t, count)
	eastNode := rootstate.Node{RaftID: 4, NodeID: fmt.Sprintf("node_%032x", 4), RegionID: "east", Address: "e1:7443"}
	east := base.view
	east.Head = rootstate.RegionCohortHead{RegionID: "east", CurrentGeneration: 1}
	east.Current.RegionID, east.Current.GroupID, east.Current.Nodes = "east", "region-east", []uint64{4}
	east.Group = rootstate.Group{GroupID: "region-east", Epoch: 2, Members: []rootstate.GroupMember{{RaftID: 4, Role: rootstate.GroupMemberVoter}}}
	r := &geoBatchRoot{reconfirmRoot: reconfirmRoot{testRoot: base, view: rootstate.RegionPlacementView{AppliedIndex: 5, Regions: []rootstate.RegionPlacement{{Progress: base.view, Nodes: base.members}, {Progress: east, Nodes: []rootstate.Node{eastNode}}}}}}
	o := &geoBatchObserver{reconfirmCutObserver: reconfirmCutObserver{t: t, nodes: map[uint64]rootstate.Node{4: eastNode}}}
	for _, n := range base.members {
		o.nodes[n.RaftID] = n
	}
	mirrors := &geoMirrorTransport{}
	w.Root, w.Observer, w.MirrorGeo = r, o, &lockedGeoMirror{base: mirrors}
	submit := w.Submit
	var submitMu sync.Mutex
	w.Submit = func(ctx context.Context, group string, data []byte) error {
		submitMu.Lock()
		defer submitMu.Unlock()
		return submit(ctx, group, data)
	}
	return w, refs, b, r, o, mirrors
}
func TestGeoConfirmationRepairPageKeepsPhysicalAndDurableProofWithTwoRootCuts(t *testing.T) {
	w, refs, b, _, o, mirrors := geoBatchFixture(t, 32)
	results, err := w.RefreshGeoConfirmationPage(context.Background(), refs, 3, 3)
	if err != nil || len(results) != 32 || b.rootCalls != 2 || o.calls != 64 || len(mirrors.mirrors) != 32 {
		t.Fatal(err, len(results), b.rootCalls, o.calls, len(mirrors.mirrors))
	}
	for i, r := range results {
		if !r.Confirmed || r.Err != nil {
			t.Fatal(i, r)
		}
		ref := refs[i]
		target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID, OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
		_, err := w.Owner.ReadRouted(context.Background(), liveOwnerBarrier{w.Owner}, target, func(v enginestate.ReadView) error {
			receipt, found, err := v.GeoCoverage(ref.Descriptor.Key)
			if err != nil {
				return err
			}
			if !found || receipt.Claim.Validate() != nil || receipt.Claim.ReferenceIndex != ref.Index || receipt.ReferenceCommandHash != ref.CommandHash {
				return errors.New("missing exact durable Geo receipt")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	assertLocalGeoMirrors(t, w, refs)
	results, err = w.RefreshGeoConfirmationPage(context.Background(), refs, 3, 3)
	if err != nil || b.rootCalls != 4 || o.calls != 64 || len(mirrors.mirrors) != 32 {
		t.Fatal("already-current receipts repeated physical work", err, b.rootCalls, o.calls)
	}
	for _, r := range results {
		if r.Confirmed || r.Err != nil {
			t.Fatal("historical receipt was returned as a fresh physical proof", r)
		}
	}
}

func TestGeoConfirmationSmallPageDoesNotSerializeTheLastTwoPublications(t *testing.T) {
	w, refs, _, _, _, _ := geoBatchFixture(t, 8)
	submit := w.Submit
	entered := make(chan struct{}, len(refs))
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Submit = func(ctx context.Context, group string, encoded []byte) error {
		entered <- struct{}{}
		<-release
		return submit(ctx, group, encoded)
	}
	done := make(chan error, 1)
	go func() {
		results, err := w.RefreshGeoConfirmationPage(ctx, refs, 3, 3)
		for _, result := range results {
			err = errors.Join(err, result.Err)
		}
		done <- err
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
	for range refs {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("small Geo page serialized its last two publication waits")
		}
	}
	close(release)
	released = true
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Geo page did not finish after its publication waits were released")
	}
}
func TestGeoConfirmationRepairPageFailsClosedAcrossChanges(t *testing.T) {
	for _, mode := range []string{"quorum-loss", "plan-change", "route-change", "physical-minority", "mirror-rejection", "behind-root", "too-many", "cancel-at-final"} {
		t.Run(mode, func(t *testing.T) {
			w, refs, b, root, o, mirrors := geoBatchFixture(t, 32)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "quorum-loss":
				b.final = func() error { return errors.New("Root quorum lost") }
			case "plan-change":
				b.final = func() error {
					root.view.Regions[0].Progress.Current.Generation++
					root.view.Regions[0].Progress.Head.CurrentGeneration++
					return nil
				}
			case "route-change":
				b.final = func() error { root.mapping.RangeEpoch++; return nil }
			case "physical-minority":
				o.absent = true
			case "mirror-rejection":
				mirrors.failed = map[uint64]bool{4: true}
			case "behind-root":
				root.view.AppliedIndex = 4
			case "too-many":
				for len(refs) <= GeoConfirmationRepairPageLimit {
					refs = append(refs, refs[0])
				}
			case "cancel-at-final":
				b.final = func() error { cancel(); return nil }
			}
			results, err := w.RefreshGeoConfirmationPage(ctx, refs, 3, 3)
			if mode == "physical-minority" || mode == "mirror-rejection" {
				if err != nil || len(results) != 32 {
					t.Fatal(err, results)
				}
				for _, r := range results {
					if r.Confirmed || r.Err == nil {
						t.Fatal("unsafe row may clear debt", r)
					}
				}
				return
			}
			if err == nil || results != nil {
				t.Fatal("unsafe page exposed debt-clearing results", err, results)
			}
			if mode == "behind-root" && o.calls != 0 {
				t.Fatal("unapplied cut reached physical IO")
			}
			if mode == "too-many" && b.rootCalls != 0 {
				t.Fatal("oversized page reached Root")
			}
		})
	}
}

type lockedGeoMirror struct {
	mu   sync.Mutex
	base *geoMirrorTransport
}

func (m *lockedGeoMirror) MirrorGeo(ctx context.Context, id uint64, group string, data []byte) (enginestate.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.base.MirrorGeo(ctx, id, group, data)
}

type geoGateObserver struct {
	base    Observer
	entered chan struct{}
	release chan struct{}
}

func (o geoGateObserver) ObserveRecordHolding(ctx context.Context, id uint64, d records.Descriptor) (raftrpc.HoldingObservation, error) {
	o.entered <- struct{}{}
	<-o.release
	return o.base.ObserveRecordHolding(ctx, id, d)
}
func TestGeoConfirmationPageBoundsWANConcurrencyAndWaitsForCanceledWorkers(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelEarly), func(t *testing.T) {
			w, refs, _, _, _, _ := geoBatchFixture(t, 32)
			gate := geoGateObserver{base: w.Observer, entered: make(chan struct{}, 64), release: make(chan struct{})}
			w.Observer = gate
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				results, err := w.RefreshGeoConfirmationPage(ctx, refs, 3, 3)
				if !cancelEarly && err == nil {
					for _, r := range results {
						if !r.Confirmed || r.Err != nil {
							err = errors.New("missing completed confirmation")
						}
					}
				}
				done <- err
			}()
			released := false
			defer func() {
				if !released {
					close(gate.release)
				}
			}()
			for range geoConfirmationPageConcurrency {
				select {
				case <-gate.entered:
				case err := <-done:
					t.Fatal("page returned before bounded WAN work", err)
				case <-time.After(5 * time.Second):
					t.Fatal("WAN calls were unnecessarily serialized")
				}
			}
			select {
			case <-gate.entered:
				t.Fatal("page exceeded its WAN concurrency bound")
			default:
			}
			if cancelEarly {
				cancel()
				select {
				case err := <-done:
					t.Fatal("cancellation returned while IO workers still held their page", err)
				case <-time.After(50 * time.Millisecond):
				}
			}
			close(gate.release)
			released = true
			select {
			case err := <-done:
				if cancelEarly && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if !cancelEarly && err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("page workers did not exit")
			}
		})
	}
}
func TestGeoConfirmationLargePagesKeepSequentialMemoryUse(t *testing.T) {
	refs := make([]enginestate.RecordReference, 32)
	for i := range refs {
		refs[i].Descriptor.Size = geoConfirmationConcurrentPageBytes / 32
	}
	if got := geoConfirmationWorkers(refs); got != geoConfirmationPageConcurrency {
		t.Fatal("small page lost bounded concurrency", got)
	}
	if len(GeoConfirmationRepairPage(refs)) != 32 {
		t.Fatal("small publications did not share their Root barriers")
	}
	refs[0].Descriptor.Size++
	if got := geoConfirmationWorkers(refs); got != 1 {
		t.Fatal("aggregate body bound was exceeded", got)
	}
	page := GeoConfirmationRepairPage(refs)
	if len(page) != 8 || geoConfirmationWorkers(page) != 8 {
		t.Fatal("larger payloads lost their previous bounded concurrent page")
	}
	refs[0].Descriptor.Size = 8 << 20
	if got := geoConfirmationWorkers(refs); got != 1 {
		t.Fatal("large verification became concurrent", got)
	}
	if page := GeoConfirmationRepairPage(refs); len(page) != 8 || geoConfirmationWorkers(page) != 1 {
		t.Fatal("large verification lost its original sequential page")
	}
}
