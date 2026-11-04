package recordwrite

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type localMirrorStore struct {
	Store
	id uint64
}

func (s localMirrorStore) Verify(ctx context.Context, d records.Descriptor) (records.Holding, error) {
	h, err := s.Store.Verify(ctx, d)
	h.StoreID = fmt.Sprintf("store_%032x", s.id)
	return h, err
}

type stalledOwnerMirror struct {
	GeoMetadataMirrorTransport
	group    string
	attempts atomic.Int32
}

func (m *stalledOwnerMirror) MirrorGeo(ctx context.Context, target uint64, group string, b []byte) (enginestate.Result, error) {
	if group == m.group {
		m.attempts.Add(1)
		<-ctx.Done()
		return enginestate.Result{}, ctx.Err()
	}
	return m.GeoMetadataMirrorTransport.MirrorGeo(ctx, target, group, b)
}

func assertLocalGeoMirrors(t *testing.T, w Writer, refs []enginestate.RecordReference) {
	t.Helper()
	for _, ref := range refs {
		target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID, OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
		_, err := w.Owner.ReadRouted(context.Background(), liveOwnerBarrier{w.Owner}, target, func(v enginestate.ReadView) error {
			m, found, err := v.GeoMetadata(ref.Descriptor.Key)
			if err != nil {
				return err
			}
			if !found || m.Index == 0 || m.Mirror.DestinationGroupID != w.Owner.GroupID() || m.Mirror.ReferenceIndex != ref.Index || m.Mirror.ReferenceRequestID != ref.RequestID || m.Mirror.ReferenceCommandHash != ref.CommandHash {
				return errors.New("local Geo checkpoint is not durably bound to the exact publication")
			}
			r, found, err := w.Owner.ResultForGeoMetadataMirror(m.RequestID, m.Mirror)
			if err != nil || !found || !r.Applied || r.Error != "" || r.Index != m.Index {
				return errors.New("local mirror lacks its exact durable Result")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestGeoReconfirmationUsesLocalOwnerInsteadOfStalledPeer(t *testing.T) {
	for _, local := range []uint64{1, 2, 3} {
		t.Run(fmt.Sprint(local), func(t *testing.T) {
			w, refs, _, root, _, remote := geoBatchFixture(t, 8)
			w.LocalID, w.LocalNodeID = local, root.members[local-1].NodeID
			w.Store = localMirrorStore{w.Store, local}
			stalled := &stalledOwnerMirror{GeoMetadataMirrorTransport: w.MirrorGeo, group: w.Owner.GroupID()}
			w.MirrorGeo = stalled
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			results, err := w.RefreshGeoConfirmationPage(ctx, refs, 3, 3)
			if err != nil || len(results) != 8 || stalled.attempts.Load() != 0 || len(remote.mirrors) != 8 {
				t.Fatal("Owner mirror was forwarded to a stalled peer", err, len(results), stalled.attempts.Load(), len(remote.mirrors))
			}
			for _, r := range results {
				if !r.Confirmed || r.Err != nil {
					t.Fatal("local mirror did not complete durable Geo confirmation", r)
				}
			}
			assertLocalGeoMirrors(t, w, refs)
		})
	}
}

func localMirrorCommand(t *testing.T, w Writer, ref enginestate.RecordReference) (routedstate.GeoMetadataMirror, []byte) {
	t.Helper()
	_, plan, err := placement.ReadRangeGeoPlanAtCut(5, w.Root.(rangeGeoRoot), ref.RangeID, 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, region := range plan.Regions() {
		if region.GroupID != w.Owner.GroupID() {
			continue
		}
		id, _ := ref.Descriptor.ID()
		m := routedstate.GeoMetadataMirror{Target: router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID, OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}, Key: ref.Descriptor.Key,
			DestinationGroupID: region.GroupID, RegionID: region.RegionID, Generation: region.Generation, GroupEpoch: region.GroupEpoch, FenceIndex: region.FenceIndex, RecordID: id,
			ReferenceRequestID: ref.RequestID, ReferenceIndex: ref.Index, ReferenceCommandHash: ref.CommandHash, Descriptor: ref.Descriptor}
		request, _ := mirrorRequestID(m)
		b, err := routedstate.EncodeGeoMetadataMirror(request, m)
		if err != nil {
			t.Fatal(err)
		}
		return m, b
	}
	t.Fatal("fixture has no local Region")
	return routedstate.GeoMetadataMirror{}, nil
}

func TestGeoLocalMirrorCannotTreatQueuedProposalAsDurable(t *testing.T) {
	w, refs, _, _, _, _ := geoBatchFixture(t, 1)
	_, encoded := localMirrorCommand(t, w, refs[0])
	w.Submit = func(context.Context, string, []byte) error { return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	r, err := (geoReconfirmationMirrors{w}).MirrorGeo(ctx, w.LocalID, w.Owner.GroupID(), encoded)
	if !errors.Is(err, context.DeadlineExceeded) || r.Index != 0 || r.Applied {
		t.Fatal("queued proposal became durable mirror success", r, err)
	}
}

type noMirrorQuorum struct{ Barrier }

func (b noMirrorQuorum) ReadIndex(context.Context, string) (uint64, error) {
	return 0, errors.New("quorum unavailable")
}

func TestGeoLocalMirrorNeedsFreshQuorumAndExactCommand(t *testing.T) {
	for _, mode := range []string{"quorum", "reused-id", "fence-rejection", "foreign-group", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			w, refs, _, _, _, _ := geoBatchFixture(t, 1)
			m, encoded := localMirrorCommand(t, w, refs[0])
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			group := w.Owner.GroupID()
			switch mode {
			case "quorum":
				w.Barrier = noMirrorQuorum{w.Barrier}
			case "reused-id":
				command, _ := routedstate.DecodeCommand(encoded)
				changed := m
				changed.Generation++
				other, _ := routedstate.EncodeGeoMetadataMirror(command.RequestID, changed)
				if err := w.Submit(ctx, group, other); err != nil {
					t.Fatal(err)
				}
			case "fence-rejection":
				m.Generation++
				id, _ := mirrorRequestID(m)
				encoded, _ = routedstate.EncodeGeoMetadataMirror(id, m)
			case "foreign-group":
				group = "region-foreign"
			case "canceled":
				cancel()
			}
			r, err := (geoReconfirmationMirrors{w}).MirrorGeo(ctx, w.LocalID, group, encoded)
			if mode == "fence-rejection" {
				if err != nil || r.Applied || r.Index == 0 || r.Error == "" {
					t.Fatal("lost durable fence rejection", r, err)
				}
			} else if err == nil || r.Applied || r.Index != 0 {
				t.Fatal("invalid local mirror returned durable success", mode, r, err)
			}
			if mode == "foreign-group" && status.Code(err) != codes.InvalidArgument {
				t.Fatal(err)
			}
		})
	}
}
