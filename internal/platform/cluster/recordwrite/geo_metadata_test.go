package recordwrite

import (
	"context"
	"errors"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/capstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/placement"
)

type geoMirrorTransport struct {
	failed   map[uint64]bool
	attempts []uint64
}

func (t *geoMirrorTransport) MirrorGeo(_ context.Context, node uint64, _ string, encoded []byte) (capstate.Result, error) {
	t.attempts = append(t.attempts, node)
	if t.failed[node] {
		return capstate.Result{}, errors.New("node unavailable")
	}
	command, err := routedstate.DecodeCommand(encoded)
	if err != nil || command.Type != routedstate.MirrorGeo {
		return capstate.Result{}, err
	}
	return capstate.Result{RequestID: command.RequestID, Index: node, Applied: true}, nil
}

func TestReplicateGeoMetadataRetriesAnotherVoter(t *testing.T) {
	plan, target, descriptor := geoStagePlan(t)
	staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, descriptor, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	recordID, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: "publish-1", Index: 9,
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:     target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, err := GeoMetadataMirrors(target, descriptor, publication, plan, staged.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	transport := &geoMirrorTransport{failed: map[uint64]bool{1: true, 4: true}}
	results, err := ReplicateGeoMetadata(context.Background(), transport, plan, mirrors)
	if err != nil || len(results) != 2 {
		t.Fatalf("replication results = %+v, err %v", results, err)
	}
	if len(transport.attempts) != 4 || transport.attempts[0] != 4 || transport.attempts[1] != 5 ||
		transport.attempts[2] != 1 || transport.attempts[3] != 2 {
		t.Fatalf("voter retry order = %v", transport.attempts)
	}
	if err := mirrors[0].Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicateGeoMetadataIdentifiesRetryableRegionDebt(t *testing.T) {
	plan, target, descriptor := geoStagePlan(t)
	staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, descriptor, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	recordID, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: "publish-2", Index: 10,
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:     target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, err := GeoMetadataMirrors(target, descriptor, publication, plan, staged.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	transport := &geoMirrorTransport{failed: map[uint64]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true}}
	_, err = ReplicateGeoMetadata(context.Background(), transport, plan, mirrors)
	var debt *GeoMetadataReplicationError
	if !errors.As(err, &debt) || !debt.Retryable || debt.RegionID == "" || debt.Mirror.RegionID != debt.RegionID {
		t.Fatal("retryable Region debt was not identified", err)
	}
}

func TestGeoMetadataRetryStoreSurvivesAndClearsDeliveryDebt(t *testing.T) {
	plan, target, descriptor := geoStagePlan(t)
	staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, descriptor, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	recordID, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: "publish-3", Index: 11,
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:     target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, err := GeoMetadataMirrors(target, descriptor, publication, plan, staged.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenGeoMetadataRetryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	transport := &geoMirrorTransport{failed: map[uint64]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true}}
	if _, err := ReplicateGeoMetadataWithRetryStore(context.Background(), transport, plan, mirrors, store); err == nil {
		t.Fatal("unreachable mirror delivery was reported successful")
	}
	pending, err := store.List()
	if err != nil || len(pending) != len(mirrors) {
		t.Fatal("retry ledger did not retain all mirror debt", pending, err)
	}
	transport.failed = nil
	if _, err := ReplicateGeoMetadataWithRetryStore(context.Background(), transport, plan, pending, store); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.List(); err != nil || len(pending) != 0 {
		t.Fatal("completed mirror debt was not cleared", pending, err)
	}
}

func TestRetryPendingGeoMetadataKeepsStalePlanDebt(t *testing.T) {
	plan, target, descriptor := geoStagePlan(t)
	staged, err := StageGeo(context.Background(), plan, 1, "node_00000000000000000000000000000001", &testStore{}, &geoStageSender{}, descriptor, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	recordID, err := descriptor.ID()
	if err != nil {
		t.Fatal(err)
	}
	publication := placement.PublicationCut{RecordID: recordID, RequestID: "publish-4", Index: 12,
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		RangeID:     target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, err := GeoMetadataMirrors(target, descriptor, publication, plan, staged.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenGeoMetadataRetryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, mirror := range mirrors {
		if err := store.Save(mirror); err != nil {
			t.Fatal(err)
		}
	}
	transport := &geoMirrorTransport{}
	report, err := RetryPendingGeoMetadata(context.Background(), transport, store, func(context.Context, routedstate.GeoMetadataMirror) (placement.LocalPlan, error) {
		return placement.LocalPlan{}, errors.New("Root plan generation changed")
	})
	if err != nil || report.Attempted != 2 || report.Completed != 0 || report.Pending != 2 {
		t.Fatal(report, err)
	}
	if pending, err := store.List(); err != nil || len(pending) != 2 {
		t.Fatal("stale mirror debt was dropped", pending, err)
	}
}
