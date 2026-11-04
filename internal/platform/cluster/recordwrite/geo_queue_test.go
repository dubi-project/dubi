package recordwrite

import (
	"context"
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/placement"
)

func TestGeoMirrorQueuePersistsFairCursorAndCountsOnlyActualTasks(t *testing.T) {
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
		CommandHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", RangeID: target.RangeID, RangeEpoch: target.Epoch, RoutingKey: target.RoutingKey}
	mirrors, err := GeoMetadataMirrors(target, descriptor, publication, plan, staged.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	store, err := OpenGeoMetadataRetryStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	for i := 0; i < 20; i++ {
		mirror := mirrors[0]
		mirror.ReferenceRequestID = fmt.Sprintf("publication-%03d", i)
		if err := store.Save(mirror); err != nil {
			t.Fatal(err)
		}
	}
	first, count, err := store.NextPage(8)
	if err != nil || len(first) != 8 || count != 20 {
		t.Fatal(len(first), count, err)
	}
	if err := store.MarkAttempt(first[0]); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenGeoMetadataRetryStore(root)
	if err != nil {
		t.Fatal(err)
	}
	next, count, err := store.NextPage(8)
	if err != nil || len(next) != 8 || count != 20 || next[0].ReferenceRequestID == first[0].ReferenceRequestID {
		t.Fatal(next, count, err)
	}
	if err := store.Delete(next[0]); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(next[0]); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Count(); got != 19 {
		t.Fatal("scheduler hint or duplicate removal corrupted task count", got)
	}
}
