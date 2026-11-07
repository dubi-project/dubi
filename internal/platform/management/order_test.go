package management

import (
	"context"
	v1 "github.com/dubi-project/dubi/api/v1"
	"testing"
)

func TestTopologyCreationOrderAndVolumeContinuation(t *testing.T) {
	ctx := context.Background()
	service := New(testSource{directory: Directory{Members: []Member{
		{NodeID: "a", RegionID: "a", CreatedIndex: 10, RegionCreatedIndex: 10},
		{NodeID: "z", RegionID: "z", CreatedIndex: 20, RegionCreatedIndex: 20},
	}}, observe: func(_ context.Context, m Member) (Observation, error) {
		o := goodObservation()
		offset := int64(0)
		if m.NodeID == "z" {
			offset = 1
		}
		o.Volumes = []v1.ManagedVolume{{VolumeID: "a", CreatedUnixNano: 100 + offset}, {VolumeID: "z", CreatedUnixNano: 200 + offset}, {VolumeID: "legacy"}}
		return o, nil
	}})
	d, err := service.Directory(ctx)
	if err != nil || d.Members[0].NodeID != "z" {
		t.Fatal(d, err)
	}
	regions := Regions(service.Nodes(ctx, d.Members))
	if regions[0].RegionID != "z" {
		t.Fatal(regions)
	}
	local, err := service.Volumes(ctx, d.Members[0])
	if err != nil || local[0].VolumeID != "z" || local[2].VolumeID != "legacy" {
		t.Fatal(local, err)
	}
	// The collector keeps a bounded page while merging asynchronously observed nodes.
	cursor := ""
	seen := map[string]bool{}
	var previous string
	for range 6 {
		rows, complete, problems := service.ClusterVolumes(ctx, d.Members, cursor, 1, "")
		if !complete || len(problems) != 0 || len(rows) == 0 || len(rows) > 2 {
			t.Fatal(rows, complete, problems)
		}
		key := VolumeKey(rows[0])
		if key <= previous || seen[key] {
			t.Fatal("unstable volume pagination", rows)
		}
		seen[key] = true
		previous = key
		cursor = key
	}
	rows, _, _ := service.ClusterVolumes(ctx, d.Members, cursor, 1, "")
	if len(rows) != 0 || len(seen) != 6 {
		t.Fatal("volume continuation lost or repeated rows")
	}
}
