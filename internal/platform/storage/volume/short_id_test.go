package volume

import (
	"errors"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"strings"
	"testing"
)

func TestShortVolumeIDsReopenAndCollisionRetry(t *testing.T) {
	root := t.TempDir()
	id, err := identity.Bootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(id.NodeID) != len("node_")+12 || len(id.ClusterID) != len("cluster_")+12 {
		t.Fatal(id)
	}
	r, err := Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := r.manifest.StoreID
	vol := r.manifest.Volumes[0].ID
	if len(store) != len("store_")+12 || len(vol) != len("volume_")+12 {
		t.Fatal(store, vol)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.manifest.StoreID != store || r.manifest.Volumes[0].ID != vol {
		t.Fatal("reopen changed identity")
	}
	existing := []Record{{ID: "volume_000000000001", State: Active}, {ID: "volume_000000000002", State: Removed}}
	calls := 0
	got, err := uniqueVolumeID(existing, func() (string, error) {
		calls++
		return []string{existing[0].ID, existing[1].ID, "volume_000000000003"}[calls-1], nil
	})
	if err != nil || got != "volume_000000000003" || calls != 3 {
		t.Fatal(got, calls, err)
	}
	calls = 0
	_, err = uniqueVolumeID(existing, func() (string, error) { calls++; return existing[0].ID, nil })
	if err == nil || calls != 32 {
		t.Fatal("unbounded collision retry", calls, err)
	}
	sentinel := errors.New("random unavailable")
	_, err = uniqueVolumeID(existing, func() (string, error) { return "", sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	for _, prefix := range []string{"store_", "volume_"} {
		if !validID(prefix+strings.Repeat("f", 32), prefix) {
			t.Fatal("old ID rejected")
		}
	}
	resource, err := randomID("")
	if err != nil || len(resource) != 32 {
		t.Fatal("internal resource ID changed", err)
	}
}
