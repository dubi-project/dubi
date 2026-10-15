package volume

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStorageAccessLifecycleAndConfinement(t *testing.T) {
	root := testStore(t)
	r, err := Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	v := status(t, r).Volumes[0]
	check := func(root *os.Root, storeID string) error {
		if storeID != r.manifest.StoreID {
			t.Fatal("wrong StoreID")
		}
		if f, err := root.Open("../DUBI_STORAGE_ID"); err == nil {
			_ = f.Close()
			t.Fatal("escaped Volume root")
		}
		return nil
	}
	if err := r.WithVolume(v.ID, true, check); err != nil {
		t.Fatal(err)
	}
	r.manifest.Volumes[0].State = Draining
	if err := r.WithVolume(v.ID, false, check); err != nil {
		t.Fatal(err)
	}
	if err := r.WithVolume(v.ID, true, check); err == nil {
		t.Fatal("write on draining volume")
	}
	r.manifest.Volumes[0].State = Removed
	if err := r.WithVolume(v.ID, false, check); err == nil {
		t.Fatal("read on removed volume")
	}
	if err := r.WithVolume("unknown", false, check); err == nil {
		t.Fatal("unknown VolumeID")
	}
	r.manifest.Volumes[0].State = Active
	marker := filepath.Join(v.Path, markerName)
	if err := os.Rename(marker, marker+".old"); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(marker + ".old")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.WithVolume(v.ID, true, check); err == nil {
		t.Fatal("replaced marker accepted")
	}
}
