package placement

import "testing"

func TestSelectWritePolicyUsesDurableTopologyCounts(t *testing.T) {
	local, err := SelectWritePolicy(3, 1)
	if err != nil || local.Mode != WriteLocalFirst || !local.AllowsLocalReady() || local.RequiresGeoReady() {
		t.Fatal(local, err)
	}
	geo, err := SelectWritePolicy(2, 3)
	if err != nil || geo.Mode != WriteGeoSynchronous || !geo.RequiresGeoReady() || geo.RequiredRegions != 2 {
		t.Fatal(geo, err)
	}
	if _, err := SelectWritePolicy(2, 1); err == nil {
		t.Fatal("small Region silently accepted without a Region majority")
	}
	if _, err := SelectWritePolicy(-1, 3); err == nil {
		t.Fatal("negative topology count accepted")
	}
}
