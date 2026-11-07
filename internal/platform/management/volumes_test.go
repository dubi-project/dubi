package management

import (
	v1 "github.com/dubi-project/dubi/api/v1"
	"testing"
)

func TestVolumeStatusUsesCurrentObservation(t *testing.T) {
	when := int64(10)
	for _, health := range []string{"healthy", "missing", "invalid", "pending", "removed", "error"} {
		v := v1.ManagedVolume{Health: health, Observation: v1.Observation{ObservedUnix: &when}}
		if got := VolumeStatus(v); got != health {
			t.Fatalf("%s: %s", health, got)
		}
		v.Stale = true
		if got := VolumeStatus(v); got != "unknown" {
			t.Fatal("stale health treated as current")
		}
	}
	if VolumeStatus(v1.ManagedVolume{Health: "healthy"}) != "unknown" {
		t.Fatal("missing observation is healthy")
	}
	if VolumeStatus(v1.ManagedVolume{Health: "future", Observation: v1.Observation{ObservedUnix: &when}}) != "unknown" {
		t.Fatal("unsupported health is not unknown")
	}
}
