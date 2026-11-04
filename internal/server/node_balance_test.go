package server

import (
	"testing"

	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

func TestCapacityMoveBudgetHasDeadbandAndUsesCapacityRatios(t *testing.T) {
	for _, tc := range []struct {
		name           string
		source, target volume.Capacity
		want           uint64
	}{
		{"equal", volume.Capacity{TotalBytes: 1000, AvailableBytes: 400}, volume.Capacity{TotalBytes: 1000, AvailableBytes: 400}, 0},
		{"deadband", volume.Capacity{TotalBytes: 1000, AvailableBytes: 400}, volume.Capacity{TotalBytes: 1000, AvailableBytes: 490}, 0},
		{"forward", volume.Capacity{TotalBytes: 1000, AvailableBytes: 400}, volume.Capacity{TotalBytes: 1000, AvailableBytes: 800}, 100},
		{"reverse", volume.Capacity{TotalBytes: 1000, AvailableBytes: 800}, volume.Capacity{TotalBytes: 1000, AvailableBytes: 400}, 0},
		{"unequal devices", volume.Capacity{TotalBytes: 1000, AvailableBytes: 200}, volume.Capacity{TotalBytes: 10000, AvailableBytes: 8000}, 272},
		{"invalid", volume.Capacity{TotalBytes: 100, AvailableBytes: 200}, volume.Capacity{TotalBytes: 1000, AvailableBytes: 800}, 0},
		{"missing", volume.Capacity{}, volume.Capacity{TotalBytes: 1000, AvailableBytes: 800}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if actual := capacityMoveBudget(tc.source, tc.target); actual != tc.want {
				t.Fatal(actual, tc.want)
			}
		})
	}
}
