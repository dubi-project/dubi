package volume

import (
	"github.com/dubi-project/dubi/internal/platform/storage/diskbudget"
	"math"
)

type BalancePlan struct {
	Source, Target string
	MaximumBytes   uint64
}

// PlanBalance compares usage ratios on distinct healthy filesystems. Leave a
// ten percentage point deadband and half the equalization distance as margin.
// Shared mount directories never create apparent extra capacity.
func PlanBalance(report Report) (BalancePlan, bool) {
	var best BalancePlan
	for _, source := range report.Volumes {
		if source.State != Active || source.Health != "healthy" || source.Device == 0 || source.Capacity.TotalBytes == 0 || source.Capacity.AvailableBytes > source.Capacity.TotalBytes {
			continue
		}
		for _, target := range report.Volumes {
			if target.State != Active || target.Health != "healthy" || target.Device == 0 || target.Device == source.Device || target.Capacity.TotalBytes == 0 || target.Capacity.AvailableBytes > target.Capacity.TotalBytes {
				continue
			}
			a, b := source.Capacity, target.Capacity
			gap := float64(b.AvailableBytes)/float64(b.TotalBytes) - float64(a.AvailableBytes)/float64(a.TotalBytes)
			if gap < 0.10 {
				continue
			}
			equalize := gap / (1/float64(a.TotalBytes) + 1/float64(b.TotalBytes))
			maximum := uint64(min(equalize/2, float64(math.MaxInt64)))
			maximum = min(maximum, writableBytes(diskSample{Total: b.TotalBytes, Available: b.AvailableBytes}, 0))
			if maximum >= 16<<20 && maximum > best.MaximumBytes {
				best = BalancePlan{Source: source.ID, Target: target.ID, MaximumBytes: maximum}
			}
		}
	}
	return best, best.Source != ""
}

// WritableCapacity is a placement hint for one append, not a reservation or
// a durability vote. Real writes still recheck state, device and safety floor.
func WritableCapacity(report Report, blocked map[string]string) uint64 {
	var best uint64
	for _, v := range report.Volumes {
		if v.State != Active || v.Health != "healthy" || blocked[v.ID] != "" || v.Capacity.TotalBytes == 0 || v.Capacity.AvailableBytes > v.Capacity.TotalBytes {
			continue
		}
		best = max(best, writableBytes(diskSample{Total: v.Capacity.TotalBytes, Available: v.Capacity.AvailableBytes}, diskbudget.Shared.Reserved(v.Device)))
	}
	return best
}

// WritableDiskCapacity describes healthy Active devices once each. It is a
// load observation only; every actual destination append still reserves bytes.
func WritableDiskCapacity(report Report, blocked map[string]string) Capacity {
	devices := make(map[uint64]Capacity)
	for _, v := range report.Volumes {
		if v.State != Active || v.Health != "healthy" || blocked[v.ID] != "" || v.Capacity.TotalBytes == 0 || v.Capacity.AvailableBytes > v.Capacity.TotalBytes {
			continue
		}
		if previous, found := devices[v.Device]; found {
			devices[v.Device] = Capacity{TotalBytes: max(previous.TotalBytes, v.Capacity.TotalBytes), AvailableBytes: min(previous.AvailableBytes, v.Capacity.AvailableBytes)}
		} else {
			devices[v.Device] = v.Capacity
		}
	}
	var total Capacity
	for _, c := range devices {
		if math.MaxUint64-total.TotalBytes < c.TotalBytes || math.MaxUint64-total.AvailableBytes < c.AvailableBytes {
			return Capacity{} // Invalid/overflowed telemetry never schedules deletion.
		}
		total.TotalBytes += c.TotalBytes
		total.AvailableBytes += c.AvailableBytes
	}
	return total
}
