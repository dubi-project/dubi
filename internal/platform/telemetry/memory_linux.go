package telemetry

import "golang.org/x/sys/unix"

// Physical memory is the node/VM total, not the Go heap or a container quota.
func physicalMemory() *uint64 {
	var info unix.Sysinfo_t
	if unix.Sysinfo(&info) != nil {
		return nil
	}
	total, unit := uint64(info.Totalram), uint64(info.Unit)
	if total == 0 || unit == 0 || total > ^uint64(0)/unit {
		return nil
	}
	value := total * unit
	return &value
}
