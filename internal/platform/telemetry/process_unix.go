//go:build linux || darwin

package telemetry

import (
	"golang.org/x/sys/unix"
	"os"
	"strconv"
	"strings"
)

func processUsage() (*float64, *uint64) {
	var cpu *float64
	var rss *uint64
	var u unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &u) == nil {
		v := float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6
		cpu = &v
	}
	// /proc reports current resident pages. Maxrss is a lifetime high-water mark
	// and must not be presented as current memory on platforms without /proc.
	if b, e := os.ReadFile("/proc/self/statm"); e == nil {
		f := strings.Fields(string(b))
		if len(f) > 1 {
			if n, e := strconv.ParseUint(f[1], 10, 64); e == nil {
				v := n * uint64(os.Getpagesize())
				rss = &v
			}
		}
	}
	return cpu, rss
}
