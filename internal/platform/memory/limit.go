// Package memory applies the process-wide Go runtime soft memory limit used by a DUBI
// Node. It is intentionally best-effort: cgroup and physical memory bounds
// are combined so containers and small VPS instances get earlier
// GC pressure instead of an abrupt kernel OOM kill during recovery.
package memory

import (
	"errors"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	// Keep enough headroom for the kernel, TLS/network buffers and Pebble's
	// native/file mappings. A 75% Go runtime budget still lets a 2 GiB VPS reach
	// the OOM boundary during Raft/Pebble recovery, so the default is capped
	// at half of the host limit. Larger machines still get a proportionate
	// budget; GOMEMLIMIT remains available for deliberate expert tuning.
	softFraction = 50
)

// ConfigureFromCgroup installs a conservative Go runtime memory limit from
// the smaller available cgroup/physical memory bound. An explicit GOMEMLIMIT remains
// authoritative; this helper never overrides it.
func ConfigureFromCgroup() (int64, error) {
	if strings.TrimSpace(os.Getenv("GOMEMLIMIT")) != "" {
		return 0, nil
	}
	limit, err := readLimit()
	if err != nil || limit == 0 || limit == math.MaxInt64 {
		return 0, err
	}
	soft := softLimit(limit)
	debug.SetMemoryLimit(soft)
	return soft, nil
}

// Never raise a small host's budget to a fixed minimum: on a 512 MiB
// container, a 512 MiB Go budget would leave no room for the rest of the process.
func softLimit(limit int64) int64 { return limit / 100 * softFraction }

func readLimit() (int64, error) { return readLimitFrom(os.ReadFile) }

func readLimitFrom(readFile func(string) ([]byte, error)) (int64, error) {
	var bound int64
	for _, path := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		data, err := readFile(path)
		if err != nil {
			continue
		}
		value := strings.TrimSpace(string(data))
		if value == "max" {
			continue // Unlimited cgroup is not unlimited guest physical RAM.
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed <= 0 {
			return 0, errors.New("invalid cgroup memory limit")
		}
		if bound == 0 || parsed < bound {
			bound = parsed
		}
	}
	// cgroup v1 uses a huge numeric sentinel for unlimited. Taking the minimum
	// with MemTotal handles that sentinel and oversized real limits alike.
	if data, err := readFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 3 && fields[0] == "MemTotal:" && fields[2] == "kB" {
				kib, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil && kib > 0 && kib <= math.MaxInt64/1024 {
					physical := kib * 1024
					if bound == 0 || physical < bound {
						bound = physical
					}
				}
				break
			}
		}
	}
	return bound, nil
}
