// Package memory applies the process-wide Go heap soft limit used by a DUBI
// Node. It is intentionally best-effort: hosts without a cgroup limit keep
// the runtime default, while containers and small VPS instances get earlier
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
	minimumLimit = 512 << 20
	// Keep enough headroom for the kernel, TLS/network buffers and Pebble's
	// native/file mappings. A 75% heap budget still lets a 2 GiB VPS reach
	// the OOM boundary during Raft/Pebble recovery, so the default is capped
	// at half of the host limit. Larger machines still get a proportionate
	// budget; GOMEMLIMIT remains available for deliberate expert tuning.
	softFraction = 50
)

// ConfigureFromCgroup installs a conservative Go heap limit when the host
// exposes a finite cgroup memory limit. An explicit GOMEMLIMIT remains
// authoritative; this helper never overrides it.
func ConfigureFromCgroup() (int64, error) {
	if strings.TrimSpace(os.Getenv("GOMEMLIMIT")) != "" {
		return 0, nil
	}
	limit, err := readLimit()
	if err != nil || limit == 0 || limit == math.MaxInt64 {
		return 0, err
	}
	soft := limit / 100 * softFraction
	if soft < minimumLimit {
		soft = minimumLimit
	}
	debug.SetMemoryLimit(soft)
	return soft, nil
}

func readLimit() (int64, error) {
	for _, path := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		value := strings.TrimSpace(string(data))
		if value == "max" || value == "" {
			return 0, nil
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed <= 0 {
			return 0, errors.New("invalid cgroup memory limit")
		}
		return parsed, nil
	}
	// Some VPS providers expose no cgroup limit even though the guest has a
	// small fixed MemTotal. Use that as a conservative fallback so the Node
	// still gets a heap budget on ordinary systemd services.
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, nil
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kib, parseErr := strconv.ParseInt(fields[1], 10, 64)
			if parseErr != nil || kib <= 0 {
				return 0, nil
			}
			return kib * 1024, nil
		}
	}
	return 0, nil
}
