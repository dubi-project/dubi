package memory

import (
	"os"
	"runtime/debug"
	"testing"
)

func TestMemoryBoundUsesAvailablePhysicalAndCgroupLimits(t *testing.T) {
	const gib = int64(1 << 30)
	for _, tc := range []struct {
		name, v2, v1, mem string
		want              int64
		bad               bool
	}{
		{"v2 unlimited guest", "max", "", "MemTotal: 2097152 kB\n", 2 * gib, false},
		{"v1 unlimited guest", "", "9223372036854771712", "MemTotal: 2097152 kB\n", 2 * gib, false},
		{"container smaller", "536870912", "", "MemTotal: 2097152 kB\n", gib / 2, false},
		{"physical smaller", "4294967296", "", "MemTotal: 2097152 kB\n", 2 * gib, false},
		{"only guest", "", "", "MemTotal: 1048576 kB\n", gib, false},
		{"only container", "268435456", "", "", gib / 4, false},
		{"v2 unlimited v1 finite", "max", "536870912", "MemTotal: 2097152 kB\n", gib / 2, false},
		{"unknown", "", "", "", 0, false},
		{"bad cgroup", "invalid", "", "MemTotal: 2097152 kB\n", 0, true},
		{"bad physical does not erase container", "536870912", "", "MemTotal: invalid kB\n", gib / 2, false},
		{"physical overflow", "max", "", "MemTotal: 9223372036854775807 kB\n", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := map[string]string{"/sys/fs/cgroup/memory.max": tc.v2, "/sys/fs/cgroup/memory/memory.limit_in_bytes": tc.v1, "/proc/meminfo": tc.mem}
			got, err := readLimitFrom(func(path string) ([]byte, error) {
				if files[path] == "" {
					return nil, os.ErrNotExist
				}
				return []byte(files[path]), nil
			})
			if got != tc.want || (err != nil) != tc.bad {
				t.Fatalf("bound=%d error=%v, want=%d bad=%v", got, err, tc.want, tc.bad)
			}
		})
	}
}

func TestSmallMemoryBudgetLeavesHostHeadroom(t *testing.T) {
	for _, total := range []int64{128 << 20, 256 << 20, 512 << 20, 1 << 30, 2 << 30} {
		if got := softLimit(total); got <= 0 || got > total/2 {
			t.Fatalf("total=%d soft=%d", total, got)
		}
	}
}

func TestExplicitMemoryLimitIsPreserved(t *testing.T) {
	old := debug.SetMemoryLimit(123 << 20)
	t.Cleanup(func() { debug.SetMemoryLimit(old) })
	t.Setenv("GOMEMLIMIT", "123MiB")
	got, err := ConfigureFromCgroup()
	if err != nil || got != 0 || debug.SetMemoryLimit(-1) != 123<<20 {
		t.Fatalf("explicit limit changed: %d %v", got, err)
	}
}
