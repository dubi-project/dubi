// Package diskpressure rejects discretionary writes before the Store filesystem
// reserve is exhausted. It is admission control, not filesystem preallocation:
// Raft catch-up, already-committed Apply and external writers still consume disk.
package diskpressure

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	MinFreeBytes   = uint64(1 << 30)
	RecoveryBytes  = uint64(64 << 20)
	RecoveryChecks = 3
	ProbeInterval  = 250 * time.Millisecond
	RetryDelay     = 30 * time.Second
)

var ErrPressure = errors.New("Store filesystem is under disk pressure")
var ErrProbe = errors.New("Store filesystem capacity cannot be verified")

type Capacity struct{ TotalBytes, AvailableBytes uint64 }

type Status struct {
	Path            string    `json:"path"`
	TotalBytes      uint64    `json:"total_bytes"`
	AvailableBytes  uint64    `json:"available_bytes"`
	ReserveBytes    uint64    `json:"reserve_bytes"`
	RecoveryAtBytes uint64    `json:"recovery_at_bytes"`
	HealthyChecks   int       `json:"healthy_checks"`
	Pressured       bool      `json:"pressured"`
	CheckedAt       time.Time `json:"checked_at"`
	PressuredSince  time.Time `json:"pressured_since,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
}

type Guard struct {
	mu                sync.Mutex
	path              string
	directory         *os.File
	identity          os.FileInfo
	last              Status
	recoveryCheckedAt time.Time
	closed            bool
	probe             func() (Capacity, error) // nil uses the pinned directory descriptor.
	now               func() time.Time         // nil uses the real clock; tests never need to sleep.
}

// Open pins an existing directory. Defaults are automatic and intentionally
// not exposed as per-node deployment knobs. Low space does not prevent opening
// a Host for reads/recovery; Check gates only new proposals and optional work.
func Open(path string) (*Guard, error) {
	if path == "" {
		return nil, errors.New("Store path is required")
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	directory, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := directory.Stat()
	if err != nil || !info.IsDir() {
		return nil, errors.Join(err, errors.New("Store path is not a directory"), directory.Close())
	}
	return &Guard{path: path, directory: directory, identity: info}, nil
}

func (g *Guard) Check() error {
	s := g.Status()
	if s.LastError != "" {
		return fmt.Errorf("%w: %s", ErrProbe, s.LastError)
	}
	if s.Pressured {
		return fmt.Errorf("%w: available=%d reserve=%d recovery=%d healthy_probes=%d/%d",
			ErrPressure, s.AvailableBytes, s.ReserveBytes, s.RecoveryAtBytes, s.HealthyChecks, RecoveryChecks)
	}
	return nil
}

// Status probes on every call so admission observes new pressure immediately.
// Frequent requests cannot accelerate recovery: successful probes must be
// separated by ProbeInterval. Transient probe errors also enter pressure.
func (g *Guard) Status() Status {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	if g.now != nil {
		now = g.now()
	}
	s := g.last
	s.Path, s.CheckedAt = g.path, now.UTC()
	var capacity Capacity
	var err error
	if g.closed {
		err = errors.New("disk pressure guard is closed")
	} else if g.probe != nil {
		capacity, err = g.probe()
	} else {
		capacity, err = g.capacity()
	}
	if err == nil && (capacity.TotalBytes == 0 || capacity.AvailableBytes > capacity.TotalBytes) {
		err = errors.New("filesystem returned invalid capacity")
	}
	s.TotalBytes, s.AvailableBytes = capacity.TotalBytes, capacity.AvailableBytes
	s.ReserveBytes = max(MinFreeBytes, capacity.TotalBytes/20)
	margin := max(RecoveryBytes, capacity.TotalBytes/100)
	s.RecoveryAtBytes = s.ReserveBytes
	if margin > math.MaxUint64-s.RecoveryAtBytes {
		s.RecoveryAtBytes = math.MaxUint64
	} else {
		s.RecoveryAtBytes += margin
	}
	s.LastError = ""
	if err != nil || capacity.AvailableBytes <= s.ReserveBytes {
		if !s.Pressured {
			s.PressuredSince = now.UTC()
		}
		s.Pressured, s.HealthyChecks = true, 0
		g.recoveryCheckedAt = time.Time{}
		if err != nil {
			s.LastError = err.Error()
		}
	} else if s.Pressured {
		if capacity.AvailableBytes < s.RecoveryAtBytes {
			s.HealthyChecks = 0
			g.recoveryCheckedAt = time.Time{}
		} else if g.recoveryCheckedAt.IsZero() || now.Sub(g.recoveryCheckedAt) >= ProbeInterval {
			s.HealthyChecks++
			g.recoveryCheckedAt = now
			if s.HealthyChecks >= RecoveryChecks {
				s.Pressured = false
				s.PressuredSince = time.Time{}
			}
		}
	}
	g.last = s
	return s
}

func (g *Guard) capacity() (Capacity, error) {
	info, err := os.Lstat(g.path)
	if err != nil {
		return Capacity{}, err
	}
	if !info.IsDir() || !os.SameFile(info, g.identity) {
		return Capacity{}, errors.New("Store directory identity changed")
	}
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(g.directory.Fd()), &stat); err != nil {
		return Capacity{}, err
	}
	if stat.Bsize <= 0 || uint64(stat.Blocks) > math.MaxUint64/uint64(stat.Bsize) ||
		uint64(stat.Bavail) > math.MaxUint64/uint64(stat.Bsize) {
		return Capacity{}, errors.New("invalid filesystem capacity")
	}
	return Capacity{TotalBytes: uint64(stat.Blocks) * uint64(stat.Bsize), AvailableBytes: uint64(stat.Bavail) * uint64(stat.Bsize)}, nil
}

func (g *Guard) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil
	}
	g.closed = true
	return g.directory.Close()
}
