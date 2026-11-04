// Package diskbudget coordinates in-flight allocations by filesystem within a
// Node process. It is admission accounting, not an OS quota or preallocation.
package diskbudget

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

var ErrSpace = errors.New("filesystem has insufficient unreserved space")

type Capacity struct{ Device, Total, Available uint64 }

type Class uint8

const (
	Discretionary Class = iota // Data, backup and temporary snapshot writes.
	Committed                  // Raft/LSM persistence must retain access to the protected reserve.
)

type Ledger struct {
	mu       sync.Mutex
	reserved map[uint64]uint64
}

var Shared = &Ledger{}

type Lease struct {
	state *leaseState
}

type leaseState struct {
	ledger        *Ledger
	device, bytes uint64
	released      bool // Protected by ledger.mu; copied handles share the same lease.
}

func Floor(total uint64) uint64 { return max(uint64(1<<30), total/20) }

func (b *Ledger) Reserved(device uint64) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reserved[device]
}

func (b *Ledger) Reserve(c Capacity, bytes uint64, class Class) (*Lease, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reserveLocked(c, bytes, class)
}

func (b *Ledger) reserveLocked(c Capacity, bytes uint64, class Class) (*Lease, error) {
	if c.Total == 0 || c.Available > c.Total || bytes == 0 || (class != Discretionary && class != Committed) {
		return nil, errors.New("invalid filesystem reservation")
	}
	floor := uint64(0)
	if class == Discretionary {
		floor = Floor(c.Total)
	}
	reserved := b.reserved[c.Device]
	if c.Available < floor || reserved > c.Available-floor || bytes > c.Available-floor-reserved {
		return nil, fmt.Errorf("%w: available=%d pending=%d floor=%d requested=%d", ErrSpace, c.Available, reserved, floor, bytes)
	}
	if b.reserved == nil {
		b.reserved = make(map[uint64]uint64)
	}
	b.reserved[c.Device] += bytes
	return &Lease{state: &leaseState{ledger: b, device: c.Device, bytes: bytes}}, nil
}

func (l *Lease) Release() {
	if l == nil || l.state == nil || l.state.ledger == nil {
		return
	}
	s := l.state
	s.ledger.mu.Lock()
	defer s.ledger.mu.Unlock()
	if s.released {
		return
	}
	s.released = true
	s.ledger.reserved[s.device] -= s.bytes
	if s.ledger.reserved[s.device] == 0 {
		delete(s.ledger.reserved, s.device)
	}
}

func ProbeFD(fd uintptr) (Capacity, error) {
	var identity unix.Stat_t
	if err := unix.Fstat(int(fd), &identity); err != nil {
		return Capacity{}, err
	}
	var fs unix.Statfs_t
	if err := unix.Fstatfs(int(fd), &fs); err != nil {
		return Capacity{}, err
	}
	if fs.Bsize <= 0 || uint64(fs.Blocks) > math.MaxUint64/uint64(fs.Bsize) || uint64(fs.Bavail) > math.MaxUint64/uint64(fs.Bsize) {
		return Capacity{}, errors.New("invalid filesystem capacity")
	}
	return Capacity{Device: uint64(identity.Dev), Total: uint64(fs.Blocks) * uint64(fs.Bsize), Available: uint64(fs.Bavail) * uint64(fs.Bsize)}, nil
}

func ReserveFD(fd uintptr, bytes uint64, class Class) (*Lease, error) {
	Shared.mu.Lock()
	defer Shared.mu.Unlock()
	c, err := ProbeFD(fd)
	if err != nil {
		return nil, err
	}
	return Shared.reserveLocked(c, bytes, class)
}

func ReservePath(path string, bytes uint64, class Class) (*Lease, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ReserveFD(f.Fd(), bytes, class)
}
