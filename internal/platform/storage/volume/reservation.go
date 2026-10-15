package volume

import (
	"errors"
	"fmt"
	"math"
	"os"

	"golang.org/x/sys/unix"
)

const MinFreeBytes uint64 = 1 << 30

var (
	ErrNoWritableVolume   = errors.New("no healthy active Volume is eligible")
	ErrInsufficientSpace  = errors.New("insufficient disk space above the safety reserve")
	ErrInvalidReservation = errors.New("reservation is released or no longer valid")
)

type diskSample struct{ Device, Total, Available uint64 }

type reservationState struct {
	volumeID      string
	device, bytes uint64
	directory     os.FileInfo
	released      bool // Protected by Registry.mu; copies of Reservation share this state.
}

// Reservation is a short-lived in-process budget, NOT filesystem preallocation
// or a durable placement intent. Release only after all writes using it stop.
type Reservation struct {
	registry *Registry
	state    *reservationState
}

func (l *Reservation) VolumeID() string { return l.state.volumeID }
func (l *Reservation) Bytes() uint64    { return l.state.bytes }

// Reserve selects the eligible Volume with most writable bytes. Equal device
// budgets rotate in registration order, including directories sharing a disk.
// nil candidates means all; an explicitly empty list means none.
func (r *Registry) Reserve(bytes uint64, candidates []string) (*Reservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.failed != nil {
		return nil, errors.Join(r.failed, ErrInvalidReservation)
	}
	if bytes == 0 {
		return nil, errors.New("reservation must request positive bytes")
	}
	allowed := make(map[string]bool, len(candidates))
	for _, id := range candidates {
		allowed[id] = true
	}
	type candidate struct {
		index     int
		id        string
		sample    diskSample
		directory os.FileInfo
	}
	var choices []candidate
	devices := make(map[uint64]diskSample)
	var failures []error
	for i, record := range r.manifest.Volumes {
		if record.State != Active || (candidates != nil && !allowed[record.ID]) {
			continue
		}
		var sample diskSample
		var directory os.FileInfo
		err := r.withVolumeLocked(record.ID, true, false, func(root *os.Root, _ string) error {
			var err error
			directory, err = root.Stat(".")
			if err != nil {
				return err
			}
			probe := r.probe
			if probe == nil {
				probe = probeDisk
			}
			sample, err = probe(root)
			return err
		})
		if err != nil {
			failures = append(failures, fmt.Errorf("Volume %s: %w", record.ID, err))
			continue
		}
		if sample.Available > sample.Total {
			failures = append(failures, errors.New("invalid filesystem capacity"))
			continue
		}
		choices = append(choices, candidate{i, record.ID, sample, directory})
		if previous, ok := devices[sample.Device]; ok {
			// Sequential observations of one filesystem may differ. Use the
			// conservative minimum free space and maximum protected floor.
			previous.Available = min(previous.Available, sample.Available)
			previous.Total = max(previous.Total, sample.Total)
			devices[sample.Device] = previous
		} else {
			devices[sample.Device] = sample
		}
	}
	if len(choices) == 0 {
		return nil, errors.Join(append([]error{ErrNoWritableVolume}, failures...)...)
	}
	best := -1
	var bestFree uint64
	bestDistance := len(r.manifest.Volumes)
	for i, c := range choices {
		s := devices[c.sample.Device]
		free := writableBytes(s, r.reserved[s.Device])
		if bytes > free {
			continue
		}
		distance := (c.index - r.reserveCursor + len(r.manifest.Volumes)) % len(r.manifest.Volumes)
		if best < 0 || free > bestFree || (free == bestFree && distance < bestDistance) {
			best = i
			bestFree = free
			bestDistance = distance
		}
	}
	if best < 0 {
		return nil, ErrInsufficientSpace
	}
	c := choices[best]
	if r.reserved == nil {
		r.reserved = make(map[uint64]uint64)
	}
	// writableBytes already proves addition cannot overflow.
	r.reserved[c.sample.Device] += bytes
	r.reserveCursor = (c.index + 1) % len(r.manifest.Volumes)
	return &Reservation{r, &reservationState{volumeID: c.id, device: c.sample.Device, bytes: bytes, directory: c.directory}}, nil
}

// Check revalidates lifecycle, pinned directory identity and current free space.
// External writers can still race after Check; callers must handle real IO errors.
func (l *Reservation) Check() error {
	if l == nil || l.registry == nil || l.state == nil {
		return ErrInvalidReservation
	}
	r := l.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	s := l.state
	if s.released || r.closed || r.failed != nil {
		return errors.Join(r.failed, ErrInvalidReservation)
	}
	return r.withVolumeLocked(s.volumeID, true, false, func(root *os.Root, _ string) error {
		info, err := root.Stat(".")
		if err != nil {
			return err
		}
		if !os.SameFile(info, s.directory) {
			return ErrInvalidReservation
		}
		probe := r.probe
		if probe == nil {
			probe = probeDisk
		}
		disk, err := probe(root)
		if err != nil {
			return err
		}
		if disk.Device != s.device || disk.Available > disk.Total {
			return ErrInvalidReservation
		}
		// All budgets still count, even if part of an operation is already on
		// disk. This intentional double-counting errs on the conservative side.
		if writableBytes(disk, 0) < r.reserved[s.device] {
			return ErrInsufficientSpace
		}
		return nil
	})
}

// Release is idempotent, including across copied handles and registry shutdown.
func (l *Reservation) Release() {
	if l == nil || l.registry == nil || l.state == nil {
		return
	}
	r := l.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if l.state.released {
		return
	}
	l.state.released = true
	r.reserved[l.state.device] -= l.state.bytes
	if r.reserved[l.state.device] == 0 {
		delete(r.reserved, l.state.device)
	}
}

func writableBytes(s diskSample, reserved uint64) uint64 {
	floor := max(MinFreeBytes, s.Total/20) // Keep at least 1 GiB or 5%, whichever is larger.
	if s.Available <= floor {
		return 0
	}
	available := s.Available - floor
	if reserved >= available {
		return 0
	}
	return available - reserved
}

func probeDisk(root *os.Root) (diskSample, error) {
	f, err := root.Open(".")
	if err != nil {
		return diskSample{}, err
	}
	defer f.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &stat); err != nil {
		return diskSample{}, err
	}
	var fs unix.Statfs_t
	if err := unix.Fstatfs(int(f.Fd()), &fs); err != nil {
		return diskSample{}, err
	}
	if fs.Bsize <= 0 || uint64(fs.Blocks) > math.MaxUint64/uint64(fs.Bsize) || uint64(fs.Bavail) > math.MaxUint64/uint64(fs.Bsize) {
		return diskSample{}, errors.New("invalid filesystem capacity")
	}
	return diskSample{Device: uint64(stat.Dev), Total: uint64(fs.Blocks) * uint64(fs.Bsize), Available: uint64(fs.Bavail) * uint64(fs.Bsize)}, nil
}
