package volume

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func reservationRegistry(t *testing.T) (*Registry, []Record) {
	t.Helper()
	r, err := Initialize(testStore(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := r.Add(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	return r, r.manifest.Volumes
}

func TestReservationSharesFilesystemBudgetAndRotates(t *testing.T) {
	r, vols := reservationRegistry(t)
	r.probe = func(*os.Root) (diskSample, error) {
		return diskSample{Device: 7, Total: 10 << 30, Available: MinFreeBytes + 100}, nil
	}
	a, err := r.Reserve(60, nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.VolumeID() != vols[0].ID || a.Bytes() != 60 {
		t.Fatal("wrong first allocation")
	}
	if _, err := r.Reserve(41, []string{vols[1].ID}); !errors.Is(err, ErrInsufficientSpace) {
		t.Fatal("same disk counted twice", err)
	}
	b, err := r.Reserve(40, nil)
	if err != nil {
		t.Fatal(err)
	}
	if b.VolumeID() != vols[1].ID {
		t.Fatal("equal budgets did not rotate")
	}
	if err := a.Check(); err != nil {
		t.Fatal(err)
	}
	copyOfA := *a
	a.Release()
	copyOfA.Release()
	if r.reserved[7] != 40 {
		t.Fatal("copied handle released twice", r.reserved)
	}
	if err := a.Check(); !errors.Is(err, ErrInvalidReservation) {
		t.Fatal(err)
	}
	b.Release()
	if len(r.reserved) != 0 {
		t.Fatal("reservation leaked")
	}
	if _, err := r.Reserve(1, []string{}); !errors.Is(err, ErrNoWritableVolume) {
		t.Fatal("empty candidates expanded to all", err)
	}
}

func TestReservationCapacityChoiceAndSafetyFloor(t *testing.T) {
	r, vols := reservationRegistry(t)
	available := uint64(5 << 30)
	r.probe = func(root *os.Root) (diskSample, error) {
		if root.Name() == vols[0].Path {
			return diskSample{Device: 1, Total: 100 << 30, Available: available}, nil
		}
		return diskSample{Device: 2, Total: 10 << 30, Available: MinFreeBytes + 1024}, nil
	}
	l, err := r.Reserve(1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.VolumeID() != vols[1].ID {
		t.Fatal("5 percent floor ignored")
	}
	l.Release()
	available = 6 << 30
	l, err = r.Reserve(1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.VolumeID() != vols[0].ID {
		t.Fatal("did not prefer most writable bytes")
	}
	available = 5 << 30
	if err := l.Check(); !errors.Is(err, ErrInsufficientSpace) {
		t.Fatal("shrinking free space not rechecked", err)
	}
	l.Release()
	if writableBytes(diskSample{Total: math.MaxUint64, Available: math.MaxUint64}, math.MaxUint64) != 0 {
		t.Fatal("integer overflow")
	}
	if writableBytes(diskSample{Total: 500, Available: 500}, 0) != 0 {
		t.Fatal("minimum safety floor bypassed")
	}
}

func TestReservationSkipsUnsafeVolumesAndChecksLifecycle(t *testing.T) {
	r, vols := reservationRegistry(t)
	r.probe = func(*os.Root) (diskSample, error) {
		return diskSample{Device: 3, Total: 10 << 30, Available: 3 << 30}, nil
	}
	if err := os.Rename(vols[0].Path, vols[0].Path+".offline"); err != nil {
		t.Fatal(err)
	}
	l, err := r.Reserve(100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.VolumeID() != vols[1].ID {
		t.Fatal("missing Volume selected")
	}
	if _, err := os.Stat(vols[0].Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing volume recreated")
	}
	r.manifest.Volumes[1].State = Draining
	if err := l.Check(); err == nil {
		t.Fatal("lease remained writable during Drain")
	}
	if _, err := r.Reserve(1, nil); !errors.Is(err, ErrNoWritableVolume) {
		t.Fatal(err)
	}
	l.Release()
	r.manifest.Volumes[1].State = Active
	l, err = r.Reserve(100, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.probe = func(*os.Root) (diskSample, error) {
		return diskSample{Device: 4, Total: 10 << 30, Available: 3 << 30}, nil
	}
	if err := l.Check(); !errors.Is(err, ErrInvalidReservation) {
		t.Fatal("filesystem replacement accepted", err)
	}
	l.Release()
	l, err = r.Reserve(100, nil)
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(vols[1].Path, markerName)
	if err := os.Rename(marker, marker+".lost"); err != nil {
		t.Fatal(err)
	}
	if err := l.Check(); err == nil {
		t.Fatal("missing marker accepted")
	}
	l.Release()
}

func TestConcurrentReservationsCannotOverbook(t *testing.T) {
	r, _ := reservationRegistry(t)
	r.probe = func(*os.Root) (diskSample, error) {
		return diskSample{Device: 7, Total: 10 << 30, Available: MinFreeBytes + 1000}, nil
	}
	leases := make(chan *Reservation, 100)
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			l, err := r.Reserve(20, nil)
			if err == nil {
				leases <- l
			} else if !errors.Is(err, ErrInsufficientSpace) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	close(leases)
	if len(leases) != 50 || r.reserved[7] != 1000 {
		t.Fatal("overbooked or leaked capacity", len(leases), r.reserved)
	}
	for l := range leases {
		wg.Go(func() { l.Release() })
	}
	wg.Wait()
	if len(r.reserved) != 0 {
		t.Fatal("release leaked bytes")
	}
}

func TestReservationShutdownAndRealProbe(t *testing.T) {
	r, vols := reservationRegistry(t)
	if err := r.WithVolume(vols[0].ID, false, func(root *os.Root, _ string) error {
		s, err := probeDisk(root)
		if err != nil {
			return err
		}
		if s.Total == 0 || s.Available > s.Total {
			t.Fatal("bad real filesystem observation")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r.probe = func(*os.Root) (diskSample, error) {
		return diskSample{Device: 1, Total: 10 << 30, Available: 3 << 30}, nil
	}
	l, err := r.Reserve(100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Check(); !errors.Is(err, ErrInvalidReservation) {
		t.Fatal(err)
	}
	l.Release()
	l.Release()
	if len(r.reserved) != 0 {
		t.Fatal("shutdown leaked byte budget")
	}
	if _, err := r.Reserve(1, nil); !errors.Is(err, ErrInvalidReservation) {
		t.Fatal(err)
	}
}
