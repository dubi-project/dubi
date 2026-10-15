package diskpressure

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testGuard(t *testing.T) (*Guard, *Capacity, *time.Time) {
	t.Helper()
	g, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := g.Close(); err != nil {
			t.Error(err)
		}
	})
	capacity := &Capacity{TotalBytes: 10 << 30, AvailableBytes: 5 << 30}
	now := time.Now()
	g.probe = func() (Capacity, error) { return *capacity, nil }
	g.now = func() time.Time { return now }
	return g, capacity, &now
}

func TestGuardImmediatePressureAndSpacedRecovery(t *testing.T) {
	g, capacity, now := testGuard(t)
	if err := g.Check(); err != nil {
		t.Fatal(err)
	}
	capacity.AvailableBytes = MinFreeBytes
	if err := g.Check(); !errors.Is(err, ErrPressure) {
		t.Fatal("reserve not enforced", err)
	}
	s := g.Status()
	if !s.Pressured || s.PressuredSince.IsZero() || s.HealthyChecks != 0 {
		t.Fatal(s)
	}
	capacity.AvailableBytes = s.RecoveryAtBytes - 1
	for i := 0; i < 5; i++ {
		*now = now.Add(ProbeInterval)
		if err := g.Check(); !errors.Is(err, ErrPressure) {
			t.Fatal("recovered below hysteresis", err)
		}
	}
	capacity.AvailableBytes = s.RecoveryAtBytes
	for i := 0; i < 100; i++ {
		if err := g.Check(); !errors.Is(err, ErrPressure) {
			t.Fatal("requests accelerated recovery", err)
		}
	}
	if g.Status().HealthyChecks != 1 {
		t.Fatal("duplicate probes counted")
	}
	*now = now.Add(ProbeInterval)
	if err := g.Check(); !errors.Is(err, ErrPressure) {
		t.Fatal(err)
	}
	// A dip resets the required consecutive observations.
	capacity.AvailableBytes--
	if g.Status().HealthyChecks != 0 {
		t.Fatal("dip failed to reset recovery")
	}
	capacity.AvailableBytes++
	for i := 0; i < RecoveryChecks; i++ {
		*now = now.Add(ProbeInterval)
		err := g.Check()
		if i < RecoveryChecks-1 && !errors.Is(err, ErrPressure) {
			t.Fatal("early recovery", err)
		}
		if i == RecoveryChecks-1 && err != nil {
			t.Fatal("recovery failed", err)
		}
	}
	if !g.Status().PressuredSince.IsZero() {
		t.Fatal("recovered guard kept incident time")
	}
}

func TestGuardProbeFailureAndCapacityValidation(t *testing.T) {
	g, capacity, now := testGuard(t)
	goodProbe := g.probe
	g.probe = func() (Capacity, error) { return Capacity{}, errors.New("unavailable") }
	if err := g.Check(); !errors.Is(err, ErrProbe) {
		t.Fatal(err)
	}
	if s := g.Status(); !s.Pressured || s.LastError == "" {
		t.Fatal(s)
	}
	g.probe = goodProbe
	for i := 0; i < RecoveryChecks; i++ {
		*now = now.Add(ProbeInterval)
		err := g.Check()
		if i == RecoveryChecks-1 && err != nil {
			t.Fatal(err)
		}
	}
	for _, sample := range []Capacity{{}, {TotalBytes: 1, AvailableBytes: 2}} {
		*capacity = sample
		if err := g.Check(); !errors.Is(err, ErrProbe) {
			t.Fatal("invalid capacity accepted", err)
		}
	}
}

func TestGuardUsesIntegerReserveAndNoOverflow(t *testing.T) {
	g, capacity, _ := testGuard(t)
	*capacity = Capacity{TotalBytes: math.MaxUint64, AvailableBytes: math.MaxUint64}
	s := g.Status()
	if s.ReserveBytes != uint64(math.MaxUint64)/20 || s.RecoveryAtBytes != uint64(math.MaxUint64)/20+uint64(math.MaxUint64)/100 || s.Pressured {
		t.Fatal(s)
	}
	*capacity = Capacity{TotalBytes: MinFreeBytes, AvailableBytes: MinFreeBytes}
	if err := g.Check(); !errors.Is(err, ErrPressure) {
		t.Fatal("tiny filesystem accepted", err)
	}
}

func TestGuardPinnedDirectoryAndLifecycle(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	if err := os.Mkdir(root, 0o750); err != nil {
		t.Fatal(err)
	}
	g, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	s := g.Status()
	if s.LastError != "" || s.TotalBytes == 0 || s.AvailableBytes > s.TotalBytes {
		t.Fatal("real descriptor probe", s)
	}
	if err := os.Rename(root, root+"-offline"); err != nil {
		t.Fatal(err)
	}
	if err := g.Check(); !errors.Is(err, ErrProbe) {
		t.Fatal("missing directory accepted", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("probe recreated Store")
	}
	if err := os.Mkdir(root, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := g.Check(); !errors.Is(err, ErrProbe) {
		t.Fatal("replacement directory accepted", err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Check(); !errors.Is(err, ErrProbe) {
		t.Fatal("closed guard accepted", err)
	}
}

func TestGuardConcurrentProbes(t *testing.T) {
	g, _, _ := testGuard(t)
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := g.Check(); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}
