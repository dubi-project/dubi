package resource

import (
	"errors"
	"math"
	"sync"
	"testing"
)

func TestByteBudgetBoundsAndIdempotentRelease(t *testing.T) {
	b := NewByteBudget(10)
	release, err := b.TryAcquire(8)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.TryAcquire(3); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatal(err)
	}
	last, err := b.TryAcquire(2)
	if err != nil {
		t.Fatal(err)
	}
	copy := release
	release()
	copy()
	last()
	last()
	s := b.Status()
	if s.UsedBytes != 0 || s.PeakBytes != 10 || s.Rejected != 1 {
		t.Fatal(s)
	}
	b = NewByteBudget(math.MaxUint64)
	release, err = b.TryAcquire(math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.TryAcquire(1); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatal("integer overflow", err)
	}
	release()
	zero := NewByteBudget(0)
	if _, err := zero.TryAcquire(1); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatal("zero budget allocated", err)
	}
}

func TestByteBudgetConcurrentAdmission(t *testing.T) {
	b := NewByteBudget(100)
	var wg sync.WaitGroup
	releases := make(chan func(), 200)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := b.TryAcquire(2)
			if err == nil {
				releases <- release
			} else if !errors.Is(err, ErrBudgetExceeded) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	close(releases)
	if len(releases) != 50 || b.Status().UsedBytes != 100 {
		t.Fatal("overbooked budget", b.Status(), len(releases))
	}
	for release := range releases {
		wg.Add(1)
		go func() { defer wg.Done(); release(); release() }()
	}
	wg.Wait()
	if b.Status().UsedBytes != 0 {
		t.Fatal("leaked lease")
	}
}
