package diskbudget

import (
	"errors"
	"math"
	"sync"
	"testing"
)

func TestSharedFilesystemAdmissionAndCommittedReserve(t *testing.T) {
	ledger := &Ledger{}
	c := Capacity{Device: 1, Total: 8 << 30, Available: (1 << 30) + (16 << 20)}
	pack, err := ledger.Reserve(c, 12<<20, Discretionary)
	if err != nil {
		t.Fatal(err)
	}
	defer pack.Release()
	if _, err := ledger.Reserve(c, 8<<20, Discretionary); !errors.Is(err, ErrSpace) {
		t.Fatal("another writer overbooked the same device", err)
	}
	wal, err := ledger.Reserve(c, 8<<20, Committed)
	if err != nil {
		t.Fatal("committed WAL lost access to the safety reserve", err)
	}
	wal.Release()
	other, err := ledger.Reserve(Capacity{Device: 2, Total: c.Total, Available: c.Available}, 16<<20, Discretionary)
	if err != nil {
		t.Fatal("independent device was charged for another filesystem", err)
	}
	other.Release()
	copy := *pack
	pack.Release()
	copy.Release()
	if ledger.Reserved(1) != 0 {
		t.Fatal("copied lease double released or leaked")
	}
}

func TestFilesystemReservationConcurrentAdmissionIsBounded(t *testing.T) {
	ledger := &Ledger{}
	c := Capacity{Device: 1, Total: 8 << 30, Available: (1 << 30) + (8 << 20)}
	var wg sync.WaitGroup
	ready := make(chan *Lease, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := ledger.Reserve(c, 1<<20, Discretionary)
			if err == nil {
				ready <- lease
			}
		}()
	}
	wg.Wait()
	close(ready)
	if ledger.Reserved(1) != 8<<20 {
		t.Fatal("concurrent writers exceeded or failed to use the shared budget", ledger.Reserved(1))
	}
	for lease := range ready {
		lease.Release()
	}
	if ledger.Reserved(1) != 0 {
		t.Fatal("reservation leak", ledger.Reserved(1))
	}
}

func TestFilesystemReservationRejectsInvalidAndOverflowingInputs(t *testing.T) {
	ledger := &Ledger{}
	for _, c := range []Capacity{{}, {Device: 1, Total: 1, Available: 2}, {Device: 1, Total: math.MaxUint64, Available: math.MaxUint64}} {
		if lease, err := ledger.Reserve(c, math.MaxUint64, Discretionary); err == nil {
			lease.Release()
			t.Fatal("invalid/overflowing allocation accepted", c)
		}
	}
}
