package storelock

import "testing"

func TestAcquireIsExclusiveAndReleaseIsIdempotent(t *testing.T) {
	root := t.TempDir()
	first, err := Acquire(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(root); err == nil {
		t.Fatal("second process lock unexpectedly acquired")
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	second, err := Acquire(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
}
