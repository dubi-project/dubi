package recordwrite

import (
	"context"
	"errors"
	"testing"
	"time"
)

type blockingOwnerCuts struct {
	started chan int
	release chan struct{}
	calls   int
	failure error
}

func (b *blockingOwnerCuts) ReadIndex(ctx context.Context, _ string) (uint64, error) {
	b.calls++
	b.started <- b.calls
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-b.release:
		return uint64(b.calls), b.failure
	}
}
func TestOwnerBatchNeverSharesAlreadyStartedCutAndCoalescesQueuedCalls(t *testing.T) {
	base := &blockingOwnerCuts{started: make(chan int, 3), release: make(chan struct{})}
	b := newOwnerConfirmationBatch(context.Background(), base, "owner")
	defer b.close()
	first := make(chan ownerConfirmationCut, 1)
	go func() { n, e := b.ReadIndex(context.Background(), "owner"); first <- ownerConfirmationCut{n, e} }()
	if n := <-base.started; n != 1 {
		t.Fatal(n)
	}
	var waiters []ownerConfirmationWait
	for i := 0; i < 8; i++ {
		w := ownerConfirmationWait{ctx: context.Background(), done: make(chan ownerConfirmationCut, 1)}
		b.waits <- w
		waiters = append(waiters, w)
	}
	base.release <- struct{}{}
	if r := <-first; r.index != 1 || r.err != nil {
		t.Fatal(r)
	}
	if n := <-base.started; n != 2 {
		t.Fatal(n)
	}
	base.release <- struct{}{}
	for _, w := range waiters {
		select {
		case r := <-w.done:
			if r.index != 2 || r.err != nil {
				t.Fatal("shared an earlier cut", r)
			}
		case <-time.After(time.Second):
			t.Fatal("batch did not complete")
		}
	}
	base.failure = errors.New("quorum lost")
	go func() { n, e := b.ReadIndex(context.Background(), "owner"); first <- ownerConfirmationCut{n, e} }()
	if n := <-base.started; n != 3 {
		t.Fatal(n)
	}
	base.release <- struct{}{}
	if r := <-first; r.err == nil {
		t.Fatal("reused earlier successful cut")
	}
}
func TestOwnerBatchCancellationJoinsInFlightRead(t *testing.T) {
	base := &blockingOwnerCuts{started: make(chan int, 1), release: make(chan struct{})}
	b := newOwnerConfirmationBatch(context.Background(), base, "owner")
	done := make(chan error, 1)
	go func() { _, e := b.ReadIndex(context.Background(), "owner"); done <- e }()
	<-base.started
	b.close()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}
