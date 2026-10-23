package raftrpc

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAcquireQueuedSlotKeepsBoundAndDrainsWaiters(t *testing.T) {
	active := make(chan struct{}, 2)
	waiters := make(chan struct{}, 2)
	active <- struct{}{}
	active <- struct{}{}
	type outcome struct {
		release func()
		err     error
	}
	results := make(chan outcome, 2)
	for range 2 {
		go func() {
			release, err := acquireQueuedSlot(context.Background(), active, waiters, time.Second, "busy")
			results <- outcome{release: release, err: err}
		}()
	}
	deadline := time.Now().Add(time.Second)
	for len(waiters) != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(waiters) != 2 {
		t.Fatal("callers did not enter the bounded wait queue")
	}
	if _, err := acquireQueuedSlot(context.Background(), active, waiters, time.Second, "busy"); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("overflow was not rejected: %v", err)
	}
	<-active
	for range 2 {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatal(result.err)
			}
			result.release()
		case <-time.After(time.Second):
			t.Fatal("queued caller did not acquire released capacity")
		}
	}
	<-active // release the second slot prefilled by the test
	if len(active) != 0 || len(waiters) != 0 {
		t.Fatal("admission slots leaked after queued calls completed")
	}
}

func TestAcquireQueuedSlotHonorsContextDeadline(t *testing.T) {
	active := make(chan struct{}, 1)
	active <- struct{}{}
	waiters := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := acquireQueuedSlot(ctx, active, waiters, time.Second, "busy"); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("queued call ignored context deadline: %v", err)
	}
	if len(waiters) != 0 {
		t.Fatal("timed-out waiter retained queue capacity")
	}
}
