package enginedata

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReadConfirmationRefreshIsBoundedAndDoesNotQueue(t *testing.T) {
	var gate readConfirmationRefresh
	entered, release := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		attempted, err := gate.run(context.Background(), readConfirmationMaxBytes, true, func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > readConfirmationTimeout {
				return errors.New("missing finite refresh deadline")
			}
			close(entered)
			<-release
			return nil
		})
		if !attempted && err == nil {
			err = errors.New("eligible refresh skipped")
		}
		finished <- err
	}()
	select {
	case <-entered:
	case err := <-finished:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	for i := 0; i < 32; i++ {
		attempted, err := gate.run(context.Background(), 1, true, func(context.Context) error { t.Fatal("busy refresh queued work"); return nil })
		if attempted || err != nil {
			t.Fatal("busy refresh did not skip", attempted, err)
		}
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	marker := errors.New("failed refresh")
	for _, scenario := range []string{"follower", "too large", "canceled", "failed", "success"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			size := uint64(1)
			if scenario == "too large" {
				size = readConfirmationMaxBytes + 1
			}
			if scenario == "canceled" {
				cancel()
			}
			calls := 0
			attempted, err := gate.run(ctx, size, scenario != "follower", func(context.Context) error {
				calls++
				if scenario == "failed" {
					return marker
				}
				return nil
			})
			switch scenario {
			case "follower", "too large":
				if attempted || calls != 0 || err != nil {
					t.Fatal(attempted, calls, err)
				}
			case "canceled":
				if !attempted || calls != 0 || !errors.Is(err, context.Canceled) {
					t.Fatal(attempted, calls, err)
				}
			case "failed":
				if !attempted || calls != 1 || !errors.Is(err, marker) {
					t.Fatal(attempted, calls, err)
				}
			case "success":
				if !attempted || calls != 1 || err != nil {
					t.Fatal("slot not released after failure", attempted, calls, err)
				}
			}
		})
	}
}

func TestReadConfirmationRefreshHonorsEarlierDeadlineAndReleasesSlot(t *testing.T) {
	var gate readConfirmationRefresh
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	wantDeadline, _ := ctx.Deadline()
	attempted, err := gate.run(ctx, 1, true, func(call context.Context) error {
		if deadline, ok := call.Deadline(); !ok || deadline != wantDeadline {
			t.Fatal("caller deadline extended", deadline)
		}
		<-call.Done()
		return nil // Completion after cancellation cannot report success.
	})
	if !attempted || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expired work reported success", attempted, err)
	}
	attempted, err = gate.run(context.Background(), 1, true, func(context.Context) error { return nil })
	if !attempted || err != nil {
		t.Fatal("expired work retained slot", attempted, err)
	}
}
