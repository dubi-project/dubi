package server

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadRootTargetKeepsConfirmedCutWhileLocalNodeCatchesUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var applied, calls atomic.Uint64
	confirmed := make(chan struct{})
	type result struct {
		index uint64
		err   error
	}
	done := make(chan result, 1)
	go func() {
		index, err := readRootTarget(ctx, 20*time.Millisecond, func(context.Context) (uint64, error) {
			if calls.Add(1) == 1 {
				close(confirmed)
				return 10, nil
			}
			return 20, nil // A later global head must not replace the confirmed cut.
		}, applied.Load)
		done <- result{index, err}
	}()
	select {
	case <-confirmed:
	case <-ctx.Done():
		t.Fatal("Root proof was not obtained")
	}
	select {
	case value := <-done:
		t.Fatal("local catch-up incorrectly inherited the shorter voter RPC budget", value)
	case <-time.After(60 * time.Millisecond):
	}
	applied.Store(10)
	select {
	case value := <-done:
		if value.err != nil || value.index != 10 || calls.Load() != 1 {
			t.Fatalf("fixed Root cut: index=%d calls=%d err=%v", value.index, calls.Load(), value.err)
		}
	case <-ctx.Done():
		t.Fatal("local catch-up chased a newer Root position")
	}
}

func TestReadRootTargetLocalWaitHonorsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	confirmed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := readRootTarget(ctx, time.Second, func(context.Context) (uint64, error) {
			close(confirmed)
			return 10, nil
		}, func() uint64 { return 9 })
		done <- err
	}()
	<-confirmed
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "local Root application") {
			t.Fatal("lost the local-application cancellation diagnosis", err)
		}
	case <-time.After(time.Second):
		t.Fatal("local Root waiter did not exit on caller cancellation")
	}
}

func TestReadRootTargetRejectsUnconfirmedCuts(t *testing.T) {
	for _, proofError := range []error{nil, errors.New("quorum unavailable")} {
		_, err := readRootTarget(context.Background(), time.Second,
			func(context.Context) (uint64, error) { return 0, proofError },
			func() uint64 { t.Error("unconfirmed cut reached local-application check"); return 100 })
		if err == nil || (proofError != nil && !errors.Is(err, proofError)) {
			t.Fatal("unconfirmed Root proof was accepted or its error lost", err)
		}
	}
}
