package server

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Confirm one cut within the voter RPC budget, then wait for that exact cut
// using the caller's remaining lifetime. A locally lagging Node must not turn
// a successful quorum read into a request for an ever newer Root position.
func readRootTarget(ctx context.Context, budget time.Duration,
	read func(context.Context) (uint64, error), applied func() uint64,
) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	attempt, cancel := context.WithTimeout(ctx, budget)
	index, err := read(attempt)
	cancel()
	if err != nil {
		return 0, fmt.Errorf("confirm Root quorum: %w", err)
	}
	if index == 0 {
		return 0, errors.New("Root voter returned an empty read cut")
	}
	return waitRootApplication(ctx, index, applied)
}

func waitRootApplication(ctx context.Context, index uint64, applied func() uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("wait for local Root application (confirmed=%d applied=%d): %w", index, applied(), err)
	}
	if applied() >= index {
		return index, nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("wait for local Root application (confirmed=%d applied=%d): %w", index, applied(), ctx.Err())
		case <-ticker.C:
			if applied() >= index {
				return index, nil
			}
		}
	}
}
