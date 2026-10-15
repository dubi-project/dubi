// Package resource provides node-local admission primitives shared by platform
// components. ByteBudget bounds cooperating buffers, not total process RSS.
package resource

import (
	"errors"
	"fmt"
	"sync"
)

var ErrBudgetExceeded = errors.New("resource byte budget exhausted")

type BudgetStatus struct {
	LimitBytes uint64 `json:"limit_bytes"`
	UsedBytes  uint64 `json:"used_bytes"`
	PeakBytes  uint64 `json:"peak_bytes"`
	Rejected   uint64 `json:"rejected"`
}

// ByteBudget is non-blocking: a Raft sender must shed/retry work through Raft,
// not wait for network IO while holding application or consensus locks.
type ByteBudget struct {
	mu     sync.Mutex
	status BudgetStatus
}

func NewByteBudget(limit uint64) *ByteBudget {
	return &ByteBudget{status: BudgetStatus{LimitBytes: limit}}
}

// TryAcquire returns an idempotent release shared by all copies of the closure.
// The caller acquires before allocating and releases after the final consumer.
func (b *ByteBudget) TryAcquire(bytes uint64) (func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if bytes > b.status.LimitBytes-b.status.UsedBytes {
		b.status.Rejected++
		return nil, fmt.Errorf("%w: requested=%d used=%d limit=%d", ErrBudgetExceeded, bytes, b.status.UsedBytes, b.status.LimitBytes)
	}
	b.status.UsedBytes += bytes
	b.status.PeakBytes = max(b.status.PeakBytes, b.status.UsedBytes)
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.status.UsedBytes -= bytes
			b.mu.Unlock()
		})
	}, nil
}

func (b *ByteBudget) Status() BudgetStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.status
}
