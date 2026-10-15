package recordrepair

import (
	"context"
	"time"
)

// Give a stalled source only its share of the caller's remaining deadline.
// Reserve one additional share for local Pack Sync and final verification, so
// a slow first source cannot consume nearly all useful fallback time under IO
// contention. The confirmed cohort still determines eligible sources.
func sourceAttemptContext(ctx context.Context, remainingSources int) (context.Context, context.CancelFunc) {
	deadline, bounded := ctx.Deadline()
	if !bounded || remainingSources <= 1 {
		return context.WithCancel(ctx)
	}
	share := time.Until(deadline) / time.Duration(remainingSources+1)
	if share <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, share)
}
