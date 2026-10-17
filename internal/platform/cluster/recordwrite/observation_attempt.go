package recordwrite

import (
	"context"
	"time"
)

// observationAttemptContext prevents one offline cohort Node from consuming
// the whole repair/reconfirmation deadline. Keep a share for each remaining
// candidate; the final candidate receives the remainder so a healthy fallback
// can still complete over a WAN.
func observationAttemptContext(ctx context.Context, remaining int) (context.Context, context.CancelFunc) {
	deadline, bounded := ctx.Deadline()
	if !bounded || remaining <= 1 {
		return context.WithCancel(ctx)
	}
	share := time.Until(deadline) / time.Duration(remaining+1)
	if share <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, share)
}
