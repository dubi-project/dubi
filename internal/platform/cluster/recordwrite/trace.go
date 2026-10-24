package recordwrite

import (
	"context"
	"time"
)

// TraceFunc receives opt-in timings for one data operation. Production calls
// leave it unset; tracing is enabled only by the server's diagnostic switch.
type TraceFunc func(phase string, elapsed time.Duration)

type traceContextKey struct{}

func WithTrace(ctx context.Context, trace TraceFunc) context.Context {
	if ctx == nil || trace == nil {
		return ctx
	}
	return context.WithValue(ctx, traceContextKey{}, trace)
}

func Trace(ctx context.Context, phase string, started time.Time) {
	if ctx == nil {
		return
	}
	trace, _ := ctx.Value(traceContextKey{}).(TraceFunc)
	if trace != nil {
		trace(phase, time.Since(started))
	}
}
