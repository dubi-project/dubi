package server

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
)

func withDataTrace(ctx context.Context, operation, requestID string) context.Context {
	if os.Getenv("DUBI_TRACE_DATA") != "1" {
		return ctx
	}
	return recordwrite.WithTrace(ctx, func(phase string, elapsed time.Duration) {
		log.Printf("dubi_data_trace operation=%s request_id=%q phase=%s elapsed_ms=%d",
			operation, requestID, phase, elapsed.Milliseconds())
	})
}
