package pack

import (
	"log"
	"os"
	"time"
)

func tracePack(phase, subject string, started time.Time) {
	if os.Getenv("DUBI_TRACE_DATA") == "1" {
		log.Printf("dubi_data_trace storage=pack subject=%q phase=%s elapsed_us=%d", subject, phase, time.Since(started).Microseconds())
	}
}
