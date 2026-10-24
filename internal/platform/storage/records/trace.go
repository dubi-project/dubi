package records

import (
	"log"
	"os"
	"time"
)

func traceRecordStore(phase, recordID string, started time.Time) {
	if os.Getenv("DUBI_TRACE_DATA") == "1" {
		log.Printf("dubi_data_trace storage=records record_id=%s phase=%s elapsed_us=%d", recordID, phase, time.Since(started).Microseconds())
	}
}
