package raftrpc

import (
	"log"
	"os"
	"time"
)

func traceRecordRPC(operation, recordID, phase string, started time.Time) {
	if os.Getenv("DUBI_TRACE_DATA") == "1" {
		log.Printf("dubi_data_trace operation=%s record_id=%s phase=%s elapsed_ms=%d",
			operation, recordID, phase, time.Since(started).Milliseconds())
	}
}

func traceRecordID(descriptorID func() (string, error)) string {
	id, err := descriptorID()
	if err != nil || id == "" {
		return "invalid"
	}
	return id
}
