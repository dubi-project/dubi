package server

import (
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

func TestBackupRequestIDIsBoundedAndValid(t *testing.T) {
	jobID := "job-" + string(make([]byte, 120))
	requestID := backupRequestID("claim", jobID, 42, 7)
	if len(requestID) > 128 {
		t.Fatalf("request ID is too long: %d", len(requestID))
	}
	if err := rootstate.ValidateRequestID(requestID); err != nil {
		t.Fatal(err)
	}
}
