package server

import (
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/auth"
)

func TestRuntimeMemoryMetricsBeforeNodeReady(t *testing.T) {
	old := debug.SetMemoryLimit(321 << 20)
	t.Cleanup(func() { debug.SetMemoryLimit(old) })
	s := newPublicHTTPServerWithMonitoring(auth.Verifier{}, nil, nil, nil)
	w := httptest.NewRecorder()
	s.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}
	values := map[string]uint64{}
	for _, line := range strings.Split(w.Body.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || !strings.HasPrefix(fields[0], "dubi_go_") {
			continue
		}
		n, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		values[fields[0]] = n
	}
	if len(values) != 10 || values["dubi_go_memory_limit_bytes"] != 321<<20 || values["dubi_go_goroutines"] == 0 || values["dubi_go_memory_total_bytes"] < values["dubi_go_heap_objects_bytes"] {
		t.Fatalf("invalid runtime metrics: %v", values)
	}
	if !strings.Contains(w.Body.String(), "# TYPE dubi_go_gc_cycles_total counter") || !strings.Contains(w.Body.String(), "# TYPE dubi_go_heap_live_bytes gauge") {
		t.Fatal("incorrect metric types")
	}
}
