package telemetry

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestBusinessInstrumentationAndBoundedHistory(t *testing.T) {
	r := New("node", func() bool { return true })
	start := time.Now().Add(-7 * time.Hour)
	r.last = start
	r.started = start
	handler := r.Wrap(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.Method == "DELETE" {
			w.WriteHeader(409)
		} else {
			w.WriteHeader(200)
		}
	}), "/business")
	for _, x := range []struct{ method, path string }{{"GET", "/business"}, {"PUT", "/business"}, {"DELETE", "/business"}, {"GET", "/api/v1/admin/monitoring"}, {"GET", "/metrics"}} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(x.method, x.path, nil))
	}
	r.sample(start.Add(10 * time.Second))
	v := r.Query(start.Add(10*time.Second).Unix(), 900)
	if v.Latest.Read.Count() != 1 || v.Latest.Write.Count() != 2 || v.Latest.Errors != 1 || v.Latest.Read.P95() == nil {
		t.Fatal("incorrect business counts", v.Latest)
	}
	for i := 2; i < 2600; i++ {
		r.sample(start.Add(time.Duration(i) * 10 * time.Second))
	}
	v = r.Query(start.Add(2599*10*time.Second).Unix(), 21600)
	if r.count != Capacity || len(v.Points) > 121 || len(v.Points) == 0 {
		t.Fatal("unbounded or empty history", r.count, len(v.Points))
	}
	for _, p := range v.Points {
		if p.Read.P95() != nil || p.Write.P95() != nil {
			t.Fatal("idle latency fabricated")
		}
	}
	var b bytes.Buffer
	r.Metrics(&b)
	if strings.Count(b.String(), "# TYPE dubi_http_business_requests_total") != 1 || !strings.Contains(b.String(), `dubi_http_business_requests_total{operation="write"} 2`) {
		t.Fatal("invalid counter exposition")
	}
}
func TestHistogramAndMissingIntervals(t *testing.T) {
	var h Histogram
	h[3] = 19
	h[10] = 1
	if h.P95() == nil || *h.P95() != 8 {
		t.Fatal("wrong percentile")
	}
	r := New("node", nil)
	now := time.Now()
	r.last = now
	r.sample(now.Add(10 * time.Second))
	r.sample(now.Add(100 * time.Second))
	v := r.Query(now.Add(100*time.Second).Unix(), 900)
	if len(v.Points) != 2 {
		t.Fatal("missing intervals filled", v.Points)
	}
}

type blockedMetricsWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w blockedMetricsWriter) Write(p []byte) (int, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	<-w.release
	return len(p), nil
}
func TestSlowMetricsClientDoesNotBlockBusinessAccounting(t *testing.T) {
	r := New("node", nil)
	w := blockedMetricsWriter{make(chan struct{}, 1), make(chan struct{})}
	done := make(chan struct{})
	go func() { r.Metrics(w); close(done) }()
	<-w.entered
	observed := make(chan struct{})
	go func() { r.Observe(false, time.Millisecond, 200); close(observed) }()
	select {
	case <-observed:
	case <-time.After(time.Second):
		close(w.release)
		<-done
		t.Fatal("slow metrics writer blocked request accounting")
	}
	close(w.release)
	<-done
}
func TestPeerSeriesValidation(t *testing.T) {
	r := New("node", nil)
	now := time.Now()
	r.sample(now.Add(time.Second))
	end := now.Add(time.Second).Unix()
	v := r.Query(end, 900)
	if !v.Valid("node", end, 900) {
		t.Fatal("own series invalid")
	}
	if v.Valid("other", end, 900) {
		t.Fatal("wrong peer accepted")
	}
	v.Points = append(v.Points, v.Points[0])
	if v.Valid("node", end, 900) {
		t.Fatal("duplicate bucket accepted")
	}
}

func TestResourceCapacitySurvivesPeerTransport(t *testing.T) {
	r := New("node", nil)
	now := time.Now().Add(time.Second)
	r.sample(now)
	source := r.Query(now.Unix(), 900)
	data, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var view View
	if err := json.Unmarshal(data, &view); err != nil {
		t.Fatal(err)
	}
	if !view.Valid("node", now.Unix(), 900) || view.Latest.CPUCores == nil || *view.Latest.CPUCores != runtime.NumCPU() {
		t.Fatal("missing CPU denominator", view.Latest)
	}
	if runtime.GOOS == "linux" && (view.Latest.MemoryTotal == nil || *view.Latest.MemoryTotal == 0 || view.Latest.RSS == nil) {
		t.Fatal("missing Linux memory denominator", view.Latest)
	}
	zero := 0
	view.Latest.CPUCores = &zero
	if view.Valid("node", now.Unix(), 900) {
		t.Fatal("zero CPU denominator accepted")
	}
	view.Latest.CPUCores = nil // Older peers may omit capacity; do not invent it.
	if !view.Valid("node", now.Unix(), 900) {
		t.Fatal("legacy missing capacity rejected")
	}
	zeroBytes := uint64(0)
	view.Latest.MemoryTotal = &zeroBytes
	if view.Valid("node", now.Unix(), 900) {
		t.Fatal("zero memory denominator accepted")
	}
}
