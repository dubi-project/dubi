// Package telemetry keeps bounded, process-local monitoring samples. It does
// not read user keys, participate in consensus, or retain request labels.
package telemetry

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"sync"
	"time"
)

const Interval = 10 * time.Second
const Retention = 6 * time.Hour
const Capacity = int(Retention/Interval) + 2
const Bins = 17

var Bounds = [Bins]float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 65536, 300000}

type Histogram [Bins]uint64

func (h Histogram) Count() (n uint64) {
	for _, v := range h {
		n += v
	}
	return
}
func (h *Histogram) Add(other Histogram) {
	for i, v := range other {
		h[i] += v
	}
}
func (h Histogram) P95() *float64 {
	n := h.Count()
	if n == 0 {
		return nil
	}
	var seen uint64
	for i, v := range h {
		seen += v
		if float64(seen) >= math.Ceil(float64(n)*.95) {
			x := Bounds[i]
			return &x
		}
	}
	return nil
}

type Sample struct {
	CPUCores    *int      `json:"cpu_cores"`
	MemoryTotal *uint64   `json:"memory_total_bytes"`
	Unix        int64     `json:"unix"`
	Seconds     float64   `json:"seconds"`
	Read        Histogram `json:"read"`
	Write       Histogram `json:"write"`
	Errors      uint64    `json:"errors"`
	CPU         *float64  `json:"cpu_percent"`
	RSS         *uint64   `json:"rss_bytes"`
	Ready       bool      `json:"ready"`
}
type View struct {
	NodeID      string   `json:"node_id"`
	StartedUnix int64    `json:"started_unix"`
	Latest      *Sample  `json:"latest"`
	Points      []Sample `json:"points"`
}
type Recorder struct {
	mu                        sync.Mutex
	node                      string
	started                   time.Time
	ready                     func() bool
	samples                   [Capacity]Sample
	next, count               int
	read, write               Histogram
	totalRead, totalWrite     Histogram
	totalReadMS, totalWriteMS float64
	errors, totalErrors       uint64
	last                      time.Time
	lastCPU                   *float64
}

func New(node string, ready func() bool) *Recorder {
	now := time.Now()
	return &Recorder{node: node, started: now, ready: ready, last: now}
}
func (r *Recorder) Observe(write bool, duration time.Duration, status int) {
	ms := float64(duration) / float64(time.Millisecond)
	i := 0
	for i < Bins-1 && ms > Bounds[i] {
		i++
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if write {
		r.write[i]++
		r.totalWrite[i]++
		r.totalWriteMS += ms
	} else {
		r.read[i]++
		r.totalRead[i]++
		r.totalReadMS += ms
	}
	if status >= 400 {
		r.errors++
		r.totalErrors++
	}
}

// Wrap only the configured business route. Health checks, management, static
// files and internal replication are deliberately excluded from business QPS.
func (r *Recorder) Wrap(next http.Handler, path string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path != path || (q.Method != "GET" && q.Method != "PUT" && q.Method != "DELETE") {
			next.ServeHTTP(w, q)
			return
		}
		start := time.Now()
		s := &response{ResponseWriter: w, status: 200}
		defer func() { r.Observe(q.Method != "GET", time.Since(start), s.status) }()
		next.ServeHTTP(s, q)
	})
}

type response struct {
	http.ResponseWriter
	status  int
	written bool
}

func (w *response) WriteHeader(code int) {
	if w.written {
		return
	}
	w.status = code
	w.written = true
	w.ResponseWriter.WriteHeader(code)
}
func (w *response) Write(b []byte) (int, error) {
	if !w.written {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (w *response) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (r *Recorder) sample(now time.Time) {
	cpu, rss := processUsage()
	cores, total := runtime.NumCPU(), physicalMemory()
	ready := r.ready != nil && r.ready()
	r.mu.Lock()
	defer r.mu.Unlock()
	seconds := now.Sub(r.last).Seconds()
	if seconds <= 0 {
		return
	}
	var percent *float64
	if cpu != nil && r.lastCPU != nil && *cpu >= *r.lastCPU {
		v := (*cpu - *r.lastCPU) / seconds * 100
		percent = &v
	}
	r.samples[r.next] = Sample{CPUCores: &cores, MemoryTotal: total, Unix: now.Unix(), Seconds: seconds, Read: r.read, Write: r.write, Errors: r.errors, CPU: percent, RSS: rss, Ready: ready}
	r.next = (r.next + 1) % Capacity
	r.count = min(r.count+1, Capacity)
	r.last = now
	r.lastCPU = cpu
	r.read = Histogram{}
	r.write = Histogram{}
	r.errors = 0
}
func (r *Recorder) Run(ctx context.Context) {
	r.sample(time.Now())
	ticker := time.NewTicker(Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.sample(now)
		}
	}
}

// Query returns no more than 121 buckets. Samples are never fabricated for
// intervals before startup or across missing sampling periods.
func (r *Recorder) Query(end int64, seconds int64) View {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := View{NodeID: r.node, StartedUnix: r.started.Unix(), Points: []Sample{}}
	if r.count > 0 {
		s := r.samples[(r.next+Capacity-1)%Capacity]
		v.Latest = &s
	}
	if seconds != 900 && seconds != 3600 && seconds != 21600 {
		return v
	}
	step := int64(math.Ceil(float64(seconds)/120/10)) * 10
	start := end - seconds
	buckets := make([]Sample, int(seconds/step)+1)
	used := make([]bool, len(buckets))
	for j := 0; j < r.count; j++ {
		s := r.samples[(r.next-r.count+j+Capacity)%Capacity]
		if s.Unix <= start || s.Unix > end {
			continue
		}
		i := int((s.Unix - start - 1) / step)
		if i >= len(buckets) {
			continue
		}
		b := &buckets[i]
		b.Unix = min(start+int64(i+1)*step, end)
		b.Seconds += s.Seconds
		b.Read.Add(s.Read)
		b.Write.Add(s.Write)
		b.Errors += s.Errors
		used[i] = true
	}
	for i, b := range buckets {
		if used[i] {
			v.Points = append(v.Points, b)
		}
	}
	return v
}
func (r *Recorder) Metrics(w io.Writer) {
	r.mu.Lock()
	reads, writes := r.totalRead, r.totalWrite
	readMS, writeMS, errors := r.totalReadMS, r.totalWriteMS, r.totalErrors
	var latest *Sample
	if r.count > 0 {
		copy := r.samples[(r.next+Capacity-1)%Capacity]
		latest = &copy
	}
	r.mu.Unlock()
	// Never hold the recorder lock while writing to a potentially slow client.
	fmt.Fprintln(w, "# TYPE dubi_http_business_requests_total counter")
	for i, h := range []Histogram{reads, writes} {
		fmt.Fprintf(w, "dubi_http_business_requests_total{operation=%q} %d\n", []string{"read", "write"}[i], h.Count())
	}
	fmt.Fprintln(w, "# TYPE dubi_http_business_duration_milliseconds histogram")
	for i, h := range []Histogram{reads, writes} {
		kind := []string{"read", "write"}[i]
		var sum uint64
		for j, n := range h {
			sum += n
			fmt.Fprintf(w, "dubi_http_business_duration_milliseconds_bucket{operation=%q,le=%q} %d\n", kind, fmt.Sprint(Bounds[j]), sum)
		}
		fmt.Fprintf(w, "dubi_http_business_duration_milliseconds_bucket{operation=%q,le=\"+Inf\"} %d\ndubi_http_business_duration_milliseconds_count{operation=%q} %d\ndubi_http_business_duration_milliseconds_sum{operation=%q} %g\n", kind, sum, kind, sum, kind, []float64{readMS, writeMS}[i])
	}
	fmt.Fprintf(w, "# TYPE dubi_http_business_errors_total counter\ndubi_http_business_errors_total %d\n", errors)
	if latest != nil {
		s := *latest
		if s.RSS != nil {
			fmt.Fprintf(w, "# TYPE dubi_process_resident_memory_bytes gauge\ndubi_process_resident_memory_bytes %d\n", *s.RSS)
		}
		if s.CPU != nil {
			fmt.Fprintf(w, "# TYPE dubi_process_cpu_percent gauge\ndubi_process_cpu_percent %g\n", *s.CPU)
		}
	}
}

// Valid protects aggregation against malformed peer samples, duplicate buckets
// and unsupported values. It never treats missing data as a zero-valued sample.
func (v View) Valid(node string, end, seconds int64) bool {
	if v.NodeID != node || v.StartedUnix <= 0 || v.StartedUnix > end+60 || len(v.Points) > 121 {
		return false
	}
	valid := func(s Sample) bool {
		if s.Unix <= 0 || s.Unix > end+2 || s.Seconds <= 0 || s.Seconds > 86400 || math.IsNaN(s.Seconds) || math.IsInf(s.Seconds, 0) {
			return false
		}
		if s.CPUCores != nil && (*s.CPUCores <= 0 || *s.CPUCores > 1e6) {
			return false
		}
		if s.MemoryTotal != nil && *s.MemoryTotal == 0 {
			return false
		}
		if s.CPU != nil && (*s.CPU < 0 || *s.CPU > 1e6 || math.IsNaN(*s.CPU) || math.IsInf(*s.CPU, 0)) {
			return false
		}
		for _, h := range []Histogram{s.Read, s.Write} {
			for _, n := range h {
				if n > 1e12 {
					return false
				}
			}
		}
		return true
	}
	if v.Latest != nil && !valid(*v.Latest) {
		return false
	}
	previous := end - seconds
	step := int64(math.Ceil(float64(seconds)/120/10)) * 10
	for _, p := range v.Points {
		if !valid(p) || p.Unix <= previous || p.Unix > end || (p.Unix-(end-seconds))%step != 0 {
			return false
		}
		previous = p.Unix
	}
	return true
}
