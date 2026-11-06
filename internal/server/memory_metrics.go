package server

import (
	"fmt"
	"io"
	"runtime/metrics"
)

// Runtime counters distinguish live Go objects from retained heap pages.
// Scraping does not force GC or expose heap contents, keys, or allocation stacks.
func writeRuntimeMemoryMetrics(w io.Writer) {
	definitions := []struct{ runtime, name, help, kind string }{
		{"/memory/classes/heap/objects:bytes", "dubi_go_heap_objects_bytes", "Bytes occupied by allocated Go heap objects, including uncollected garbage.", "gauge"},
		{"/gc/heap/live:bytes", "dubi_go_heap_live_bytes", "Go heap bytes marked live by the most recent garbage collection.", "gauge"},
		{"/gc/heap/goal:bytes", "dubi_go_heap_goal_bytes", "Target Go heap size for the next garbage collection.", "gauge"},
		{"/memory/classes/heap/free:bytes", "dubi_go_heap_free_bytes", "Free Go heap pages retained for reuse.", "gauge"},
		{"/memory/classes/heap/released:bytes", "dubi_go_heap_released_bytes", "Go heap pages returned to the operating system.", "gauge"},
		{"/memory/classes/total:bytes", "dubi_go_memory_total_bytes", "Total Go runtime mapped memory, including released heap pages; not process RSS.", "gauge"},
		{"/gc/gomemlimit:bytes", "dubi_go_memory_limit_bytes", "Configured Go runtime soft memory limit; not an RSS or heap-only hard limit.", "gauge"},
		{"/gc/heap/allocs:bytes", "dubi_go_heap_allocated_bytes_total", "Cumulative Go heap bytes allocated since process start.", "counter"},
		{"/gc/cycles/total:gc-cycles", "dubi_go_gc_cycles_total", "Completed Go garbage collection cycles.", "counter"},
		{"/sched/goroutines:goroutines", "dubi_go_goroutines", "Current Go goroutine count.", "gauge"},
	}
	samples := make([]metrics.Sample, len(definitions))
	for i, d := range definitions {
		samples[i].Name = d.runtime
	}
	metrics.Read(samples)
	for i, d := range definitions {
		if samples[i].Value.Kind() != metrics.KindUint64 {
			continue // Unsupported runtime metrics are absent, never reported as zero.
		}
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", d.name, d.help, d.name, d.kind, d.name, samples[i].Value.Uint64())
	}
}
