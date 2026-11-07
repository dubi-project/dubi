package v1

// Monitoring values are observations. Nil is unavailable, not zero.
type MonitoringMetrics struct {
	CPUCores    *int     `json:"cpu_cores"`
	MemoryTotal *uint64  `json:"memory_total_bytes"`
	CPU         *float64 `json:"cpu_percent"`
	RSS         *uint64  `json:"rss_bytes"`
	ReadRate    *float64 `json:"read_rate"`
	WriteRate   *float64 `json:"write_rate"`
	ReadP95     *float64 `json:"read_p95_ms"`
	WriteP95    *float64 `json:"write_p95_ms"`
	Ready       *bool    `json:"ready"`
	SampleUnix  *int64   `json:"sample_unix"`
}
type MonitoringNode struct {
	ManagedNode
	Metrics MonitoringMetrics `json:"metrics"`
}
type MonitoringPoint struct {
	Unix          int64    `json:"unix"`
	ReadRate      float64  `json:"read_rate"`
	WriteRate     float64  `json:"write_rate"`
	ReadP95       *float64 `json:"read_p95_ms"`
	WriteP95      *float64 `json:"write_p95_ms"`
	ObservedNodes int      `json:"observed_nodes"`
}
type Monitoring struct {
	Nodes            []MonitoringNode  `json:"nodes"`
	Points           []MonitoringPoint `json:"points"`
	Summary          ManagementSummary `json:"summary"`
	EndUnix          int64             `json:"end_unix"`
	WindowSeconds    int64             `json:"window_seconds"`
	StepSeconds      int64             `json:"step_seconds"`
	SampleSeconds    int               `json:"sample_seconds"`
	RetentionSeconds int               `json:"retention_seconds"`
}
