package v1

// Management responses describe observations, not data durability or write admission.
type ManagementResponse struct {
	Data any            `json:"data"`
	Meta ManagementMeta `json:"meta"`
}
type ManagementMeta struct {
	RequestID       string             `json:"request_id"`
	GeneratedUnix   int64              `json:"generated_unix"`
	Complete        bool               `json:"complete"`
	Errors          []ObservationError `json:"errors"`
	ErrorsTruncated bool               `json:"errors_truncated"`
}
type ObservationError struct {
	NodeID  string `json:"node_id,omitempty"`
	Code    string `json:"code"`
	Message string `json:"message"`
}
type ManagementList[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor"`
}
type Observation struct {
	ObservedUnix *int64 `json:"observed_unix"`
	Stale        bool   `json:"stale"`
	Complete     bool   `json:"complete"`
}
type OperationalHealth struct {
	Status  string             `json:"status"`
	Reasons []ObservationError `json:"reasons"`
}
type FilesystemCapacity struct {
	FilesystemTotalBytes     uint64 `json:"filesystem_total_bytes"`
	FilesystemAvailableBytes uint64 `json:"filesystem_available_bytes"`
	// Only Node observations expose the placement hint. It is not additive.
	WritableBytes *uint64 `json:"writable_bytes,omitempty"`
}
type ManagedVolume struct {
	CreatedUnixNano int64               `json:"created_unix_nano"`
	VolumeID        string              `json:"volume_id"`
	Path            string              `json:"path"`
	State           string              `json:"state"`
	Health          string              `json:"health"`
	Error           string              `json:"error,omitempty"`
	WriteIsolated   bool                `json:"write_isolated"`
	WriteError      string              `json:"write_error,omitempty"`
	Capacity        *FilesystemCapacity `json:"capacity"`
	Observation
	Migration *MaintenanceActivity `json:"migration,omitempty"`
}

// ClusterVolume identifies the node that owns a volume; IDs are scoped as a pair.
type ClusterVolume struct {
	NodeID   string `json:"node_id"`
	NodeName string `json:"node_name"`
	RegionID string `json:"region_id"`
	ManagedVolume
}
type MaintenanceActivity struct {
	Kind             string   `json:"kind"`
	Supported        bool     `json:"supported"`
	Phase            string   `json:"phase"`
	Processed        *uint64  `json:"processed"`
	Unit             string   `json:"unit,omitempty"`
	Total            *uint64  `json:"total"`
	Pending          *uint64  `json:"pending"`
	LastProgressUnix *int64   `json:"last_progress_unix"`
	NeedsFullScan    *bool    `json:"needs_full_scan,omitempty"`
	SourceVolumeID   string   `json:"source_volume_id,omitempty"`
	TargetVolumeID   string   `json:"target_volume_id,omitempty"`
	Errors           []string `json:"errors"`
}
type NodeMaintenance struct {
	CreatedIndex uint64 `json:"created_index"`
	NodeID       string `json:"node_id"`
	RegionID     string `json:"region_id"`
	Observation
	Items []MaintenanceActivity `json:"items"`
}
type ManagedNode struct {
	CreatedIndex       uint64 `json:"created_index"`
	RegionCreatedIndex uint64 `json:"-"`
	NodeID             string `json:"node_id"`
	Name               string `json:"name"`
	RegionID           string `json:"region_id"`
	Membership         string `json:"membership"`
	OperationalHealth
	Observation
	Capacity                *FilesystemCapacity `json:"capacity"`
	VolumeCount             *uint32             `json:"volume_count"`
	CapacityObservedVolumes *uint32             `json:"capacity_observed_volumes"`
	Maintenance             *NodeMaintenance    `json:"maintenance"`
}
type ManagementSummary struct {
	OperationalHealth
	NodeCount             int                 `json:"node_count"`
	NodeStates            map[string]int      `json:"node_states"`
	ObservedNodes         int                 `json:"observed_nodes"`
	CapacityObservedNodes int                 `json:"capacity_observed_nodes"`
	Capacity              *FilesystemCapacity `json:"capacity"`
	// Sum of node filesystem observations, not globally deduplicated physical storage.
	CapacityScope            string            `json:"capacity_scope"`
	MaintenanceObservedNodes int               `json:"maintenance_observed_nodes"`
	MaintenancePending       map[string]uint64 `json:"maintenance_pending"`
	Observation
}
type ManagedRegion struct {
	CreatedIndex uint64 `json:"created_index"`
	RegionID     string `json:"region_id"`
	ManagementSummary
}
type ManagedCluster struct {
	ClusterID string `json:"cluster_id"`
	ManagementSummary
	Regions []ManagedRegion `json:"regions"`
}
