package v1

// Administrative DTOs deliberately contain neither verifier hashes nor encrypted secrets.
type ManagedCredential struct {
	Permission   string   `json:"permission,omitempty"`
	CreatedIndex uint64   `json:"created_index"`
	CredentialID string   `json:"credential_id"`
	Name         string   `json:"name"`
	Kind         string   `json:"kind"`
	TenantID     string   `json:"tenant_id,omitempty"`
	TenantName   string   `json:"tenant_name,omitempty"`
	Modules      []string `json:"modules,omitempty"`
	CreatedUnix  int64    `json:"created_unix"`
	ExpiresUnix  int64    `json:"expires_unix"`
	Status       string   `json:"status"`
	RequestID    string   `json:"request_id,omitempty"`
	Current      bool     `json:"current"`
}
type CredentialMutation struct {
	Modules      []string `json:"modules,omitempty"`
	Permission   string   `json:"permission,omitempty"`
	RequestID    string   `json:"request_id"`
	Name         string   `json:"name"`
	Kind         string   `json:"kind"`
	TenantID     string   `json:"tenant_id,omitempty"`
	TTLSeconds   uint64   `json:"ttl_seconds"`
	NeverExpires bool     `json:"never_expires,omitempty"`
}
type CredentialPage struct {
	ManagementList[ManagedCredential]
	Total   int            `json:"total"`
	Summary map[string]int `json:"summary"`
}
type ManagedRepository struct {
	CreatedIndex          uint64 `json:"created_index"`
	Name                  string `json:"name"`
	Type                  string `json:"type"`
	Path                  string `json:"path"`
	Bucket                string `json:"bucket"`
	Prefix                string `json:"prefix"`
	Endpoint              string `json:"endpoint"`
	Region                string `json:"region"`
	PathStyle             bool   `json:"path_style"`
	CredentialsConfigured bool   `json:"credentials_configured"`
	Revision              string `json:"revision"`
	Default               bool   `json:"default"`
	CompletedBackups      int    `json:"completed_backups"`
	LatestCreatedUnix     int64  `json:"latest_created_unix,omitempty"`
}
type RepositoryMutation struct {
	RequestID    string `json:"request_id"`
	Name         string `json:"name"`
	Type         string `json:"type"`
	Path         string `json:"path"`
	Bucket       string `json:"bucket"`
	Prefix       string `json:"prefix"`
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	PathStyle    bool   `json:"path_style"`
	Revision     string `json:"revision,omitempty"`
	AccessKey    string `json:"access_key,omitempty"`
	SecretKey    string `json:"secret_key,omitempty"`
	SessionToken string `json:"session_token,omitempty"`
}
type BackupMutation struct {
	RequestID  string `json:"request_id"`
	Repository string `json:"repository"`
}
type ManagedBackupWorker struct {
	NodeID  string `json:"node_id"`
	Name    string `json:"name"`
	Region  string `json:"region"`
	State   string `json:"state"`
	Bytes   int64  `json:"bytes"`
	Objects uint64 `json:"objects"`
	Error   string `json:"error,omitempty"`
}
type ManagedBackup struct {
	JobID          string                `json:"job_id"`
	Repository     string                `json:"repository"`
	RepositoryType string                `json:"repository_type"`
	ClusterID      string                `json:"cluster_id"`
	Epoch          uint64                `json:"epoch"`
	CreatedIndex   uint64                `json:"created_index"`
	CreatedUnix    int64                 `json:"created_unix,omitempty"`
	State          string                `json:"state"`
	Error          string                `json:"error,omitempty"`
	Bytes          int64                 `json:"bytes"`
	Objects        uint64                `json:"objects"`
	CompletedNodes int                   `json:"completed_nodes"`
	TotalNodes     int                   `json:"total_nodes"`
	Workers        []ManagedBackupWorker `json:"workers,omitempty"`
}
type BackupPage struct {
	ManagementList[ManagedBackup]
	Total int `json:"total"`
}
type RestorePreparation struct {
	Backup     ManagedBackup     `json:"backup"`
	Repository ManagedRepository `json:"repository"`
	Target     string            `json:"target"`
	Steps      []string          `json:"steps"`
}
