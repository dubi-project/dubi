package v1

type TenantCredential struct {
	CreatedIndex uint64 `json:"created_index"`
	CredentialID string `json:"credential_id"`
	CreatedUnix  int64  `json:"created_unix"`
	ExpiresUnix  int64  `json:"expires_unix"`
	Status       string `json:"status"`
}
type ManagedTenant struct {
	CreatedIndex      uint64             `json:"created_index"`
	TenantID          string             `json:"tenant_id"`
	Name              string             `json:"name"`
	Modules           []string           `json:"modules"`
	Revision          uint64             `json:"revision"`
	StorageLimitBytes uint64             `json:"storage_limit_bytes"`
	UsedBytes         *uint64            `json:"used_bytes"`
	ReservedBytes     *uint64            `json:"reserved_bytes"`
	Accounting        bool               `json:"accounting"`
	Status            string             `json:"status"`
	ActiveCredentials int                `json:"active_credentials"`
	CredentialCount   int                `json:"credential_count"`
	Credentials       []TenantCredential `json:"credentials,omitempty"`
}
type TenantMutation struct {
	RequestID         string   `json:"request_id"`
	TenantID          string   `json:"tenant_id"`
	Name              string   `json:"name"`
	Modules           []string `json:"modules"`
	StorageLimitBytes uint64   `json:"storage_limit_bytes"`
	Revision          *uint64  `json:"revision,omitempty"`
}
type TenantSummary struct {
	Total              int `json:"total"`
	ActiveCredentials  int `json:"active_credentials"`
	WithoutCredentials int `json:"without_credentials"`
}
type TenantPage struct {
	ManagementList[ManagedTenant]
	Total   int           `json:"total"`
	Summary TenantSummary `json:"summary"`
}
type TenantModule struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type TenantTokenRequest struct {
	RequestID    string `json:"request_id"`
	TTLSeconds   uint64 `json:"ttl_seconds"`
	NeverExpires bool   `json:"never_expires,omitempty"`
}
type TenantRevokeRequest struct {
	RequestID string `json:"request_id"`
}
type TenantToken struct {
	CredentialID string `json:"credential_id"`
	Token        string `json:"token"`
	ExpiresUnix  int64  `json:"expires_unix"`
}
