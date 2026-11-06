package v1

const (
	IdentityPath    = "/api/v1/me"
	LoginPath       = "/api/v1/auth/login"
	LogoutPath      = "/api/v1/auth/logout"
	GatewayInfoPath = "/api/v1/admin/gateway"
)

// Admin login uses a high-entropy operator credential created through the
// Node-authenticated CLI. It grants no Tenant data access.
type LoginRequest struct {
	Token string `json:"token"`
}
type AccessIdentity struct {
	CredentialID string `json:"credential_id,omitempty"`
	Role         string `json:"role"`
	ExpiresUnix  int64  `json:"expires_unix"`
	CSRFToken    string `json:"csrf_token,omitempty"`
}
type GatewayInfo struct {
	ClusterID       string   `json:"cluster_id"`
	NodeID          string   `json:"node_id"`
	APIVersion      string   `json:"api_version"`
	Console         string   `json:"console"`
	BusinessModules []string `json:"business_modules"`
	Management      string   `json:"management"`
	ObservedUnix    int64    `json:"observed_unix"`
	Certificate     any      `json:"certificate,omitempty"`
}
