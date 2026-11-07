package auth

import "github.com/dubi-project/dubi/internal/platform/cluster/rootstate"

// ManagementPrincipal is for a server-side management adapter AFTER it has
// authenticated and freshly revalidated the administrator. Neither argument is
// authentication by itself. The adapter reads tenant from a confirmed Root cut.
// No application credential is minted; normal Module authorization still runs.
func ManagementPrincipal(tenant rootstate.Tenant, verifiedCredentialID string) (Principal, error) {
	if tenant.Validate() != nil || verifiedCredentialID == "" {
		return Principal{}, ErrForbiddenTenant
	}
	return Principal{tenantID: tenant.TenantID, credentialID: verifiedCredentialID, tenant: tenant}, nil
}

// ConsolePrincipal binds a verified account role to module access without minting a token.
func ConsolePrincipal(tenant rootstate.Tenant, credential, role string) (Principal, error) {
	p, e := ManagementPrincipal(tenant, credential)
	if e != nil {
		return p, e
	}
	if !rootstate.ValidAccountRole(role) {
		return Principal{}, ErrForbiddenTenant
	}
	p.readOnly = role == "readonly"
	return p, nil
}
