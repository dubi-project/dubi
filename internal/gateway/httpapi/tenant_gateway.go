package httpapi

import (
	"context"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"net/http"
	"strings"
)

// Scope is derived only from the freshly verified account, never URL/header claims.
func (g *Gateway) principal(ctx context.Context, c rootstate.AccessCredential) (AdminPrincipal, error) {
	p := AdminPrincipal{CredentialID: c.CredentialID, Role: c.Role}
	if c.Role == "session" {
		p.Role = "admin"
	}
	if c.AccountID != "" {
		if g.options.Accounts == nil {
			return p, auth.ErrAuthorizationUnavailable
		}
		a, e := g.options.Accounts.current(ctx, c.CredentialID)
		if e != nil {
			return p, auth.ErrInvalidCredential
		}
		p.TenantID = a.TenantID
		p.Role = a.EffectiveRole()
		p.AccountID = a.ID
	}
	return p, nil
}
func (g *Gateway) tenantRequest(w http.ResponseWriter, r *http.Request, c rootstate.AccessCredential, token string, cookie bool, p AdminPrincipal) {
	if !cookie || c.Role != "session" || p.AccountID == "" || p.TenantID == "" || !rootstate.ValidAccountRole(p.Role) {
		gatewayError(w, 403, "forbidden", "需要租户账号登录。")
		return
	}
	if r.Method != "GET" && r.Method != "HEAD" && (!sameOrigin(r) || !validCSRF(r, token)) {
		gatewayError(w, 403, "csrf_rejected", "需要同源请求与 CSRF 校验。")
		return
	}
	for _, name := range []string{"X-Tenant-ID", "X-Module-ID", "X-Engine-ID"} {
		if len(r.Header.Values(name)) > 0 {
			gatewayError(w, 400, "invalid_request", "身份由登录会话确定。")
			return
		}
	}
	r = r.WithContext(context.WithValue(r.Context(), adminContextKey{}, p))
	r = r.WithContext(context.WithValue(r.Context(), adminRecheckKey{}, func() error {
		current, _, _, err := g.authenticate(r)
		if err != nil {
			return err
		}
		fresh, err := g.principal(r.Context(), current)
		if err != nil {
			return err
		}
		if fresh != p {
			return auth.ErrInvalidCredential
		}
		return nil
	}))
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/tenant/")
	if path == "accounts" || strings.HasPrefix(path, "accounts/") || path == "profile" || path == "password" {
		if g.options.Accounts == nil {
			gatewayError(w, 503, "unavailable", "账号服务暂不可用。")
			return
		}
		g.options.Accounts.ServeHTTP(w, r)
	} else if g.options.TenantConsole != nil {
		g.options.TenantConsole.ServeHTTP(w, r)
	} else {
		gatewayError(w, 404, "not_found", "租户接口不存在。")
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		g.audit("tenant_"+r.Method, p.CredentialID, "handled")
	}
}
