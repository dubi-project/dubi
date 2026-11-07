package httpapi

import (
	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
	"net/http"
	"strings"
	"time"
)

// TenantConsole exposes an explicit allowlist, never the system management mux.
type TenantConsole struct {
	Tenants    *management.Tenants
	Operations *management.Operations
	Config     *AdminConfig
}

func (h *TenantConsole) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p, ok := AdminIdentity(r.Context())
	if !ok || p.TenantID == "" || !rootstate.ValidAccountRole(p.Role) {
		gatewayError(w, 403, "forbidden", "需要租户身份。")
		return
	}
	if !revalidateAdmin(w, r) {
		return
	}
	if r.URL.Path == "/api/v1/tenant/config" || r.URL.Path == "/api/v1/tenant/config/entry" {
		h.Config.ServeHTTP(w, r)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/tenant/")
	if path == "overview" {
		if !method(w, r, "GET") {
			return
		}
		if _, err := parseManagementQuery(r); err != nil {
			gatewayError(w, 400, "invalid_request", "查询参数无效。")
			return
		}
		tenant, e := h.Tenants.Get(r.Context(), p.TenantID)
		if e != nil {
			tenantFailure(w, e)
			return
		}
		tenant.Credentials = nil
		accounts, e := h.Operations.Root.AdminAccounts()
		if e != nil {
			tenantFailure(w, e)
			return
		}
		n, enabled := 0, 0
		for _, a := range accounts {
			if a.TenantID == p.TenantID {
				n++
				if !a.Disabled {
					enabled++
				}
			}
		}
		modules := []v1.TenantModule{}
		for _, m := range h.Tenants.Modules {
			for _, id := range tenant.Modules {
				if m.ID == id {
					modules = append(modules, m)
				}
			}
		}
		if !revalidateAdmin(w, r) {
			return
		}
		managementReply(w, struct {
			Tenant          v1.ManagedTenant  `json:"tenant"`
			Role            string            `json:"role"`
			Accounts        int               `json:"accounts"`
			EnabledAccounts int               `json:"enabled_accounts"`
			Modules         []v1.TenantModule `json:"modules"`
		}{tenant, p.Role, n, enabled, modules}, true, nil)
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) > 3 || parts[0] != "credentials" || len(parts) == 3 && parts[2] != "revoke" {
		gatewayError(w, 404, "not_found", "租户接口不存在。")
		return
	}
	if p.Role != "admin" {
		gatewayError(w, 403, "forbidden", "仅租户管理员可管理凭证。")
		return
	}
	id := ""
	if len(parts) > 1 {
		id = parts[1]
		if rootstate.ValidateTenantCredentialID(id) != nil {
			gatewayError(w, 404, "not_found", "凭证不存在。")
			return
		}
	}
	allowed := []string{}
	if len(parts) == 1 && r.Method == "GET" {
		allowed = []string{"q", "status", "limit", "cursor"}
	}
	q, e := parseManagementQuery(r, allowed...)
	if e != nil {
		gatewayError(w, 400, "invalid_request", "查询参数无效。")
		return
	}
	// Reading the scope before any operation prevents ID-based cross-tenant access.
	tenant, e := h.Tenants.Get(r.Context(), p.TenantID)
	if e != nil {
		tenantFailure(w, e)
		return
	}
	all, e := h.Operations.Credentials(r.Context(), p.CredentialID)
	if e != nil {
		tenantFailure(w, e)
		return
	}
	rows := []v1.ManagedCredential{}
	summary := map[string]int{"total": 0, "active": 0, "revoked": 0, "expired": 0}
	found := false
	for _, c := range all {
		if c.Kind != "tenant" || c.TenantID != p.TenantID {
			continue
		}
		summary["total"]++
		summary[c.Status]++
		if id != "" && c.CredentialID != id {
			continue
		}
		if id != "" {
			found = true
		}
		if q.values.Get("status") != "" && q.values.Get("status") != c.Status {
			continue
		}
		if !strings.Contains(strings.ToLower(c.Name+" "+c.CredentialID), strings.ToLower(q.values.Get("q"))) {
			continue
		}
		rows = append(rows, c)
	}
	if id != "" && !found {
		gatewayError(w, 404, "not_found", "凭证不存在。")
		return
	}
	if r.Method == "GET" && len(parts) <= 2 {
		if !revalidateAdmin(w, r) {
			return
		}
		if id != "" {
			managementReply(w, rows[0], true, nil)
		} else {
			managementReply(w, v1.CredentialPage{ManagementList: page(rows, q, func(c v1.ManagedCredential) string { return management.CredentialListKey(c) }), Total: len(rows), Summary: summary}, true, nil)
		}
		return
	}
	if r.Method == "POST" && len(parts) == 1 {
		var in struct {
			RequestID    string   `json:"request_id"`
			Name         string   `json:"name"`
			Permission   string   `json:"permission"`
			Modules      []string `json:"modules"`
			TTLSeconds   uint64   `json:"ttl_seconds"`
			NeverExpires bool     `json:"never_expires"`
		}
		if !tenantBody(w, r, &in) {
			return
		}
		if len(in.Modules) == 0 || len(in.Modules) > 64 || (in.Permission != "readwrite" && in.Permission != "readonly") {
			gatewayError(w, 400, "invalid_request", "请选择模块和读写权限。")
			return
		}
		seen := map[string]bool{}
		for _, m := range in.Modules {
			authorized := false
			for _, a := range tenant.Modules {
				if a == m {
					authorized = true
				}
			}
			if !authorized || seen[m] {
				gatewayError(w, 400, "invalid_request", "凭证不能超出租户的模块权限。")
				return
			}
			seen[m] = true
		}
		if !revalidateAdmin(w, r) {
			return
		}
		v, e := h.Operations.Issue(r.Context(), v1.CredentialMutation{RequestID: in.RequestID, Name: in.Name, Kind: "tenant", TenantID: p.TenantID, Permission: in.Permission, Modules: in.Modules, TTLSeconds: in.TTLSeconds, NeverExpires: in.NeverExpires})
		if e != nil {
			tenantFailure(w, e)
			return
		}
		if !revalidateAdmin(w, r) {
			return
		}
		managementReply(w, v, true, nil)
		return
	}
	if r.Method == "POST" && len(parts) == 3 || r.Method == "DELETE" && len(parts) == 2 {
		var in struct {
			RequestID string `json:"request_id"`
		}
		if !tenantBody(w, r, &in) || !revalidateAdmin(w, r) {
			return
		}
		if r.Method == "DELETE" {
			e = h.Operations.DeleteCredential(r.Context(), id, in.RequestID, p.CredentialID)
		} else {
			e = h.Operations.Revoke(r.Context(), id, in.RequestID, p.CredentialID)
		}
		if e != nil {
			tenantFailure(w, e)
			return
		}
		managementReply(w, map[string]any{"completed": true, "observed_unix": time.Now().Unix()}, true, nil)
		return
	}
	gatewayError(w, 405, "method_not_allowed", "不支持此请求方法。")
}
