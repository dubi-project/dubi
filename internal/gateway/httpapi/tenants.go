package httpapi

import (
	"encoding/json"
	"errors"
	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
	"io"
	"net/http"
	"strings"
)

type TenantManagement struct{ Service *management.Tenants }

func tenantFailure(w http.ResponseWriter, err error) {
	var e *management.TenantError
	if errors.As(err, &e) {
		gatewayError(w, e.Status, e.Code, e.Message)
	} else {
		gatewayError(w, 503, "unavailable", "管理服务暂不可用，请稍后重试。")
	}
}
func tenantBody(w http.ResponseWriter, r *http.Request, out any) bool {
	if strings.Split(r.Header.Get("Content-Type"), ";")[0] != "application/json" {
		gatewayError(w, 415, "unsupported_media_type", "需要 application/json 请求体。")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		gatewayError(w, 400, "invalid_request", "请求格式无效。")
		return false
	}
	return true
}
func (h *TenantManagement) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, ok := AdminIdentity(r.Context()); !ok {
		gatewayError(w, 403, "forbidden", "需要管理员权限。")
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/admin/"), "/")
	if len(parts) == 1 && parts[0] == "tenant-modules" {
		if !method(w, r, "GET") {
			return
		}
		if _, err := parseManagementQuery(r); err != nil {
			gatewayError(w, 400, "invalid_request", err.Error())
			return
		}
		managementReply(w, h.Service.Modules, true, nil)
		return
	}
	if len(parts) < 1 || parts[0] != "tenants" || len(parts) > 4 {
		gatewayError(w, 404, "not_found", "接口不存在。")
		return
	}
	id := ""
	if len(parts) >= 2 {
		id = parts[1]
		if rootstate.ValidateTenantID(id) != nil {
			gatewayError(w, 400, "invalid_request", "租户标识无效。")
			return
		}
	}
	allowedMethod := "GET, POST"
	if len(parts) == 2 {
		allowedMethod = "GET, PUT"
	}
	if len(parts) == 3 && parts[2] == "credentials" {
		allowedMethod = "POST"
	}
	if len(parts) == 4 && parts[2] == "credentials" {
		allowedMethod = "DELETE"
	}
	if len(parts) >= 3 && parts[2] != "credentials" {
		gatewayError(w, 404, "not_found", "接口不存在。")
		return
	}
	if !strings.Contains(", "+allowedMethod+",", ", "+r.Method+",") {
		w.Header().Set("Allow", allowedMethod)
		gatewayError(w, 405, "method_not_allowed", "不支持此请求方法。")
		return
	}
	allowed := []string{}
	if r.Method == "GET" && id == "" {
		allowed = []string{"q", "status", "limit", "cursor"}
	}
	q, err := parseManagementQuery(r, allowed...)
	if err != nil {
		gatewayError(w, 400, "invalid_request", err.Error())
		return
	}
	if r.Method == "GET" {
		if id != "" {
			v, err := h.Service.Get(r.Context(), id)
			if err != nil {
				tenantFailure(w, err)
				return
			}
			managementReply(w, v, true, nil)
			return
		}
		all, err := h.Service.List(r.Context())
		if err != nil {
			tenantFailure(w, err)
			return
		}
		summary := v1.TenantSummary{Total: len(all)}
		filtered := []v1.ManagedTenant{}
		for _, v := range all {
			summary.ActiveCredentials += v.ActiveCredentials
			if v.ActiveCredentials == 0 {
				summary.WithoutCredentials++
			}
			if q.values.Get("status") != "" && v.Status != q.values.Get("status") {
				continue
			}
			if search := strings.ToLower(q.values.Get("q")); search != "" && !strings.Contains(strings.ToLower(v.Name+" "+v.TenantID), search) {
				continue
			}
			v.Credentials = nil
			filtered = append(filtered, v)
		}
		managementReply(w, v1.TenantPage{ManagementList: page(filtered, q, func(v v1.ManagedTenant) string { return management.CreationKey(v.CreatedIndex, v.TenantID) }), Total: len(filtered), Summary: summary}, true, nil)
		return
	}
	switch {
	case len(parts) <= 2:
		var in v1.TenantMutation
		if !tenantBody(w, r, &in) {
			return
		}
		if id != "" && id != in.TenantID {
			gatewayError(w, 400, "invalid_request", "租户标识不可修改。")
			return
		}
		if !revalidateAdmin(w, r) {
			return
		}
		v, err := h.Service.Save(r.Context(), in, id == "")
		if err != nil {
			tenantFailure(w, err)
			return
		}
		managementReply(w, v, true, nil)
	case len(parts) == 3:
		var in v1.TenantTokenRequest
		if !tenantBody(w, r, &in) || !revalidateAdmin(w, r) {
			return
		}
		v, err := h.Service.Issue(r.Context(), id, in)
		if err != nil {
			tenantFailure(w, err)
			return
		}
		if !revalidateAdmin(w, r) {
			return
		}
		managementReply(w, v, true, nil)
	case len(parts) == 4:
		var in v1.TenantRevokeRequest
		if !tenantBody(w, r, &in) || !revalidateAdmin(w, r) {
			return
		}
		if err := h.Service.Revoke(r.Context(), id, parts[3], in.RequestID); err != nil {
			tenantFailure(w, err)
			return
		}
		managementReply(w, map[string]bool{"revoked": true}, true, nil)
	}
}
