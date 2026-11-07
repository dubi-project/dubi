package httpapi

import (
	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
	"net/http"
	"strings"
	"time"
)

type Operations struct{ Service *management.Operations }

func (h *Operations) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	principal, ok := AdminIdentity(r.Context())
	if !ok {
		gatewayError(w, 403, "forbidden", "需要管理员权限。")
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/admin/"), "/")
	if len(parts) > 3 || len(parts) == 0 {
		gatewayError(w, 404, "not_found", "接口不存在。")
		return
	}
	resource := parts[0]
	id := ""
	if len(parts) > 1 {
		id = parts[1]
		if id == "" {
			gatewayError(w, 404, "not_found", "接口不存在。")
			return
		}
	}
	valid := resource == "credentials" || resource == "repositories" || resource == "backups"
	if !valid || (len(parts) == 3 && !(resource == "repositories" && parts[2] == "default") && !(resource == "backups" && parts[2] == "restore-preparation") && !(resource == "credentials" && parts[2] == "revoke")) {
		gatewayError(w, 404, "not_found", "接口不存在。")
		return
	}
	if id != "" && ((resource == "credentials" && rootstate.ValidateTenantCredentialID(id) != nil) || (resource == "repositories" && repository.ValidateName(id) != nil) || (resource == "backups" && rootstate.ValidateRequestID(id) != nil)) {
		gatewayError(w, 400, "invalid_request", "资源标识无效。")
		return
	}
	allowed := "GET"
	if len(parts) == 1 {
		allowed = "GET, POST"
	} else if len(parts) == 2 && resource == "credentials" {
		allowed = "GET, DELETE"
	} else if len(parts) == 2 && resource == "repositories" {
		allowed = "GET, PUT"
	} else if len(parts) == 3 && (resource == "repositories" || resource == "credentials") {
		allowed = "POST"
	}
	if !strings.Contains(", "+allowed+",", ", "+r.Method+",") {
		w.Header().Set("Allow", allowed)
		gatewayError(w, 405, "method_not_allowed", "不支持此请求方法。")
		return
	}
	params := []string{}
	if r.Method == "GET" && id == "" {
		switch resource {
		case "credentials":
			params = []string{"q", "kind", "status", "expiring", "tenant_id", "limit", "cursor"}
		case "backups":
			params = []string{"q", "status", "repository", "limit", "cursor"}
		}
	}
	q, err := parseManagementQuery(r, params...)
	if err != nil {
		gatewayError(w, 400, "invalid_request", err.Error())
		return
	}
	if r.Method == "GET" {
		switch resource {
		case "credentials":
			all, err := h.Service.Credentials(r.Context(), principal.CredentialID)
			if err != nil {
				tenantFailure(w, err)
				return
			}
			summary := map[string]int{"admin": 0, "tenant": 0, "monitor": 0, "expiring": 0, "active": 0, "inactive": 0, "revoked": 0, "expired": 0, "total": len(all)}
			filtered := []v1.ManagedCredential{}
			for _, c := range all {
				summary[c.Kind]++
				if c.Status == "active" {
					summary["active"]++
				} else {
					summary["inactive"]++
					summary[c.Status]++
				}
				soon := c.Status == "active" && c.ExpiresUnix != 0 && c.ExpiresUnix-time.Now().Unix() <= 7*86400
				if soon {
					summary["expiring"]++
				}
				if id != "" {
					if c.CredentialID == id {
						managementReply(w, c, true, nil)
						return
					}
					continue
				}
				v := q.values
				if v.Get("kind") != "" && v.Get("kind") != c.Kind || v.Get("status") != "" && v.Get("status") != c.Status || v.Get("tenant_id") != "" && v.Get("tenant_id") != c.TenantID || v.Get("expiring") == "true" && !soon {
					continue
				}
				if text := strings.ToLower(v.Get("q")); text != "" && !strings.Contains(strings.ToLower(c.Name+" "+c.CredentialID+" "+c.TenantID+" "+c.TenantName+" "+c.RequestID), text) {
					continue
				}
				filtered = append(filtered, c)
			}
			if id != "" {
				gatewayError(w, 404, "not_found", "凭证不存在。")
				return
			}
			managementReply(w, v1.CredentialPage{ManagementList: page(filtered, q, func(c v1.ManagedCredential) string {
				return management.CredentialListKey(c)
			}), Total: len(filtered), Summary: summary}, true, nil)
		case "repositories":
			all, err := h.Service.Repositories(r.Context())
			if err != nil {
				tenantFailure(w, err)
				return
			}
			if id != "" {
				for _, v := range all {
					if v.Name == id {
						managementReply(w, v, true, nil)
						return
					}
				}
				gatewayError(w, 404, "not_found", "仓库不存在。")
				return
			}
			managementReply(w, all, true, nil)
		case "backups":
			if len(parts) == 3 {
				v, err := h.Service.Preparation(r.Context(), id)
				if err != nil {
					tenantFailure(w, err)
					return
				}
				managementReply(w, v, true, nil)
				return
			}
			all, err := h.Service.Backups(r.Context())
			if err != nil {
				tenantFailure(w, err)
				return
			}
			filtered := []v1.ManagedBackup{}
			for _, j := range all {
				if id != "" {
					if j.JobID == id {
						managementReply(w, j, true, nil)
						return
					}
					continue
				}
				v := q.values
				if v.Get("status") != "" && v.Get("status") != j.State || v.Get("repository") != "" && v.Get("repository") != j.Repository {
					continue
				}
				if text := strings.ToLower(v.Get("q")); text != "" && !strings.Contains(strings.ToLower(j.JobID+" "+j.Repository), text) {
					continue
				}
				j.Workers = nil
				filtered = append(filtered, j)
			}
			if id != "" {
				gatewayError(w, 404, "not_found", "备份不存在。")
				return
			}
			managementReply(w, v1.BackupPage{ManagementList: page(filtered, q, func(j v1.ManagedBackup) string { return management.CreationKey(j.CreatedIndex, j.JobID) }), Total: len(filtered)}, true, nil)
		}
		return
	}
	switch resource {
	case "credentials":
		if r.Method == "POST" && len(parts) == 1 {
			var in v1.CredentialMutation
			if !tenantBody(w, r, &in) || !revalidateAdmin(w, r) {
				return
			}
			v, err := h.Service.Issue(r.Context(), in)
			if err != nil {
				tenantFailure(w, err)
				return
			}
			if !revalidateAdmin(w, r) {
				return
			}
			managementReply(w, v, true, nil)
			return
		}
		var in v1.TenantRevokeRequest
		if !tenantBody(w, r, &in) || !revalidateAdmin(w, r) {
			return
		}
		if len(parts) == 3 {
			if err := h.Service.Revoke(r.Context(), id, in.RequestID, principal.CredentialID); err != nil {
				tenantFailure(w, err)
				return
			}
			managementReply(w, map[string]bool{"revoked": true}, true, nil)
		} else {
			if err := h.Service.DeleteCredential(r.Context(), id, in.RequestID, principal.CredentialID); err != nil {
				tenantFailure(w, err)
				return
			}
			managementReply(w, map[string]bool{"deleted": true}, true, nil)
		}

	case "repositories":
		if len(parts) == 3 {
			var in v1.TenantRevokeRequest
			if !tenantBody(w, r, &in) || !revalidateAdmin(w, r) {
				return
			}
			if err := h.Service.Default(r.Context(), id, in.RequestID); err != nil {
				tenantFailure(w, err)
				return
			}
		} else {
			var in v1.RepositoryMutation
			if !tenantBody(w, r, &in) {
				return
			}
			if id != "" && in.Name != id {
				gatewayError(w, 400, "invalid_request", "仓库名称不可修改。")
				return
			}
			if !revalidateAdmin(w, r) {
				return
			}
			if err := h.Service.SaveRepository(r.Context(), in, id == ""); err != nil {
				tenantFailure(w, err)
				return
			}
		}
		managementReply(w, map[string]bool{"saved": true}, true, nil)
	case "backups":
		var in v1.BackupMutation
		if !tenantBody(w, r, &in) || !revalidateAdmin(w, r) {
			return
		}
		job, err := h.Service.Run(r.Context(), in)
		if err != nil {
			tenantFailure(w, err)
			return
		}
		managementReply(w, map[string]string{"job_id": job}, true, nil)
	}
}
