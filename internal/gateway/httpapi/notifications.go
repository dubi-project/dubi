package httpapi

import (
	"net/http"
	"slices"
	"sort"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
)

type Notifications struct{ Service *management.Notifications }

func (h *Notifications) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	principal, ok := AdminIdentity(r.Context())
	if !ok {
		gatewayError(w, 403, "forbidden", "需要管理员权限。")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/admin/")
	if path == "notifications/read" {
		if !method(w, r, "POST") {
			return
		}
		if _, e := parseManagementQuery(r); e != nil {
			gatewayError(w, 400, "invalid_request", e.Error())
			return
		}
		var in struct {
			RequestID    string `json:"request_id"`
			ID           string `json:"id"`
			All          bool   `json:"all"`
			Read         bool   `json:"read"`
			ThroughIndex uint64 `json:"through_index"`
		}
		if !tenantBody(w, r, &in) || !revalidateAdmin(w, r) {
			return
		}
		if e := h.Service.Mark(r.Context(), principal.CredentialID, in.RequestID, rootstate.NotificationMark{ID: in.ID, All: in.All, Read: in.Read, ThroughIndex: in.ThroughIndex}); e != nil {
			tenantFailure(w, e)
			return
		}
		managementReply(w, struct {
			Updated bool `json:"updated"`
		}{true}, true, nil)
		return
	}
	if path != "notifications" && !strings.HasPrefix(path, "notifications/") {
		gatewayError(w, 404, "not_found", "接口不存在。")
		return
	}
	if !method(w, r, "GET") {
		return
	}
	id := ""
	if path != "notifications" {
		id = strings.TrimPrefix(path, "notifications/")
		if rootstate.ValidateRequestID(id) != nil {
			gatewayError(w, 400, "invalid_request", "消息标识无效。")
			return
		}
	}
	allowed := []string{}
	if id == "" {
		allowed = []string{"q", "level", "read", "limit", "cursor"}
	}
	q, e := parseManagementQuery(r, allowed...)
	if e != nil {
		gatewayError(w, 400, "invalid_request", e.Error())
		return
	}
	if level := q.values.Get("level"); level != "" && !slices.Contains([]string{"warning", "error", "success", "info"}, level) {
		gatewayError(w, 400, "invalid_request", "消息类型无效。")
		return
	}
	if read := q.values.Get("read"); read != "" && read != "true" && read != "false" {
		gatewayError(w, 400, "invalid_request", "消息阅读状态无效。")
		return
	}
	data, e := h.Service.List(r.Context(), principal.CredentialID)
	if e != nil {
		tenantFailure(w, e)
		return
	}
	rows := []management.NotificationItem{}
	for _, n := range data.Items {
		if id != "" {
			if n.ID == id {
				managementReply(w, n, true, nil)
				return
			}
			continue
		}
		if level := q.values.Get("level"); level != "" && level != n.Level {
			continue
		}
		if read := q.values.Get("read"); read != "" && (read == "true") != n.Read {
			continue
		}
		if !strings.Contains(strings.ToLower(n.Title+" "+n.Summary+" "+n.Source), strings.ToLower(q.values.Get("q"))) {
			continue
		}
		rows = append(rows, n)
	}
	if id != "" {
		gatewayError(w, 404, "not_found", "消息不存在或已超出保留数量。")
		return
	}
	sort.Slice(rows, func(i, j int) bool {
		return management.CreationKey(rows[i].CreatedIndex, rows[i].ID) < management.CreationKey(rows[j].CreatedIndex, rows[j].ID)
	})
	result := page(rows, q, func(n management.NotificationItem) string { return management.CreationKey(n.CreatedIndex, n.ID) })
	managementReply(w, struct {
		Items        []management.NotificationItem `json:"items"`
		NextCursor   string                        `json:"next_cursor"`
		Total        int                           `json:"total"`
		Unread       int                           `json:"unread"`
		Retained     int                           `json:"retained"`
		ThroughIndex uint64                        `json:"through_index"`
		Retention    int                           `json:"retention"`
	}{result.Items, result.NextCursor, len(rows), data.Unread, len(data.Items), data.ThroughIndex, data.Retention}, true, nil)
}
