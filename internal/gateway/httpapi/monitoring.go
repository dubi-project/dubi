package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
)

type Monitoring struct{ Service *management.MonitoringService }
type diagnosticSummary struct {
	ID            string `json:"id"`
	RegionID      string `json:"region_id,omitempty"`
	NodeID        string `json:"node_id,omitempty"`
	CreatedUnix   int64  `json:"created_unix"`
	CreatedIndex  uint64 `json:"created_index"`
	NodeCount     int    `json:"node_count"`
	ObservedNodes int    `json:"observed_nodes"`
	Issues        int    `json:"issues"`
	State         string `json:"state"`
}

func summarizeDiagnostic(d rootstate.DiagnosticRecord) diagnosticSummary {
	s := diagnosticSummary{ID: d.ID, RegionID: d.RegionID, NodeID: d.NodeID, CreatedUnix: d.CreatedUnix, CreatedIndex: d.CreatedIndex, NodeCount: len(d.Nodes), State: "passed"}
	for _, n := range d.Nodes {
		for _, c := range n.Checks {
			if c.Kind == "reachability" && c.State == "passed" {
				s.ObservedNodes++
			}
			if c.State != "passed" {
				s.Issues++
				if c.State == "warning" || s.State == "passed" {
					s.State = c.State
				}
			}
		}
	}
	return s
}
func (h *Monitoring) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, ok := AdminIdentity(r.Context()); !ok {
		gatewayError(w, 403, "forbidden", "需要管理员权限。")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/admin/")
	if path == "monitoring" {
		if !method(w, r, "GET") {
			return
		}
		q, err := parseManagementQuery(r, "region", "node_id", "window")
		if err != nil {
			gatewayError(w, 400, "invalid_request", err.Error())
			return
		}
		seconds := int64(3600)
		if x := q.values.Get("window"); x != "" {
			seconds, err = strconv.ParseInt(x, 10, 64)
		}
		if err != nil || (seconds != 900 && seconds != 3600 && seconds != 21600) {
			gatewayError(w, 400, "invalid_request", "时间范围仅支持 900、3600 或 21600 秒。")
			return
		}
		data, _, err := h.Service.Collect(r.Context(), q.values.Get("region"), q.values.Get("node_id"), seconds)
		if err != nil {
			tenantFailure(w, err)
			return
		}
		problems := []v1.ObservationError{}
		for _, n := range data.Nodes {
			if !n.Complete {
				problems = append(problems, v1.ObservationError{NodeID: n.NodeID, Code: "incomplete_observation", Message: "节点观测不完整，缺失值标记为未知。"})
			}
		}
		managementReply(w, data, data.Summary.Complete, problems)
		return
	}
	if path != "diagnostics" && !strings.HasPrefix(path, "diagnostics/") {
		gatewayError(w, 404, "not_found", "接口不存在。")
		return
	}
	id := strings.TrimPrefix(path, "diagnostics/")
	if path == "diagnostics" {
		id = ""
	}
	if id != "" && rootstate.ValidateRequestID(id) != nil {
		gatewayError(w, 400, "invalid_request", "记录标识无效。")
		return
	}
	if r.Method == "POST" && id == "" {
		if _, err := parseManagementQuery(r); err != nil {
			gatewayError(w, 400, "invalid_request", err.Error())
			return
		}
		var in struct {
			RequestID string `json:"request_id"`
			RegionID  string `json:"region_id"`
			NodeID    string `json:"node_id"`
		}
		if !tenantBody(w, r, &in) || !revalidateAdmin(w, r) {
			return
		}
		d, err := h.Service.Run(r.Context(), in.RequestID, in.RegionID, in.NodeID, func() error {
			fn, ok := r.Context().Value(adminRecheckKey{}).(func() error)
			if !ok {
				return errors.New("missing management identity")
			}
			return fn()
		})
		if err != nil {
			tenantFailure(w, err)
			return
		}
		managementReply(w, d, true, nil)
		return
	}
	if r.Method != "GET" {
		allow := "GET"
		if id == "" {
			allow = "GET, POST"
		}
		w.Header().Set("Allow", allow)
		gatewayError(w, 405, "method_not_allowed", "不支持此请求方法。")
		return
	}
	allowed := []string{}
	if id == "" {
		allowed = []string{"region", "node_id", "q", "status", "limit", "cursor"}
	}
	q, err := parseManagementQuery(r, allowed...)
	if err != nil {
		gatewayError(w, 400, "invalid_request", err.Error())
		return
	}
	records, err := h.Service.Reports(r.Context())
	if err != nil {
		tenantFailure(w, err)
		return
	}
	items := []diagnosticSummary{}
	for _, d := range records {
		if id != "" {
			if d.ID == id {
				managementReply(w, d, true, nil)
				return
			}
			continue
		}
		s := summarizeDiagnostic(d)
		v := q.values
		match := false
		for _, n := range d.Nodes {
			if (v.Get("region") == "" || v.Get("region") == n.RegionID) && (v.Get("node_id") == "" || v.Get("node_id") == n.NodeID) {
				match = true
			}
		}
		if !match || (v.Get("status") != "" && v.Get("status") != s.State) || !strings.Contains(strings.ToLower(d.ID+" "+d.RegionID+" "+d.NodeID), strings.ToLower(v.Get("q"))) {
			continue
		}
		items = append(items, s)
	}
	if id != "" {
		gatewayError(w, 404, "not_found", "诊断记录不存在或已超出保留数量。")
		return
	}
	result := page(items, q, func(d diagnosticSummary) string { return management.CreationKey(d.CreatedIndex, d.ID) })
	managementReply(w, struct {
		Items      []diagnosticSummary `json:"items"`
		NextCursor string              `json:"next_cursor"`
		Total      int                 `json:"total"`
		Retention  int                 `json:"retention"`
	}{Items: result.Items, NextCursor: result.NextCursor, Total: len(items), Retention: rootstate.MaxDiagnosticRecords}, true, nil)
}
