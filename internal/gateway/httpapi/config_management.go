package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/engine/state"
	configmodule "github.com/dubi-project/dubi/internal/module/config"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

type AdminConfig struct {
	Service *configmodule.Service
	Root    *rootstate.State
	Barrier func(context.Context) (uint64, error)
}
type configCursor struct {
	Tenant   string             `json:"tenant"`
	Search   string             `json:"search"`
	Position state.ListPosition `json:"position"`
}

func (h *AdminConfig) authorize(w http.ResponseWriter, r *http.Request, tenant string) (auth.Principal, bool) {
	actor, ok := AdminIdentity(r.Context())
	if !ok || (actor.TenantID == "" && actor.Role != "admin") || (actor.TenantID != "" && (actor.TenantID != tenant || !rootstate.ValidAccountRole(actor.Role))) {
		gatewayError(w, 403, "forbidden", "需要管理员权限。")
		return auth.Principal{}, false
	}
	if !revalidateAdmin(w, r) {
		return auth.Principal{}, false
	}
	index, err := h.Barrier(r.Context())
	if err != nil || index == 0 || h.Root.AppliedIndex() < index {
		gatewayError(w, 503, "authorization_unavailable", "暂时无法确认租户权限。")
		return auth.Principal{}, false
	}
	t, found, err := h.Root.Tenant(tenant)
	if err != nil {
		gatewayError(w, 503, "unavailable", "暂时无法读取租户。")
		return auth.Principal{}, false
	}
	if !found {
		gatewayError(w, 404, "tenant_not_found", "租户不存在。")
		return auth.Principal{}, false
	}
	p, err := auth.ConsolePrincipal(t, actor.CredentialID, actor.Role)
	if err == nil {
		err = p.Authorize(rootstate.Keyspace{TenantID: tenant, ModuleID: configmodule.ModuleID, EngineID: state.EngineID})
	}
	if err != nil {
		gatewayError(w, 403, "module_forbidden", "此租户未开通配置中心。")
		return auth.Principal{}, false
	}
	return p, true
}
func configQuery(r *http.Request, list bool) (url.Values, error) {
	if len(r.URL.RawQuery) > 12000 {
		return nil, state.ErrInvalid
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, state.ErrInvalid
	}
	for k, v := range q {
		if len(v) != 1 || v[0] == "" {
			return nil, state.ErrInvalid
		}
		if k != "tenant" && !(list && (k == "q" || k == "cursor" || k == "limit")) && !(!list && k == "key") {
			return nil, state.ErrInvalid
		}
	}
	if rootstate.ValidateTenantID(q.Get("tenant")) != nil || (!list && (q.Get("key") == "" || len(q.Get("key")) > state.MaxKeyBytes || !utf8.ValidString(q.Get("key")))) {
		return nil, state.ErrInvalid
	}
	for _, header := range []string{"X-Tenant-ID", "X-Module-ID", "X-Engine-ID"} {
		if len(r.Header.Values(header)) != 0 {
			return nil, state.ErrInvalid
		}
	}
	return q, nil
}
func (h *AdminConfig) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	actor, known := AdminIdentity(r.Context())
	if !known || (actor.TenantID == "" && actor.Role != "admin") || !rootstate.ValidAccountRole(actor.Role) {
		gatewayError(w, 403, "forbidden", "需要管理员权限。")
		return
	}
	if actor.TenantID != "" {
		if r.URL.Path != "/api/v1/tenant/config" && r.URL.Path != "/api/v1/tenant/config/entry" {
			gatewayError(w, 404, "not_found", "接口不存在。")
			return
		}
		q, e := url.ParseQuery(r.URL.RawQuery)
		if e != nil || q.Has("tenant") || q.Has("tenant_id") {
			gatewayError(w, 400, "invalid_request", "租户由登录会话确定。")
			return
		}
		if r.Method != "GET" && actor.Role == "readonly" {
			gatewayError(w, 403, "forbidden", "只读账号不能修改配置。")
			return
		}
		clone := r.Clone(r.Context())
		u := *r.URL
		u.Path = strings.Replace(u.Path, "/api/v1/tenant/", "/api/v1/admin/", 1)
		q.Set("tenant", actor.TenantID)
		u.RawQuery = q.Encode()
		clone.URL = &u
		r = clone
	}
	list := r.URL.Path == v1.AdminConfigPath
	if !list && r.URL.Path != v1.AdminConfigEntryPath {
		gatewayError(w, 404, "not_found", "配置接口不存在。")
		return
	}
	if list {
		if !method(w, r, http.MethodGet) {
			return
		}
	} else if r.Method != "GET" && r.Method != "PUT" && r.Method != "DELETE" {
		w.Header().Set("Allow", "GET, PUT, DELETE")
		gatewayError(w, 405, "method_not_allowed", "不支持此操作。")
		return
	}
	q, err := configQuery(r, list)
	if err != nil {
		gatewayError(w, 400, "invalid_request", "配置查询参数无效。")
		return
	}
	var mutation v1.ConfigMutation
	if r.Method != "GET" {
		if strings.Split(r.Header.Get("Content-Type"), ";")[0] != "application/json" {
			gatewayError(w, 415, "unsupported_media_type", "需要 JSON 请求体。")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 96<<10)
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		if d.Decode(&mutation) != nil || d.Decode(new(any)) != io.EOF || rootstate.ValidateRequestID(mutation.RequestID) != nil || (r.Method == "DELETE" && (len(mutation.Value) > 0 || mutation.IfAbsent)) || (mutation.Version == "" && !mutation.IfAbsent) {
			gatewayError(w, 400, "invalid_request", "请提供请求标识，以及创建条件或当前版本。")
			return
		}
	}
	p, ok := h.authorize(w, r, q.Get("tenant"))
	if !ok {
		return
	}
	var result any
	if list {
		limit := 20
		if q.Get("limit") != "" {
			limit, err = strconv.Atoi(q.Get("limit"))
		}
		if err != nil || limit < 1 || limit > 100 || len(q.Get("q")) > 256 {
			gatewayError(w, 400, "invalid_request", "分页或搜索参数无效。")
			return
		}
		in := state.ListRequest{Limit: limit, Search: q.Get("q")}
		if raw := q.Get("cursor"); raw != "" {
			b, e := base64.RawURLEncoding.DecodeString(raw)
			var cursor configCursor
			if e != nil || len(b) > 6000 || json.Unmarshal(b, &cursor) != nil || cursor.Tenant != q.Get("tenant") || cursor.Search != in.Search {
				gatewayError(w, 400, "invalid_cursor", "游标不属于当前租户或搜索条件。")
				return
			}
			in.Position = cursor.Position
		}
		var page state.ListPage
		page, err = h.Service.List(r.Context(), p, in)
		out := v1.ManagedConfigPage{Items: []v1.ManagedConfigItem{}}
		if err == nil {
			for _, item := range page.Items {
				if !utf8.Valid(item.Key) {
					err = state.ErrInvalid
					break
				}
				out.Items = append(out.Items, v1.ManagedConfigItem{Key: string(item.Key), Version: item.Version, ValueBytes: item.ValueBytes, CreatedOrder: item.CreatedOrder, Confirmed: item.Confirmed})
			}
			if page.Next != nil {
				b, _ := json.Marshal(configCursor{Tenant: p.TenantID(), Search: in.Search, Position: *page.Next})
				out.NextCursor = base64.RawURLEncoding.EncodeToString(b)
			}
		}
		result = out
	} else if r.Method == "GET" {
		var entry state.Entry
		entry, err = h.Service.Get(r.Context(), p, q.Get("key"))
		result = v1.ConfigEntry{Version: entry.Version, Value: entry.Value}
	} else {
		var out state.Result
		condition := state.Condition{IfAbsent: mutation.IfAbsent, Version: mutation.Version}
		if r.Method == "PUT" {
			out, err = h.Service.Put(r.Context(), p, q.Get("key"), mutation.Value, mutation.RequestID, condition)
		} else {
			out, err = h.Service.Delete(r.Context(), p, q.Get("key"), mutation.RequestID, condition)
		}
		result = v1.ConfigResult{RequestID: out.RequestID, Version: out.Version, Deleted: out.Deleted, Replayed: out.Replayed}
	}
	if err != nil {
		(&Config{}).respondError(w, err, mutation.RequestID)
		return
	}
	// Re-check authorization before returning tenant data or a definitive outcome.
	check := newConfigResponseBuffer() // buffer failures: writes may already have committed.
	if _, ok = h.authorize(check, r, p.TenantID()); !ok {
		if r.Method != "GET" {
			gatewayError(w, 503, "outcome_unknown", "操作结果待确认，请使用相同请求标识重试。")
		} else {
			for k, vs := range check.Header() {
				for _, v := range vs {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(check.status)
			_, _ = w.Write(check.body)
		}
		return
	}
	managementReply(w, result, true, nil)
}

// Small response buffer keeps post-mutation authorization failures from being
// confused with a definite rejection; no tokens or values are logged.
type configResponseBuffer struct {
	headers http.Header
	status  int
	body    []byte
}

func newConfigResponseBuffer() *configResponseBuffer {
	return &configResponseBuffer{headers: make(http.Header), status: 200}
}
func (b *configResponseBuffer) Header() http.Header { return b.headers }
func (b *configResponseBuffer) WriteHeader(n int)   { b.status = n }
func (b *configResponseBuffer) Write(p []byte) (int, error) {
	b.body = append(b.body, p...)
	return len(p), nil
}
