package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
)

// Accounts uses the existing Root directory and never stores plaintext passwords.
type Accounts struct {
	Root    *rootstate.State
	Barrier func(context.Context) (uint64, error)
	Commit  func(context.Context, rootstate.Command) error
	mu      sync.Mutex
	window  time.Time
	checks  int
}
type accountView struct {
	TenantName   string `json:"tenant_name,omitempty"`
	TenantID     string `json:"tenant_id"`
	Role         string `json:"role"`
	ID           string `json:"id"`
	Username     string `json:"username"`
	Name         string `json:"name"`
	Disabled     bool   `json:"disabled"`
	CreatedUnix  int64  `json:"created_unix"`
	CreatedIndex uint64 `json:"created_index"`
	Revision     uint64 `json:"revision"`
}

func (h *Accounts) accountDTO(a rootstate.AdminAccount) accountView {
	name := ""
	if a.TenantID != "" {
		if t, ok, e := h.Root.Tenant(a.TenantID); e == nil && ok {
			name = t.Name
		}
	}
	return accountView{TenantName: name, ID: a.ID, Username: a.Username, Name: a.Name, Disabled: a.Disabled, CreatedUnix: a.CreatedUnix, CreatedIndex: a.CreatedIndex, Revision: a.Revision, TenantID: a.TenantID, Role: a.EffectiveRole()}
}
func (h *Accounts) Authenticate(ctx context.Context, username, password string) (rootstate.AdminAccount, error) {
	if _, e := h.Barrier(ctx); e != nil {
		return rootstate.AdminAccount{}, auth.ErrAuthorizationUnavailable
	}
	all, e := h.Root.AdminAccounts()
	if e != nil {
		return rootstate.AdminAccount{}, auth.ErrAuthorizationUnavailable
	}
	var selected rootstate.AdminAccount
	for _, a := range all {
		if a.Username == strings.ToLower(strings.TrimSpace(username)) {
			selected = a
			break
		}
	}
	// Unknown/disabled users take the same password-hashing path and error response.
	ok, e := auth.CheckPassword(ctx, selected, password)
	if e != nil {
		return selected, auth.ErrAuthorizationUnavailable
	}
	if !ok || selected.ID == "" || selected.Disabled {
		return rootstate.AdminAccount{}, auth.ErrInvalidCredential
	}
	return selected, nil
}
func (h *Accounts) current(ctx context.Context, id string) (rootstate.AdminAccount, error) {
	c, ok, e := h.Root.AccessCredential(id)
	if e != nil {
		return rootstate.AdminAccount{}, e
	}
	if !ok || c.AccountID == "" {
		return rootstate.AdminAccount{}, errors.New("account login required")
	}
	a, ok, e := h.Root.AdminAccount(c.AccountID)
	if e != nil {
		return a, e
	}
	if !ok || a.Disabled || a.AuthVersion != c.AccountVersion {
		return a, errors.New("account login required")
	}
	return a, nil
}
func (h *Accounts) save(w http.ResponseWriter, r *http.Request, id string, a rootstate.AdminAccount, revision uint64) bool {
	if !revalidateAdmin(w, r) {
		return false
	}
	c := rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.PutAdminAccount, RequestID: id, AdminAccountUpdate: &rootstate.AdminAccountUpdate{Account: a, ExpectedRevision: revision}}
	if e := h.Commit(r.Context(), c); e != nil {
		encoded, _ := rootstate.EncodeCommand(c)
		result, found, _ := h.Root.ResultForCommand(encoded)
		if found && result.Error != "" {
			messages := map[string]string{"Account revision conflict": "账号已被修改，请刷新后重试。", "Account username already exists": "账号已存在，请使用其他账号。", "At least one enabled admin account is required": "每个归属范围至少保留一个启用的管理员账号。", "Account limit reached": "账号数量已达上限。"}
			m := messages[result.Error]
			if m == "" {
				m = "操作未完成，请刷新账号列表后核对。"
			}
			gatewayError(w, 409, "conflict", m)
		} else {
			gatewayError(w, 503, "outcome_unknown", "操作结果尚未确认，请刷新账号资料或尝试新密码登录后核对，勿重复创建。")
		}
		return false
	}
	return true
}
func (h *Accounts) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	principal, ok := AdminIdentity(r.Context())
	if !ok {
		gatewayError(w, 403, "forbidden", "需要管理员权限。")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/admin/")
	if principal.TenantID != "" {
		path = strings.TrimPrefix(r.URL.Path, "/api/v1/tenant/")
	}
	self := path == "profile" || path == "password"
	if !self && principal.Role != "admin" {
		gatewayError(w, 403, "forbidden", "需要管理员权限。")
		return
	}
	id := ""
	if strings.HasPrefix(path, "accounts/") {
		id = strings.TrimPrefix(path, "accounts/")
		if rootstate.ValidateAccountID(id) != nil {
			gatewayError(w, 404, "not_found", "账号不存在。")
			return
		}
	} else if path != "accounts" && !self {
		gatewayError(w, 404, "not_found", "接口不存在。")
		return
	}
	allowedMethod := "GET, PATCH, DELETE"
	if path == "accounts" {
		allowedMethod = "GET, POST"
	}
	if path == "profile" {
		allowedMethod = "GET, PATCH"
	}
	if path == "password" {
		allowedMethod = "POST"
	}
	if !strings.Contains(", "+allowedMethod+",", ", "+r.Method+",") {
		w.Header().Set("Allow", allowedMethod)
		gatewayError(w, 405, "method_not_allowed", "不支持此请求方法。")
		return
	}
	allowed := []string{}
	if path == "accounts" && r.Method == "GET" {
		allowed = []string{"q", "limit", "cursor", "scope", "tenant_id"}
	}
	if principal.TenantID != "" {
		for _, k := range []string{"scope", "tenant_id"} {
			if r.URL.Query().Has(k) {
				gatewayError(w, 400, "invalid_request", "不能指定账号归属。")
				return
			}
		}
	}
	q, e := parseManagementQuery(r, allowed...)
	if e != nil {
		gatewayError(w, 400, "invalid_request", e.Error())
		return
	}
	if scope := q.values.Get("scope"); scope != "" && scope != "system" && scope != "tenant" {
		gatewayError(w, 400, "invalid_request", "账号归属筛选无效。")
		return
	}
	if _, e = h.Barrier(r.Context()); e != nil {
		gatewayError(w, 503, "unavailable", "暂时无法读取账号。")
		return
	}
	if path == "accounts" && r.Method == "GET" {
		all, e := h.Root.AdminAccounts()
		if e != nil {
			gatewayError(w, 503, "unavailable", "暂时无法读取账号。")
			return
		}
		rows := []accountView{}
		for _, a := range all {
			if principal.TenantID != "" && a.TenantID != principal.TenantID {
				continue
			}
			if q.values.Get("scope") == "system" && a.TenantID != "" || q.values.Get("scope") == "tenant" && a.TenantID == "" || q.values.Get("tenant_id") != "" && a.TenantID != q.values.Get("tenant_id") {
				continue
			}
			if strings.Contains(strings.ToLower(a.Username+" "+a.Name+" "+a.TenantID+" "+h.accountDTO(a).TenantName), strings.ToLower(q.values.Get("q"))) {
				rows = append(rows, h.accountDTO(a))
			}
		}
		sort.Slice(rows, func(i, j int) bool {
			return accountListKey(rows[i]) < accountListKey(rows[j])
		})
		p := page(rows, q, func(a accountView) string { return accountListKey(a) })
		if !revalidateAdmin(w, r) {
			return
		}
		managementReply(w, struct {
			Items      []accountView `json:"items"`
			NextCursor string        `json:"next_cursor"`
			Total      int           `json:"total"`
		}{p.Items, p.NextCursor, len(rows)}, true, nil)
		return
	}
	if id != "" && principal.TenantID != "" {
		a, found, e := h.Root.AdminAccount(id)
		if e != nil {
			gatewayError(w, 503, "unavailable", "账号读取失败。")
			return
		}
		if !found || a.TenantID != principal.TenantID {
			gatewayError(w, 404, "not_found", "账号不存在。")
			return
		}
	}
	if r.Method == "DELETE" {
		h.remove(w, r, id, principal.CredentialID)
		return
	}
	var a rootstate.AdminAccount
	if self {
		a, e = h.current(r.Context(), principal.CredentialID)
		if e != nil {
			gatewayError(w, 409, "account_required", "当前使用管理 Token 登录，请先在访问与凭证中创建账号，再使用账号密码登录。")
			return
		}
	} else if id != "" {
		var found bool
		a, found, e = h.Root.AdminAccount(id)
		if e != nil {
			gatewayError(w, 503, "unavailable", "暂时无法读取账号。")
			return
		}
		if !found {
			gatewayError(w, 404, "not_found", "账号不存在。")
			return
		}
	}
	if r.Method == "GET" {
		if !revalidateAdmin(w, r) {
			return
		}
		managementReply(w, h.accountDTO(a), true, nil)
		return
	}
	var in struct {
		TenantID        *string `json:"tenant_id"`
		Role            *string `json:"role"`
		RequestID       string  `json:"request_id"`
		Revision        uint64  `json:"revision"`
		Username        string  `json:"username"`
		Name            *string `json:"name"`
		Password        string  `json:"password"`
		CurrentPassword string  `json:"current_password"`
		Disabled        *bool   `json:"disabled"`
	}
	if !tenantBody(w, r, &in) {
		return
	}
	if rootstate.ValidateRequestID(in.RequestID) != nil {
		gatewayError(w, 400, "invalid_request", "请求标识无效。")
		return
	}
	if _, found, e := h.Root.Result(in.RequestID); e != nil || found {
		gatewayError(w, 409, "request_id_reused", "请求已处理，请刷新资料或使用新密码登录核对结果。")
		return
	}
	creating := path == "accounts"
	if self && (in.TenantID != nil || in.Role != nil) || principal.TenantID != "" && in.TenantID != nil || !creating && in.TenantID != nil && *in.TenantID != a.TenantID {
		gatewayError(w, 400, "invalid_request", "不能修改账号归属或个人权限。")
		return
	}
	if creating {
		a.TenantID = principal.TenantID
		if in.TenantID != nil {
			a.TenantID = *in.TenantID
		}
		a.Role = "admin"
	}
	if in.Role != nil {
		a.Role = *in.Role
	}
	if (creating && (in.Name == nil || in.Password == "" || in.Revision != 0 || in.Disabled != nil || in.CurrentPassword != "")) || (!creating && (in.Username != "" || in.Revision != a.Revision)) || (path == "profile" && (in.Name == nil || in.Disabled != nil || in.Password != "" || in.CurrentPassword != "")) || (path == "password" && (in.Name != nil || in.Disabled != nil || in.Password == "" || in.CurrentPassword == "")) || (!self && in.CurrentPassword != "") {
		gatewayError(w, 400, "invalid_request", "参数无效或资料已更新，请刷新后重试。")
		return
	}
	if creating {
		var value [6]byte
		for {
			if _, e = rand.Read(value[:]); e != nil {
				gatewayError(w, 503, "unavailable", "暂时无法创建账号。")
				return
			}
			a.ID = "account_" + hex.EncodeToString(value[:])
			_, found, e := h.Root.AdminAccount(a.ID)
			if e != nil {
				gatewayError(w, 503, "unavailable", "暂时无法创建账号。")
				return
			}
			if !found {
				break
			}
		}
		a.Username = strings.ToLower(strings.TrimSpace(in.Username))
		a.CreatedUnix = time.Now().Unix()
	}
	if in.Name != nil {
		a.Name = strings.TrimSpace(*in.Name)
	}
	if in.Disabled != nil {
		a.Disabled = *in.Disabled
	}
	if in.Password != "" {
		if e = auth.ValidatePassword(in.Password); e != nil {
			gatewayError(w, 400, "invalid_password", e.Error())
			return
		}
		h.mu.Lock()
		if time.Since(h.window) >= time.Minute {
			h.window = time.Now()
			h.checks = 0
		}
		allowed := h.checks < 20
		if allowed {
			h.checks++
		}
		h.mu.Unlock()
		if !allowed {
			w.Header().Set("Retry-After", "60")
			gatewayError(w, 429, "rate_limited", "密码操作过于频繁，请稍后重试。")
			return
		}
		if self {
			valid, err := auth.CheckPassword(r.Context(), a, in.CurrentPassword)
			if err != nil {
				gatewayError(w, 503, "unavailable", "暂时无法校验密码。")
				return
			}
			if !valid {
				gatewayError(w, 400, "invalid_password", "当前密码不正确。")
				return
			}
		}
		if e = auth.SetPassword(r.Context(), &a, in.Password); e != nil {
			gatewayError(w, 503, "unavailable", "暂时无法处理密码。")
			return
		}
		in.Password = ""
		in.CurrentPassword = ""
	}
	if a.Validate() != nil {
		gatewayError(w, 400, "invalid_request", "账号须为 3–64 位小写字母、数字、点、下划线或连字符；名称须为 1–128 字节。")
		return
	}
	if !h.save(w, r, in.RequestID, a, in.Revision) {
		return
	}
	// Root commit application must be visible before returning the new revision.
	if _, e = h.Barrier(r.Context()); e != nil {
		gatewayError(w, 503, "outcome_unknown", "操作已提交，请刷新核对结果。")
		return
	}
	saved, found, e := h.Root.AdminAccount(a.ID)
	if e != nil || !found {
		gatewayError(w, 503, "unavailable", "请刷新核对账号状态。")
		return
	}
	managementReply(w, h.accountDTO(saved), true, nil)
}

func accountListKey(a accountView) string {
	rank := "0"
	if a.Disabled {
		rank = "1"
	}
	return rank + management.CreationKey(a.CreatedIndex, a.ID)
}
func (h *Accounts) remove(w http.ResponseWriter, r *http.Request, id, credentialID string) {
	var in struct {
		RequestID string `json:"request_id"`
		Revision  uint64 `json:"revision"`
	}
	if !tenantBody(w, r, &in) {
		return
	}
	d := rootstate.AdminAccountDelete{ID: id, ExpectedRevision: in.Revision}
	if rootstate.ValidateRequestID(in.RequestID) != nil || d.Validate() != nil {
		gatewayError(w, 400, "invalid_request", "请求标识或账号版本无效。")
		return
	}
	current, e := h.current(r.Context(), credentialID)
	if e == nil && current.ID == id {
		gatewayError(w, 409, "current_account", "不能删除当前登录的管理账号，请使用其他管理员身份操作。")
		return
	}
	if !revalidateAdmin(w, r) {
		return
	}
	c := rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.DeleteAdminAccount, RequestID: in.RequestID, AdminAccountDelete: &d}
	if e = h.Commit(r.Context(), c); e != nil {
		encoded, _ := rootstate.EncodeCommand(c)
		result, found, _ := h.Root.ResultForCommand(encoded)
		if found && result.Error != "" {
			m := "账号已被修改，请刷新后重试。"
			if result.Error == "At least one enabled admin account is required" {
				m = "每个归属范围至少保留一个启用的管理员账号。"
			}
			gatewayError(w, 409, "conflict", m)
		} else {
			gatewayError(w, 503, "outcome_unknown", "删除结果尚未确认，请刷新列表核对，或重试本次请求。")
		}
		return
	}
	if _, e = h.Barrier(r.Context()); e != nil {
		gatewayError(w, 503, "outcome_unknown", "删除已提交，请刷新列表核对。")
		return
	}
	managementReply(w, struct {
		Deleted bool `json:"deleted"`
	}{true}, true, nil)
}
