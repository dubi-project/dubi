package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

const sessionCookie = "__Host-dubi_session"

// Admin handlers receive only verified management identity. Module handlers
// remain responsible for their own Tenant-bound contract.
type adminContextKey struct{}
type adminRecheckKey struct{}

func revalidateAdmin(w http.ResponseWriter, r *http.Request) bool {
	if fn, ok := r.Context().Value(adminRecheckKey{}).(func() error); ok {
		if err := fn(); err != nil {
			gatewayError(w, 503, "authorization_unavailable", "无法重新确认管理权限，请核对操作结果后重试。")
			return false
		}
		return true
	}
	gatewayError(w, 403, "forbidden", "管理身份不可用。")
	return false
}

type AdminPrincipal struct {
	TenantID     string
	AccountID    string
	CredentialID string
	Role         string
}

func AdminIdentity(ctx context.Context) (AdminPrincipal, bool) {
	p, ok := ctx.Value(adminContextKey{}).(AdminPrincipal)
	return p, ok
}

type GatewayOptions struct {
	Metrics       func(io.Writer)
	Admin         http.Handler
	TenantConsole http.Handler
	Accounts      *Accounts
	Audit         func(action, credential, result string)
	Certificate   func() any

	Legacy http.Handler
	Access auth.AccessVerifier
	Tenant auth.Verifier
	Commit func(context.Context, rootstate.Command) error
	Info   func(context.Context) (any, error)
}
type Gateway struct {
	options         GatewayOptions
	slots           chan struct{}
	mu              sync.Mutex
	loginWindow     time.Time
	logins          int
	consoleManifest consoleManifest
	consoleErr      error
}

func NewGateway(o GatewayOptions) *Gateway {
	files, err := fs.Sub(console, "console")
	var manifest consoleManifest
	if err == nil {
		manifest, err = validateConsole(files)
	}
	return &Gateway{options: o, slots: make(chan struct{}, 32), consoleManifest: manifest, consoleErr: err}
}
func jsonReply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func gatewayError(w http.ResponseWriter, status int, code, message string) {
	failure(w, status, code, message, w.Header().Get("X-Request-ID"))
}
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if strings.HasPrefix(r.URL.Path, "/api/v1/admin/") || strings.HasPrefix(r.URL.Path, "/api/v1/tenant/") {
		managementRequestID(w)
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
	if isConsoleRequest(r) {
		g.static(w, r)
		return
	}
	if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
		g.options.Legacy.ServeHTTP(w, r)
		return
	}
	if r.URL.Path == ConfigPath || r.URL.Path == "/v1/me" {
		g.options.Legacy.ServeHTTP(w, r)
		return
	}
	if r.URL.Path != "/metrics" && !strings.HasPrefix(r.URL.Path, "/api/") {
		http.NotFound(w, r)
		return
	}
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		gatewayError(w, 503, "busy", "Gateway is busy")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	r = r.WithContext(ctx)
	if r.URL.Path == "/api/v1/auth/login" {
		g.login(w, r)
		return
	}
	if r.URL.Path == "/api/v1/me" && strings.HasPrefix(r.Header.Get("Authorization"), "Bearer dubi.") {
		clone := r.Clone(ctx)
		u := *r.URL
		u.Path = "/v1/me"
		clone.URL = &u
		g.options.Legacy.ServeHTTP(w, clone)
		return
	}
	known := r.URL.Path == "/api/v1/me" || r.URL.Path == "/api/v1/auth/logout" || strings.HasPrefix(r.URL.Path, "/api/v1/admin/") || strings.HasPrefix(r.URL.Path, "/api/v1/tenant/") || r.URL.Path == "/metrics"
	if !known {
		gatewayError(w, 404, "not_found", "API not found")
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/v1/admin/") && len(r.Header.Values("Authorization")) == 1 && strings.HasPrefix(r.Header.Get("Authorization"), "Bearer dubi.") {
		_, err := g.options.Tenant.Verify(ctx, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if err != nil {
			g.authError(w, err)
		} else {
			gatewayError(w, 403, "forbidden", "admin permission required")
		}
		return
	}
	c, token, cookie, err := g.authenticate(r)
	if err != nil {
		g.authError(w, err)
		return
	}
	principal, err := g.principal(r.Context(), c)
	if err != nil {
		g.authError(w, err)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/v1/tenant/") {
		g.tenantRequest(w, r, c, token, cookie, principal)
		return
	}
	if r.URL.Path == "/metrics" {
		if principal.TenantID != "" {
			gatewayError(w, 403, "forbidden", "集群监控不可访问。")
			return
		}
		if c.Role != "monitor" && c.Role != "admin" && c.Role != "session" {
			gatewayError(w, 403, "forbidden", "monitor permission required")
			return
		}
		g.options.Legacy.ServeHTTP(w, r)
		if r.Method == http.MethodGet && g.options.Metrics != nil {
			g.options.Metrics(w)
		}
		if r.Method == http.MethodGet && g.options.Certificate != nil {
			if s, ok := g.options.Certificate().(interface{ GatewayMetrics() string }); ok {
				_, _ = io.WriteString(w, s.GatewayMetrics())
			}
		}
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/v1/admin/") {
		if principal.TenantID != "" || principal.Role != "admin" {
			gatewayError(w, 403, "forbidden", "admin permission required")
			return
		}
		if cookie && r.Method != "GET" && r.Method != "HEAD" && (!sameOrigin(r) || !validCSRF(r, token)) {
			gatewayError(w, 403, "csrf_rejected", "same-origin CSRF token required")
			return
		}
		if r.URL.Path != "/api/v1/admin/gateway" {
			if g.options.Admin == nil {
				gatewayError(w, 404, "not_found", "management API not implemented")
				return
			}
			r = r.WithContext(context.WithValue(ctx, adminContextKey{}, principal))
			r = r.WithContext(context.WithValue(r.Context(), adminRecheckKey{}, func() error {
				current, _, _, err := g.authenticate(r)
				if err != nil {
					return err
				}
				p, e := g.principal(r.Context(), current)
				if e != nil {
					return e
				}
				if p.TenantID != "" || p.Role != "admin" || p.CredentialID != principal.CredentialID {
					return auth.ErrInvalidCredential
				}
				return nil
			}))
			if g.options.Accounts != nil && (r.URL.Path == "/api/v1/admin/accounts" || strings.HasPrefix(r.URL.Path, "/api/v1/admin/accounts/") || r.URL.Path == "/api/v1/admin/profile" || r.URL.Path == "/api/v1/admin/password") {
				g.options.Accounts.ServeHTTP(w, r)
			} else {
				g.options.Admin.ServeHTTP(w, r)
			}
			if r.Method != "GET" && r.Method != "HEAD" {
				g.audit("management_"+r.Method, c.CredentialID, "handled")
			}
			return
		}
	}
	switch r.URL.Path {
	case "/api/v1/me":
		if !method(w, r, "GET") {
			return
		}
		role := principal.Role
		v := map[string]any{"credential_id": c.CredentialID, "role": role, "expires_unix": c.ExpiresUnix, "tenant_id": principal.TenantID}
		if c.AccountID != "" && g.options.Accounts != nil {
			a, e := g.options.Accounts.current(ctx, c.CredentialID)
			if e != nil {
				g.authError(w, auth.ErrInvalidCredential)
				return
			}
			v["account_id"] = a.ID
			v["username"] = a.Username
			v["name"] = a.Name
		}
		if cookie {
			v["csrf_token"] = csrf(token)
		}
		jsonReply(w, 200, v)
	case "/api/v1/auth/logout":
		if !method(w, r, "POST") {
			return
		}
		if !cookie || c.Role != "session" {
			gatewayError(w, 403, "forbidden", "browser session required")
			return
		}
		if !sameOrigin(r) || !validCSRF(r, token) {
			gatewayError(w, 403, "csrf_rejected", "same-origin CSRF token required")
			return
		}
		err = g.options.Commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, RequestID: "logout-" + c.CredentialID, Type: rootstate.RevokeAccessCredential, AccessRevocation: &rootstate.TenantCredentialRevocation{CredentialID: c.CredentialID}})
		if err != nil {
			gatewayError(w, 503, "unavailable", "logout not confirmed; retry")
			return
		}
		http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: -1})
		g.audit("logout", c.CredentialID, "success")
		jsonReply(w, 200, map[string]bool{"logged_out": true})
	case "/api/v1/admin/gateway":
		if !method(w, r, "GET") {
			return
		}
		if principal.TenantID != "" || principal.Role != "admin" {
			gatewayError(w, 403, "forbidden", "admin permission required")
			return
		}
		if _, err := parseManagementQuery(r); err != nil {
			gatewayError(w, 400, "invalid_request", err.Error())
			return
		}
		v, err := g.options.Info(ctx)
		if err != nil {
			gatewayError(w, 503, "unavailable", "gateway state unavailable")
			return
		}
		managementReply(w, v, true, nil)
	}
}
func method(w http.ResponseWriter, r *http.Request, want string) bool {
	if r.Method == want {
		return true
	}
	w.Header().Set("Allow", want)
	gatewayError(w, 405, "method_not_allowed", "unsupported method")
	return false
}
func (g *Gateway) authenticate(r *http.Request) (rootstate.AccessCredential, string, bool, error) {
	values := r.Header.Values("Authorization")
	if len(values) > 0 {
		if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
			return rootstate.AccessCredential{}, "", false, auth.ErrInvalidCredential
		}
		token := strings.TrimPrefix(values[0], "Bearer ")
		c, err := g.options.Access.Verify(r.Context(), token)
		if err == nil && c.Role == "session" {
			err = auth.ErrInvalidCredential
		}
		return c, token, false, err
	}
	cookies := r.CookiesNamed(sessionCookie)
	if len(cookies) != 1 {
		return rootstate.AccessCredential{}, "", false, auth.ErrInvalidCredential
	}
	c, err := g.options.Access.Verify(r.Context(), cookies[0].Value)
	if err == nil && c.Role != "session" {
		err = auth.ErrInvalidCredential
	}
	return c, cookies[0].Value, true, err
}
func (g *Gateway) authError(w http.ResponseWriter, err error) {
	if errors.Is(err, auth.ErrAuthorizationUnavailable) {
		gatewayError(w, 503, "unavailable", "authorization unavailable")
	} else {
		w.Header().Set("WWW-Authenticate", "Bearer")
		gatewayError(w, 401, "unauthorized", "valid credential required")
	}
}
func sameOrigin(r *http.Request) bool {
	u, err := url.Parse(r.Header.Get("Origin"))
	return err == nil && u.Scheme == "https" && u.Host == r.Host && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}
func csrf(token string) string {
	b := sha256.Sum256([]byte("dubi csrf v1\x00" + token))
	return hex.EncodeToString(b[:])
}
func validCSRF(r *http.Request, token string) bool {
	want := csrf(token)
	got := r.Header.Get("X-Dubi-CSRF")
	return subtle.ConstantTimeCompare([]byte(want), []byte(got)) == 1
}
func (g *Gateway) login(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "POST") {
		return
	}
	if !sameOrigin(r) {
		gatewayError(w, 403, "csrf_rejected", "same-origin login required")
		return
	}
	if strings.Split(r.Header.Get("Content-Type"), ";")[0] != "application/json" {
		gatewayError(w, 415, "unsupported_media_type", "application/json required")
		return
	}
	g.mu.Lock()
	now := time.Now()
	if now.Sub(g.loginWindow) >= time.Minute {
		g.loginWindow = now
		g.logins = 0
	}
	allowed := g.logins < 20
	if allowed {
		g.logins++
	}
	g.mu.Unlock()
	if !allowed {
		w.Header().Set("Retry-After", "60")
		gatewayError(w, 429, "rate_limited", "login rate limit reached")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var input v1.LoginRequest
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if d.Decode(&input) != nil || d.Decode(new(any)) != io.EOF {
		gatewayError(w, 400, "invalid_request", "invalid login request")
		return
	}
	var token string
	var c rootstate.AccessCredential
	var err error
	var account rootstate.AdminAccount
	ttl := 12 * time.Hour
	if input.Token != "" && input.Username == "" && input.Password == "" {
		parent, e := g.options.Access.Verify(r.Context(), input.Token)
		input.Token = ""
		if e != nil {
			g.authError(w, e)
			return
		}
		if parent.Role != "admin" {
			gatewayError(w, 403, "forbidden", "admin credential required")
			return
		}
		if parent.ExpiresUnix != 0 {
			ttl = min(ttl, time.Until(time.Unix(parent.ExpiresUnix, 0)))
		}
		token, c, err = auth.GenerateAccess("session", parent.CredentialID, time.Now(), ttl)
	} else if input.Token == "" && input.Username != "" && input.Password != "" && g.options.Accounts != nil {
		account, err = g.options.Accounts.Authenticate(r.Context(), input.Username, input.Password)
		input.Password = ""
		if err != nil {
			g.audit("password_login", "", "rejected")
			if errors.Is(err, auth.ErrInvalidCredential) {
				gatewayError(w, 401, "unauthorized", "账号或密码不正确。")
			} else {
				g.authError(w, err)
			}
			return
		}
		token, c, err = auth.GenerateAccountSession(account)
	} else {
		gatewayError(w, 400, "invalid_request", "请提供账号密码或管理员 Token。")
		return
	}
	if err != nil {
		gatewayError(w, 401, "unauthorized", "无法创建会话，请重新登录。")
		return
	}
	if err = g.options.Commit(r.Context(), rootstate.Command{Version: rootstate.FormatVersion, RequestID: "session-" + c.CredentialID, Type: rootstate.CreateAccessCredential, AccessCredential: &c}); err != nil {
		gatewayError(w, 503, "unavailable", "session creation not confirmed")
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: token, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: int(ttl.Seconds()), Expires: time.Unix(c.ExpiresUnix, 0)})
	g.audit("login", c.CredentialID, "success")
	jsonReply(w, 200, v1.AccessIdentity{AccountID: account.ID, Username: account.Username, Name: account.Name, CredentialID: c.CredentialID, TenantID: account.TenantID, Role: account.EffectiveRole(), ExpiresUnix: c.ExpiresUnix, CSRFToken: csrf(token)})
}
func (g *Gateway) audit(action, id, result string) {
	if g.options.Audit != nil {
		g.options.Audit(action, id, result)
	}
}
