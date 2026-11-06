package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
)

//go:embed console/*
var console embed.FS

const sessionCookie = "__Host-dubi_session"

// Admin handlers receive only verified management identity. Module handlers
// remain responsible for their own Tenant-bound contract.
type adminContextKey struct{}
type AdminPrincipal struct {
	CredentialID string
	Role         string
}

func AdminIdentity(ctx context.Context) (AdminPrincipal, bool) {
	p, ok := ctx.Value(adminContextKey{}).(AdminPrincipal)
	return p, ok
}

type GatewayOptions struct {
	Admin       http.Handler
	Audit       func(action, credential, result string)
	Certificate func() any

	Legacy http.Handler
	Access auth.AccessVerifier
	Commit func(context.Context, rootstate.Command) error
	Info   func(context.Context) (any, error)
}
type Gateway struct {
	options     GatewayOptions
	slots       chan struct{}
	mu          sync.Mutex
	loginWindow time.Time
	logins      int
}

func NewGateway(o GatewayOptions) *Gateway {
	return &Gateway{options: o, slots: make(chan struct{}, 32)}
}
func jsonReply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func gatewayError(w http.ResponseWriter, status int, code, message string) {
	failure(w, status, code, message, "")
}
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
	if r.URL.Path == "/" || r.URL.Path == "/assets/gateway.js" || r.URL.Path == "/assets/gateway.css" {
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
	known := r.URL.Path == "/api/v1/me" || r.URL.Path == "/api/v1/auth/logout" || strings.HasPrefix(r.URL.Path, "/api/v1/admin/") || r.URL.Path == "/metrics"
	if !known {
		gatewayError(w, 404, "not_found", "API not found")
		return
	}
	c, token, cookie, err := g.authenticate(r)
	if err != nil {
		g.authError(w, err)
		return
	}
	if r.URL.Path == "/metrics" {
		if c.Role != "monitor" && c.Role != "admin" && c.Role != "session" {
			gatewayError(w, 403, "forbidden", "monitor permission required")
			return
		}
		g.options.Legacy.ServeHTTP(w, r)
		if r.Method == http.MethodGet && g.options.Certificate != nil {
			if s, ok := g.options.Certificate().(interface{ GatewayMetrics() string }); ok {
				_, _ = io.WriteString(w, s.GatewayMetrics())
			}
		}
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/v1/admin/") {
		if c.Role != "admin" && c.Role != "session" {
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
			r = r.WithContext(context.WithValue(ctx, adminContextKey{}, AdminPrincipal{CredentialID: c.CredentialID, Role: "admin"}))
			g.options.Admin.ServeHTTP(w, r)
			return
		}
	}
	switch r.URL.Path {
	case "/api/v1/me":
		if !method(w, r, "GET") {
			return
		}
		role := c.Role
		if role == "session" {
			role = "admin"
		}
		v := map[string]any{"credential_id": c.CredentialID, "role": role, "expires_unix": c.ExpiresUnix}
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
		if c.Role != "admin" && c.Role != "session" {
			gatewayError(w, 403, "forbidden", "admin permission required")
			return
		}
		v, err := g.options.Info(ctx)
		if err != nil {
			gatewayError(w, 503, "unavailable", "gateway state unavailable")
			return
		}
		jsonReply(w, 200, v)
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
	parent, err := g.options.Access.Verify(r.Context(), input.Token)
	input.Token = ""
	if err != nil {
		g.authError(w, err)
		return
	}
	if parent.Role != "admin" {
		gatewayError(w, 403, "forbidden", "admin credential required")
		return
	}
	ttl := min(12*time.Hour, time.Until(time.Unix(parent.ExpiresUnix, 0)))
	token, c, err := auth.GenerateAccess("session", parent.CredentialID, time.Now(), ttl)
	if err != nil {
		gatewayError(w, 401, "unauthorized", "credential expires too soon")
		return
	}
	if err = g.options.Commit(r.Context(), rootstate.Command{Version: rootstate.FormatVersion, RequestID: "session-" + c.CredentialID, Type: rootstate.CreateAccessCredential, AccessCredential: &c}); err != nil {
		gatewayError(w, 503, "unavailable", "session creation not confirmed")
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: token, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: int(ttl.Seconds()), Expires: time.Unix(c.ExpiresUnix, 0)})
	g.audit("login", c.CredentialID, "success")
	jsonReply(w, 200, v1.AccessIdentity{CredentialID: c.CredentialID, Role: "admin", ExpiresUnix: c.ExpiresUnix, CSRFToken: csrf(token)})
}
func (g *Gateway) static(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "HEAD" {
		w.Header().Set("Allow", "GET, HEAD")
		gatewayError(w, 405, "method_not_allowed", "unsupported method")
		return
	}
	name, kind := "index.html", "text/html; charset=utf-8"
	if r.URL.Path == "/assets/gateway.js" {
		name, kind = "gateway.js", "application/javascript"
	}
	if r.URL.Path == "/assets/gateway.css" {
		name, kind = "gateway.css", "text/css"
	}
	b, err := console.ReadFile("console/" + name)
	if err != nil {
		http.Error(w, "console asset missing", 500)
		return
	}
	w.Header().Set("Content-Type", kind)
	w.Header().Set("Content-Length", fmt.Sprint(len(b)))
	if r.Method != "HEAD" {
		_, _ = w.Write(b)
	}
}

func (g *Gateway) audit(action, id, result string) {
	if g.options.Audit != nil {
		g.options.Audit(action, id, result)
	}
}
