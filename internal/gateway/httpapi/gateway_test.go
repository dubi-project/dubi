package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type accessBarrier struct{ state *rootstate.State }

func (b accessBarrier) ReadIndex(context.Context, string) (uint64, error) {
	return b.state.AppliedIndex(), nil
}
func TestGatewaySessionCSRFRevocationAndMonitoring(t *testing.T) {
	state, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	commit := func(_ context.Context, c rootstate.Command) error {
		b, err := rootstate.EncodeCommand(c)
		if err != nil {
			return err
		}
		if err = state.Apply(state.AppliedIndex()+1, b); err != nil {
			return err
		}
		r, ok, err := state.Result(c.RequestID)
		if err != nil {
			return err
		}
		if !ok || r.Error != "" {
			return fmt.Errorf("commit: %v", r)
		}
		return nil
	}
	admin, c, _ := auth.GenerateAccess("admin", "", time.Now(), time.Hour)
	if err = commit(context.Background(), rootstate.Command{Version: rootstate.FormatVersion, RequestID: "admin", Type: rootstate.CreateAccessCredential, AccessCredential: &c}); err != nil {
		t.Fatal(err)
	}
	monitor, m, _ := auth.GenerateAccess("monitor", "", time.Now(), time.Hour)
	if err = commit(context.Background(), rootstate.Command{Version: rootstate.FormatVersion, RequestID: "monitor", Type: rootstate.CreateAccessCredential, AccessCredential: &m}); err != nil {
		t.Fatal(err)
	}
	opts := GatewayOptions{Access: auth.AccessVerifier{Barrier: accessBarrier{state}, Catalog: state}, Commit: commit, Info: func(context.Context) (any, error) { return map[string]string{"node_id": "test"}, nil }, Legacy: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })}
	opts.Admin = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p, ok := AdminIdentity(r.Context()); !ok || p.Role != "admin" || p.CredentialID == "" {
			t.Error("missing trusted admin principal")
		}
		jsonReply(w, 200, map[string]bool{"authorized": true})
	})
	a, b := NewGateway(opts), NewGateway(opts)
	request := func(g *Gateway, method, path, body, token, origin, csrf string, cookie *http.Cookie) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "https://node.test"+path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		if csrf != "" {
			r.Header.Set("X-Dubi-CSRF", csrf)
		}
		if cookie != nil {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		return w
	}
	for _, path := range []string{"/", "/assets/gateway.js", "/healthz"} {
		if w := request(a, "GET", path, "", "", "", "", nil); w.Code != 200 {
			t.Fatal(path, w.Code)
		}
	}
	if w := request(a, "GET", "/metrics", "", "", "", "", nil); w.Code != 401 {
		t.Fatal("anonymous metrics", w.Code)
	}
	if w := request(a, "GET", "/metrics", "", monitor, "", "", nil); w.Code != 200 {
		t.Fatal("monitor denied", w.Code)
	}
	if w := request(a, "GET", "/api/v1/admin/gateway", "", monitor, "", "", nil); w.Code != 403 {
		t.Fatal("monitor elevated", w.Code)
	}
	body, _ := json.Marshal(map[string]string{"token": admin})
	if w := request(a, "POST", "/api/v1/auth/login", string(body), "", "https://evil.test", "", nil); w.Code != 403 {
		t.Fatal("cross-origin login", w.Code)
	}
	w := request(a, "POST", "/api/v1/auth/login", string(body), "", "https://node.test", "", nil)
	if w.Code != 200 {
		t.Fatal("login", w.Code, w.Body.String())
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].Secure || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatal("insecure cookie")
	}
	cookie := cookies[0]
	var login struct {
		CSRF string `json:"csrf_token"`
	}
	if err = json.Unmarshal(w.Body.Bytes(), &login); err != nil {
		t.Fatal(err)
	}
	if w = request(b, "GET", "/api/v1/admin/gateway", "", "", "", "", cookie); w.Code != 200 {
		t.Fatal("cross-node session", w.Code)
	}
	if w = request(b, "POST", "/api/v1/admin/test", "", "", "https://node.test", "", cookie); w.Code != 403 {
		t.Fatal("admin extension omitted CSRF", w.Code)
	}
	if w = request(b, "POST", "/api/v1/admin/test", "", "", "https://node.test", login.CSRF, cookie); w.Code != 200 {
		t.Fatal("admin extension denied valid session", w.Code)
	}
	if w = request(b, "POST", "/api/v1/admin/test", "", admin, "", "", nil); w.Code != 200 {
		t.Fatal("admin extension denied bearer", w.Code)
	}
	if w = request(b, "GET", "/api/v1/me", "", cookie.Value, "", "", nil); w.Code != 401 {
		t.Fatal("session accepted as bearer", w.Code)
	}
	if w = request(b, "POST", "/api/v1/auth/logout", "", "", "https://node.test", "", cookie); w.Code != 403 {
		t.Fatal("missing csrf", w.Code)
	}
	if w = request(b, "POST", "/api/v1/auth/logout", "", "", "https://node.test", login.CSRF, cookie); w.Code != 200 {
		t.Fatal("logout", w.Code, w.Body.String())
	}
	if w = request(a, "GET", "/api/v1/admin/gateway", "", "", "", "", cookie); w.Code != 401 {
		t.Fatal("logged out session remained active", w.Code)
	}
	if w = request(a, "GET", "/api/v1/unknown", "", "", "", "", nil); w.Code != 404 || !strings.Contains(w.Header().Get("Content-Type"), "json") {
		t.Fatal("API swallowed by UI")
	}
	opts.Commit = func(context.Context, rootstate.Command) error { return errors.New("lost quorum") }
	g := NewGateway(opts)
	if w = request(g, "POST", "/api/v1/auth/login", string(body), "", "https://node.test", "", nil); w.Code != 503 || len(w.Result().Cookies()) != 0 {
		t.Fatal("unconfirmed session issued", w.Code)
	}
}
