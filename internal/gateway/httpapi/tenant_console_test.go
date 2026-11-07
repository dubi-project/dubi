package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTenantConsoleScopeRolesAndCredentialPermissions(t *testing.T) {
	root, e := rootstate.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	ctx := context.Background()
	sequence := 0
	commit := func(_ context.Context, c rootstate.Command) error {
		b, e := rootstate.EncodeCommand(c)
		if e != nil {
			return e
		}
		if e = root.Apply(root.AppliedIndex()+1, b); e != nil {
			return e
		}
		r, _, e := root.ResultForCommand(b)
		if e != nil {
			return e
		}
		if r.Error != "" {
			return errors.New(r.Error)
		}
		return nil
	}
	apply := func(c rootstate.Command) {
		sequence++
		c.Version = rootstate.FormatVersion
		c.RequestID = fmt.Sprint("setup-", sequence)
		if e := commit(ctx, c); e != nil {
			t.Fatal(e)
		}
	}
	barrier := func(context.Context) (uint64, error) { return root.AppliedIndex(), nil }
	for _, id := range []string{"tenant-a", "tenant-b"} {
		apply(rootstate.Command{Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: id, Name: id, Modules: []string{"config"}}})
	}
	base := rootstate.AdminAccount{ID: "account_000000000001", Username: "system.admin", Name: "System", CreatedUnix: time.Now().Unix()}
	if e = auth.SetPassword(ctx, &base, "Temporary-test-password-938!A"); e != nil {
		t.Fatal(e)
	}
	accounts := &Accounts{Root: root, Barrier: barrier, Commit: commit}
	ts := &management.Tenants{Root: root, Barrier: barrier, Commit: commit, Modules: []v1.TenantModule{{ID: "config", Name: "配置中心"}}}
	ops := &management.Operations{Root: root, Barrier: barrier, Commit: commit}
	portal := &TenantConsole{Tenants: ts, Operations: ops, Config: &AdminConfig{Root: root, Barrier: barrier}}
	g := NewGateway(GatewayOptions{Admin: http.NotFoundHandler(), Accounts: accounts, TenantConsole: portal, Access: auth.AccessVerifier{Catalog: root, Barrier: accessBarrier{root}}, Commit: commit})
	type session struct {
		account rootstate.AdminAccount
		cookie  *http.Cookie
		csrf    string
	}
	users := map[string]session{}
	for i, v := range []struct{ name, tenant, role string }{{"system.admin", "", "admin"}, {"a.admin", "tenant-a", "admin"}, {"a.writer", "tenant-a", "readwrite"}, {"a.reader", "tenant-a", "readonly"}, {"b.admin", "tenant-b", "admin"}} {
		a := base
		a.ID = fmt.Sprintf("account_%012x", i+1)
		a.Username = v.name
		a.TenantID = v.tenant
		a.Role = v.role
		apply(rootstate.Command{Type: rootstate.PutAdminAccount, AdminAccountUpdate: &rootstate.AdminAccountUpdate{Account: a}})
		a, _, _ = root.AdminAccount(a.ID)
		token, c, e := auth.GenerateAccountSession(a)
		if e != nil {
			t.Fatal(e)
		}
		apply(rootstate.Command{Type: rootstate.CreateAccessCredential, AccessCredential: &c})
		users[v.name] = session{a, &http.Cookie{Name: sessionCookie, Value: token}, csrf(token)}
	}
	call := func(user, method, path string, body any, want int) *httptest.ResponseRecorder {
		t.Helper()
		b, _ := json.Marshal(body)
		r := httptest.NewRequest(method, "https://node.test"+path, strings.NewReader(string(b)))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Origin", "https://node.test")
		u := users[user]
		r.AddCookie(u.cookie)
		r.Header.Set("X-Dubi-CSRF", u.csrf)
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s %s: %d %s", user, method, path, w.Code, w.Body.String())
		}
		return w
	}
	for _, name := range []string{"a.admin", "a.writer", "a.reader"} {
		call(name, "GET", "/api/v1/admin/accounts", nil, 403)
		call(name, "GET", "/api/v1/admin/gateway", nil, 403)
		call(name, "GET", "/metrics", nil, 403)
		w := call(name, "GET", "/api/v1/tenant/overview", nil, 200)
		if strings.Contains(w.Body.String(), "tenant-b") || strings.Contains(w.Body.String(), "system.admin") {
			t.Fatal("scope leaked")
		}
		call(name, "GET", "/api/v1/tenant/profile", nil, 200)
		call(name, "GET", "/api/v1/tenant/config?tenant=tenant-b", nil, 400)
	}
	for _, name := range []string{"a.writer", "a.reader"} {
		call(name, "GET", "/api/v1/tenant/accounts", nil, 403)
		call(name, "GET", "/api/v1/tenant/credentials", nil, 403)
	}
	call("a.reader", "PUT", "/api/v1/tenant/config/entry?key=x", map[string]any{"request_id": "x", "if_absent": true, "value": "eA=="}, 403)
	call("system.admin", "GET", "/api/v1/tenant/overview", nil, 403)
	for _, name := range []string{"system.admin", "b.admin"} {
		id := users[name].account.ID
		call("a.admin", "GET", "/api/v1/tenant/accounts/"+id, nil, 404)
		call("a.admin", "DELETE", "/api/v1/tenant/accounts/"+id, map[string]any{"request_id": "wrong", "revision": users[name].account.Revision}, 404)
	}
	w := call("a.admin", "GET", "/api/v1/tenant/accounts", nil, 200)
	if strings.Contains(w.Body.String(), "b.admin") || strings.Contains(w.Body.String(), "system.admin") {
		t.Fatal("account list leaked")
	}
	call("a.admin", "GET", "/api/v1/tenant/accounts?tenant_id=tenant-b", nil, 400)
	call("a.admin", "POST", "/api/v1/tenant/accounts", map[string]any{"request_id": "escalate", "username": "x.admin", "name": "X", "password": "Temporary-test-password-938!A", "tenant_id": "", "role": "admin"}, 400)
	call("a.reader", "PATCH", "/api/v1/tenant/profile", map[string]any{"request_id": "profile-escalate", "revision": users["a.reader"].account.Revision, "name": "Read", "role": "admin"}, 400)
	w = call("a.admin", "POST", "/api/v1/tenant/credentials", map[string]any{"request_id": "readonly-token", "name": "Read-only", "permission": "readonly", "modules": []string{"config"}, "ttl_seconds": 3600}, 200)
	var issued struct {
		Data v1.TenantToken `json:"data"`
	}
	json.Unmarshal(w.Body.Bytes(), &issued)
	p, e := (auth.Verifier{Barrier: accessBarrier{root}, Catalog: root}).Verify(ctx, issued.Data.Token)
	if e != nil || p.AuthorizeWrite() == nil {
		t.Fatal("credential write permission not enforced", e)
	}
	if p.Authorize(rootstate.Keyspace{TenantID: "tenant-a", ModuleID: "session", EngineID: "state"}) == nil {
		t.Fatal("credential module scope not enforced")
	}
	call("b.admin", "GET", "/api/v1/tenant/credentials/"+issued.Data.CredentialID, nil, 404)
	call("b.admin", "POST", "/api/v1/tenant/credentials/"+issued.Data.CredentialID+"/revoke", map[string]any{"request_id": "wrong-revoke"}, 404)
	call("a.admin", "POST", "/api/v1/tenant/credentials", map[string]any{"request_id": "scope-escalate", "name": "Bad", "permission": "readwrite", "modules": []string{"session"}, "ttl_seconds": 3600}, 400)
	call("a.admin", "POST", "/api/v1/tenant/credentials/"+issued.Data.CredentialID+"/revoke", map[string]any{"request_id": "revoke"}, 200)
	if _, e = (auth.Verifier{Barrier: accessBarrier{root}, Catalog: root}).Verify(ctx, issued.Data.Token); e == nil {
		t.Fatal("revoked token valid")
	}
	call("a.admin", "DELETE", "/api/v1/tenant/credentials/"+issued.Data.CredentialID, map[string]any{"request_id": "delete"}, 200)
	a := users["a.writer"].account
	a.Role = "readonly"
	apply(rootstate.Command{Type: rootstate.PutAdminAccount, AdminAccountUpdate: &rootstate.AdminAccountUpdate{Account: a, ExpectedRevision: a.Revision}})
	call("a.writer", "GET", "/api/v1/me", nil, 401)
}
