package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAccountLoginProfilePasswordAndDisable(t *testing.T) {
	root, e := rootstate.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
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
	token, admin, e := auth.GenerateAccess("admin", "", time.Now(), time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	if e = commit(context.Background(), rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.CreateAccessCredential, RequestID: "bootstrap", AccessCredential: &admin}); e != nil {
		t.Fatal(e)
	}
	accounts := &Accounts{Root: root, Barrier: func(context.Context) (uint64, error) { return root.AppliedIndex(), nil }, Commit: commit}
	g := NewGateway(GatewayOptions{Admin: http.NotFoundHandler(), Accounts: accounts, Access: auth.AccessVerifier{Catalog: root, Barrier: accessBarrier{root}}, Commit: commit})
	count := 0
	call := func(method, path string, body any, bearer string, cookie *http.Cookie, csrf string, want int) *httptest.ResponseRecorder {
		t.Helper()
		b, _ := json.Marshal(body)
		r := httptest.NewRequest(method, "https://node.test"+path, strings.NewReader(string(b)))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Origin", "https://node.test")
		if bearer != "" {
			r.Header.Set("Authorization", "Bearer "+bearer)
		}
		if cookie != nil {
			r.AddCookie(cookie)
		}
		if csrf != "" {
			r.Header.Set("X-Dubi-CSRF", csrf)
		}
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		return w
	}
	decode := func(w *httptest.ResponseRecorder) accountView {
		var x struct {
			Data accountView `json:"data"`
		}
		if e = json.Unmarshal(w.Body.Bytes(), &x); e != nil {
			t.Fatal(e)
		}
		if strings.Contains(w.Body.String(), "password_hash") || strings.Contains(w.Body.String(), "password_salt") {
			t.Fatal("password metadata leaked")
		}
		return x.Data
	}
	create := func(username string) accountView {
		count++
		return decode(call("POST", "/api/v1/admin/accounts", map[string]any{"request_id": fmt.Sprint("create", count), "username": username, "name": "管理员 " + username, "password": "correct-password-123"}, token, nil, "", 200))
	}
	a := create("alice")
	if !strings.HasPrefix(a.ID, "account_") || len(a.ID) != 20 {
		t.Fatalf("unexpected account ID: %s", a.ID)
	}
	b := create("bob")
	call("POST", "/api/v1/admin/accounts", map[string]any{"request_id": "duplicate", "username": "ALICE", "name": "重复", "password": "correct-password-123"}, token, nil, "", 409)
	call("POST", "/api/v1/admin/accounts", map[string]any{"request_id": "short", "username": "short", "name": "短密码", "password": "short"}, token, nil, "", 400)
	for _, username := range []string{"alice", "absent"} {
		call("POST", "/api/v1/auth/login", map[string]string{"username": username, "password": "wrong"}, "", nil, "", 401)
	}
	login := func(username, password string) (*http.Cookie, v1.AccessIdentity) {
		w := call("POST", "/api/v1/auth/login", map[string]string{"username": username, "password": password}, "", nil, "", 200)
		var me v1.AccessIdentity
		json.Unmarshal(w.Body.Bytes(), &me)
		if me.AccountID == "" || me.Username != username || me.Name == "" || me.CSRFToken == "" {
			t.Fatal("missing account identity")
		}
		return w.Result().Cookies()[0], me
	}
	cookie, me := login("alice", "correct-password-123")
	other, _ := login("bob", "correct-password-123")
	call("GET", "/api/v1/admin/accounts", nil, "", nil, "", 401)
	call("GET", "/api/v1/me", nil, "", cookie, "", 200)
	profile := map[string]any{"request_id": "profile", "revision": a.Revision, "name": "新的名字"}
	call("PATCH", "/api/v1/admin/profile", profile, "", cookie, "", 403)
	a = decode(call("PATCH", "/api/v1/admin/profile", profile, "", cookie, me.CSRFToken, 200))
	if a.Name != "新的名字" {
		t.Fatal(a)
	}
	if !strings.Contains(call("GET", "/api/v1/me", nil, "", cookie, "", 200).Body.String(), "新的名字") {
		t.Fatal("stale identity name")
	}
	change := map[string]any{"request_id": "wrong-old", "revision": a.Revision, "current_password": "wrong", "password": "changed-password-123"}
	call("POST", "/api/v1/admin/password", change, "", cookie, me.CSRFToken, 400)
	change["request_id"] = "new-password"
	change["current_password"] = "correct-password-123"
	a = decode(call("POST", "/api/v1/admin/password", change, "", cookie, me.CSRFToken, 200))
	call("GET", "/api/v1/me", nil, "", cookie, "", 401)
	call("GET", "/api/v1/me", nil, "", other, "", 200)
	call("POST", "/api/v1/auth/login", map[string]string{"username": "alice", "password": "correct-password-123"}, "", nil, "", 401)
	cookie, me = login("alice", "changed-password-123")
	a = decode(call("PATCH", "/api/v1/admin/accounts/"+a.ID, map[string]any{"request_id": "disable", "revision": a.Revision, "disabled": true}, token, nil, "", 200))
	call("GET", "/api/v1/me", nil, "", cookie, "", 401)
	call("POST", "/api/v1/auth/login", map[string]string{"username": "alice", "password": "changed-password-123"}, "", nil, "", 401)
	call("PATCH", "/api/v1/admin/accounts/"+b.ID, map[string]any{"request_id": "last-admin", "revision": b.Revision, "disabled": true}, token, nil, "", 409)
	a = decode(call("PATCH", "/api/v1/admin/accounts/"+a.ID, map[string]any{"request_id": "reset-enable", "revision": a.Revision, "disabled": false, "password": "reset-password-123"}, token, nil, "", 200))
	cookie, me = login("alice", "reset-password-123")
	c := create("charlie")
	c = decode(call("PATCH", "/api/v1/admin/accounts/"+c.ID, map[string]any{"request_id": "disable-charlie", "revision": c.Revision, "disabled": true}, token, nil, "", 200))
	var list struct {
		Data struct {
			Items      []accountView `json:"items"`
			NextCursor string        `json:"next_cursor"`
		}
	}
	json.Unmarshal(call("GET", "/api/v1/admin/accounts?limit=1", nil, token, nil, "", 200).Body.Bytes(), &list)
	if len(list.Data.Items) != 1 || list.Data.Items[0].ID != b.ID || list.Data.NextCursor == "" {
		t.Fatal("enabled-first pagination", list)
	}
	json.Unmarshal(call("GET", "/api/v1/admin/accounts?limit=1&cursor="+list.Data.NextCursor, nil, token, nil, "", 200).Body.Bytes(), &list)
	if len(list.Data.Items) != 1 || list.Data.Items[0].ID != a.ID {
		t.Fatal("pagination order", list)
	}
	decode(call("GET", "/api/v1/admin/accounts/"+b.ID, nil, token, nil, "", 200))
	call("DELETE", "/api/v1/admin/accounts/"+a.ID, map[string]any{"request_id": "self-delete", "revision": a.Revision}, "", cookie, me.CSRFToken, 409)
	call("DELETE", "/api/v1/admin/accounts/"+b.ID, map[string]any{"request_id": "csrf-delete", "revision": b.Revision}, "", cookie, "", 403)
	call("DELETE", "/api/v1/admin/accounts/"+b.ID, map[string]any{"request_id": "stale-delete", "revision": b.Revision + 1}, token, nil, "", 409)
	body := map[string]any{"request_id": "delete-bob", "revision": b.Revision}
	call("DELETE", "/api/v1/admin/accounts/"+b.ID, body, token, nil, "", 200)
	call("DELETE", "/api/v1/admin/accounts/"+b.ID, body, token, nil, "", 200)
	call("GET", "/api/v1/admin/accounts/"+b.ID, nil, token, nil, "", 404)
	call("GET", "/api/v1/me", nil, "", other, "", 401)
	call("POST", "/api/v1/auth/login", map[string]string{"username": "bob", "password": "correct-password-123"}, "", nil, "", 401)
	call("DELETE", "/api/v1/admin/accounts/"+a.ID, map[string]any{"request_id": "last-delete", "revision": a.Revision}, token, nil, "", 409)
	call("DELETE", "/api/v1/admin/accounts/"+c.ID, map[string]any{"request_id": "disabled-delete", "revision": c.Revision}, token, nil, "", 200)
}
