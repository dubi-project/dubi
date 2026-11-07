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
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCredentialNeverExpiryDeleteAndStatusOrdering(t *testing.T) {
	root, e := rootstate.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	commit := func(_ context.Context, c rootstate.Command) error {
		c.Version = rootstate.FormatVersion
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
	must := func(c rootstate.Command) {
		t.Helper()
		if e := commit(context.Background(), c); e != nil {
			t.Fatal(e)
		}
	}
	admin, a, e := auth.GenerateAccess("admin", "", time.Now(), 0)
	if e != nil {
		t.Fatal(e)
	}
	must(rootstate.Command{RequestID: "admin", Type: rootstate.CreateAccessCredential, AccessCredential: &a})
	must(rootstate.Command{RequestID: "tenant", Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: "t"}})
	barrier := func(context.Context) (uint64, error) { return root.AppliedIndex(), nil }
	ops := &management.Operations{Root: root, Commit: commit, Barrier: barrier}
	g := NewGateway(GatewayOptions{Admin: &Operations{Service: ops}, Access: auth.AccessVerifier{Barrier: accessBarrier{root}, Catalog: root}, Commit: commit})
	call := func(method, path string, body any, want int) json.RawMessage {
		t.Helper()
		b, _ := json.Marshal(body)
		r := httptest.NewRequest(method, "https://node.test/api/v1/admin/"+path, strings.NewReader(string(b)))
		r.Header.Set("Authorization", "Bearer "+admin)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		var out struct{ Data json.RawMessage }
		json.Unmarshal(w.Body.Bytes(), &out)
		return out.Data
	}
	ids := []string{}
	for i, kind := range []string{"admin", "tenant", "monitor"} {
		in := v1.CredentialMutation{RequestID: fmt.Sprint("issue", i), Name: kind, Kind: kind, NeverExpires: true}
		if kind == "tenant" {
			in.TenantID = "t"
		}
		var token v1.TenantToken
		json.Unmarshal(call("POST", "credentials", in, 200), &token)
		if token.ExpiresUnix != 0 || !strings.HasPrefix(token.CredentialID, "credential_") || len(token.CredentialID) != 23 {
			t.Fatal(token.CredentialID, token.ExpiresUnix)
		}
		ids = append(ids, token.CredentialID)
		call("DELETE", "credentials/"+token.CredentialID, v1.TenantRevokeRequest{RequestID: fmt.Sprint("active", i)}, 409)
	}
	call("POST", "credentials", v1.CredentialMutation{RequestID: "zero-not-explicit", Name: "bad", Kind: "monitor"}, 400)
	call("POST", "credentials", v1.CredentialMutation{RequestID: "conflicting-ttl", Name: "bad", Kind: "monitor", TTLSeconds: 3600, NeverExpires: true}, 400)
	var list v1.CredentialPage
	json.Unmarshal(call("GET", "credentials", nil, 200), &list)
	if list.Summary["expiring"] != 0 || list.Summary["active"] != 4 {
		t.Fatal(list.Summary)
	}
	json.Unmarshal(call("GET", "credentials?expiring=true", nil, 200), &list)
	if len(list.Items) != 0 {
		t.Fatal("permanent credentials marked expiring")
	}
	for i, id := range ids {
		call("POST", "credentials/"+id+"/revoke", v1.TenantRevokeRequest{RequestID: fmt.Sprint("revoke", i)}, 200)
	}
	json.Unmarshal(call("GET", "credentials", nil, 200), &list)
	if list.Items[0].CredentialID != a.CredentialID || list.Summary["active"] != 1 || list.Summary["revoked"] != 3 {
		t.Fatal("invalid status order/counts", list.Summary)
	}
	for i, id := range ids {
		req := v1.TenantRevokeRequest{RequestID: fmt.Sprint("delete", i)}
		call("DELETE", "credentials/"+id, req, 200)
		call("DELETE", "credentials/"+id, req, 200)
		call("GET", "credentials/"+id, nil, 404)
	}
	json.Unmarshal(call("GET", "credentials?status=revoked", nil, 200), &list)
	if list.Total != 0 || list.Summary["revoked"] != 0 {
		t.Fatal("deleted credentials counted")
	}
	// A permanent bootstrap Token still creates a finite browser session.
	r := httptest.NewRequest("POST", "https://node.test/api/v1/auth/login", strings.NewReader(`{"token":"`+admin+`"}`))
	r.Header.Set("Origin", "https://node.test")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var me v1.AccessIdentity
	json.Unmarshal(w.Body.Bytes(), &me)
	if me.ExpiresUnix <= time.Now().Unix() || me.ExpiresUnix > time.Now().Add(12*time.Hour).Unix() {
		t.Fatal("invalid browser session expiry")
	}
}
