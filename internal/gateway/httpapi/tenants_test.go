package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTenantManagementAuthorizationCASAndCredentials(t *testing.T) {
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	commit := func(_ context.Context, c rootstate.Command) error {
		encoded, e := rootstate.EncodeCommand(c)
		if e != nil {
			return e
		}
		if e = root.Apply(root.AppliedIndex()+1, encoded); e != nil {
			return e
		}
		r, ok, e := root.ResultForCommand(encoded)
		if e != nil {
			return e
		}
		if !ok {
			return errors.New("missing result")
		}
		if r.Error != "" {
			return errors.New(r.Error)
		}
		return nil
	}
	barrier := func(context.Context) (uint64, error) { return root.AppliedIndex(), nil }
	admin, c, _ := auth.GenerateAccess("admin", "", time.Now(), time.Hour)
	if err := commit(context.Background(), rootstate.Command{Version: rootstate.FormatVersion, RequestID: "admin", Type: rootstate.CreateAccessCredential, AccessCredential: &c}); err != nil {
		t.Fatal(err)
	}
	service := &management.Tenants{Root: root, Barrier: barrier, Commit: commit, Modules: []v1.TenantModule{{ID: "config", Name: "配置中心"}}}
	g := NewGateway(GatewayOptions{Admin: &TenantManagement{Service: service}, Access: auth.AccessVerifier{Barrier: accessBarrier{root}, Catalog: root}, Commit: commit})
	call := func(method, path, body, token string, want int) map[string]json.RawMessage {
		t.Helper()
		r := httptest.NewRequest(method, "https://node.test/api/v1/admin/"+path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		var out map[string]json.RawMessage
		if json.Unmarshal(w.Body.Bytes(), &out) != nil {
			t.Fatal(w.Body.String())
		}
		return out
	}
	call("GET", "tenants", "", "", 401)
	create := `{"request_id":"create","tenant_id":"t","name":"租户","modules":["config"],"storage_limit_bytes":20}`
	out := call("POST", "tenants", create, admin, 200)
	var tenant v1.ManagedTenant
	json.Unmarshal(out["data"], &tenant)
	call("POST", "tenants", create, admin, 200)
	call("POST", "tenants", strings.Replace(create, `"create"`, `"different"`, 1), admin, 409)
	call("POST", "tenants", `{"request_id":"x","tenant_id":"x","name":"X","modules":["cache"]}`, admin, 400)
	call("GET", "tenants?status=active&status=inactive", "", admin, 400)
	call("GET", "tenants?unknown=1", "", admin, 400)
	call("PATCH", "tenants/t", "{}", admin, 405)
	body, _ := json.Marshal(v1.TenantMutation{RequestID: "update", TenantID: "t", Name: "新名称", Modules: []string{}, StorageLimitBytes: 10, Revision: &tenant.Revision})
	call("PUT", "tenants/t", string(body), admin, 200)
	call("PUT", "tenants/t", string(body), admin, 200)
	call("PUT", "tenants/t", strings.Replace(string(body), `"update"`, `"stale"`, 1), admin, 409)
	issue := call("POST", "tenants/t/credentials", `{"request_id":"issue","ttl_seconds":3600}`, admin, 200)
	var issued v1.TenantToken
	json.Unmarshal(issue["data"], &issued)
	if issued.Token == "" {
		t.Fatal("no token")
	}
	call("POST", "tenants/t/credentials", `{"request_id":"issue","ttl_seconds":3600}`, admin, 409)
	info := call("GET", "tenants/t", "", admin, 200)
	if strings.Contains(string(info["data"]), issued.Token) || strings.Contains(string(info["data"]), "sha256") {
		t.Fatal("verifier leaked")
	}
	call("DELETE", "tenants/other/credentials/"+issued.CredentialID, `{"request_id":"wrong-tenant"}`, admin, 404)
	call("DELETE", "tenants/t/credentials/"+issued.CredentialID, `{"request_id":"revoke"}`, admin, 200)
	call("DELETE", "tenants/t/credentials/"+issued.CredentialID, `{"request_id":"revoke2"}`, admin, 200)
}
