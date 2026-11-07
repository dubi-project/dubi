package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOperationsCredentialsRepositoriesAndBoundaries(t *testing.T) {
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
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
	barrier := func(context.Context) (uint64, error) { return root.AppliedIndex(), nil }
	key := []byte(strings.Repeat("k", 32))
	admin, a, _ := auth.GenerateAccess("admin", "", time.Now(), time.Hour)
	must := func(c rootstate.Command) {
		t.Helper()
		c.Version = rootstate.FormatVersion
		if e := commit(context.Background(), c); e != nil {
			t.Fatal(e)
		}
	}
	must(rootstate.Command{RequestID: "bootstrap", Type: rootstate.CreateAccessCredential, AccessCredential: &a})
	must(rootstate.Command{RequestID: "tenant", Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: "test", Name: "Test", Modules: []string{"config"}}})
	service := &management.Operations{Root: root, Barrier: barrier, Commit: commit, RepositoryKey: func() ([]byte, error) { return key, nil }}
	g := NewGateway(GatewayOptions{Admin: &Operations{Service: service}, Access: auth.AccessVerifier{Barrier: accessBarrier{root}, Catalog: root}, Commit: commit})
	call := func(method, path string, body any, token string, want int) json.RawMessage {
		t.Helper()
		b, _ := json.Marshal(body)
		r := httptest.NewRequest(method, "https://node.test/api/v1/admin/"+path, strings.NewReader(string(b)))
		r.Header.Set("Content-Type", "application/json")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		var out struct {
			Data json.RawMessage `json:"data"`
		}
		json.Unmarshal(w.Body.Bytes(), &out)
		return out.Data
	}
	for _, path := range []string{"credentials", "repositories", "backups"} {
		call("GET", path, nil, "", 401)
	}
	var token v1.TenantToken
	in := v1.CredentialMutation{RequestID: "new-monitor", Name: "Monitoring", Kind: "monitor", TTLSeconds: 3600}
	json.Unmarshal(call("POST", "credentials", in, admin, 200), &token)
	call("POST", "credentials", in, admin, 409)
	for _, path := range []string{"credentials", "repositories", "backups"} {
		call("GET", path, nil, token.Token, 403)
	}
	raw := call("GET", "credentials", nil, admin, 200)
	if strings.Contains(string(raw), token.Token) || strings.Contains(string(raw), "sha256") || strings.Contains(string(raw), "session") {
		t.Fatal("credential secret or session leaked")
	}
	var page v1.CredentialPage
	json.Unmarshal(raw, &page)
	if page.Total != 2 || page.Summary["active"] != 2 {
		t.Fatalf("summary: %s", raw)
	}
	call("GET", "credentials?kind=bogus", nil, admin, 400)
	call("GET", "credentials?expiring=no", nil, admin, 400)
	call("POST", "credentials/"+a.CredentialID+"/revoke", v1.TenantRevokeRequest{RequestID: "self"}, admin, 409)
	call("POST", "credentials/"+token.CredentialID+"/revoke", v1.TenantRevokeRequest{RequestID: "revoke"}, admin, 200)
	if _, err := g.options.Access.Verify(context.Background(), token.Token); !errors.Is(err, auth.ErrInvalidCredential) {
		t.Fatalf("revoked token accepted: %v", err)
	}
	in.Kind = "tenant"
	in.TenantID = "missing"
	in.RequestID = "missing-tenant"
	call("POST", "credentials", in, admin, 404)
	in.TenantID = "test"
	in.RequestID = "new-tenant-token"
	call("POST", "credentials", in, admin, 200)
	repo := v1.RepositoryMutation{RequestID: "create-s3", Name: "primary", Type: "s3", Bucket: "backups", AccessKey: "example-access", SecretKey: "example-secret"}
	call("POST", "repositories", repo, admin, 200)
	stored, _, _ := root.Repository("primary")
	plain, e := repository.OpenS3Credentials(stored, key)
	if e != nil || plain.SecretKey != repo.SecretKey {
		t.Fatal("encryption round trip failed")
	}
	raw = call("GET", "repositories", nil, admin, 200)
	for _, secret := range []string{repo.AccessKey, repo.SecretKey, "encrypted_credentials", "credentials_nonce"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("repository secrets leaked")
		}
	}
	call("POST", "repositories/primary/default", v1.TenantRevokeRequest{RequestID: "default"}, admin, 200)
	repo.RequestID = "update-s3"
	repo.Revision = stored.Revision()
	repo.AccessKey = ""
	repo.SecretKey = ""
	repo.Prefix = "changed/"
	call("PUT", "repositories/primary", repo, admin, 200)
	preserved, _, _ := root.Repository("primary")
	if string(preserved.EncryptedCredentials) != string(stored.EncryptedCredentials) {
		t.Fatal("credentials not preserved")
	}
	repo.RequestID = "stale"
	repo.Prefix = "stale/"
	call("PUT", "repositories/primary", repo, admin, 409)
	repo.RequestID = "mismatch"
	repo.Name = "other"
	call("PUT", "repositories/primary", repo, admin, 400)
	call("POST", "repositories", map[string]any{"request_id": "injection", "name": "local", "path": "/backup", "encrypted_credentials": "xxx"}, admin, 400)
	call("GET", "backups/not-present/restore-preparation", nil, admin, 404)
	call("PUT", "backups/not-present", nil, admin, 405)
}
