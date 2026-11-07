package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
	"github.com/dubi-project/dubi/internal/platform/recovery/repository"
)

// Exercise the real state-machine and HTTP cursor path, including insertion
// between pages. Credential status groups precede newest-first issuance order.
func TestManagementCreationOrderAndPagination(t *testing.T) {
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
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
		if e == nil && r.Error != "" {
			return errors.New(r.Error)
		}
		return e
	}
	must := func(c rootstate.Command) {
		t.Helper()
		if e := commit(context.Background(), c); e != nil {
			t.Fatal(e)
		}
	}
	barrier := func(context.Context) (uint64, error) { return root.AppliedIndex(), nil }
	now := time.Now().Truncate(time.Second)
	admin, a, _ := auth.GenerateAccess("admin", "", now.Add(-time.Hour), 2*time.Hour)
	must(rootstate.Command{RequestID: "admin", Type: rootstate.CreateAccessCredential, AccessCredential: &a})
	tenants := &management.Tenants{Root: root, Commit: commit, Barrier: barrier}
	ops := &management.Operations{Root: root, Commit: commit, Barrier: barrier}
	for _, id := range []string{"z-first", "a-second", "m-third"} {
		must(rootstate.Command{RequestID: id, Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: id, Name: id}})
		must(rootstate.Command{RequestID: "repo-" + id, Type: rootstate.CreateRepository, Repository: &repository.Repository{Name: id, Type: repository.TypeFile, Path: "/backup/" + id}})
	}
	before, _, _ := root.Tenant("z-first")
	changed := before
	changed.Name = "renamed"
	changed.CreatedIndex = 99999
	must(rootstate.Command{RequestID: "rename", Type: rootstate.UpdateTenant, Tenant: &changed})
	after, _, _ := root.Tenant("z-first")
	if after.CreatedIndex != before.CreatedIndex {
		t.Fatal("tenant update changed creation order")
	}
	r, _, _ := root.Repository("z-first")
	originalIndex := r.CreatedIndex
	r.Path = "/changed"
	r.CreatedIndex = 99999
	must(rootstate.Command{RequestID: "repo-update", Type: rootstate.UpdateRepository, Repository: &r})
	must(rootstate.Command{RequestID: "repo-default", Type: rootstate.SetDefaultRepository, RepositoryDefault: &rootstate.RepositoryDefault{Name: r.Name}})
	repos, e := ops.Repositories(context.Background())
	if e != nil || len(repos) != 3 || repos[0].Name != "m-third" || repos[1].Name != "a-second" || repos[2].CreatedIndex != originalIndex {
		t.Fatalf("repository order: %+v %v", repos, e)
	}
	// Opposite lexical order, identical issuance second, mixed credential kinds.
	_, c1, _ := auth.GenerateAccess("monitor", "", now, time.Hour)
	c1.CredentialID = "cred_" + strings.Repeat("0", 32)
	must(rootstate.Command{RequestID: "cred-1", Type: rootstate.CreateAccessCredential, AccessCredential: &c1})
	_, c2, _ := auth.Generate("z-first", now, time.Hour)
	c2.CredentialID = "cred_" + strings.Repeat("f", 32)
	must(rootstate.Command{RequestID: "cred-2", Type: rootstate.CreateTenantCredential, TenantCredential: &c2})
	must(rootstate.Command{RequestID: "revoke", Type: rootstate.RevokeAccessCredential, AccessRevocation: &rootstate.TenantCredentialRevocation{CredentialID: c1.CredentialID}})
	_, c3, _ := auth.Generate("z-first", now, time.Hour)
	c3.CredentialID = "cred_" + strings.Repeat("1", 32)
	must(rootstate.Command{RequestID: "cred-3", Type: rootstate.CreateTenantCredential, TenantCredential: &c3})
	detail, e := tenants.Get(context.Background(), "z-first")
	if e != nil || detail.Credentials[0].CredentialID != c3.CredentialID {
		t.Fatal("tenant credential order", e)
	}
	mux := httpHandlerForOrder(tenants, ops)
	g := NewGateway(GatewayOptions{Admin: mux, Access: auth.AccessVerifier{Barrier: accessBarrier{root}, Catalog: root}, Commit: commit})
	get := func(path string, out any) {
		t.Helper()
		req := httptest.NewRequest("GET", "https://test/api/v1/admin/"+path, nil)
		req.Header.Set("Authorization", "Bearer "+admin)
		w := httptest.NewRecorder()
		g.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
		var envelope struct{ Data json.RawMessage }
		if e := json.Unmarshal(w.Body.Bytes(), &envelope); e != nil {
			t.Fatal(e)
		}
		if e := json.Unmarshal(envelope.Data, out); e != nil {
			t.Fatal(e)
		}
	}
	var first v1.TenantPage
	get("tenants?limit=1", &first)
	if first.Items[0].TenantID != "m-third" {
		t.Fatal(first)
	}
	must(rootstate.Command{RequestID: "new-between-pages", Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: "newest"}})
	var second v1.TenantPage
	get("tenants?limit=1&cursor="+url.QueryEscape(first.NextCursor), &second)
	if len(second.Items) != 1 || second.Items[0].TenantID != "a-second" {
		t.Fatal(second)
	}
	var third v1.TenantPage
	get("tenants?limit=1&cursor="+url.QueryEscape(second.NextCursor), &third)
	if third.Items[0].TenantID != "z-first" || third.NextCursor != "" {
		t.Fatal(third)
	}
	get("tenants?limit=1", &first)
	if first.Items[0].TenantID != "newest" {
		t.Fatal(first)
	}
	want := []string{c3.CredentialID, c2.CredentialID, a.CredentialID, c1.CredentialID}
	cursor := ""
	for _, id := range want {
		var p v1.CredentialPage
		get("credentials?limit=1"+cursor, &p)
		if len(p.Items) != 1 || p.Items[0].CredentialID != id {
			t.Fatalf("credential order: want %s, got %+v", id, p.Items)
		}
		cursor = "&cursor=" + url.QueryEscape(p.NextCursor)
	}
	var filtered v1.CredentialPage
	get("credentials?kind=tenant&limit=1", &filtered)
	if filtered.Items[0].CredentialID != c3.CredentialID {
		t.Fatal(filtered)
	}
}

func httpHandlerForOrder(tenants *management.Tenants, ops *management.Operations) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/api/v1/admin/tenants", &TenantManagement{Service: tenants})
	mux.Handle("/api/v1/admin/credentials", &Operations{Service: ops})
	return mux
}
