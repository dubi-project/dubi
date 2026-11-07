package auth

import (
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"testing"
)

func TestManagementPrincipalTenantAndModuleIsolation(t *testing.T) {
	p, e := ManagementPrincipal(rootstate.Tenant{TenantID: "a", Modules: []string{"config"}}, "verified-admin")
	if e != nil {
		t.Fatal(e)
	}
	for _, tc := range []struct {
		tenant, module string
		allow          bool
	}{{"a", "config", true}, {"b", "config", false}, {"a", "session", false}} {
		e := p.Authorize(rootstate.Keyspace{TenantID: tc.tenant, ModuleID: tc.module, EngineID: "state"})
		if (e == nil) != tc.allow {
			t.Fatal(tc, e)
		}
	}
	if _, e = ManagementPrincipal(rootstate.Tenant{TenantID: "a"}, ""); e == nil {
		t.Fatal("empty verified identity")
	}
}
