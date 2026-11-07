package tenantquota

import (
	"context"
	"errors"
	"fmt"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"testing"
)

func TestLocalSoftQuotaAdmissionDoesNotCommitAndIncludesPending(t *testing.T) {
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	apply := func(c rootstate.Command) {
		t.Helper()
		c.Version = rootstate.FormatVersion
		c.RequestID = fmt.Sprintf("c-%d", root.AppliedIndex()+1)
		b, e := rootstate.EncodeCommand(c)
		if e != nil {
			t.Fatal(e)
		}
		if e = root.Apply(root.AppliedIndex()+1, b); e != nil {
			t.Fatal(e)
		}
	}
	apply(rootstate.Command{Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: "t", Modules: []string{"config"}, Accounting: true, StorageLimit: 10}})
	q := &Service{Root: root, Commit: func(context.Context, rootstate.Command) error { t.Fatal("admission made a network commit"); return nil }}
	space := rootstate.Keyspace{TenantID: "t", ModuleID: "config", EngineID: "state"}
	a, err := q.Admit(space, []byte("a"), "a1", 0, 7)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = q.Admit(space, []byte("b"), "b1", 0, 4); !errors.Is(err, ErrExceeded) {
		t.Fatal(err)
	}
	if same, e := q.Admit(space, []byte("a"), "a1", 0, 7); e != nil || same != a {
		t.Fatal("retry reserved twice", e)
	}
	if _, e := q.Admit(space, []byte("a"), "delete", 7, 0); e != nil {
		t.Fatal("delete rejected", e)
	}
	q.Rejected(a)
	if _, err = q.Admit(space, []byte("b"), "b1", 0, 4); err != nil {
		t.Fatal(err)
	}
	tenant, _, _ := root.Tenant("t")
	tenant.StorageLimit = 3
	apply(rootstate.Command{Type: rootstate.UpdateTenant, Tenant: &tenant})
	if err = q.Refresh(); err != nil {
		t.Fatal(err)
	}
	if _, err = q.Admit(space, []byte("c"), "c1", 0, 1); !errors.Is(err, ErrExceeded) {
		t.Fatal("new limit not applied", err)
	}
}
