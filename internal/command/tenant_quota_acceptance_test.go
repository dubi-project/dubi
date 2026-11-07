package command

import (
	"context"
	"errors"
	"fmt"
	stateengine "github.com/dubi-project/dubi/internal/engine/state"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/tenantquota"
	"io"
	"path/filepath"
	"testing"
	"time"
)

func TestTenantSoftQuotaAccounting(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store := filepath.Join(t.TempDir(), "node")
	if err := runInit([]string{"--store", store}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	process, err := openServer([]string{"--store", store, "--listen", "127.0.0.1:0", "--api-listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	_, ready := startJoinTestServer(t, process)
	ready()
	for i := 0; i < 2; i++ {
		_, ticket := joinTestTicket(t, store, process.LocalAddress())
		p, err := openJoiningServerWithMux(ctx, []string{"--store", filepath.Join(t.TempDir(), fmt.Sprint("node", i)), "--listen", "127.0.0.1:0", "--api-listen", "127.0.0.1:0", "--ticket-file", ticket}, nil, io.Discard, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, ready := startJoinTestServer(t, p)
		ready()
	}
	for {
		progress, ok, e := process.Runtime.Root.RegionProgress(regiongroups.GroupID("default"))
		if e == nil && ok && progress.Eligible && progress.Pending == nil && len(progress.Current.Nodes) == 3 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	commit := func(ctx context.Context, c rootstate.Command) error {
		b, err := rootstate.EncodeCommand(c)
		if err != nil {
			return err
		}
		if err := process.Runtime.Host.Submit(raft.RootGroupID, b); err != nil {
			return err
		}
		for {
			if _, err := process.Runtime.ReadRootIndex(ctx); err != nil {
				return err
			}
			r, ok, err := process.Runtime.Root.ResultForCommand(b)
			if err != nil {
				return err
			}
			if ok {
				if r.Error != "" {
					return errors.New(r.Error)
				}
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	if err := commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, RequestID: "quota-register", Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: "quota", Accounting: true, Modules: []string{"config"}}}); err != nil {
		t.Fatal(err)
	}
	quota := process.Runtime.TenantQuota
	engine, err := stateengine.New(process.EngineData, "config")
	if err != nil {
		t.Fatal(err)
	}
	client, _ := engine.ForTenant("quota")

	waitUsage := func(want uint64) {
		t.Helper()
		for {
			u, e := process.Runtime.Root.TenantUsage("quota")
			if e == nil && u.Used == want && u.Reserved == 0 {
				if e = quota.Refresh(); e != nil {
					t.Fatal(e)
				}
				return
			}
			select {
			case <-ctx.Done():
				t.Fatalf("usage want=%d got=%+v err=%v", want, u, e)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	first, err := client.Put(ctx, []byte("a"), []byte("old"), "old-request", stateengine.Condition{})
	if err != nil {
		t.Fatal("quota write", err)
	}
	if _, err := client.Put(ctx, []byte("a"), []byte("longer"), "new-request", stateengine.Condition{}); err != nil {
		t.Fatal(err)
	}
	waitUsage(7)
	tenant, _, _ := process.Runtime.Root.Tenant("quota")
	if u, _ := process.Runtime.Root.TenantUsage("quota"); u.Used != 7 || u.Reserved != 0 {
		t.Fatal(u)
	}
	// A confirmed historical operation must not reset either data or accounting.
	replay, err := client.Put(ctx, []byte("a"), []byte("old"), "old-request", stateengine.Condition{})
	if err != nil || replay.Version != first.Version {
		t.Fatal("historical replay", replay, err)
	}
	if u, _ := process.Runtime.Root.TenantUsage("quota"); u.Used != 7 {
		t.Fatal("replay changed usage", u)
	}
	tenant.StorageLimit = 10
	tenant.Name = "Quota acceptance"
	tenant.Modules = []string{"config"}
	if err := commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, RequestID: "set-limit", Type: rootstate.UpdateTenant, Tenant: &tenant}); err != nil {
		t.Fatal(err)
	}
	if err := quota.Refresh(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Put(ctx, []byte("b"), []byte("1234"), "too-big", stateengine.Condition{}); !errors.Is(err, tenantquota.ErrExceeded) {
		t.Fatal("limit not enforced", err)
	}
	if _, err := client.Delete(ctx, []byte("a"), "delete", stateengine.Condition{}); err != nil {
		t.Fatal(err)
	}
	waitUsage(0)
	if u, _ := process.Runtime.Root.TenantUsage("quota"); u.Used != 0 {
		t.Fatal(u)
	}
}
