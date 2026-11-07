package enginedata

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/tenantquota"
)

func mutationQuota(t *testing.T) *tenantquota.Service {
	t.Helper()
	r, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	for i, id := range []string{"one", "two"} {
		c := rootstate.Command{Version: rootstate.FormatVersion, RequestID: fmt.Sprint("tenant-", i), Type: rootstate.RegisterTenant,
			Tenant: &rootstate.Tenant{TenantID: id, Modules: []string{"config", "session"}, Accounting: true, StorageLimit: 10}}
		b, err := rootstate.EncodeCommand(c)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Apply(uint64(i+1), b); err != nil {
			t.Fatal(err)
		}
		got, ok, err := r.Result(c.RequestID)
		if err != nil || !ok || got.Error != "" {
			t.Fatal(got, ok, err)
		}
	}
	return &tenantquota.Service{Root: r, Commit: func(context.Context, rootstate.Command) error {
		t.Fatal("logical mutation performed synchronous quota consensus")
		return nil
	}}
}

func mutationReport(space rootstate.Keyspace, id, key string, size uint64) *rootstate.UsageReport {
	return &rootstate.UsageReport{Space: space, Key: []byte(key), RequestID: id, Bytes: size, RangeID: "range-a", OwnerGroupID: "regional-west", Epoch: 1, Index: 10}
}

func logicalMutation(id, key string, size uint64, inspect func(context.Context) (MutationView, error)) Mutation {
	return Mutation{Write: BoundWrite{RequestID: id, Key: []byte(key)}, Value: enginestate.LogicalValue{Key: []byte(key), Bytes: size}, Inspect: inspect}
}

func TestRuntimeMutationSharesQuotaAcrossBindings(t *testing.T) {
	q := mutationQuota(t)
	space := rootstate.Keyspace{TenantID: "one", ModuleID: "config", EngineID: "state"}
	empty := func(context.Context) (MutationView, error) { return MutationView{}, nil }
	writes := 0
	write := func(context.Context, BoundWrite) (WriteResult, error) {
		writes++
		return WriteResult{State: WriteLocalReady, Local: recordwrite.LocalWrite{Reference: enginestate.RecordReference{Usage: mutationReport(space, "same", "key", 6)}}}, nil
	}
	m := logicalMutation("same", "key", 6, empty)
	if _, err := runMutation(context.Background(), space, q, m, write); err != nil {
		t.Fatal(err)
	}
	space.ModuleID, space.EngineID = "session", "another-engine"
	if _, err := runMutation(context.Background(), space, q, m, write); !errors.Is(err, tenantquota.ErrExceeded) || writes != 1 {
		t.Fatal("another binding bypassed shared Tenant limit", writes, err)
	}
	space.TenantID = "two"
	if _, err := runMutation(context.Background(), space, q, m, write); err != nil || writes != 2 {
		t.Fatal("different Tenant was charged", writes, err)
	}
}

func TestRuntimeMutationUnknownReplayAndRejection(t *testing.T) {
	q := mutationQuota(t)
	space := rootstate.Keyspace{TenantID: "one", ModuleID: "config", EngineID: "state"}
	inspect := func(context.Context) (MutationView, error) { return MutationView{}, nil }
	m := logicalMutation("unknown", "key", 10, inspect)
	lost := func(context.Context, BoundWrite) (WriteResult, error) {
		return WriteResult{State: WriteUnknown}, context.DeadlineExceeded
	}
	if _, err := runMutation(context.Background(), space, q, m, lost); !errors.Is(err, ErrMutationUnknown) {
		t.Fatal(err)
	}
	other := logicalMutation("other", "other", 1, inspect)
	if _, err := runMutation(context.Background(), space, q, other, lost); !errors.Is(err, tenantquota.ErrExceeded) {
		t.Fatal("uncertain write refunded before resolution", err)
	}
	m.Inspect = func(context.Context) (MutationView, error) {
		return MutationView{Rejected: true, Receipt: []byte("conflict")}, nil
	}
	never := func(context.Context, BoundWrite) (WriteResult, error) {
		t.Fatal("terminal replay wrote data")
		return WriteResult{}, nil
	}
	result, err := runMutation(context.Background(), space, q, m, never)
	if err != nil || !result.Replayed || string(result.Receipt) != "conflict" {
		t.Fatal(result, err)
	}
	if _, err := runMutation(context.Background(), space, q, other, lost); !errors.Is(err, ErrMutationUnknown) {
		t.Fatal("durable rejection did not release pending growth", err)
	}
	// A successful historical retry also bypasses admission at a full quota.
	other.Inspect = func(context.Context) (MutationView, error) {
		return MutationView{Confirmed: true, Receipt: []byte("done"), Published: mutationReport(space, "other", "other", 1)}, nil
	}
	if _, err := runMutation(context.Background(), space, q, other, never); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeMutationUsesCurrentSizeAndConfirmsPublishedRetry(t *testing.T) {
	q := mutationQuota(t)
	space := rootstate.Keyspace{TenantID: "one", ModuleID: "config", EngineID: "state"}
	if _, err := q.Admit(space, []byte("full"), "full", 0, 10); err != nil {
		t.Fatal(err)
	}
	for _, size := range []uint64{5, 0} {
		id := fmt.Sprint("shrink-", size)
		m := logicalMutation(id, "key", size, func(context.Context) (MutationView, error) {
			return MutationView{Exists: true, Current: mutationReport(space, "previous", "key", 10)}, nil
		})
		write := func(context.Context, BoundWrite) (WriteResult, error) {
			return WriteResult{State: WriteLocalReady, Local: recordwrite.LocalWrite{Reference: enginestate.RecordReference{Usage: mutationReport(space, id, "key", size)}}}, nil
		}
		if _, err := runMutation(context.Background(), space, q, m, write); err != nil {
			t.Fatal("shrink/delete blocked", err)
		}
	}
	r := mutationReport(space, "published", "key", 6)
	m := logicalMutation("published", "key", 6, func(context.Context) (MutationView, error) {
		return MutationView{Current: r, Published: r, Receipt: []byte("unconfirmed")}, nil
	})
	calls := 0
	_, err := runMutation(context.Background(), space, q, m, func(context.Context, BoundWrite) (WriteResult, error) {
		calls++
		return WriteResult{State: WriteLocalReady, Local: recordwrite.LocalWrite{Reference: enginestate.RecordReference{Usage: r}}}, nil
	})
	if err != nil || calls != 1 {
		t.Fatal("already published retry could not confirm at full quota", err, calls)
	}
}

func TestRawWriteCannotBypassRuntimeQuota(t *testing.T) {
	q := mutationQuota(t)
	s := &Service{runtime: &node.Runtime{Root: q.Root, TenantQuota: q}}
	r := WriteRequest{Keyspace: rootstate.Keyspace{TenantID: "one", ModuleID: "config", EngineID: "state"}}
	for _, write := range []func(context.Context, WriteRequest) (WriteResult, error){s.Write, s.WriteLocal} {
		if _, err := write(context.Background(), r); err == nil {
			t.Fatal("raw write bypassed logical accounting")
		}
	}
	r.Keyspace.TenantID = "not-yet-applied"
	if err := s.requireUnmetered(r.Keyspace); err == nil {
		t.Fatal("missing local Tenant directory treated as unmetered")
	}
}
