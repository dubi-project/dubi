package runtimecontract

import (
	"context"
	"errors"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

type bindingBackend struct {
	writes   []MutationRequest
	reads    []ReadRequest
	prepared []rootstate.Keyspace
	fail     error
}

func (b *bindingBackend) Resolve(context.Context, rootstate.Keyspace, []byte) (router.Target, error) {
	panic("Module must not resolve a route")
}
func (b *bindingBackend) Mutate(_ context.Context, r MutationRequest) (MutationResult, error) {
	b.writes = append(b.writes, r)
	return MutationResult{Applied: true}, nil
}
func (b *bindingBackend) Read(_ context.Context, r ReadRequest) (ReadResult, error) {
	b.reads = append(b.reads, r)
	return ReadResult{}, nil
}
func (b *bindingBackend) EnsureRange(_ context.Context, s rootstate.Keyspace) error {
	b.prepared = append(b.prepared, s)
	return b.fail
}
func TestBoundIdentityIsolationAndReadDoesNotInitialize(t *testing.T) {
	backend := &bindingBackend{}
	seen := map[string]bool{}
	for _, s := range []rootstate.Keyspace{
		{TenantID: "a", ModuleID: "config", EngineID: "state"},
		{TenantID: "a", ModuleID: "session", EngineID: "state"},
		{TenantID: "b", ModuleID: "config", EngineID: "state"},
		{TenantID: "a", ModuleID: "config", EngineID: "index"},
	} {
		binding, err := Bind(backend, backend, s.ModuleID, s.EngineID)
		if err != nil {
			t.Fatal(err)
		}
		client, err := binding.ForTenant(s.TenantID)
		if err != nil {
			t.Fatal(err)
		}
		before := len(backend.prepared)
		if _, err = client.Read(context.Background(), BoundRead{Key: []byte("same-key"), Payload: []byte("read")}); err != nil {
			t.Fatal(err)
		}
		if len(backend.prepared) != before || backend.reads[len(backend.reads)-1].Keyspace != s {
			t.Fatal("read allocated storage or changed identity")
		}
		result, err := client.Mutate(context.Background(), BoundMutation{RequestID: "same-request", Key: []byte("same-key"), Payload: []byte("write")})
		if err != nil || result.RequestID != "same-request" {
			t.Fatal("caller request identity changed", result, err)
		}
		id := backend.writes[len(backend.writes)-1].RequestID
		if seen[id] {
			t.Fatal("request deduplication crossed address scopes")
		}
		seen[id] = true
		if _, err = client.Mutate(context.Background(), BoundMutation{RequestID: "same-request", Key: []byte("same-key"), Payload: []byte("write")}); err != nil || backend.writes[len(backend.writes)-1].RequestID != id {
			t.Fatal("retry changed request identity", err)
		}
		if backend.writes[len(backend.writes)-1].Keyspace != s || backend.prepared[len(backend.prepared)-1] != s {
			t.Fatal("identity escaped binding")
		}
	}
}
func TestBoundInitializationFailureAndCancellationDoNotWrite(t *testing.T) {
	backend := &bindingBackend{fail: errors.New("not ready")}
	binding, _ := Bind(backend, backend, "config", "state")
	client, _ := binding.ForTenant("a")
	if _, err := client.Mutate(context.Background(), BoundMutation{RequestID: "write", Key: []byte("key"), Payload: []byte("value")}); err == nil || len(backend.writes) != 0 {
		t.Fatal("write escaped failed initialization")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := len(backend.prepared)
	if _, err := client.Mutate(ctx, BoundMutation{RequestID: "write", Key: []byte("key"), Payload: []byte("value")}); !errors.Is(err, context.Canceled) || len(backend.prepared) != before {
		t.Fatal("canceled call initialized storage", err)
	}
	if _, err := binding.ForTenant("a/other"); err == nil {
		t.Fatal("accepted ambiguous Tenant encoding")
	}
	if _, err := Bind(backend, backend, "config/session", "state"); err == nil {
		t.Fatal("accepted ambiguous Module encoding")
	}
}
