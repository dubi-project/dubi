package runtimecontract

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
)

type testResolver struct {
	target router.Target
	err    error
}

func (resolver testResolver) ResolveTarget(_ context.Context, _ rootstate.Keyspace, _ []byte) (router.Target, error) {
	return resolver.target, resolver.err
}

type testExecutor struct {
	mutation Mutation
	read     Read
}

func (executor *testExecutor) Mutate(_ context.Context, mutation Mutation) (MutationResult, error) {
	executor.mutation = mutation
	return MutationResult{Applied: true, Value: []byte("mutated"), AppliedIndex: 11}, nil
}

func (executor *testExecutor) Read(_ context.Context, read Read) (ReadResult, error) {
	executor.read = read
	return ReadResult{Value: []byte("read"), ConfirmedIndex: 12}, nil
}

func testRoute() (rootstate.Keyspace, router.Target) {
	keyspace := rootstate.Keyspace{TenantID: "tenant", EngineID: "state", DatasetID: "items"}
	return keyspace, router.Target{Keyspace: keyspace, RangeID: "range-1", OwnerGroupID: "data-1", Epoch: 3, RoutingKey: []byte("key")}
}

func TestDispatcherRoutesOpaqueEngineOperations(t *testing.T) {
	keyspace, target := testRoute()
	executor := new(testExecutor)
	dispatcher, err := New(testResolver{target: target}, executor)
	if err != nil {
		t.Fatal(err)
	}

	mutation := MutationRequest{RequestID: "write-1", Keyspace: keyspace, RoutingKey: []byte("key"), Payload: []byte("set")}
	result, err := dispatcher.Mutate(context.Background(), mutation)
	if err != nil || !result.Applied || result.RequestID != mutation.RequestID || result.AppliedIndex != 11 {
		t.Fatalf("mutation result = %+v, err %v", result, err)
	}
	if executor.mutation.RequestID != mutation.RequestID || !reflect.DeepEqual(executor.mutation.Target, target) || string(executor.mutation.Payload) != "set" {
		t.Fatalf("mutation delivered = %+v", executor.mutation)
	}

	read, err := dispatcher.Read(context.Background(), ReadRequest{Keyspace: keyspace, RoutingKey: []byte("key"), Payload: []byte("get")})
	if err != nil || string(read.Value) != "read" || read.ConfirmedIndex != 12 {
		t.Fatalf("read result = %+v, err %v", read, err)
	}
	if !reflect.DeepEqual(executor.read.Target, target) || string(executor.read.Payload) != "get" {
		t.Fatalf("read delivered = %+v", executor.read)
	}
}

func TestDispatcherDetachesRequestBuffers(t *testing.T) {
	keyspace, target := testRoute()
	executor := new(testExecutor)
	dispatcher, err := New(testResolver{target: target}, executor)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("key")
	payload := []byte("set")
	if _, err := dispatcher.Mutate(context.Background(), MutationRequest{RequestID: "write-copy", Keyspace: keyspace, RoutingKey: key, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	key[0], payload[0] = 'X', 'X'
	if !bytes.Equal(executor.mutation.Target.RoutingKey, []byte("key")) || !bytes.Equal(executor.mutation.Payload, []byte("set")) {
		t.Fatalf("executor observed caller-owned buffers: %+v", executor.mutation)
	}
}

func TestDispatcherRejectsInvalidOrMismatchedRoutes(t *testing.T) {
	keyspace, target := testRoute()
	executor := new(testExecutor)
	bad := target
	bad.Epoch = 0
	dispatcher, err := New(testResolver{target: bad}, executor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Mutate(context.Background(), MutationRequest{RequestID: "write-bad-route", Keyspace: keyspace, RoutingKey: []byte("key"), Payload: []byte("set")}); !errors.Is(err, ErrInvalidRoute) {
		t.Fatalf("invalid route error = %v", err)
	}
	if executor.mutation.Payload != nil {
		t.Fatal("executor ran for invalid route")
	}
	if _, err := dispatcher.Read(context.Background(), ReadRequest{Keyspace: keyspace, RoutingKey: []byte("key"), Payload: nil}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid read error = %v", err)
	}
	if _, err := New(nil, executor); err == nil {
		t.Fatal("nil resolver accepted")
	}
}

func TestDispatcherPropagatesResolverError(t *testing.T) {
	keyspace, _ := testRoute()
	want := errors.New("route unavailable")
	dispatcher, err := New(testResolver{err: want}, new(testExecutor))
	if err != nil {
		t.Fatal(err)
	}
	_, err = dispatcher.Read(context.Background(), ReadRequest{Keyspace: keyspace, RoutingKey: []byte("key"), Payload: []byte("get")})
	if !errors.Is(err, want) {
		t.Fatalf("resolver error = %v", err)
	}
}
