package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

type barrier struct{ s *enginestate.State }

func (b barrier) ReadIndex(context.Context, string) (uint64, error) { return b.s.AppliedIndex(), nil }

func TestStateCASDeletionRetryIsolationAndRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	s, err := enginestate.Open(dir, "data-1", Handler{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	activate := func(tenant, module, name string) router.Target {
		r := rootstate.Range{RangeID: name, TenantID: tenant, ModuleID: module, EngineID: EngineID, OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
		if err := s.ApplyRange(s.AppliedIndex()+1, "activate-"+name, r); err != nil {
			t.Fatal(err)
		}
		return router.Target{Keyspace: r.Keyspace(), RangeID: name, OwnerGroupID: "data-1", Epoch: 1, RoutingKey: dataKey([]byte("key"))}
	}
	a := activate("tenant-a", "config", "a")
	b := activate("tenant-b", "config", "b")
	c := activate("tenant-a", "session", "c")
	apply := func(target router.Target, o operation) receipt {
		d, err := records.Describe(target.Keyspace, target.OwnerGroupID, target.RoutingKey, o.body())
		if err != nil {
			t.Fatal(err)
		}
		payload, _ := json.Marshal(publication{Operation: o, Descriptor: d})
		id, _ := target.Keyspace.BindRequestID(o.RequestID)
		if err := s.ApplyRouted(s.AppliedIndex()+1, id, target, payload); err != nil {
			t.Fatal(err)
		}
		r, found, err := s.ResultForMutation(id, target, payload)
		if err != nil || !found || !r.Applied || r.Error != "" {
			t.Fatal(r, err)
		}
		var got receipt
		if err := decode(r.Value, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	read := func(target router.Target, o operation) queryResult {
		q := query{Format: 1, RequestID: o.RequestID, Version: o.version()}
		payload, _ := json.Marshal(q)
		out, _, err := s.QueryRouted(context.Background(), barrier{s}, target, payload)
		if err != nil || out.Rejection != "" {
			t.Fatal(out, err)
		}
		var got queryResult
		if err := decode(out.Value, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	first := operation{Format: 1, RequestID: "create", Key: []byte("key"), Value: []byte("one"), Condition: Condition{IfAbsent: true}}
	r := apply(a, first)
	if r.Version != first.version() || r.Confirmed {
		t.Fatal("publication alone became confirmed", r)
	}
	if retry := apply(a, first); !reflect.DeepEqual(retry, r) {
		t.Fatal("duplicate changed result", retry, r)
	}
	for _, target := range []router.Target{b, c} {
		if got := read(target, first); got.Head != nil || got.Receipt != nil {
			t.Fatal("scope leaked", got)
		}
		if got := apply(target, first); got.Code != "" {
			t.Fatal("same request ID not isolated", got)
		}
	}
	winner := operation{Format: 1, RequestID: "cas-one", Key: []byte("key"), Value: []byte("two"), Condition: Condition{Version: r.Version}}
	if got := apply(a, winner); got.Code != "" {
		t.Fatal(got)
	}
	loser := winner
	loser.RequestID = "cas-two"
	if got := apply(a, loser); got.Code != "conflict" {
		t.Fatal("two CAS operations succeeded", got)
	}
	deleted := operation{Format: 1, RequestID: "delete", Key: []byte("key"), Delete: true, Condition: Condition{Version: winner.version()}}
	if got := apply(a, deleted); got.Code != "" {
		t.Fatal(got)
	}
	recreated := first
	recreated.RequestID = "recreate"
	recreated.Value = []byte{}
	if got := apply(a, recreated); got.Code != "" || got.Version == r.Version {
		t.Fatal("ABA generation reused", got)
	}
	stale := winner
	stale.RequestID = "old-version"
	if got := apply(a, stale); got.Code != "conflict" {
		t.Fatal("ABA accepted", got)
	}
	// Snapshots preserve tombstone generations, head, receipts and references.
	image, index, conf, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = enginestate.Open(dir, "data-1", Handler{})
	if err != nil {
		t.Fatal(err)
	}
	if got := read(a, recreated); got.Head == nil || got.Head.Version != recreated.version() || got.Receipt.Confirmed {
		t.Fatal("reopen changed state", got)
	}
	restored, err := enginestate.Open(filepath.Join(t.TempDir(), "restored"), "data-1", Handler{})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := restored.Restore(index, image, conf); err != nil {
		t.Fatal(err)
	}
	original := s
	s = restored
	if got := read(a, loser); got.Receipt == nil || got.Receipt.Code != "conflict" {
		t.Fatal("restore lost rejection", got)
	}
	s = original
	changed := recreated
	changed.Value = []byte("changed")
	d, _ := records.Describe(a.Keyspace, a.OwnerGroupID, a.RoutingKey, changed.body())
	payload, _ := json.Marshal(publication{Operation: changed, Descriptor: d})
	id, _ := a.Keyspace.BindRequestID(changed.RequestID)
	if _, _, err := s.ResultForMutation(id, a, payload); !errors.Is(err, enginestate.ErrRequestIDReused) {
		t.Fatal("changed request accepted", err)
	}
}

func TestStateCodecBoundariesAndDeterminism(t *testing.T) {
	o := operation{Format: 1, RequestID: "r", Key: []byte("key"), Value: bytes.Repeat([]byte("x"), MaxValueBytes)}
	if err := o.validate(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(o.body(), o.body()) || !validVersion(o.version()) {
		t.Fatal("non deterministic encoding")
	}
	o.Value = append(o.Value, 'x')
	if o.validate() == nil {
		t.Fatal("oversize value accepted")
	}
	o.Value = nil
	o.Condition = Condition{IfAbsent: true, Version: "bad"}
	if o.validate() == nil {
		t.Fatal("invalid condition accepted")
	}
	if decode([]byte(`{"format":1,"unknown":2}`), new(query)) == nil || decode([]byte(`{"format":1} {}`), new(query)) == nil {
		t.Fatal("ambiguous wire payload accepted")
	}
}

func TestQuotaAbortFencesLatePublication(t *testing.T) {
	s, err := enginestate.Open(filepath.Join(t.TempDir(), "state"), "data-1", Handler{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := rootstate.Range{RangeID: "quota-range", TenantID: "t", ModuleID: "config", EngineID: EngineID, OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
	if err := s.ApplyRange(1, "activate", r); err != nil {
		t.Fatal(err)
	}
	target := router.Target{Keyspace: r.Keyspace(), RangeID: r.RangeID, OwnerGroupID: r.OwnerGroupID, Epoch: 1, RoutingKey: dataKey([]byte("key"))}
	o := operation{Format: 1, RequestID: "late", Key: []byte("key"), Value: []byte("should-not-appear")}
	payload, _ := json.Marshal(publication{Abort: &abortOperation{RequestID: o.RequestID, Version: o.version(), Key: o.Key}})
	if err := s.ApplyRouted(2, "cancel", target, payload); err != nil {
		t.Fatal(err)
	}
	d, _ := records.Describe(target.Keyspace, target.OwnerGroupID, target.RoutingKey, o.body())
	late, _ := json.Marshal(publication{Operation: o, Descriptor: d})
	if err := s.ApplyRouted(3, "late", target, late); err != nil {
		t.Fatal(err)
	}
	queryPayload, _ := json.Marshal(query{Format: 1, RequestID: o.RequestID, Version: o.version()})
	out, _, err := s.QueryRouted(context.Background(), barrier{s}, target, queryPayload)
	if err != nil {
		t.Fatal(err)
	}
	var got queryResult
	if decode(out.Value, &got) != nil || got.Head != nil || got.Receipt == nil || got.Receipt.Code != "cancelled" {
		t.Fatal("cancelled write published", got)
	}
}
