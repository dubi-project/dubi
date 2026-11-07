package state

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStateListOrderIsolationPaginationAndRestore(t *testing.T) {
	s, err := enginestate.Open(filepath.Join(t.TempDir(), "state"), "data-1", Handler{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	activate := func(tenant, module, id string) router.Target {
		r := rootstate.Range{RangeID: id, TenantID: tenant, ModuleID: module, EngineID: EngineID, OwnerGroupID: "data-1", Epoch: 1, State: rootstate.RangeStateActive}
		if err := s.ApplyRange(s.AppliedIndex()+1, "activate-"+id, r); err != nil {
			t.Fatal(err)
		}
		return router.Target{Keyspace: r.Keyspace(), RangeID: id, OwnerGroupID: "data-1", Epoch: 1, RoutingKey: []byte("v/")}
	}
	a := activate("tenant-a", "config", "a")
	b := activate("tenant-b", "config", "b")
	c := activate("tenant-a", "session", "c")
	apply := func(target router.Target, o operation) {
		target.RoutingKey = dataKey(o.Key)
		d, e := records.Describe(target.Keyspace, target.OwnerGroupID, target.RoutingKey, o.body())
		if e != nil {
			t.Fatal(e)
		}
		payload, _ := json.Marshal(publication{Operation: o, Descriptor: d})
		id, _ := target.Keyspace.BindRequestID(o.RequestID)
		if e = s.ApplyRouted(s.AppliedIndex()+1, id, target, payload); e != nil {
			t.Fatal(e)
		}
	}
	list := func(target router.Target, r ListRequest) ListPage {
		payload, _ := json.Marshal(query{Format: 1, List: &r})
		out, _, e := s.QueryRouted(context.Background(), barrier{s}, target, payload)
		if e != nil || out.Rejection != "" {
			t.Fatal(e, out)
		}
		var page ListPage
		if e = json.Unmarshal(out.Value, &page); e != nil {
			t.Fatal(e)
		}
		return page
	}
	keys := func(p ListPage) []string {
		out := []string{}
		for _, item := range p.Items {
			out = append(out, string(item.Key))
		}
		return out
	}
	one := operation{Format: 1, RequestID: "first", Key: []byte("first"), Value: []byte("one"), Condition: Condition{IfAbsent: true}}
	two := operation{Format: 1, RequestID: "second", Key: []byte("second"), Value: []byte("二"), Condition: Condition{IfAbsent: true}}
	apply(a, one)
	apply(a, two)
	apply(a, one)
	apply(b, one)
	apply(c, two)
	page := list(a, ListRequest{Limit: 1})
	if !reflect.DeepEqual(keys(page), []string{"second"}) || page.Next == nil || *page.Items[0].ValueBytes != 3 {
		t.Fatal(page)
	}
	last := list(a, ListRequest{Limit: 1, Position: *page.Next})
	if !reflect.DeepEqual(keys(last), []string{"first"}) || last.Next != nil {
		t.Fatal(last)
	}
	if !reflect.DeepEqual(keys(list(b, ListRequest{Limit: 10})), []string{"first"}) || !reflect.DeepEqual(keys(list(c, ListRequest{Limit: 10})), []string{"second"}) {
		t.Fatal("isolation failed")
	}
	update := one
	update.RequestID = "update"
	update.Condition = Condition{Version: one.version()}
	update.Value = []byte("changed")
	apply(a, update)
	if !reflect.DeepEqual(keys(list(a, ListRequest{Limit: 10})), []string{"second", "first"}) {
		t.Fatal("update reordered")
	}
	conflict := two
	conflict.RequestID = "conflict"
	apply(a, conflict)
	del := update
	del.RequestID = "delete"
	del.Delete = true
	del.Value = nil
	del.Condition = Condition{Version: update.version()}
	apply(a, del)
	if !reflect.DeepEqual(keys(list(a, ListRequest{Limit: 10})), []string{"second"}) {
		t.Fatal("delete remained")
	}
	one.RequestID = "recreate"
	apply(a, one)
	if !reflect.DeepEqual(keys(list(a, ListRequest{Limit: 10})), []string{"first", "second"}) {
		t.Fatal("recreate not newest")
	}
	if !reflect.DeepEqual(keys(list(a, ListRequest{Limit: 10, Search: "SEC"})), []string{"second"}) {
		t.Fatal("search")
	}
	for i := 0; i < 520; i++ {
		apply(a, operation{Format: 1, RequestID: fmt.Sprintf("bulk-%d", i), Key: []byte(fmt.Sprintf("bulk-%d", i)), Value: []byte("x")})
	}
	scanned := list(a, ListRequest{Limit: 10, Search: "second"})
	if len(scanned.Items) != 0 || scanned.Next == nil {
		t.Fatal("unbounded search", scanned)
	}
	scanned = list(a, ListRequest{Limit: 10, Search: "second", Position: *scanned.Next})
	if !reflect.DeepEqual(keys(scanned), []string{"second"}) {
		t.Fatal(scanned)
	}
	image, index, conf, e := s.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	restored, e := enginestate.Open(filepath.Join(t.TempDir(), "restore"), "data-1", Handler{})
	if e != nil {
		t.Fatal(e)
	}
	if e = restored.Restore(index, image, conf); e != nil {
		t.Fatal(e)
	}
	s.Close()
	s = restored
	got := list(a, ListRequest{Limit: 1})
	if string(got.Items[0].Key) != "bulk-519" || got.Items[0].CreatedOrder != 523 {
		t.Fatal("restore changed index", got)
	}
}
