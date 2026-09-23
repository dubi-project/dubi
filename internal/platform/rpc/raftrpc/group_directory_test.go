package raftrpc

import (
	"reflect"
	"testing"
)

func TestStaticGroupRoutesKeepsLeaderHintWithinMembership(t *testing.T) {
	routes := NewStaticGroupRoutes()
	if err := routes.Update("data-1", []uint64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	routes.ObserveLeader("data-1", 2)
	if got := routes.Candidates("data-1"); !reflect.DeepEqual(got, []uint64{2, 1, 3}) {
		t.Fatalf("Leader-first candidates = %v", got)
	}
	routes.ObserveLeader("data-1", 9)
	if got := routes.Candidates("data-1"); !reflect.DeepEqual(got, []uint64{2, 1, 3}) {
		t.Fatalf("non-member Leader changed candidates = %v", got)
	}
	if err := routes.Update("data-1", []uint64{1, 3}); err != nil {
		t.Fatal(err)
	}
	if got := routes.Candidates("data-1"); !reflect.DeepEqual(got, []uint64{1, 3}) {
		t.Fatalf("removed Leader remained cached = %v", got)
	}
	if err := routes.Update("data-1", []uint64{1, 1}); err == nil {
		t.Fatal("duplicate Group member was accepted")
	}
}
