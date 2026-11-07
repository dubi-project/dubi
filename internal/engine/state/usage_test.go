package state

import (
	"context"
	"encoding/json"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"os"
	"path/filepath"
	"testing"
)

func TestUsagePublicationCursorCASAndSnapshot(t *testing.T) {
	s, err := enginestate.Open(filepath.Join(t.TempDir(), "engine"), "data", Handler{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := rootstate.Range{RangeID: "r", TenantID: "t", ModuleID: "config", EngineID: "state", OwnerGroupID: "data", Epoch: 1, State: rootstate.RangeStateActive}
	if err = s.ApplyRange(1, "activate", r); err != nil {
		t.Fatal(err)
	}
	target := router.Target{Keyspace: r.Keyspace(), RangeID: "r", OwnerGroupID: "data", Epoch: 1}
	apply := func(o operation) {
		t.Helper()
		target.RoutingKey = dataKey(o.Key)
		d, e := records.Describe(target.Keyspace, "data", target.RoutingKey, o.body())
		if e != nil {
			t.Fatal(e)
		}
		b, _ := json.Marshal(publication{Operation: o, Descriptor: d})
		id, _ := target.Keyspace.BindRequestID(o.RequestID)
		if e = s.ApplyRouted(s.AppliedIndex()+1, id, target, b); e != nil {
			t.Fatal(e)
		}
	}
	first := operation{Format: 1, RequestID: "first", Key: []byte("key"), Value: []byte("value")}
	apply(first)
	page, cursor, err := s.UsagePage(nil, 64)
	if err != nil || len(page) != 1 || page[0].Bytes != 8 {
		t.Fatal(page, err)
	}
	apply(first)
	page, _, err = s.UsagePage(cursor, 64)
	if err != nil || len(page) != 0 {
		t.Fatal("replay changed usage", page, err)
	}
	conflict := first
	conflict.RequestID = "conflict"
	conflict.Condition.IfAbsent = true
	apply(conflict)
	page, _, err = s.UsagePage(cursor, 64)
	if err != nil || len(page) != 0 {
		t.Fatal("CAS rejection changed usage", page, err)
	}
	del := operation{Format: 1, RequestID: "delete", Key: []byte("key"), Delete: true}
	apply(del)
	page, _, err = s.UsagePage(cursor, 64)
	if err != nil || len(page) != 1 || page[0].Bytes != 0 {
		t.Fatal("delete missing", page, err)
	}
	page, _, err = s.UsagePage(nil, 64)
	if err != nil || len(page) != 1 {
		t.Fatal("superseded index not removed", page, err)
	}
	image, index, conf, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := enginestate.Open(filepath.Join(t.TempDir(), "restore"), "data", Handler{})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err = restored.Restore(index, image, conf); err != nil {
		t.Fatal(err)
	}
	artifactDir := filepath.Join(t.TempDir(), "snapshots")
	if err := os.Mkdir(artifactDir, 0700); err != nil {
		t.Fatal(err)
	}
	artifacts, err := snapshot.OpenStore(artifactDir)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := s.PrepareSnapshotTo(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := prepared.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	streamed, err := enginestate.Open(filepath.Join(t.TempDir(), "stream-restore"), "data", Handler{})
	if err != nil {
		t.Fatal(err)
	}
	defer streamed.Close()
	if err := streamed.RestoreFrom(context.Background(), artifacts, prepared.Index, stream, prepared.Conf); err != nil {
		t.Fatal(err)
	}
	page, _, err = streamed.UsagePage(nil, 64)
	if err != nil || len(page) != 1 || page[0].Bytes != 0 {
		t.Fatal("snapshot lost accounting", page, err)
	}
}
