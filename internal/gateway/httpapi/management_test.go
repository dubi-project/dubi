package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/management"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

type managementTestSource struct {
	down          bool
	directoryDown bool
	volumeHealth  bool
}

func (s *managementTestSource) Directory(context.Context) (management.Directory, error) {
	if s.directoryDown {
		return management.Directory{}, errors.New("root unavailable")
	}
	return management.Directory{ClusterID: "cluster", Members: []management.Member{{NodeID: "b", RegionID: "east"}, {NodeID: "a", RegionID: "east"}, {NodeID: "c", RegionID: "west"}}}, nil
}
func (s *managementTestSource) Observe(_ context.Context, m management.Member) (management.Observation, error) {
	if s.down && m.RegionID == "west" {
		return management.Observation{}, errors.New("offline")
	}
	if s.volumeHealth {
		return management.Observation{ObservedUnix: time.Now().Unix(), Complete: true, Volumes: []v1.ManagedVolume{{VolumeID: "v1", Health: "healthy"}, {VolumeID: "v2", Health: "missing"}}}, nil
	}
	return management.Observation{ObservedUnix: time.Now().Unix(), Complete: true, Health: v1.OperationalHealth{Status: "healthy", Reasons: []v1.ObservationError{}}, Volumes: []v1.ManagedVolume{{VolumeID: "v1", Path: "/disk1"}, {VolumeID: "v2", Path: "/disk2"}}, Maintenance: []v1.MaintenanceActivity{{Kind: "rebalance", Supported: false, Phase: "unknown", Errors: []string{}}}}, nil
}

func TestVolumeStatusFilteringBeforePagination(t *testing.T) {
	h := NewManagement(management.New(&managementTestSource{volumeHealth: true}))
	w := adminRead(h, "volumes?region=east&status=missing&limit=1")
	var reply struct {
		Data v1.ManagementList[v1.ClusterVolume] `json:"data"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &reply) != nil || len(reply.Data.Items) != 1 || reply.Data.Items[0].NodeID != "a" || reply.Data.Items[0].VolumeID != "v2" || reply.Data.NextCursor == "" {
		t.Fatalf("incorrect filtered page: %s", w.Body.String())
	}
	cursor := url.QueryEscape(reply.Data.NextCursor)
	w = adminRead(h, "volumes?region=east&status=missing&limit=1&cursor="+cursor)
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &reply) != nil || len(reply.Data.Items) != 1 || reply.Data.Items[0].NodeID != "b" || reply.Data.NextCursor != "" {
		t.Fatalf("incorrect filtered continuation: %s", w.Body.String())
	}
	for path, code := range map[string]int{"volumes?status=healthy&cursor=" + cursor: 400, "volumes?status=bogus": 400, "nodes?status=pending": 400, "volumes?status=pending": 200, "volumes?region=east&node_id=c": 200} {
		if got := adminRead(h, path); got.Code != code {
			t.Fatalf("%s: %d", path, got.Code)
		}
	}
}
func adminRead(h *Management, path string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "https://node/api/v1/admin/"+path, nil)
	r = r.WithContext(context.WithValue(r.Context(), adminContextKey{}, AdminPrincipal{CredentialID: "c", Role: "admin"}))
	w := httptest.NewRecorder()
	managementRequestID(w)
	h.ServeHTTP(w, r)
	return w
}
func TestManagementRoutesPaginationAndFailures(t *testing.T) {
	source := &managementTestSource{down: true}
	h := NewManagement(management.New(source))
	for _, path := range []string{"cluster", "regions", "regions/east/nodes", "nodes", "nodes/a", "nodes/a/volumes", "nodes/a/volumes/v1", "maintenance", "nodes/a/maintenance"} {
		w := adminRead(h, path)
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
		var v v1.ManagementResponse
		if json.Unmarshal(w.Body.Bytes(), &v) != nil || v.Meta.RequestID == "" {
			t.Fatal("missing envelope")
		}
	}
	first := adminRead(h, "regions/east/nodes?limit=1")
	var a struct {
		Data v1.ManagementList[v1.ManagedNode] `json:"data"`
	}
	json.Unmarshal(first.Body.Bytes(), &a)
	if len(a.Data.Items) != 1 || a.Data.Items[0].NodeID != "a" || a.Data.NextCursor == "" {
		t.Fatal(first.Body.String())
	}
	second := adminRead(h, "regions/east/nodes?limit=1&cursor="+url.QueryEscape(a.Data.NextCursor))
	var b struct {
		Data v1.ManagementList[v1.ManagedNode] `json:"data"`
	}
	json.Unmarshal(second.Body.Bytes(), &b)
	if second.Code != 200 || len(b.Data.Items) != 1 || b.Data.Items[0].NodeID != "b" || b.Data.NextCursor != "" {
		t.Fatal(second.Body.String())
	}
	for path, want := range map[string]int{"regions/west/nodes": 200, "nodes/c": 200, "nodes/c/volumes": 503, "nodes/c/maintenance": 503, "nodes/missing": 404, "regions/missing/nodes": 404, "nodes/a/volumes/missing": 404, "nodes?limit=201": 400, "nodes?limit=1&limit=2": 400, "nodes?status=bad": 400, "nodes?secret=value": 400, "nodes?x=%zz": 400, "nodes/a?region=east": 400, "regions/east/nodes?region=east": 400, "maintenance?kind=bad": 400, "nodes?cursor=" + url.QueryEscape(a.Data.NextCursor): 400} {
		if w := adminRead(h, path); w.Code != want {
			t.Fatalf("%s: got %d want %d", path, w.Code, want)
		}
	}
	w := adminRead(h, "regions")
	var regions struct {
		Data v1.ManagementList[v1.ManagedRegion] `json:"data"`
		Meta v1.ManagementMeta                   `json:"meta"`
	}
	json.Unmarshal(w.Body.Bytes(), &regions)
	if regions.Meta.Complete || len(regions.Data.Items) != 2 || regions.Data.Items[1].Status != "unavailable" {
		t.Fatal(w.Body.String())
	}
	source.directoryDown = true
	if w := adminRead(h, "nodes/missing"); w.Code != 503 {
		t.Fatalf("unconfirmed absence returned %d", w.Code)
	}
	r := httptest.NewRequest("GET", "https://node/api/v1/admin/nodes", nil)
	denied := httptest.NewRecorder()
	h.ServeHTTP(denied, r)
	if denied.Code != 403 {
		t.Fatal("untrusted direct invocation allowed")
	}
}

func TestClusterVolumePaginationAndPartialResults(t *testing.T) {
	h := NewManagement(management.New(&managementTestSource{down: true}))
	read := func(path string) (v1.ManagementList[v1.ClusterVolume], v1.ManagementMeta) {
		t.Helper()
		w := adminRead(h, path)
		var reply struct {
			Data v1.ManagementList[v1.ClusterVolume] `json:"data"`
			Meta v1.ManagementMeta                   `json:"meta"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &reply) != nil {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
		return reply.Data, reply.Meta
	}
	first, meta := read("volumes?limit=3")
	if len(first.Items) != 3 || first.NextCursor == "" || meta.Complete || len(meta.Errors) != 1 || meta.Errors[0].NodeID != "c" {
		t.Fatalf("invalid partial page: %+v %+v", first, meta)
	}
	if first.Items[0].NodeID != "a" || first.Items[0].VolumeID != "v1" || first.Items[1].VolumeID != "v2" || first.Items[2].NodeID != "b" || first.Items[2].VolumeID != "v1" || first.Items[0].Path != "/disk1" {
		t.Fatal("node/volume pair order or ownership lost")
	}
	last, _ := read("volumes?limit=3&cursor=" + url.QueryEscape(first.NextCursor))
	if len(last.Items) != 1 || last.Items[0].NodeID != "b" || last.Items[0].VolumeID != "v2" || last.NextCursor != "" {
		t.Fatal("volume cursor skipped or duplicated rows")
	}
	filtered, filteredMeta := read("volumes?region=east&node_id=b")
	if len(filtered.Items) != 2 || !filteredMeta.Complete || filtered.Items[0].RegionID != "east" {
		t.Fatal("filter scope incorrect")
	}
	empty, missing := read("volumes?region=west")
	if len(empty.Items) != 0 || missing.Complete {
		t.Fatal("unavailable node disguised as an empty complete inventory")
	}
	if w := adminRead(h, "volumes?region=east&cursor="+url.QueryEscape(first.NextCursor)); w.Code != 400 {
		t.Fatal("cursor reused across filters")
	}
	if w := adminRead(h, "volumes?limit=201"); w.Code != 400 {
		t.Fatal("unbounded page permitted")
	}
	r := httptest.NewRequest("GET", "https://node/api/v1/admin/volumes", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("volume listing bypassed admin authorization")
	}
}
