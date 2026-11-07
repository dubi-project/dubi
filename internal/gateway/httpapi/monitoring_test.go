package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
	"github.com/dubi-project/dubi/internal/platform/telemetry"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type monitorTestSource struct{}

func (monitorTestSource) Directory(context.Context) (management.Directory, error) {
	return management.Directory{Members: []management.Member{{NodeID: "node_00000000000000000000000000000001", RegionID: "a", CreatedIndex: 1}, {NodeID: "node_00000000000000000000000000000002", RegionID: "a", CreatedIndex: 2}, {NodeID: "node_00000000000000000000000000000003", RegionID: "b", CreatedIndex: 3}}}, nil
}
func (monitorTestSource) Observe(context.Context, management.Member) (management.Observation, error) {
	return management.Observation{}, nil
}
func (monitorTestSource) Probe(_ context.Context, m management.Member, end, seconds int64) (management.MonitorProbe, error) {
	if m.RegionID == "b" {
		return management.MonitorProbe{}, errors.New("private dial failure")
	}
	cpu := 12.
	rss := uint64(100)
	zero := uint64(0)
	h := telemetry.Histogram{}
	h[3] = 10
	s := telemetry.Sample{Unix: end, Seconds: 10, Read: h, CPU: &cpu, RSS: &rss, Ready: true}
	return management.MonitorProbe{Observation: management.Observation{ObservedUnix: end, Complete: true, Health: v1.OperationalHealth{Status: "healthy"}, Volumes: []v1.ManagedVolume{{VolumeID: "v", Health: "healthy"}}, Maintenance: []v1.MaintenanceActivity{{Kind: "geo_sync", Supported: true, Phase: "observed", Pending: &zero}}}, Telemetry: telemetry.View{NodeID: m.NodeID, StartedUnix: end - 100, Latest: &s, Points: []telemetry.Sample{s}}}, nil
}
func TestMonitoringAndDiagnosticAPI(t *testing.T) {
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	commit := func(_ context.Context, c rootstate.Command) error {
		b, e := rootstate.EncodeCommand(c)
		if e != nil {
			return e
		}
		if e = root.Apply(root.AppliedIndex()+1, b); e != nil {
			return e
		}
		r, _, e := root.ResultForCommand(b)
		if e == nil && r.Error != "" {
			return errors.New(r.Error)
		}
		return e
	}
	admin, a, _ := auth.GenerateAccess("admin", "", time.Now(), time.Hour)
	monitor, m, _ := auth.GenerateAccess("monitor", "", time.Now(), time.Hour)
	for i, c := range []rootstate.AccessCredential{a, m} {
		if e := commit(context.Background(), rootstate.Command{Version: rootstate.FormatVersion, RequestID: fmt.Sprint("auth-", i), Type: rootstate.CreateAccessCredential, AccessCredential: &c}); e != nil {
			t.Fatal(e)
		}
	}
	source := monitorTestSource{}
	service := management.NewMonitoring(management.New(source), source, root, commit)
	g := NewGateway(GatewayOptions{Admin: &Monitoring{Service: service}, Access: auth.AccessVerifier{Barrier: accessBarrier{root}, Catalog: root}, Commit: commit})
	call := func(method, path string, body any, token string, want int) json.RawMessage {
		t.Helper()
		b, _ := json.Marshal(body)
		r := httptest.NewRequest(method, "https://node.test/api/v1/admin/"+path, strings.NewReader(string(b)))
		r.Header.Set("Content-Type", "application/json")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "private dial") {
			t.Fatal("raw peer error leaked")
		}
		var out struct{ Data json.RawMessage }
		json.Unmarshal(w.Body.Bytes(), &out)
		return out.Data
	}
	call("GET", "monitoring", nil, "", 401)
	call("GET", "monitoring", nil, monitor, 403)
	for _, path := range []string{"monitoring?window=1", "monitoring?window=900&window=900", "monitoring?unknown=1", "diagnostics?status=healthy"} {
		call("GET", path, nil, admin, 400)
	}
	call("DELETE", "monitoring", nil, admin, 405)
	call("GET", "monitoring?node_id=gone", nil, admin, 404)
	var data v1.Monitoring
	json.Unmarshal(call("GET", "monitoring", nil, admin, 200), &data)
	if len(data.Nodes) != 3 || data.Summary.Complete || data.Nodes[0].Metrics.ReadRate != nil || data.Points[0].ObservedNodes != 2 || data.Points[0].ReadRate != 2 || *data.Points[0].ReadP95 != 8 {
		t.Fatalf("bad partial aggregation: %+v", data)
	}
	for _, id := range []string{"z-check", "a-check"} {
		var report rootstate.DiagnosticRecord
		in := map[string]string{"request_id": id}
		json.Unmarshal(call("POST", "diagnostics", in, admin, 200), &report)
		if report.CreatedIndex == 0 || len(report.Nodes) != 3 || report.Nodes[0].Checks[0].State != "unknown" {
			t.Fatal(report)
		}
		var retry rootstate.DiagnosticRecord
		json.Unmarshal(call("POST", "diagnostics", in, admin, 200), &retry)
		if retry.CreatedIndex != report.CreatedIndex {
			t.Fatal("retry duplicated")
		}
	}
	call("POST", "diagnostics", map[string]string{"request_id": "a-check", "region_id": "a"}, admin, 409)
	var page struct {
		Items      []diagnosticSummary
		NextCursor string `json:"next_cursor"`
	}
	json.Unmarshal(call("GET", "diagnostics?limit=1", nil, admin, 200), &page)
	if len(page.Items) != 1 || page.Items[0].ID != "a-check" || page.Items[0].State != "unknown" {
		t.Fatal(page)
	}
	cursor := url.QueryEscape(page.NextCursor)
	call("GET", "diagnostics?region=a&cursor="+cursor, nil, admin, 400)
	json.Unmarshal(call("GET", "diagnostics?limit=1&cursor="+cursor, nil, admin, 200), &page)
	if page.Items[0].ID != "z-check" {
		t.Fatal(page)
	}
	call("GET", "diagnostics/a-check", nil, admin, 200)
	call("GET", "diagnostics/absent", nil, admin, 404)
	// Cookie mutation protection belongs to Gateway, not only the UI.
	r := httptest.NewRequest("POST", "https://node.test/api/v1/auth/login", strings.NewReader(`{"token":"`+admin+`"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Origin", "https://node.test")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	r = httptest.NewRequest(http.MethodPost, "https://node.test/api/v1/admin/diagnostics", strings.NewReader(`{"request_id":"csrf"}`))
	r.Header.Set("Content-Type", "application/json")
	for _, c := range w.Result().Cookies() {
		r.AddCookie(c)
	}
	w = httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("missing CSRF accepted", w.Code)
	}
}
