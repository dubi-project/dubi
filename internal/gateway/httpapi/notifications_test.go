package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/management"
)

type noticeSource struct {
	offline   bool
	available uint64
}

func (s *noticeSource) Directory(context.Context) (management.Directory, error) {
	return management.Directory{Members: []management.Member{{NodeID: "node_000000000001", Name: "node-one", RegionID: "a", RaftID: 1}}}, nil
}
func (s *noticeSource) Observe(context.Context, management.Member) (management.Observation, error) {
	if s.offline {
		return management.Observation{}, errors.New("secret internal dial failure")
	}
	return management.Observation{Health: v1.OperationalHealth{Status: "healthy"}, ObservedUnix: time.Now().Unix(), Complete: true, Volumes: []v1.ManagedVolume{{VolumeID: "volume_000000000001", Health: "healthy", State: "active", Capacity: &v1.FilesystemCapacity{FilesystemTotalBytes: 100, FilesystemAvailableBytes: s.available}}}}, nil
}
func TestNotificationsAutomaticRulesAndAPI(t *testing.T) {
	root, e := rootstate.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
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
	admin, a, _ := auth.GenerateAccess("admin", "", time.Now(), 30*24*time.Hour)
	other, b, _ := auth.GenerateAccess("admin", "", time.Now(), 30*24*time.Hour)
	monitor, m, _ := auth.GenerateAccess("monitor", "", time.Now(), time.Hour)
	for i, c := range []rootstate.AccessCredential{a, b, m} {
		if e = commit(context.Background(), rootstate.Command{Version: rootstate.FormatVersion, RequestID: fmt.Sprint("auth-", i), Type: rootstate.CreateAccessCredential, AccessCredential: &c}); e != nil {
			t.Fatal(e)
		}
	}
	source := &noticeSource{available: 10}
	service := &management.Notifications{Root: root, Barrier: func(context.Context) (uint64, error) { return root.AppliedIndex(), nil }, Commit: commit, Directory: management.New(source)}
	scan := func() {
		t.Helper()
		service.Directory = management.New(source)
		if e := service.Scan(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	scan()
	feed, _ := root.NotificationFeed()
	if len(feed.Items) != 2 {
		t.Fatal("capacity and expiry messages missing", len(feed.Items))
	}
	unchanged := root.AppliedIndex()
	scan()
	if root.AppliedIndex() != unchanged {
		t.Fatal("stable observations wrote consensus")
	}
	source.available = 17
	scan()
	feed, _ = root.NotificationFeed()
	if len(feed.Items) != 2 {
		t.Fatal("capacity oscillation spam")
	}
	source.offline = true
	scan()
	feed, _ = root.NotificationFeed()
	if len(feed.Items) != 2 {
		t.Fatal("single failure alarmed")
	}
	scan()
	feed, _ = root.NotificationFeed()
	if len(feed.Items) != 3 {
		t.Fatal("consecutive failure not alarmed")
	}
	source.offline = false
	source.available = 30
	scan()
	feed, _ = root.NotificationFeed()
	if len(feed.Items) != 5 {
		t.Fatal("recovery missing", len(feed.Items))
	}
	gateway := NewGateway(GatewayOptions{Admin: &Notifications{Service: service}, Access: auth.AccessVerifier{Barrier: accessBarrier{root}, Catalog: root}, Commit: commit})
	call := func(method, path, token string, body any, want int) json.RawMessage {
		t.Helper()
		b, _ := json.Marshal(body)
		r := httptest.NewRequest(method, "https://node.test/api/v1/admin/"+path, strings.NewReader(string(b)))
		r.Header.Set("Content-Type", "application/json")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		gateway.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "secret internal") || strings.Contains(w.Body.String(), a.TokenSHA256) {
			t.Fatal("secret leaked")
		}
		var v struct {
			Data json.RawMessage `json:"data"`
		}
		json.Unmarshal(w.Body.Bytes(), &v)
		return v.Data
	}
	call("GET", "notifications", "", nil, 401)
	call("GET", "notifications", monitor, nil, 403)
	var page struct {
		Items  []management.NotificationItem `json:"items"`
		Unread int                           `json:"unread"`
		Next   string                        `json:"next_cursor"`
	}
	json.Unmarshal(call("GET", "notifications?limit=1", admin, nil, 200), &page)
	if len(page.Items) != 1 || page.Unread != 5 || page.Next == "" {
		t.Fatal("invalid first page", page)
	}
	first := page.Items[0].ID
	mutation := map[string]any{"request_id": "mark-test", "id": first, "read": true}
	call("POST", "notifications/read", admin, mutation, 200)
	call("POST", "notifications/read", admin, mutation, 200)
	json.Unmarshal(call("GET", "notifications?read=false", admin, nil, 200), &page)
	if page.Unread != 4 || len(page.Items) != 4 {
		t.Fatal("read state not applied", page)
	}
	json.Unmarshal(call("GET", "notifications?read=false", other, nil, 200), &page)
	if page.Unread != 4 {
		t.Fatal("cluster read state not shared")
	}
	call("POST", "notifications/read", admin, map[string]any{"request_id": "spoof", "credential_id": b.CredentialID, "id": first, "read": true}, 400)
	call("GET", "notifications?level=invalid", admin, nil, 400)
	call("GET", "notifications/not-found", admin, nil, 404)
	call("POST", "notifications", admin, nil, 405)
	// Same-commit bursts must paginate without dropping messages whose IDs sort differently.
	feed, _ = root.NotificationFeed()
	obs := []rootstate.NotificationObservation{}
	for i := 0; i < 40; i++ {
		obs = append(obs, rootstate.NotificationObservation{Key: fmt.Sprint(i), State: "bad", Event: &rootstate.Notification{Kind: "node", Level: "error", Title: "burst", Target: "globe"}})
	}
	if e = commit(context.Background(), rootstate.Command{Version: rootstate.FormatVersion, RequestID: "burst", Type: rootstate.UpdateNotifications, NotificationUpdate: &rootstate.NotificationUpdate{Revision: feed.Revision, Scope: "node", ObservedUnix: time.Now().Unix(), Observations: obs}}); e != nil {
		t.Fatal(e)
	}
	seen := map[string]bool{}
	cursor := ""
	for {
		path := "notifications?limit=7"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		json.Unmarshal(call("GET", path, admin, nil, 200), &page)
		for _, n := range page.Items {
			if seen[n.ID] {
				t.Fatal("duplicate cursor row")
			}
			seen[n.ID] = true
		}
		cursor = page.Next
		if cursor == "" {
			break
		}
	}
	if len(seen) != 45 {
		t.Fatal("cursor skipped notifications", len(seen))
	}
}
