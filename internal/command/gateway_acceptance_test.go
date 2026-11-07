package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	v1 "github.com/dubi-project/dubi/api/v1"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// Called by the four-node/two-region State integration fixture, including a
// gateway node that does not host the data Owner Group.
func verifyGatewayThroughRemoteEntry(t *testing.T, ctx context.Context, client *http.Client, stores, rpc, public []string, tenantToken string) {
	t.Helper()
	var out bytes.Buffer
	if err := runAccess(ctx, []string{"create", "--role", "admin", "--ttl", "1h", "--store", stores[3], "--server", rpc[3]}, &out, io.Discard); err != nil {
		t.Fatal("issue admin through remote entry", err)
	}
	var credential struct {
		Token string `json:"token"`
		ID    string `json:"credential_id"`
	}
	if err := json.Unmarshal(out.Bytes(), &credential); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]string{"token": credential.Token})
	r, err := http.NewRequestWithContext(ctx, "POST", "https://"+public[3]+"/api/v1/auth/login", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Origin", "https://"+public[3])
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	cookies := response.Cookies()
	if response.StatusCode != 200 || len(cookies) != 1 {
		t.Fatal("login through remote entry", response.StatusCode)
	}
	probe := func(index, want int) {
		t.Helper()
		r, err := http.NewRequestWithContext(ctx, "GET", "https://"+public[index]+"/api/v1/admin/gateway", nil)
		if err != nil {
			t.Fatal(err)
		}
		r.AddCookie(cookies[0])
		response, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != want {
			t.Fatal("cross-node session", index, response.StatusCode, want)
		}
	}
	for i := range public {
		probe(i, 200)
	}
	for i := range public {
		read := func(path, token string, want int) []byte {
			t.Helper()
			r, err := http.NewRequestWithContext(ctx, "GET", "https://"+public[i]+"/api/v1/admin/"+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set("Authorization", "Bearer "+token)
			res, err := client.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil || res.StatusCode != want {
				t.Fatalf("management node=%d path=%s status=%d body=%s err=%v", i, path, res.StatusCode, b, err)
			}
			return b
		}
		var nodes struct {
			Data v1.ManagementList[v1.ManagedNode] `json:"data"`
			Meta v1.ManagementMeta                 `json:"meta"`
		}
		if err := json.Unmarshal(read("nodes", credential.Token, 200), &nodes); err != nil {
			t.Fatal(err)
		}
		if len(nodes.Data.Items) != 4 || !nodes.Meta.Complete {
			t.Fatalf("management nodes=%+v meta=%+v", nodes.Data, nodes.Meta)
		}
		var regions struct {
			Data v1.ManagementList[v1.ManagedRegion] `json:"data"`
		}
		json.Unmarshal(read("regions", credential.Token, 200), &regions)
		if len(regions.Data.Items) != 2 {
			t.Fatalf("regions=%+v", regions.Data)
		}
		for _, path := range []string{"cluster", "regions/gateway/nodes", "maintenance"} {
			read(path, credential.Token, 200)
		}
		target := nodes.Data.Items[(i+1)%4].NodeID
		read("nodes/"+target, credential.Token, 200)
		read("nodes/"+target+"/maintenance", credential.Token, 200)
		var volumes struct {
			Data v1.ManagementList[v1.ManagedVolume] `json:"data"`
		}
		json.Unmarshal(read("nodes/"+target+"/volumes", credential.Token, 200), &volumes)
		if len(volumes.Data.Items) != 1 || volumes.Data.Items[0].Path == "" || volumes.Data.Items[0].Capacity == nil {
			t.Fatalf("remote volumes=%+v", volumes.Data)
		}
		read("nodes/"+target+"/volumes/"+volumes.Data.Items[0].VolumeID, credential.Token, 200)
		var firstPage struct {
			Data v1.ManagementList[v1.ManagedNode] `json:"data"`
		}
		json.Unmarshal(read("nodes?limit=1", credential.Token, 200), &firstPage)
		if firstPage.Data.NextCursor == "" {
			t.Fatal("missing cursor")
		}
		read("nodes?limit=1&cursor="+url.QueryEscape(firstPage.Data.NextCursor), credential.Token, 200)
		read("nodes?limit=201", credential.Token, 400)
		read("nodes/missing", credential.Token, 404)
		read("regions", tenantToken, 403)
		read("regions", "invalid", 401)
	}
	t.Log("Nine management routes verified from all four entries, including remote Volumes, two Regions, pagination and Tenant permission rejection")
	if err := runAccess(ctx, []string{"revoke", "--store", stores[1], "--server", rpc[1], credential.ID}, io.Discard, io.Discard); err != nil {
		t.Fatal("cross-node revocation", err)
	}
	for i := range public {
		probe(i, 401)
	}
	t.Log("Gateway management issuance/login through non-owner entry, cross-node session and parent revocation passed")
}

// At the end of the disposable fixture, stop the sole member of one Region.
// The member directory must retain it; health must not shrink the denominator.
func verifyManagementRegionUnavailable(t *testing.T, ctx context.Context, client *http.Client, store, rpc, public string, stopRegion func()) {
	t.Helper()
	var out bytes.Buffer
	if err := runAccess(ctx, []string{"create", "--role", "admin", "--ttl", "1h", "--store", store, "--server", rpc}, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	var c struct {
		Token string `json:"token"`
	}
	json.Unmarshal(out.Bytes(), &c)
	stopRegion()
	deadline := time.Now().Add(20 * time.Second)
	for {
		r, _ := http.NewRequestWithContext(ctx, "GET", "https://"+public+"/api/v1/admin/regions", nil)
		r.Header.Set("Authorization", "Bearer "+c.Token)
		res, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		var reply struct {
			Data v1.ManagementList[v1.ManagedRegion] `json:"data"`
			Meta v1.ManagementMeta                   `json:"meta"`
		}
		body, readErr := io.ReadAll(res.Body)
		err = errors.Join(readErr, json.Unmarshal(body, &reply))
		res.Body.Close()
		if err != nil || (res.StatusCode != 200 && res.StatusCode != 503) {
			t.Fatalf("region outage query: %d %v", res.StatusCode, err)
		}
		// A stop can temporarily interrupt Root confirmation during an election.
		// Authorization must fail closed until it recovers; retry only within this deadline.
		for _, region := range reply.Data.Items {
			if region.RegionID == "gateway" && region.Status == "unavailable" && region.NodeCount == 1 && !reply.Meta.Complete {
				t.Log("Entire Region offline remains listed with its member and incomplete observation")
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("region outage not observed: status=%d body=%s", res.StatusCode, body)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
