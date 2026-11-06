package command

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// Called by the four-node/two-region State integration fixture, including a
// gateway node that does not host the data Owner Group.
func verifyGatewayThroughRemoteEntry(t *testing.T, ctx context.Context, client *http.Client, stores, rpc, public []string) {
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
	if err := runAccess(ctx, []string{"revoke", "--store", stores[1], "--server", rpc[1], credential.ID}, io.Discard, io.Discard); err != nil {
		t.Fatal("cross-node revocation", err)
	}
	for i := range public {
		probe(i, 401)
	}
	t.Log("Gateway management issuance/login through non-owner entry, cross-node session and parent revocation passed")
}
