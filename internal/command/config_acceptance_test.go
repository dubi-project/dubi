package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	stateengine "github.com/dubi-project/dubi/internal/engine/state"
	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	platformconfig "github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/server"
)

// Run on a VPS: independent stores, ephemeral listeners and real Raft/Pack/TLS.
func TestStateConfigPublicAcceptance(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	type running struct {
		store, address string
		process        *server.Server
		stop           func()
	}
	nodes := make([]running, 4)
	nodes[0].store = filepath.Join(t.TempDir(), "first")
	if err := runInit([]string{"--store", nodes[0].store}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	first, err := openServer([]string{"--store", nodes[0].store, "--listen", "127.0.0.1:0", "--api-listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	nodes[0].process = first
	stop, ready := startJoinTestServer(t, first)
	nodes[0].stop = stop
	ready()
	nodes[0].address = first.LocalAddress()
	for i := 1; i < len(nodes); i++ {
		_, ticket := joinTestTicket(t, nodes[0].store, nodes[0].address)
		nodes[i].store = filepath.Join(t.TempDir(), fmt.Sprint("node", i))
		args := []string{"--store", nodes[i].store, "--listen", "127.0.0.1:0", "--api-listen", "127.0.0.1:0", "--ticket-file", ticket}
		if i == 3 {
			args = append(args, "--region", "gateway")
		}
		p, err := openJoiningServerWithMux(ctx, args, nil, io.Discard, nil)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i].process = p
		nodes[i].address = p.LocalAddress()
		stop, ready := startJoinTestServer(t, p)
		nodes[i].stop = stop
		ready()
	}
	for {
		p, ok, err := first.Runtime.Root.RegionProgress(regiongroups.GroupID("default"))
		if err == nil && ok && p.Eligible && p.Pending == nil && len(p.Current.Nodes) == 3 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("Region formation", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	if _, ok := nodes[3].process.Runtime.Engines.Resolve(regiongroups.GroupID("default")); ok {
		t.Fatal("gateway unexpectedly hosts Owner Group")
	}
	apply := func(c rootstate.Command) {
		t.Helper()
		c.Version = rootstate.FormatVersion
		b, err := rootstate.EncodeCommand(c)
		if err != nil {
			t.Fatal(err)
		}
		if err := nodes[0].process.Runtime.Host.Submit(raft.RootGroupID, b); err != nil {
			t.Fatal(err)
		}
		for {
			if _, err := nodes[0].process.Runtime.ReadRootIndex(ctx); err != nil {
				t.Fatal(err)
			}
			r, ok, err := nodes[0].process.Runtime.Root.ResultForCommand(b)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				if r.Error != "" {
					t.Fatal(r.Error)
				}
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	tokens := make([]string, 2)
	credentials := make([]rootstate.TenantCredential, 2)
	for i, tenant := range []string{"config-a", "config-b"} {
		apply(rootstate.Command{RequestID: "tenant-" + tenant, Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: tenant}})
		token, credential, err := auth.Generate(tenant, time.Now(), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		apply(rootstate.Command{RequestID: "token-" + tenant, Type: rootstate.CreateTenantCredential, TenantCredential: &credential})
		tokens[i], credentials[i] = token, credential
	}
	_, tls, err := platformconfig.LoadTLS(nodes[0].store, first.Runtime.Identity)
	if err != nil {
		t.Fatal(err)
	}
	tls.Certificates = nil
	tls.GetClientCertificate = nil
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tls}, Timeout: 65 * time.Second}
	defer client.CloseIdleConnections()
	gatewayStores, gatewayRPC, gatewayPublic := make([]string, len(nodes)), make([]string, len(nodes)), make([]string, len(nodes))
	for i, n := range nodes {
		gatewayStores[i] = n.store
		gatewayRPC[i] = n.address
		gatewayPublic[i] = n.process.PublicAddress()
	}
	verifyGatewayThroughRemoteEntry(t, ctx, client, gatewayStores, gatewayRPC, gatewayPublic)
	call := func(n int, token, method, query string, payload any) (int, []byte, error) {
		var b []byte
		if payload != nil {
			b, _ = json.Marshal(payload)
		}
		req, err := http.NewRequestWithContext(ctx, method, "https://"+nodes[n].process.PublicAddress()+"/api/v1/config/entry?"+query, bytes.NewReader(b))
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		response, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer response.Body.Close()
		out, err := io.ReadAll(response.Body)
		return response.StatusCode, out, err
	}
	must := func(n int, token, method, query string, payload any, want int) []byte {
		t.Helper()
		started := time.Now()
		code, b, err := call(n, token, method, query, payload)
		t.Logf("Config %s node=%d status=%d elapsed=%s", method, n, code, time.Since(started))
		if err != nil || code != want {
			t.Fatalf("Config %s: code=%d body=%s err=%v", method, code, b, err)
		}
		return b
	}
	key := "key=service%2Fflags"
	must(0, tokens[0], "GET", key, nil, 404)
	create := map[string]any{"request_id": "create", "value": []byte("enabled"), "if_absent": true}
	var created stateengine.Result
	if err := json.Unmarshal(must(0, tokens[0], "PUT", key, create, 200), &created); err != nil {
		t.Fatal(err)
	}
	var got stateengine.Entry
	if err := json.Unmarshal(must(1, tokens[0], "GET", key, nil, 200), &got); err != nil || string(got.Value) != "enabled" || got.Version != created.Version {
		t.Fatal(got, err)
	}
	must(3, tokens[0], "GET", key, nil, 200)
	must(3, tokens[0], "PUT", key, create, 200)
	must(1, tokens[1], "GET", key, nil, 404)
	must(1, tokens[1], "PUT", key, create, 200) // same key and request ID, another Tenant
	must(0, tokens[0], "PUT", key, map[string]any{"request_id": "create", "value": []byte("different"), "if_absent": true}, 409)
	must(0, tokens[0], "PUT", key, map[string]any{"request_id": "bad", "value": []byte("x"), "tenant_id": "config-b"}, 400)
	must(0, tokens[0], "GET", key+"&tenant_id=config-b", nil, 400)
	must(0, "invalid", "GET", key, nil, 401)
	// Real concurrent CAS through different Node entrypoints: exactly one wins.
	var wg sync.WaitGroup
	codes := make([]int, 2)
	bodies := make([][]byte, 2)
	failures := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i], bodies[i], failures[i] = call([]int{1, 3}[i], tokens[0], "PUT", key, map[string]any{"request_id": fmt.Sprint("cas-", i), "value": []byte(fmt.Sprint("winner-", i)), "version": created.Version})
		}(i)
	}
	wg.Wait()
	if failures[0] != nil || failures[1] != nil || !((codes[0] == 200 && codes[1] == 409) || (codes[1] == 200 && codes[0] == 409)) {
		t.Fatal("CAS", codes, string(bodies[0]), string(bodies[1]), failures)
	}
	win := 0
	if codes[1] == 200 {
		win = 1
	}
	var winner stateengine.Result
	if err := json.Unmarshal(bodies[win], &winner); err != nil {
		t.Fatal(err)
	}
	var replay stateengine.Result
	if err := json.Unmarshal(must(2, tokens[0], "PUT", key, create, 200), &replay); err != nil || replay.Version != created.Version || !replay.Replayed {
		t.Fatal("historical replay", replay, err)
	}
	must(2, tokens[0], "DELETE", key, map[string]any{"request_id": "delete", "version": winner.Version}, 200)
	must(1, tokens[0], "GET", key, nil, 404)
	recreate := map[string]any{"request_id": "recreate", "value": []byte{}, "if_absent": true}
	var recreated stateengine.Result
	if err := json.Unmarshal(must(0, tokens[0], "PUT", key, recreate, 200), &recreated); err != nil {
		t.Fatal(err)
	}
	must(1, tokens[0], "PUT", key, map[string]any{"request_id": "stale", "value": []byte("stale"), "version": created.Version}, 409)
	// Module isolation uses the same State implementation and logical key.
	other, err := stateengine.New(nodes[1].process.EngineData, "session")
	if err != nil {
		t.Fatal(err)
	}
	bound, err := other.ForTenant("config-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bound.Get(ctx, []byte("service/flags")); !errors.Is(err, stateengine.ErrNotFound) {
		t.Fatal("Module leak", err)
	}
	if _, err := bound.Put(ctx, []byte("service/flags"), []byte("session"), "create", stateengine.Condition{}); err != nil {
		t.Fatal(err)
	}
	canceled, stopCanceled := context.WithCancel(ctx)
	stopCanceled()
	if _, err := bound.Put(canceled, []byte("canceled"), []byte("x"), "canceled", stateengine.Condition{}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
	max := bytes.Repeat([]byte("x"), stateengine.MaxValueBytes)
	must(3, tokens[0], "PUT", "key=max-value", map[string]any{"request_id": "max", "value": max}, 200)
	var maxRead stateengine.Entry
	if err := json.Unmarshal(must(2, tokens[0], "GET", "key=max-value", nil, 200), &maxRead); err != nil || !bytes.Equal(maxRead.Value, max) {
		t.Fatal("bounded large value", err)
	}
	must(3, tokens[0], "PUT", "key=max-value", map[string]any{"request_id": "oversize", "value": append(max, 'x')}, 400)
	// Reopen a real Node with its existing identity, Raft state and Pack files.
	nodes[0].stop()
	p, err := openServer([]string{"--store", nodes[0].store, "--listen", nodes[0].address, "--api-listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	nodes[0].process = p
	stop, ready = startJoinTestServer(t, p)
	nodes[0].stop = stop
	ready()
	if err := json.Unmarshal(must(0, tokens[0], "GET", key, nil, 200), &got); err != nil || len(got.Value) != 0 || got.Version != recreated.Version {
		t.Fatal("restart", got, err)
	}
	must(0, tokens[0], "PUT", key, create, 200)
	apply(rootstate.Command{RequestID: "revoke-config", Type: rootstate.RevokeTenantCredential, TenantRevocation: &rootstate.TenantCredentialRevocation{CredentialID: credentials[0].CredentialID}})
	must(2, tokens[0], "GET", key, nil, 401)
	t.Log("State + Config acceptance: CAS, tombstone/ABA, retry history, Tenant/Module isolation, TLS auth/revocation and Node restart passed")
}
