package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

func TestPublicHTTPSIsolatedTokenIdentityAndRevocation(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, clientTLS := serverTestTLS(t)
	internal, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	public, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	process, err := Open(Options{StoreRoot: store, Listener: internal, PublicListener: public,
		ServerTLS: serverTLS, ClientTLS: clientTLS,
		AuthorizePeer: func(context.Context, uint64) error { return nil }, CapabilityMux: serverTestMux(t),
		RootBootstrapPeers: []uint64{id.RaftID}, TickInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- process.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-process.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("Node did not become ready")
	}
	apply := func(requestID, kind string, tenant *rootstate.Tenant, credential *rootstate.TenantCredential, revoke *rootstate.TenantCredentialRevocation) {
		t.Helper()
		command, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion,
			RequestID: requestID, Type: kind, Tenant: tenant, TenantCredential: credential, TenantRevocation: revoke})
		if err != nil {
			t.Fatal(err)
		}
		if err := process.Runtime.Host.Submit(raft.RootGroupID, command); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := process.Runtime.Host.ReadIndex(context.Background(), raft.RootGroupID); err != nil {
				t.Fatal(err)
			}
			result, found, err := process.Runtime.Root.ResultForCommand(command)
			if err != nil {
				t.Fatal(err)
			}
			if found {
				if result.Error != "" {
					t.Fatal(result.Error)
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("Root command was not applied")
	}
	apply("public-tenant", rootstate.RegisterTenant, &rootstate.Tenant{TenantID: "tenant-a"}, nil, nil)
	token, credential, err := auth.Generate("tenant-a", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	apply("public-token", rootstate.CreateTenantCredential, nil, &credential, nil)
	noCertificate := clientTLS.Clone()
	noCertificate.Certificates = nil
	noCertificate.GetClientCertificate = nil
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: noCertificate}}
	defer client.CloseIdleConnections()
	endpoint := "https://" + process.PublicAddress()
	request := func(method, path, bearer string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, endpoint+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, body
	}
	if status, _ := request(http.MethodGet, "/v1/me", ""); status != http.StatusUnauthorized {
		t.Fatal("Node CA without a Token accessed public API", status)
	}
	status, body := request(http.MethodGet, "/v1/me", token)
	var identity struct {
		TenantID string `json:"tenant_id"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &identity) != nil || identity.TenantID != "tenant-a" {
		t.Fatal("Tenant Token did not authenticate without a Node certificate", status, string(body))
	}
	if status, _ := request(http.MethodGet, "/v1/me", token+"wrong"); status != http.StatusUnauthorized {
		t.Fatal("invalid Token was accepted", status)
	}
	if status, _ := request(http.MethodPost, "/dubi.raft.GroupService/ReadIndex", token); status != http.StatusNotFound {
		t.Fatal("internal RPC reachable on public listener", status)
	}
	if status, _ := request(http.MethodPost, "/v1/me", token); status != http.StatusMethodNotAllowed {
		t.Fatal("public API method is unrestricted", status)
	}
	apply("public-revoke", rootstate.RevokeTenantCredential, nil, nil,
		&rootstate.TenantCredentialRevocation{CredentialID: credential.CredentialID})
	if status, _ := request(http.MethodGet, "/v1/me", token); status != http.StatusUnauthorized {
		t.Fatal("revoked Token was accepted", status)
	}
}

func TestPublicHTTPSRequiresVerifiedServerCertificate(t *testing.T) {
	_, clientTLS := serverTestTLS(t)
	client := clientTLS.Clone()
	client.Certificates = nil
	client.GetClientCertificate = nil
	if client.InsecureSkipVerify || client.RootCAs == nil || client.MinVersion != tls.VersionTLS13 ||
		strings.TrimSpace(client.ServerName) == "" {
		t.Fatal("public test client disabled TLS verification")
	}
}

type unavailableRootBarrier struct{}

func (unavailableRootBarrier) ReadIndex(context.Context, string) (uint64, error) {
	return 0, errors.New("Root majority unavailable")
}

type emptyCredentialCatalog struct{}

func (emptyCredentialCatalog) AppliedIndex() uint64 { return 0 }
func (emptyCredentialCatalog) TenantCredential(string) (rootstate.TenantCredential, bool, error) {
	return rootstate.TenantCredential{}, false, nil
}

func TestPublicHTTPSFailsClosedWithoutRootMajority(t *testing.T) {
	token, _, err := auth.Generate("tenant-a", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	server := newPublicHTTPServer(auth.Verifier{Barrier: unavailableRootBarrier{}, Catalog: emptyCredentialCatalog{}})
	request := httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), token) {
		t.Fatal("public endpoint did not fail closed", response.Code, response.Body.String())
	}
}

func TestPublicHTTPSListenerFailureInterruptsStartup(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, clientTLS := serverTestTLS(t)
	internal, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	public, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("public listener failed")
	process, err := Open(Options{StoreRoot: store, Listener: internal,
		PublicListener: failedListener{Listener: public, err: fault},
		ServerTLS:      serverTLS, ClientTLS: clientTLS,
		AuthorizePeer: func(context.Context, uint64) error { return nil }, CapabilityMux: serverTestMux(t),
		RootBootstrapPeers: []uint64{id.RaftID}, TickInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := process.Run(ctx); !errors.Is(err, fault) {
		t.Fatal("public listener failure was lost", err)
	}
	select {
	case <-process.Ready():
		t.Fatal("Node became ready without its requested Tenant listener")
	default:
	}
}
