package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/node"
)

// This listener intentionally has no internal RPC registrations. Module data
// endpoints will be added only with a constrained, module-owned public API.
func newPublicHTTPServer(verifier auth.Verifier) *http.Server {
	return newPublicHTTPServerWithMonitoring(verifier, nil, nil, nil)
}

func newPublicHTTPServerWithMonitoring(verifier auth.Verifier, runtime *node.Runtime, ready <-chan struct{}, repairSnapshot func() repairObservation) *http.Server {
	slots := make(chan struct{}, 32)
	monitoring := monitoringView{runtime: runtime, ready: ready, repairSnapshot: repairSnapshot}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
			writeMonitoringEndpoint(w, r, monitoring)
			return
		}
		if r.URL.Path != "/v1/me" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			http.Error(w, "public API is busy", http.StatusServiceUnavailable)
			return
		}
		values := r.Header.Values("Authorization")
		if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
			http.Error(w, "Bearer Token required", http.StatusUnauthorized)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		principal, err := verifier.Verify(ctx, strings.TrimPrefix(values[0], "Bearer "))
		if errors.Is(err, auth.ErrAuthorizationUnavailable) {
			http.Error(w, "Tenant authorization unavailable", http.StatusServiceUnavailable)
			return
		}
		if err != nil {
			http.Error(w, "invalid Tenant credential", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			TenantID     string `json:"tenant_id"`
			CredentialID string `json:"credential_id"`
		}{TenantID: principal.TenantID(), CredentialID: principal.CredentialID()})
	})
	return &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10}
}
