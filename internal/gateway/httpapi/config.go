// Package httpapi adapts public HTTP requests to business Modules.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/engine/state"
	configmodule "github.com/dubi-project/dubi/internal/module/config"
	"github.com/dubi-project/dubi/internal/platform/auth"
)

const ConfigPath = v1.ConfigEntryPath

type Config struct {
	service  *configmodule.Service
	verifier auth.Verifier
	slots    chan struct{}
}

func NewConfig(service *configmodule.Service, verifier auth.Verifier) *Config {
	return &Config{service: service, verifier: verifier, slots: make(chan struct{}, 16)}
}

func failure(w http.ResponseWriter, code int, name, message, request string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v1.Error{Code: name, Message: message, RequestID: request})
}
func (h *Config) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path != ConfigPath {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPut && r.Method != http.MethodDelete {
		w.Header().Set("Allow", "GET, PUT, DELETE")
		failure(w, 405, "method_not_allowed", "unsupported Config operation", "")
		return
	}
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		failure(w, 503, "busy", "Config API is busy", "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	values := r.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		failure(w, 401, "unauthorized", "Bearer Token required", "")
		return
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	principal, err := h.verifier.Verify(ctx, token)
	if err != nil {
		h.respondError(w, err, "")
		return
	}
	params, err := parseKey(r)
	if err != nil {
		h.respondError(w, err, "")
		return
	}
	var result any
	var m v1.ConfigMutation
	if r.Method == http.MethodGet {
		var entry state.Entry
		entry, err = h.service.Get(ctx, principal, params)
		result = v1.ConfigEntry{Version: entry.Version, Value: entry.Value}
	} else {
		r.Body = http.MaxBytesReader(w, r.Body, 96<<10)
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		if err = d.Decode(&m); err != nil {
			failure(w, 400, "invalid_request", "invalid or oversized Config body", "")
			return
		}
		if err = d.Decode(new(any)); err != io.EOF {
			failure(w, 400, "invalid_request", "exactly one JSON object required", m.RequestID)
			return
		}
		if r.Method == http.MethodDelete && (len(m.Value) != 0 || m.IfAbsent) {
			h.respondError(w, state.ErrInvalid, m.RequestID)
			return
		}
		// Revalidate after reading the body, before any Module or Runtime work.
		principal, err = h.verifier.Verify(ctx, token)
		if err != nil {
			h.respondError(w, err, m.RequestID)
			return
		}
		condition := state.Condition{IfAbsent: m.IfAbsent, Version: m.Version}
		var written state.Result
		if r.Method == http.MethodPut {
			written, err = h.service.Put(ctx, principal, params, m.Value, m.RequestID, condition)
		} else {
			written, err = h.service.Delete(ctx, principal, params, m.RequestID, condition)
		}
		result = v1.ConfigResult{RequestID: written.RequestID, Version: written.Version, Deleted: written.Deleted, Replayed: written.Replayed}
	}
	if err != nil {
		h.respondError(w, err, m.RequestID)
		return
	}
	if _, err := h.verifier.Verify(ctx, token); err != nil {
		if r.Method != http.MethodGet {
			failure(w, 503, "outcome_unknown", "authorization could not be reconfirmed after mutation; retry the same request ID", m.RequestID)
		} else {
			h.respondError(w, err, "")
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
func parseKey(r *http.Request) (string, error) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return "", err
	}
	if len(q) != 1 || len(q["key"]) != 1 || q.Get("key") == "" {
		return "", state.ErrInvalid
	}
	// Identity fields are not public routing inputs, even if supplied as headers.
	for _, header := range []string{"X-Tenant-ID", "X-Module-ID", "X-Engine-ID"} {
		if len(r.Header.Values(header)) != 0 {
			return "", state.ErrInvalid
		}
	}
	return q.Get("key"), nil
}
func (h *Config) respondError(w http.ResponseWriter, err error, id string) {
	switch {
	case errors.Is(err, state.ErrUnknown):
		failure(w, 503, "outcome_unknown", state.ErrUnknown.Error(), id)
	case errors.Is(err, state.ErrInvalid):
		failure(w, 400, "invalid_request", state.ErrInvalid.Error(), id)
	case errors.Is(err, state.ErrNotFound):
		failure(w, 404, "not_found", state.ErrNotFound.Error(), id)
	case errors.Is(err, state.ErrConflict):
		failure(w, 409, "version_conflict", state.ErrConflict.Error(), id)
	case errors.Is(err, state.ErrRequestReused):
		failure(w, 409, "request_id_reused", state.ErrRequestReused.Error(), id)
	case errors.Is(err, auth.ErrInvalidCredential):
		failure(w, 401, "unauthorized", "invalid Tenant credential", id)
	case errors.Is(err, auth.ErrForbiddenTenant):
		failure(w, 403, "forbidden", "Tenant access denied", id)
	default:
		failure(w, 503, "unavailable", "Config service unavailable", id)
	}
}
