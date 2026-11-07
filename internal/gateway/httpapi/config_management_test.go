package httpapi

import (
	"context"
	v1 "github.com/dubi-project/dubi/api/v1"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminConfigRejectsIdentityAndUnsafeMutations(t *testing.T) {
	h := &AdminConfig{}
	for _, tc := range []struct {
		method, path, body, role, header string
		status                           int
	}{
		{"GET", v1.AdminConfigPath + "?tenant=t", "", "", "", 403},
		{"GET", v1.AdminConfigPath + "?tenant=t", "", "monitor", "", 403},
		{"POST", v1.AdminConfigPath + "?tenant=t", "{}", "admin", "", 405},
		{"GET", v1.AdminConfigPath + "?tenant=t&module=x", "", "admin", "", 400},
		{"GET", v1.AdminConfigPath + "?tenant=t&tenant=b", "", "admin", "", 400},
		{"GET", v1.AdminConfigPath + "?tenant=t", "", "admin", "X-Engine-ID", 400},
		{"GET", v1.AdminConfigEntryPath + "?tenant=t", "", "admin", "", 400},
		{"PUT", v1.AdminConfigEntryPath + "?tenant=t&key=k", `{"request_id":"a","value":"eA=="}`, "admin", "", 400},
		{"DELETE", v1.AdminConfigEntryPath + "?tenant=t&key=k", `{"request_id":"a","if_absent":true}`, "admin", "", 400},
		{"PUT", v1.AdminConfigEntryPath + "?tenant=t&key=k", `{"request_id":"a","if_absent":true,"module":"x"}`, "admin", "", 400},
	} {
		r := httptest.NewRequest(tc.method, "https://node"+tc.path, strings.NewReader(tc.body))
		r.Header.Set("Content-Type", "application/json")
		if tc.header != "" {
			r.Header.Set(tc.header, "x")
		}
		if tc.role != "" {
			r = r.WithContext(context.WithValue(r.Context(), adminContextKey{}, AdminPrincipal{Role: tc.role}))
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
}
