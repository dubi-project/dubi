package httpapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	v1 "github.com/dubi-project/dubi/api/v1"
)

func TestComponentsRegistrationReadOnlyAndAuthorization(t *testing.T) {
	catalog := v1.ComponentCatalog{NodeID: "node_a", Scope: "entry_node", Items: []v1.RegisteredComponent{{ID: "state", Kind: "engine", Status: "registered"}, {ID: "config", Kind: "module", Status: "registered", Engines: []string{"state"}}}}
	h := NewComponents(catalog)
	for _, tc := range []struct {
		method, path, role string
		status             int
	}{
		{"GET", v1.ComponentsPath, "", 403},
		{"GET", v1.ComponentsPath, "monitor", 403},
		{"GET", v1.ComponentsPath, "admin", 200},
		{"POST", v1.ComponentsPath, "admin", 405},
		{"PATCH", v1.ComponentsPath, "admin", 405},
		{"DELETE", v1.ComponentsPath, "admin", 405},
		{"GET", v1.ComponentsPath + "/unknown", "admin", 404},
		{"GET", v1.ComponentsPath + "?node_id=other", "admin", 400},
	} {
		r := httptest.NewRequest(tc.method, "https://node"+tc.path, nil)
		if tc.role != "" {
			r = r.WithContext(context.WithValue(r.Context(), adminContextKey{}, AdminPrincipal{Role: tc.role}))
		}
		w := httptest.NewRecorder()
		managementRequestID(w)
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s %s role %s: %d %s", tc.method, tc.path, tc.role, w.Code, w.Body.String())
		}
		if tc.status == 200 {
			var result struct {
				Data v1.ComponentCatalog `json:"data"`
				Meta v1.ManagementMeta   `json:"meta"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.Data.Scope != "entry_node" || result.Data.NodeID != "node_a" || len(result.Data.Items) != 2 || result.Data.Items[1].Engines[0] != "state" || !result.Meta.Complete || result.Meta.RequestID == "" {
				t.Fatalf("invalid catalog envelope: %s", w.Body.String())
			}
		}
	}
}
