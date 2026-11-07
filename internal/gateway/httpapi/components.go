package httpapi

import (
	"net/http"

	v1 "github.com/dubi-project/dubi/api/v1"
)

// NewComponents exposes the catalog assembled by the server alongside its
// actual Engine / Module services. It does not infer global availability.
func NewComponents(catalog v1.ComponentCatalog) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p, ok := AdminIdentity(r.Context()); !ok || p.Role != "admin" {
			gatewayError(w, http.StatusForbidden, "forbidden", "admin permission required")
			return
		}
		if r.URL.Path != v1.ComponentsPath {
			gatewayError(w, http.StatusNotFound, "not_found", "component API not found")
			return
		}
		if !method(w, r, http.MethodGet) {
			return
		}
		if _, err := parseManagementQuery(r); err != nil {
			gatewayError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		managementReply(w, catalog, true, nil)
	})
}
