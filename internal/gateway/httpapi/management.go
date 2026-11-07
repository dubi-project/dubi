package httpapi

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	v1 "github.com/dubi-project/dubi/api/v1"
	"github.com/dubi-project/dubi/internal/platform/management"
)

type Management struct{ service *management.Service }

func NewManagement(service *management.Service) *Management { return &Management{service: service} }
func managementReply(w http.ResponseWriter, data any, complete bool, problems []v1.ObservationError) {
	meta := v1.ManagementMeta{RequestID: w.Header().Get("X-Request-ID"), GeneratedUnix: time.Now().Unix(), Complete: complete, Errors: []v1.ObservationError{}}
	meta.ErrorsTruncated = len(problems) > 64
	meta.Errors = append(meta.Errors, problems[:min(64, len(problems))]...)
	jsonReply(w, 200, v1.ManagementResponse{Data: data, Meta: meta})
}
func managementRequestID(w http.ResponseWriter) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err == nil {
		w.Header().Set("X-Request-ID", hex.EncodeToString(id[:]))
	}
}

type managementQuery struct {
	values         url.Values
	limit          int
	after, binding string
}

func parseManagementQuery(r *http.Request, allowed ...string) (managementQuery, error) {
	bad := errors.New("invalid management query")
	if len(r.URL.RawQuery) > 4096 {
		return managementQuery{}, bad
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return managementQuery{}, bad
	}
	q := managementQuery{values: values, limit: 50}
	for k, v := range values {
		if !slices.Contains(allowed, k) || len(v) != 1 || v[0] == "" || len(v[0]) > 1024 {
			return q, bad
		}
	}
	if v := values.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			return q, bad
		}
		q.limit = n
	}
	statuses := []string{"healthy", "degraded", "unavailable", "unknown"}
	if r.URL.Path == "/api/v1/admin/tenants" {
		statuses = []string{"active", "inactive", "unauthorized"}
	}
	if r.URL.Path == "/api/v1/admin/volumes" {
		statuses = []string{"healthy", "missing", "invalid", "pending", "removed", "unknown", "error"}
	}
	if r.URL.Path == "/api/v1/admin/credentials" || r.URL.Path == "/api/v1/tenant/credentials" {
		statuses = []string{"active", "expired", "revoked"}
		if v := values.Get("expiring"); v != "" && v != "true" {
			return q, bad
		}
		if v := values.Get("kind"); v != "" && !slices.Contains([]string{"admin", "tenant", "monitor"}, v) {
			return q, bad
		}
	}
	if r.URL.Path == "/api/v1/admin/backups" {
		statuses = []string{"pending", "running", "completed", "failed"}
	}
	if r.URL.Path == "/api/v1/admin/diagnostics" {
		statuses = []string{"passed", "warning", "unknown"}
	}
	if v := values.Get("status"); v != "" && !slices.Contains(statuses, v) {
		return q, bad
	}
	if v := values.Get("kind"); r.URL.Path != "/api/v1/admin/credentials" && v != "" && !slices.Contains([]string{"repair", "rebalance", "geo_sync", "volume_migration"}, v) {
		return q, bad
	}
	bindingValues := make(url.Values, len(values))
	for k, v := range values {
		bindingValues[k] = v
	}
	bindingValues.Del("cursor")
	bindingValues.Del("limit")
	orderVersion := ""
	if r.URL.Path != "/api/v1/admin/backups" {
		orderVersion = "created-desc-v1:"
	}
	scope := ""
	if strings.HasPrefix(r.URL.Path, "/api/v1/tenant/") {
		if actor, ok := AdminIdentity(r.Context()); ok {
			scope = actor.TenantID + ":"
		}
	}
	digest := sha256.Sum256([]byte(scope + orderVersion + r.URL.Path + "?" + bindingValues.Encode()))
	q.binding = hex.EncodeToString(digest[:])
	if cursor := values.Get("cursor"); cursor != "" {
		b, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return q, bad
		}
		var c struct {
			After   string `json:"after"`
			Binding string `json:"binding"`
		}
		if json.Unmarshal(b, &c) != nil || c.Binding != q.binding || c.After == "" || len(c.After) > 256 {
			return q, bad
		}
		q.after = c.After
	}
	return q, nil
}
func page[T any](items []T, q managementQuery, id func(T) string) v1.ManagementList[T] {
	out := v1.ManagementList[T]{Items: []T{}}
	for _, item := range items {
		if id(item) <= q.after {
			continue
		}
		if len(out.Items) == q.limit {
			b, _ := json.Marshal(map[string]string{"after": id(out.Items[len(out.Items)-1]), "binding": q.binding})
			out.NextCursor = base64.RawURLEncoding.EncodeToString(b)
			break
		}
		out.Items = append(out.Items, item)
	}
	return out
}
func nodeProblems(nodes []v1.ManagedNode) (bool, []v1.ObservationError) {
	complete := true
	problems := []v1.ObservationError{}
	for _, n := range nodes {
		if !n.Complete {
			complete = false
			problems = append(problems, v1.ObservationError{NodeID: n.NodeID, Code: "incomplete_observation", Message: "Node observation is missing, stale or incomplete"})
		}
	}
	return complete, problems
}
func (h *Management) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, ok := AdminIdentity(r.Context()); !ok {
		gatewayError(w, 403, "forbidden", "admin permission required")
		return
	}
	if !method(w, r, http.MethodGet) {
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/admin/"), "/")
	route, nodeID, regionID, volumeID := "", "", "", ""
	switch {
	case len(parts) == 1 && slices.Contains([]string{"cluster", "nodes", "regions", "maintenance", "volumes"}, parts[0]):
		route = parts[0]
	case len(parts) == 3 && parts[0] == "regions" && parts[1] != "" && parts[2] == "nodes":
		route = "region_nodes"
		regionID = parts[1]
	case len(parts) >= 2 && len(parts) <= 4 && parts[0] == "nodes" && parts[1] != "":
		nodeID = parts[1]
		if len(parts) == 2 {
			route = "node"
		} else if len(parts) == 3 && slices.Contains([]string{"volumes", "maintenance"}, parts[2]) {
			route = "node_" + parts[2]
		} else if len(parts) == 4 && parts[2] == "volumes" && parts[3] != "" {
			route = "volume"
			volumeID = parts[3]
		}
	}
	if route == "" {
		gatewayError(w, 404, "not_found", "management API not found")
		return
	}
	allowed := []string{}
	switch route {
	case "volumes":
		allowed = []string{"region", "name", "node_id", "status", "limit", "cursor"}
	case "nodes":
		allowed = []string{"region", "name", "status", "limit", "cursor"}
	case "region_nodes":
		allowed = []string{"name", "status", "limit", "cursor"}
	case "regions":
		allowed = []string{"status", "limit", "cursor"}
	case "maintenance":
		allowed = []string{"node_id", "region", "kind", "limit", "cursor"}
	case "node_maintenance":
		allowed = []string{"kind"}
	case "node_volumes":
		allowed = []string{"limit", "cursor"}
	}
	q, err := parseManagementQuery(r, allowed...)
	if err != nil {
		gatewayError(w, 400, "invalid_request", err.Error())
		return
	}
	directory, err := h.service.Directory(r.Context())
	if err != nil {
		gatewayError(w, 503, "unavailable", "cluster directory unavailable")
		return
	}
	members := make([]management.Member, 0, len(directory.Members))
	regionFound := regionID == ""
	for _, m := range directory.Members {
		if m.RegionID == regionID {
			regionFound = true
		}
		if nodeID != "" && m.NodeID != nodeID {
			continue
		}
		if regionID != "" && m.RegionID != regionID {
			continue
		}
		if v := q.values.Get("region"); v != "" && v != m.RegionID {
			continue
		}
		if v := q.values.Get("name"); v != "" && v != m.Name {
			continue
		}
		if v := q.values.Get("node_id"); v != "" && v != m.NodeID {
			continue
		}
		members = append(members, m)
	}
	if !regionFound || nodeID != "" && len(members) == 0 {
		gatewayError(w, 404, "not_found", "resource not found")
		return
	}
	if route == "volumes" {
		volumes, complete, problems := h.service.ClusterVolumes(r.Context(), members, q.after, q.limit, q.values.Get("status"))
		managementReply(w, page(volumes, q, management.VolumeKey), complete, problems)
		return
	}
	if route == "node_volumes" || route == "volume" {
		volumes, err := h.service.Volumes(r.Context(), members[0])
		if err != nil {
			gatewayError(w, 503, "unavailable", "volume observation unavailable")
			return
		}
		complete := true
		for _, v := range volumes {
			complete = complete && v.Complete
		}
		problems := []v1.ObservationError{}
		if !complete {
			problems = append(problems, v1.ObservationError{NodeID: nodeID, Code: "incomplete_observation", Message: "Volume observation is stale or incomplete"})
		}
		if route == "volume" {
			for _, v := range volumes {
				if v.VolumeID == volumeID {
					managementReply(w, v, complete, problems)
					return
				}
			}
			if !complete {
				gatewayError(w, 503, "unavailable", "volume absence cannot be confirmed")
			} else {
				gatewayError(w, 404, "not_found", "volume not found")
			}
			return
		}
		managementReply(w, page(volumes, q, func(v v1.ManagedVolume) string { return management.LocalVolumeKey(v) }), complete, problems)
		return
	}
	// Without a health filter a node page needs only its own observations.
	var memberPage *v1.ManagementList[management.Member]
	if (route == "nodes" || route == "region_nodes") && q.values.Get("status") == "" {
		p := page(members, q, func(m management.Member) string { return management.CreationKey(m.CreatedIndex, m.NodeID) })
		memberPage = &p
		members = p.Items
	}
	nodes := h.service.Nodes(r.Context(), members)
	complete, problems := nodeProblems(nodes)
	switch route {
	case "cluster":
		managementReply(w, v1.ManagedCluster{ClusterID: directory.ClusterID, ManagementSummary: management.Summary(nodes), Regions: management.Regions(nodes)}, complete, problems)
	case "regions":
		regions := management.Regions(nodes)
		filtered := []v1.ManagedRegion{}
		for _, v := range regions {
			if q.values.Get("status") == "" || v.Status == q.values.Get("status") {
				filtered = append(filtered, v)
			}
		}
		managementReply(w, page(filtered, q, func(v v1.ManagedRegion) string { return management.CreationKey(v.CreatedIndex, v.RegionID) }), complete, problems)
	case "node":
		managementReply(w, nodes[0], complete, problems)
	case "nodes", "region_nodes":
		filtered := []v1.ManagedNode{}
		for _, n := range nodes {
			if q.values.Get("status") == "" || n.Status == q.values.Get("status") {
				filtered = append(filtered, n)
			}
		}
		p := page(filtered, q, func(n v1.ManagedNode) string { return management.CreationKey(n.CreatedIndex, n.NodeID) })
		if memberPage != nil {
			p.NextCursor = memberPage.NextCursor
		}
		managementReply(w, p, complete, problems)
	case "maintenance", "node_maintenance":
		items := []v1.NodeMaintenance{}
		for _, n := range nodes {
			item := v1.NodeMaintenance{CreatedIndex: n.CreatedIndex, NodeID: n.NodeID, RegionID: n.RegionID, Observation: n.Observation, Items: []v1.MaintenanceActivity{}}
			if n.Maintenance != nil {
				for _, a := range n.Maintenance.Items {
					if q.values.Get("kind") == "" || q.values.Get("kind") == a.Kind {
						item.Items = append(item.Items, a)
					}
				}
			}
			items = append(items, item)
		}
		if route == "node_maintenance" {
			if nodes[0].Maintenance == nil {
				gatewayError(w, 503, "unavailable", "maintenance observation unavailable")
				return
			}
			managementReply(w, items[0], complete, problems)
		} else {
			managementReply(w, page(items, q, func(v v1.NodeMaintenance) string { return management.CreationKey(v.CreatedIndex, v.NodeID) }), complete, problems)
		}
	}
}
