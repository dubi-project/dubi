package v1

const ComponentsPath = "/api/v1/admin/components"

// ComponentCatalog reports successfully assembled components at the responding
// entry node. Registration is not a cluster-wide runtime health observation.
type ComponentCatalog struct {
	NodeID string                `json:"node_id"`
	Scope  string                `json:"scope"`
	Items  []RegisteredComponent `json:"items"`
}
type RegisteredComponent struct {
	ID           string             `json:"id"`
	Kind         string             `json:"kind"`
	Name         string             `json:"name"`
	Subtitle     string             `json:"subtitle"`
	Description  string             `json:"description"`
	Status       string             `json:"status"`
	Capabilities []string           `json:"capabilities"`
	Engines      []string           `json:"engines"`
	Settings     []ComponentSetting `json:"settings"`
}

// Settings describe current effective limits; this API does not mutate them.
type ComponentSetting struct {
	Label string `json:"label"`
	Value string `json:"value"`
}
