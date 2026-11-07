package v1

const AdminConfigPath = "/api/v1/admin/config"
const AdminConfigEntryPath = "/api/v1/admin/config/entry"

type ManagedConfigItem struct {
	Key          string  `json:"key"`
	Version      string  `json:"version"`
	ValueBytes   *uint64 `json:"value_bytes"`
	CreatedOrder uint64  `json:"created_order"`
	Confirmed    bool    `json:"confirmed"`
}
type ManagedConfigPage struct {
	Items      []ManagedConfigItem `json:"items"`
	NextCursor string              `json:"next_cursor"`
}
