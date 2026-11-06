// Package v1 defines public HTTP contracts, independent of server internals.
package v1

const ConfigEntryPath = "/api/v1/config/entry"

// ConfigMutation is the JSON body for PUT/DELETE ConfigEntryPath?key=... .
// Value uses standard JSON base64 encoding and is limited to 64 KiB decoded.
// RequestID must remain unchanged when retrying the same logical operation.
// IfAbsent and Version are mutually exclusive; neither means unconditional.
// DELETE accepts neither a nonempty Value nor IfAbsent.
type ConfigMutation struct {
	RequestID string `json:"request_id"`
	Value     []byte `json:"value,omitempty"`
	IfAbsent  bool   `json:"if_absent,omitempty"`
	Version   string `json:"version,omitempty"`
}

type ConfigEntry struct {
	Version string `json:"version"`
	Value   []byte `json:"value"`
}

// ConfigResult acknowledges the operation, not a lease on current data.
// Replayed may describe a historical successful operation already superseded
// by a later value. Use GET to obtain the current configuration.
type ConfigResult struct {
	RequestID string `json:"request_id"`
	Version   string `json:"version"`
	Deleted   bool   `json:"deleted"`
	Replayed  bool   `json:"replayed"`
}

// Error is shared by public module endpoints. outcome_unknown must not be
// interpreted as "not committed"; retain RequestID and identical request bytes.
type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
}
