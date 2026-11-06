package rootstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// BindRequestID isolates caller idempotency keys across trusted address scopes.
// The business key/payload is deliberately excluded: changing either while
// retrying the same scoped request must still be rejected by the state machine.
func (s Keyspace) BindRequestID(requestID string) (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	if err := ValidateRequestID(requestID); err != nil {
		return "", err
	}
	encoded, _ := json.Marshal([4]string{s.TenantID, s.ModuleID, s.EngineID, requestID})
	digest := sha256.Sum256(append([]byte("dubi bound request v2\x00"), encoded...))
	return "bound-" + hex.EncodeToString(digest[:]), nil
}
