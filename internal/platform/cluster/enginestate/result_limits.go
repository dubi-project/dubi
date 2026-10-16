package enginestate

import (
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
)

// MaxResultBytes bounds the complete persisted JSON row, not just the binary
// handler value. Every accepted result must fit a file snapshot record and
// an authoritative reference's publication backlink.
const MaxResultBytes = snapshot.MaxValueBytes
const MaxOutcomeRejectionBytes = 64 << 10

var ErrOutcomeTooLarge = errors.New("engine outcome exceeds result limit")

// Called before transaction.flush: reject oversized outcomes before Pack IO,
// even when the handler staged both ordinary values and record references.
// The byte limit parameter exists for exact-boundary tests, not configuration.
func encodeHandlerResult(result Result, limit int) ([]byte, error) {
	if limit <= 0 || len(result.Error) > MaxOutcomeRejectionBytes || len(result.Value) > (limit/4)*3 {
		return nil, ErrOutcomeTooLarge
	}
	header := result
	header.Value = nil
	encodedHeader, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	size := len(encodedHeader)
	if len(result.Value) != 0 {
		// Value is the last optional JSON field; its contents are base64 and
		// need no JSON escaping. Empty and nil values are both omitted.
		size += len(`,"value":""`) + base64.StdEncoding.EncodedLen(len(result.Value))
	}
	if size > limit {
		return nil, ErrOutcomeTooLarge
	}
	return json.Marshal(result)
}
