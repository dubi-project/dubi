// Package resourceid defines compact, opaque resource identities. It is not
// used for credentials, content hashes, Pack IDs or Raft numeric identities.
package resourceid

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
)

const HexLength = 12

func PrefixSupported(prefix string) bool {
	switch prefix {
	case "cluster_", "node_", "store_", "volume_":
		return true
	}
	return false
}

func New(prefix string) (string, error) {
	if !PrefixSupported(prefix) {
		return "", errors.New("unsupported resource identity prefix")
	}
	var value [HexLength / 2]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(value[:]), nil
}

// Valid accepts existing 128-bit identities without changing them. A prefix
// shown in the UI is never sufficient to identify an existing long identity.
func Valid(value, prefix string) bool {
	if !PrefixSupported(prefix) || !strings.HasPrefix(value, prefix) {
		return false
	}
	suffix := value[len(prefix):]
	if len(suffix) != HexLength && len(suffix) != 32 {
		return false
	}
	decoded, err := hex.DecodeString(suffix)
	return err == nil && hex.EncodeToString(decoded) == suffix
}
