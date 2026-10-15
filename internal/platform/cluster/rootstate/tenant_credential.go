package rootstate

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/cockroachdb/pebble"
)

const maxTenantCredentialLifetimeSeconds = 366 * 24 * 60 * 60

func validTenantCredentialID(value string) bool {
	if !strings.HasPrefix(value, "cred_") || len(value) != len("cred_")+32 {
		return false
	}
	encoded := strings.TrimPrefix(value, "cred_")
	decoded, err := hex.DecodeString(encoded)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == encoded
}

func ValidateTenantCredentialID(value string) error {
	if !validTenantCredentialID(value) {
		return errors.New("invalid Tenant credential ID")
	}
	return nil
}

func (value TenantCredential) Validate() error {
	if !validTenantCredentialID(value.CredentialID) || ValidateTenantID(value.TenantID) != nil ||
		!validDigest(value.TokenSHA256) || value.CreatedUnix <= 0 || value.ExpiresUnix <= value.CreatedUnix ||
		value.ExpiresUnix-value.CreatedUnix > maxTenantCredentialLifetimeSeconds {
		return errors.New("invalid Tenant credential")
	}
	return nil
}

func tenantCredentialKey(id string) []byte { return []byte("data/tenant-credential/" + id) }

func (s *State) applyTenantCredential(batch *pebble.Batch, command Command) (string, error) {
	switch command.Type {
	case CreateTenantCredential:
		value := command.TenantCredential
		if _, found, err := s.get(tenantKey(value.TenantID)); err != nil {
			return "", err
		} else if !found {
			return "Tenant is not registered", nil
		}
		if _, found, err := s.get(tenantCredentialKey(value.CredentialID)); err != nil {
			return "", err
		} else if found {
			return "Tenant credential ID is already registered", nil
		}
		return "", setJSON(batch, tenantCredentialKey(value.CredentialID), value)
	case RevokeTenantCredential:
		value := command.TenantRevocation
		encoded, found, err := s.get(tenantCredentialKey(value.CredentialID))
		if err != nil {
			return "", err
		}
		if !found {
			return "Tenant credential is not registered", nil
		}
		var current TenantCredential
		if err := json.Unmarshal(encoded, &current); err != nil || current.Validate() != nil {
			return "", errors.New("stored Tenant credential is invalid")
		}
		if current.RevokedIndex != 0 {
			return "Tenant credential is already revoked", nil
		}
		current.RevokedIndex = s.applied + 1
		return "", setJSON(batch, tenantCredentialKey(value.CredentialID), current)
	default:
		return "", errors.New("unsupported Tenant credential mutation")
	}
}

// TenantCredential returns public verifier metadata, never the plaintext Token.
// A caller must obtain a confirmed Root read cut before using this for auth.
func (s *State) TenantCredential(id string) (TenantCredential, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || !validTenantCredentialID(id) {
		return TenantCredential{}, false, errors.New("Root state is closed or Tenant credential ID is invalid")
	}
	encoded, found, err := s.get(tenantCredentialKey(id))
	if !found || err != nil {
		return TenantCredential{}, found, err
	}
	var value TenantCredential
	err = json.Unmarshal(encoded, &value)
	if err == nil {
		err = value.Validate()
	}
	return value, err == nil, err
}
