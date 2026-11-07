package rootstate

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/pebble"
)

const maxTenantCredentialLifetimeSeconds = 366 * 24 * 60 * 60

func validTenantCredentialID(value string) bool {
	prefix, size := "credential_", 6
	if strings.HasPrefix(value, "cred_") {
		prefix, size = "cred_", 16
	}
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	encoded := strings.TrimPrefix(value, prefix)
	decoded, err := hex.DecodeString(encoded)
	return err == nil && len(decoded) == size && hex.EncodeToString(decoded) == encoded
}

// A zero expiry denotes a deliberately non-expiring access/application credential.
func validCredentialLifetime(created, expires int64) bool {
	return created > 0 && (expires == 0 || expires > created && expires-created <= maxTenantCredentialLifetimeSeconds)
}

func ValidateTenantCredentialID(value string) error {
	if !validTenantCredentialID(value) {
		return errors.New("invalid Tenant credential ID")
	}
	return nil
}

func (value TenantCredential) Validate() error {
	if value.Permission != "" && value.Permission != "readwrite" && value.Permission != "readonly" {
		return errors.New("invalid credential permission")
	}
	if value.Modules != nil && len(value.Modules) == 0 {
		return errors.New("empty credential module scope")
	}
	if len(value.Modules) > 64 {
		return errors.New("too many credential modules")
	}
	for i, m := range value.Modules {
		if !validID(m) || slices.Contains(value.Modules[:i], m) {
			return errors.New("invalid credential modules")
		}
	}
	if ValidateCredentialName(value.Name) != nil || (value.RequestID != "" && ValidateRequestID(value.RequestID) != nil) || !validTenantCredentialID(value.CredentialID) || ValidateTenantID(value.TenantID) != nil ||
		!validDigest(value.TokenSHA256) || !validCredentialLifetime(value.CreatedUnix, value.ExpiresUnix) {
		return errors.New("invalid Tenant credential")
	}
	return nil
}

func tenantCredentialKey(id string) []byte { return []byte("data/tenant-credential/" + id) }

func (s *State) applyTenantCredential(batch *pebble.Batch, command Command) (string, error) {
	switch command.Type {
	case DeleteTenantCredential:
		id := command.TenantRevocation.CredentialID
		encoded, found, err := s.get(tenantCredentialKey(id))
		if err != nil {
			return "", err
		}
		if !found {
			return "", nil
		}
		var current TenantCredential
		if json.Unmarshal(encoded, &current) != nil || current.Validate() != nil {
			return "", errors.New("stored Tenant credential is invalid")
		}
		if current.RevokedIndex == 0 {
			return "Only revoked credentials can be deleted", nil
		}
		return "", batch.Delete(tenantCredentialKey(id), nil)
	case CreateTenantCredential:
		value := *command.TenantCredential
		value.CreatedIndex = s.applied + 1
		if _, found, err := s.get(accessCredentialKey(value.CredentialID)); err != nil {
			return "", err
		} else if found {
			return "credential ID is already registered", nil
		}
		if b, found, err := s.get(tenantKey(value.TenantID)); err != nil {
			return "", err
		} else if !found {
			return "Tenant is not registered", nil
		} else {
			var tenant Tenant
			if json.Unmarshal(b, &tenant) != nil {
				return "", errors.New("invalid tenant")
			}
			for _, m := range value.Modules {
				if !tenant.AllowsModule(m) {
					return "Credential module is not authorized", nil
				}
			}
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

func ValidateCredentialName(name string) error {
	if !utf8.ValidString(name) || utf8.RuneCountInString(name) > 80 {
		return errors.New("invalid credential name")
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return errors.New("invalid credential name")
		}
	}
	return nil
}
