package rootstate

import (
	"encoding/json"
	"errors"

	"github.com/cockroachdb/pebble"
)

// AccessCredential is a cluster management/monitoring credential, never a Tenant.
// Sessions reference an admin credential; revoking the parent revokes all sessions.
type AccessCredential struct {
	CredentialID string `json:"credential_id"`
	Role         string `json:"role"`
	TokenSHA256  string `json:"token_sha256"`
	CreatedUnix  int64  `json:"created_unix"`
	ExpiresUnix  int64  `json:"expires_unix"`
	RevokedIndex uint64 `json:"revoked_index,omitempty"`
	ParentID     string `json:"parent_id,omitempty"`
}

func (v AccessCredential) Validate() error {
	if !validTenantCredentialID(v.CredentialID) || !validDigest(v.TokenSHA256) || v.CreatedUnix <= 0 || v.ExpiresUnix <= v.CreatedUnix || v.ExpiresUnix-v.CreatedUnix > maxTenantCredentialLifetimeSeconds {
		return errors.New("invalid access credential")
	}
	switch v.Role {
	case "admin", "monitor":
		if v.ParentID != "" {
			return errors.New("access token cannot have parent")
		}
	case "session":
		if !validTenantCredentialID(v.ParentID) || v.ParentID == v.CredentialID || v.ExpiresUnix-v.CreatedUnix > 12*60*60 {
			return errors.New("invalid admin session")
		}
	default:
		return errors.New("invalid access role")
	}
	return nil
}
func accessCredentialKey(id string) []byte { return []byte("data/access-credential/" + id) }
func (s *State) AccessCredential(id string) (AccessCredential, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return AccessCredential{}, false, err
	}
	if !validTenantCredentialID(id) {
		return AccessCredential{}, false, errors.New("invalid access credential ID")
	}
	return s.accessCredential(id)
}
func (s *State) accessCredential(id string) (AccessCredential, bool, error) {
	b, found, err := s.get(accessCredentialKey(id))
	if err != nil || !found {
		return AccessCredential{}, found, err
	}
	var v AccessCredential
	if err = json.Unmarshal(b, &v); err == nil {
		err = v.Validate()
	}
	return v, err == nil, err
}
func (s *State) applyAccessCredential(batch *pebble.Batch, c Command) (string, error) {
	if c.Type == RevokeAccessCredential {
		v, found, err := s.accessCredential(c.AccessRevocation.CredentialID)
		if err != nil {
			return "", err
		}
		if !found {
			return "access credential not found", nil
		}
		if v.RevokedIndex == 0 {
			v.RevokedIndex = s.applied + 1
		}
		return "", setJSON(batch, accessCredentialKey(v.CredentialID), v)
	}
	v := *c.AccessCredential
	if _, found, err := s.accessCredential(v.CredentialID); err != nil {
		return "", err
	} else if found {
		return "access credential already exists", nil
	}
	if v.ParentID != "" {
		p, found, err := s.accessCredential(v.ParentID)
		if err != nil {
			return "", err
		}
		if !found || p.Role != "admin" || p.RevokedIndex != 0 || v.CreatedUnix < p.CreatedUnix || v.ExpiresUnix > p.ExpiresUnix {
			return "admin credential unavailable", nil
		}
	}
	// A bounded scan prevents login traffic from growing session state forever.
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/access-credential/"), UpperBound: []byte("data/access-credential0")})
	if err != nil {
		return "", err
	}
	count, children := 0, 0
	for ok := it.First(); ok; ok = it.Next() {
		var prior AccessCredential
		if json.Unmarshal(it.Value(), &prior) != nil || prior.Validate() != nil {
			_ = it.Close()
			return "", errors.New("invalid stored access credential")
		}
		if prior.Role == "session" && (prior.ExpiresUnix <= v.CreatedUnix || prior.RevokedIndex != 0) {
			if err := batch.Delete(it.Key(), nil); err != nil {
				_ = it.Close()
				return "", err
			}
			continue
		}
		count++
		if v.ParentID != "" && prior.ParentID == v.ParentID {
			children++
		}
		if count >= 4096 || children >= 32 {
			_ = it.Close()
			return "access credential limit reached; revoke unused sessions", nil
		}
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return "", err
	}
	return "", setJSON(batch, accessCredentialKey(v.CredentialID), v)
}
