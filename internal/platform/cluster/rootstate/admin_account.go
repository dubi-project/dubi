package rootstate

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/cockroachdb/pebble"
	"strings"
	"unicode/utf8"
)

const PutAdminAccount = "put_admin_account"
const DeleteAdminAccount = "delete_admin_account"
const MaxAdminAccounts = 4096

// Password material is confined to Root and is never exposed in management DTOs.
type AdminAccount struct {
	TenantID       string `json:"tenant_id,omitempty"`
	Role           string `json:"role,omitempty"`
	ID             string `json:"id"`
	Username       string `json:"username"`
	Name           string `json:"name"`
	PasswordSalt   string `json:"password_salt"`
	PasswordHash   string `json:"password_hash"`
	PasswordScheme string `json:"password_scheme"`
	Disabled       bool   `json:"disabled"`
	CreatedUnix    int64  `json:"created_unix"`
	CreatedIndex   uint64 `json:"created_index"`
	Revision       uint64 `json:"revision"`
	AuthVersion    uint64 `json:"auth_version"`
}
type AdminAccountUpdate struct {
	Account          AdminAccount `json:"account"`
	ExpectedRevision uint64       `json:"expected_revision"`
}

func ValidateUsername(v string) error {
	if len(v) < 3 || len(v) > 64 {
		return errors.New("username must contain 3–64 lowercase letters, digits, dots, underscores or hyphens")
	}
	for _, r := range v {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return errors.New("invalid username")
		}
	}
	return nil
}
func ValidateAccountID(v string) error {
	prefix := "account_"
	// Keep persisted accounts and their session/snapshot references readable.
	// New accounts are always issued with the full account_ prefix.
	if strings.HasPrefix(v, "acct_") {
		prefix = "acct_"
	}
	b, e := hex.DecodeString(strings.TrimPrefix(v, prefix))
	if !strings.HasPrefix(v, prefix) || len(b) != 6 || e != nil {
		return errors.New("invalid admin account ID")
	}
	return nil
}

func (a AdminAccount) EffectiveRole() string {
	if a.Role == "" && a.TenantID == "" {
		return "admin"
	}
	return a.Role
}
func ValidAccountRole(role string) bool {
	return role == "admin" || role == "readwrite" || role == "readonly"
}
func (a AdminAccount) Validate() error {
	if !ValidAccountRole(a.EffectiveRole()) || (a.TenantID == "" && a.EffectiveRole() != "admin") || (a.TenantID != "" && ValidateTenantID(a.TenantID) != nil) {
		return errors.New("invalid account scope or role")
	}
	salt, e := hex.DecodeString(a.PasswordSalt)
	if ValidateAccountID(a.ID) != nil || ValidateUsername(a.Username) != nil || a.Name == "" || strings.TrimSpace(a.Name) != a.Name || !utf8.ValidString(a.Name) || len(a.Name) > 128 || strings.ContainsAny(a.Name, "\x00\r\n") || e != nil || len(salt) != 16 || !validDigest(a.PasswordHash) || a.PasswordScheme != "argon2id-v19-m65536-t3-p1" || a.CreatedUnix <= 0 {
		return errors.New("invalid admin account")
	}
	return nil
}
func (u AdminAccountUpdate) Validate() error { return u.Account.Validate() }
func adminAccountKey(id string) []byte       { return []byte("data/admin-account/" + id) }
func (s *State) adminAccount(id string) (AdminAccount, bool, error) {
	b, ok, e := s.get(adminAccountKey(id))
	var a AdminAccount
	if e != nil || !ok {
		return a, ok, e
	}
	if json.Unmarshal(b, &a) != nil || a.Validate() != nil {
		return a, false, errors.New("invalid stored admin account")
	}
	return a, true, nil
}
func (s *State) AdminAccount(id string) (AdminAccount, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.usable(); e != nil {
		return AdminAccount{}, false, e
	}
	return s.adminAccount(id)
}
func (s *State) adminAccounts() ([]AdminAccount, error) {
	out := []AdminAccount{}
	it, e := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/admin-account/"), UpperBound: []byte("data/admin-account0")})
	if e != nil {
		return nil, e
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		var a AdminAccount
		if json.Unmarshal(it.Value(), &a) != nil || a.Validate() != nil || len(out) >= MaxAdminAccounts {
			return nil, errors.New("invalid account directory")
		}
		out = append(out, a)
	}
	return out, it.Error()
}
func (s *State) AdminAccounts() ([]AdminAccount, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.usable(); e != nil {
		return nil, e
	}
	return s.adminAccounts()
}
func (s *State) applyAdminAccount(batch *pebble.Batch, u AdminAccountUpdate) (string, error) {
	a := u.Account
	if a.TenantID != "" {
		if _, found, err := s.get(tenantKey(a.TenantID)); err != nil {
			return "", err
		} else if !found {
			return "Account tenant does not exist", nil
		}
	}
	old, found, e := s.adminAccount(a.ID)
	if e != nil {
		return "", e
	}
	if (found && old.Revision != u.ExpectedRevision) || (!found && u.ExpectedRevision != 0) {
		return "Account revision conflict", nil
	}
	all, e := s.adminAccounts()
	if e != nil {
		return "", e
	}
	enabled := 0
	for _, other := range all {
		if other.ID != a.ID && other.Username == a.Username {
			return "Account username already exists", nil
		}
		if other.ID != a.ID && other.TenantID == a.TenantID && other.EffectiveRole() == "admin" && !other.Disabled {
			enabled++
		}
	}
	if !found && len(all) >= MaxAdminAccounts {
		return "Account limit reached", nil
	}
	if (a.Disabled || a.EffectiveRole() != "admin") && enabled == 0 {
		return "At least one enabled admin account is required", nil
	}
	changed := !found || old.PasswordHash != a.PasswordHash || old.PasswordSalt != a.PasswordSalt || old.Disabled != a.Disabled || old.EffectiveRole() != a.EffectiveRole()
	if found {
		if a.TenantID != old.TenantID {
			return "Account tenant cannot change", nil
		}
		if a.Username != old.Username {
			return "Account username cannot change", nil
		}
		a.CreatedUnix = old.CreatedUnix
		a.CreatedIndex = old.CreatedIndex
		a.AuthVersion = old.AuthVersion
	} else {
		a.CreatedIndex = s.applied + 1
	}
	a.Revision = s.applied + 1
	if changed {
		a.AuthVersion = a.Revision
	}
	// Drop superseded sessions immediately; disabled/password-reset accounts cannot
	// retain live sessions or exhaust their per-account session budget.
	if found && changed {
		it, e := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/access-credential/"), UpperBound: []byte("data/access-credential0")})
		if e != nil {
			return "", e
		}
		for it.First(); it.Valid(); it.Next() {
			var c AccessCredential
			if json.Unmarshal(it.Value(), &c) != nil {
				_ = it.Close()
				return "", errors.New("invalid access credential")
			}
			if c.AccountID == a.ID {
				if e = batch.Delete(it.Key(), nil); e != nil {
					_ = it.Close()
					return "", e
				}
			}
		}
		if e = errors.Join(it.Error(), it.Close()); e != nil {
			return "", e
		}
	}
	if err := batch.Set([]byte("data/admin-username/"+a.Username), []byte(a.ID), nil); err != nil {
		return "", err
	}
	return "", setJSON(batch, adminAccountKey(a.ID), a)
}

// AdminAccountDelete removes an account and its sessions at one Root commit.
type AdminAccountDelete struct {
	ID               string `json:"id"`
	ExpectedRevision uint64 `json:"expected_revision"`
}

func (d AdminAccountDelete) Validate() error {
	if ValidateAccountID(d.ID) != nil || d.ExpectedRevision == 0 {
		return errors.New("invalid account deletion")
	}
	return nil
}
func (s *State) applyDeleteAdminAccount(batch *pebble.Batch, d AdminAccountDelete) (string, error) {
	a, found, e := s.adminAccount(d.ID)
	if e != nil {
		return "", e
	}
	if !found {
		return "", nil
	}
	if a.Revision != d.ExpectedRevision {
		return "Account revision conflict", nil
	}
	all, e := s.adminAccounts()
	if e != nil {
		return "", e
	}
	enabled := 0
	for _, other := range all {
		if other.ID != a.ID && other.TenantID == a.TenantID && other.EffectiveRole() == "admin" && !other.Disabled {
			enabled++
		}
	}
	if !a.Disabled && a.EffectiveRole() == "admin" && enabled == 0 {
		return "At least one enabled admin account is required", nil
	}
	it, e := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/access-credential/"), UpperBound: []byte("data/access-credential0")})
	if e != nil {
		return "", e
	}
	for it.First(); it.Valid(); it.Next() {
		var c AccessCredential
		if json.Unmarshal(it.Value(), &c) != nil || c.Validate() != nil {
			_ = it.Close()
			return "", errors.New("invalid stored access credential")
		}
		if c.AccountID == a.ID {
			if e = batch.Delete(it.Key(), nil); e != nil {
				_ = it.Close()
				return "", e
			}
		}
	}
	if e = errors.Join(it.Error(), it.Close()); e != nil {
		return "", e
	}
	if e = batch.Delete([]byte("data/admin-username/"+a.Username), nil); e != nil {
		return "", e
	}
	return "", batch.Delete(adminAccountKey(a.ID), nil)
}
