package rootstate

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/pebble"
)

func (t Tenant) Validate() error {
	if !validID(t.TenantID) || !utf8.ValidString(t.Name) || utf8.RuneCountInString(t.Name) > 80 || strings.TrimSpace(t.Name) != t.Name || t.StorageLimit > 1<<50 || len(t.Modules) > 64 {
		return errors.New("invalid Tenant settings")
	}
	for _, r := range t.Name {
		if unicode.IsControl(r) {
			return errors.New("invalid Tenant name")
		}
	}
	for i, m := range t.Modules {
		if !validID(m) || slices.Contains(t.Modules[:i], m) {
			return errors.New("invalid Tenant modules")
		}
	}
	if t.StorageLimit != 0 && !t.Accounting {
		return errors.New("Tenant accounting is unavailable")
	}
	return nil
}
func (t Tenant) AllowsModule(module string) bool {
	// Legacy public tenants predate module policy and only exposed Config.
	if t.Modules == nil {
		return module == "config" || module == "items"
	}
	return slices.Contains(t.Modules, module)
}
func (s *State) applyTenant(batch *pebble.Batch, c Command) (string, error) {
	v := *c.Tenant
	b, found, err := s.get(tenantKey(v.TenantID))
	if err != nil {
		return "", err
	}
	if c.Type == RegisterTenant {
		v.CreatedIndex = s.applied + 1
		if found {
			return "TenantID is already registered", nil
		}
	} else {
		if !found {
			return "Tenant is not registered", nil
		}
		var old Tenant
		if json.Unmarshal(b, &old) != nil {
			return "", errors.New("invalid stored Tenant")
		}
		v.CreatedIndex = old.CreatedIndex
		if v.Revision != old.Revision {
			return "Tenant revision conflict", nil
		}
		if v.Accounting != old.Accounting {
			return "Tenant accounting mode cannot be changed", nil
		}
	}
	v.Revision = s.applied + 1
	return "", setJSON(batch, tenantKey(v.TenantID), v)
}

// TenantDirectory returns a single bounded local cut. Callers must first pass a
// Root read barrier. Credential hashes remain internal to the management service.
func (s *State) TenantDirectory() ([]Tenant, []TenantCredential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, nil, errors.New("Root state is closed")
	}
	tenants := []Tenant{}
	credentials := []TenantCredential{}
	for _, prefix := range []string{"data/tenant/", "data/tenant-credential/"} {
		it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(prefix), UpperBound: []byte(prefix[:len(prefix)-1] + "0")})
		if err != nil {
			return nil, nil, err
		}
		for it.First(); it.Valid(); it.Next() {
			if prefix == "data/tenant/" {
				var v Tenant
				if json.Unmarshal(it.Value(), &v) != nil || v.Validate() != nil || len(tenants) >= 4096 {
					it.Close()
					return nil, nil, errors.New("invalid or oversized Tenant directory")
				}
				tenants = append(tenants, v)
			} else {
				var v TenantCredential
				if json.Unmarshal(it.Value(), &v) != nil || v.Validate() != nil || len(credentials) >= 16384 {
					it.Close()
					return nil, nil, errors.New("invalid or oversized Tenant credential directory")
				}
				credentials = append(credentials, v)
			}
		}
		err = it.Error()
		it.Close()
		if err != nil {
			return nil, nil, err
		}
	}
	return tenants, credentials, nil
}
