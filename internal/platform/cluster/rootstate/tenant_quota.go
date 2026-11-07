package rootstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/cockroachdb/pebble"
)

const ReserveTenantQuota = "reserve_tenant_quota"
const CloseTenantQuota = "close_tenant_quota"
const FinishTenantQuota = "finish_tenant_quota"

type TenantUsage struct {
	Used     uint64 `json:"used_bytes"`
	Reserved uint64 `json:"reserved_bytes"`
}

// A reservation holds only addressing metadata, never the business value.
// Closing forbids further admission; it does NOT return capacity. Capacity is
// released only after the Engine has durably resolved or cancelled the operation.
type QuotaTicket struct {
	ID          string `json:"id"`
	TenantID    string `json:"tenant_id"`
	ModuleID    string `json:"module_id"`
	EngineID    string `json:"engine_id"`
	Key         []byte `json:"key"`
	RequestID   string `json:"request_id"`
	Version     string `json:"version"`
	Bytes       uint64 `json:"bytes"`
	OldBytes    uint64 `json:"old_bytes"`
	CreatedUnix int64  `json:"created_unix"`
	Closed      bool   `json:"closed"`
	Done        bool   `json:"done"`
	Applied     bool   `json:"applied"`
}
type QuotaMutation struct {
	Ticket  QuotaTicket `json:"ticket"`
	Applied bool        `json:"applied,omitempty"`
}
type QuotaKey struct {
	Report  *UsageReport `json:"report,omitempty"`
	Bytes   uint64       `json:"bytes"`
	Current string       `json:"current"`
	Pending string       `json:"pending"`
}

func QuotaID(space Keyspace, request string) string {
	h := sha256.Sum256([]byte(space.TenantID + "\x00" + space.ModuleID + "\x00" + space.EngineID + "\x00" + request))
	return hex.EncodeToString(h[:])
}
func (t QuotaTicket) Space() Keyspace {
	return Keyspace{TenantID: t.TenantID, ModuleID: t.ModuleID, EngineID: t.EngineID}
}
func (t QuotaTicket) Validate() error {
	if t.Space().Validate() != nil || !validID(t.RequestID) || t.ID != QuotaID(t.Space(), t.RequestID) || !validDigest(t.Version) || len(t.Key) == 0 || len(t.Key) > 2048 || t.Bytes > 1<<30 || t.OldBytes > 1<<30 || t.CreatedUnix <= 0 || t.Applied && !t.Done {
		return errors.New("invalid Tenant quota ticket")
	}
	return nil
}
func quotaUsageKey(id string) []byte   { return []byte("data/tenant-usage/" + id) }
func quotaTicketKey(id string) []byte  { return []byte("data/tenant-quota-ticket/" + id) }
func quotaPendingKey(id string) []byte { return []byte("data/tenant-quota-pending/" + id) }
func quotaItemKey(space Keyspace, key []byte) []byte {
	b, _ := json.Marshal(struct {
		Space Keyspace
		Key   []byte
	}{space, key})
	h := sha256.Sum256(b)
	return []byte("data/tenant-quota-key/" + hex.EncodeToString(h[:]))
}
func (s *State) quotaJSON(key []byte, out any) (bool, error) {
	b, ok, err := s.get(key)
	if err == nil && ok {
		err = json.Unmarshal(b, out)
	}
	return ok, err
}
func (s *State) applyQuota(batch *pebble.Batch, c Command) (string, error) {
	in := c.Quota.Ticket
	var tenant Tenant
	ok, err := s.quotaJSON(tenantKey(in.TenantID), &tenant)
	if err != nil {
		return "", err
	}
	if !ok || !tenant.Accounting {
		return "Tenant accounting is unavailable", nil
	}
	var t QuotaTicket
	exists, err := s.quotaJSON(quotaTicketKey(in.ID), &t)
	if err != nil {
		return "", err
	}
	var u TenantUsage
	_, err = s.quotaJSON(quotaUsageKey(in.TenantID), &u)
	if err != nil {
		return "", err
	}
	var k QuotaKey
	_, err = s.quotaJSON(quotaItemKey(in.Space(), in.Key), &k)
	if err != nil {
		return "", err
	}
	if exists && (t.Version != in.Version || t.Space() != in.Space() || !bytes.Equal(t.Key, in.Key) || t.Bytes != in.Bytes || t.RequestID != in.RequestID) {
		return "Quota request ID reused", nil
	}
	switch c.Type {
	case ReserveTenantQuota:
		if exists {
			return "", nil
		}
		if !tenant.AllowsModule(in.ModuleID) {
			return "Tenant module is forbidden", nil
		}
		if k.Pending != "" {
			return "Tenant quota key is busy", nil
		}
		growth := uint64(0)
		if in.Bytes > k.Bytes {
			growth = in.Bytes - k.Bytes
		}
		if growth > 0 && (u.Used > 1<<50 || u.Reserved > 1<<50 || u.Used+u.Reserved > 1<<50 || growth > 1<<50-u.Used-u.Reserved) {
			return "Tenant quota exceeded", nil
		}
		if growth > 0 && tenant.StorageLimit > 0 && (u.Used+u.Reserved >= tenant.StorageLimit || growth > tenant.StorageLimit-u.Used-u.Reserved) {
			return "Tenant quota exceeded", nil
		}
		t = in
		t.OldBytes = k.Bytes
		t.Closed = false
		t.Done = false
		t.Applied = false
		k.Pending = t.ID
		u.Reserved += growth
		if err := setJSON(batch, quotaPendingKey(t.ID), t); err != nil {
			return "", err
		}
	case CloseTenantQuota:
		if !exists {
			return "Tenant quota ticket not found", nil
		}
		if t.Done {
			return "", nil
		}
		t.Closed = true
		if err := setJSON(batch, quotaPendingKey(t.ID), t); err != nil {
			return "", err
		}
	case FinishTenantQuota:
		if !exists {
			return "Tenant quota ticket not found", nil
		}
		if t.Done {
			if t.Applied != c.Quota.Applied {
				return "Tenant quota outcome conflict", nil
			}
			return "", nil
		}
		if k.Pending != t.ID || k.Bytes != t.OldBytes {
			return "", errors.New("inconsistent Tenant quota key")
		}
		growth := uint64(0)
		if t.Bytes > t.OldBytes {
			growth = t.Bytes - t.OldBytes
		}
		if u.Reserved < growth || u.Used < t.OldBytes {
			return "", errors.New("inconsistent Tenant quota usage")
		}
		u.Reserved -= growth
		if c.Quota.Applied {
			u.Used = u.Used - t.OldBytes + t.Bytes
			k.Bytes = t.Bytes
			k.Current = t.ID
		}
		k.Pending = ""
		t.Done = true
		t.Applied = c.Quota.Applied
		if err := batch.Delete(quotaPendingKey(t.ID), nil); err != nil {
			return "", err
		}
	}
	if err := setJSON(batch, quotaTicketKey(t.ID), t); err != nil {
		return "", err
	}
	if err := setJSON(batch, quotaItemKey(t.Space(), t.Key), k); err != nil {
		return "", err
	}
	return "", setJSON(batch, quotaUsageKey(t.TenantID), u)
}
func (s *State) TenantUsage(id string) (TenantUsage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var u TenantUsage
	if s.closed || !validID(id) {
		return u, errors.New("invalid quota read")
	}
	_, err := s.quotaJSON(quotaUsageKey(id), &u)
	return u, err
}
func (s *State) QuotaState(space Keyspace, key []byte, id string) (QuotaTicket, bool, QuotaKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var t QuotaTicket
	var k QuotaKey
	if s.closed || space.Validate() != nil || !validDigest(id) {
		return t, false, k, errors.New("invalid quota read")
	}
	ok, err := s.quotaJSON(quotaTicketKey(id), &t)
	if err == nil {
		_, err = s.quotaJSON(quotaItemKey(space, key), &k)
	}
	return t, ok, k, err
}
func (s *State) QuotaTicket(id string) (QuotaTicket, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var t QuotaTicket
	if s.closed || !validDigest(id) {
		return t, false, errors.New("invalid quota read")
	}
	ok, err := s.quotaJSON(quotaTicketKey(id), &t)
	return t, ok, err
}
func (s *State) PendingQuota(after string, limit int) ([]QuotaTicket, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || limit < 1 || limit > 64 {
		return nil, errors.New("invalid quota scan")
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("data/tenant-quota-pending/"), UpperBound: []byte("data/tenant-quota-pending0")})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := []QuotaTicket{}
	for it.SeekGE([]byte("data/tenant-quota-pending/" + after)); it.Valid() && len(out) < limit; it.Next() {
		if string(it.Key()) <= "data/tenant-quota-pending/"+after {
			continue
		}
		var t QuotaTicket
		if json.Unmarshal(it.Value(), &t) != nil || t.Validate() != nil {
			return nil, errors.New("invalid pending quota")
		}
		out = append(out, t)
	}
	return out, it.Error()
}
