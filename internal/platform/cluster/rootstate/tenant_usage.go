package rootstate

import (
	"bytes"
	"errors"
	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

const ReportTenantUsage = "report_tenant_usage"
const MaxUsageReports = 64

// UsageReport is an absolute logical key size at a committed Owner position.
// Replication, retries and reordered batches must never increment it twice.
type UsageReport struct {
	Space        Keyspace `json:"space"`
	Key          []byte   `json:"key"`
	RequestID    string   `json:"request_id"`
	RangeID      string   `json:"range_id"`
	OwnerGroupID string   `json:"owner_group_id"`
	Epoch        uint64   `json:"epoch"`
	Index        uint64   `json:"index"`
	Bytes        uint64   `json:"bytes"`
}

func (r UsageReport) Validate() error {
	if r.Space.Validate() != nil || len(r.Key) == 0 || len(r.Key) > 2048 || ValidateRequestID(r.RequestID) != nil || ValidateRangeID(r.RangeID) != nil || raft.ValidateGroupID(r.OwnerGroupID) != nil || r.OwnerGroupID == raft.RootGroupID || r.Epoch == 0 || r.Index == 0 || r.Bytes > 1<<30 {
		return errors.New("invalid logical usage report")
	}
	return nil
}
func (r UsageReport) Covers(other UsageReport) bool {
	return r.Space == other.Space && bytes.Equal(r.Key, other.Key) && r.RangeID == other.RangeID && (r.Epoch > other.Epoch || r.Epoch == other.Epoch && r.OwnerGroupID == other.OwnerGroupID && r.Index >= other.Index)
}
func (s *State) applyUsageReports(batch *pebble.Batch, reports []UsageReport) (string, error) {
	totals := map[string]TenantUsage{}
	for _, r := range reports {
		var t Tenant
		ok, err := s.quotaJSON(tenantKey(r.Space.TenantID), &t)
		if err != nil {
			return "", err
		}
		if !ok || !t.Accounting {
			continue
		}
		var route Range
		ok, err = s.quotaJSON(keyspaceRangeKey(r.Space), &route)
		if err != nil {
			return "", err
		}
		// A delayed former Owner cannot change usage after a Range ownership change.
		if !ok || route.State != RangeStateActive || route.RangeID != r.RangeID || route.OwnerGroupID != r.OwnerGroupID || route.Epoch != r.Epoch {
			continue
		}
		var k QuotaKey
		_, err = s.quotaJSON(quotaItemKey(r.Space, r.Key), &k)
		if err != nil {
			return "", err
		}
		if k.Report != nil && k.Report.Covers(r) {
			continue
		}
		if k.Pending != "" {
			return "Tenant has an unresolved legacy reservation", nil
		}
		u, found := totals[r.Space.TenantID]
		if !found {
			_, err = s.quotaJSON(quotaUsageKey(r.Space.TenantID), &u)
			if err != nil {
				return "", err
			}
		}
		if u.Used < k.Bytes || u.Used-k.Bytes > 1<<50-r.Bytes {
			return "", errors.New("invalid logical usage total")
		}
		u.Used = u.Used - k.Bytes + r.Bytes
		totals[r.Space.TenantID] = u
		k = QuotaKey{Bytes: r.Bytes, Report: &r}
		if err := setJSON(batch, quotaItemKey(r.Space, r.Key), k); err != nil {
			return "", err
		}
	}
	for tenant, u := range totals {
		if err := setJSON(batch, quotaUsageKey(tenant), u); err != nil {
			return "", err
		}
	}
	return "", nil
}

// LocalQuota reads a single local replicated cut. It deliberately performs no
// quorum barrier: quota is a soft limit, whereas authentication remains separate.
func (s *State) LocalQuota(id string) (Tenant, TenantUsage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var t Tenant
	var u TenantUsage
	if s.closed || !validID(id) {
		return t, u, errors.New("invalid quota read")
	}
	ok, err := s.quotaJSON(tenantKey(id), &t)
	if err != nil {
		return t, u, err
	}
	if !ok {
		return t, u, errors.New("Tenant not found")
	}
	_, err = s.quotaJSON(quotaUsageKey(id), &u)
	return t, u, err
}
func (s *State) UsageKey(space Keyspace, key []byte) (QuotaKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var k QuotaKey
	if s.closed || space.Validate() != nil {
		return k, errors.New("invalid quota read")
	}
	_, err := s.quotaJSON(quotaItemKey(space, key), &k)
	return k, err
}
