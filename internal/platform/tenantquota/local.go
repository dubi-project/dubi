package tenantquota

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"sync"
	"time"
)

type localCache struct {
	mu      sync.Mutex
	tenants map[string]*localTenant
}
type localTenant struct {
	tenant    rootstate.Tenant
	usage     rootstate.TenantUsage
	refreshed time.Time
	pending   map[string]*Admission
}

// Admission holds only this node's estimated positive growth, never a global
// reservation. It is not sent to Root or used as a distributed lock.
type Admission struct {
	report rootstate.UsageReport
	growth uint64
}

// Refresh uses the local Root replica only; it does not make writes wait on
// network accounting. Pending growth stays held until its Owner report arrives.
func (s *Service) Refresh() error {
	// Do local metadata IO outside the admission lock. Root apply/fsync must
	// never hold up an already cached quota decision through this worker.
	s.local.mu.Lock()
	ids := make([]string, 0, len(s.local.tenants))
	for id := range s.local.tenants {
		ids = append(ids, id)
	}
	s.local.mu.Unlock()
	for _, id := range ids {
		started := time.Now()
		s.local.mu.Lock()
		t := s.local.tenants[id]
		pending := make(map[string]rootstate.UsageReport, len(t.pending))
		for request, p := range t.pending {
			r := p.report
			r.Key = append([]byte(nil), r.Key...)
			pending[request] = r
		}
		s.local.mu.Unlock()
		reported := make(map[string]rootstate.UsageReport, len(pending))
		for request, p := range pending {
			k, err := s.Root.UsageKey(p.Space, p.Key)
			if err != nil {
				return err
			}
			if k.Report != nil {
				reported[request] = *k.Report
			}
		}
		tenant, usage, err := s.Root.LocalQuota(id)
		if err != nil {
			return err
		}
		s.local.mu.Lock()
		t = s.local.tenants[id]
		if !t.refreshed.After(started) {
			for request, r := range reported {
				p := t.pending[request]
				if p != nil && (r.RequestID == p.report.RequestID || p.report.Index != 0 && r.Covers(p.report)) {
					delete(t.pending, request)
				}
			}
			t.tenant, t.usage, t.refreshed = tenant, usage, started
		}
		s.local.mu.Unlock()
	}
	return nil
}

// Admit performs no RPC and no consensus. Cache misses load local metadata once.
// oldSize comes from Runtime metadata in the existing business read.
func (s *Service) Admit(space rootstate.Keyspace, key []byte, request string, oldSize, newSize uint64) (*Admission, error) {
	s.local.mu.Lock()
	if s.local.tenants == nil {
		s.local.tenants = map[string]*localTenant{}
	}
	t := s.local.tenants[space.TenantID]
	if t == nil {
		s.local.mu.Unlock()
		tenant, usage, err := s.Root.LocalQuota(space.TenantID)
		if err != nil {
			return nil, err
		}
		s.local.mu.Lock()
		t = s.local.tenants[space.TenantID]
		if t == nil {
			t = &localTenant{tenant: tenant, usage: usage, refreshed: time.Now(), pending: map[string]*Admission{}}
			s.local.tenants[space.TenantID] = t
		}
	}
	defer s.local.mu.Unlock()
	if !t.tenant.AllowsModule(space.ModuleID) {
		return nil, errors.New("Tenant module is forbidden")
	}
	if !t.tenant.Accounting {
		return nil, nil
	}
	id := rootstate.QuotaID(space, request)
	if p := t.pending[id]; p != nil {
		return p, nil
	}
	if newSize <= oldSize {
		return nil, nil
	}
	// A stopped maintenance loop must not permit indefinite new growth.
	if time.Since(t.refreshed) > 30*time.Second || len(t.pending) >= 4096 {
		return nil, ErrBusy
	}
	growth := newSize - oldSize
	used := t.usage.Used + t.usage.Reserved
	for _, p := range t.pending {
		used += p.growth
	}
	if limit := t.tenant.StorageLimit; limit != 0 && (used >= limit || growth > limit-used) {
		return nil, ErrExceeded
	}
	p := &Admission{report: rootstate.UsageReport{Space: space, Key: append([]byte(nil), key...), RequestID: request}, growth: growth}
	t.pending[id] = p
	return p, nil
}

// Published supplies the durable position already returned by the normal
// data write. No extra Owner query is needed to settle the local estimate.
func (s *Service) Published(a *Admission, r *rootstate.UsageReport) {
	if a == nil || r == nil {
		return
	}
	s.local.mu.Lock()
	defer s.local.mu.Unlock()
	a.report = *r
}
func (s *Service) Rejected(a *Admission) {
	if a == nil {
		return
	}
	s.local.mu.Lock()
	defer s.local.mu.Unlock()
	if t := s.local.tenants[a.report.Space.TenantID]; t != nil {
		delete(t.pending, rootstate.QuotaID(a.report.Space, a.report.RequestID))
	}
}

// Observe resolves an earlier local estimate even when a retry returns its
// durable receipt before admitting a new write. Unknown outcomes never refund.
func (s *Service) Observe(space rootstate.Keyspace, request string, published *rootstate.UsageReport, rejected bool) {
	s.local.mu.Lock()
	defer s.local.mu.Unlock()
	t := s.local.tenants[space.TenantID]
	if t == nil {
		return
	}
	id := rootstate.QuotaID(space, request)
	if rejected {
		delete(t.pending, id)
	} else if a := t.pending[id]; a != nil && published != nil {
		a.report = *published
		a.report.Key = append([]byte(nil), published.Key...)
	}
}
func (s *Service) Report(ctx context.Context, reports []rootstate.UsageReport) error {
	if len(reports) == 0 {
		return nil
	}
	var n [16]byte
	if _, err := rand.Read(n[:]); err != nil {
		return err
	}
	return s.Commit(ctx, rootstate.Command{Version: rootstate.FormatVersion, RequestID: "usage-" + hex.EncodeToString(n[:]), Type: rootstate.ReportTenantUsage, UsageReports: reports})
}
