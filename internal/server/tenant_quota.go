package server

import (
	"context"
	"fmt"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/tenantquota"
	"time"
)

// Every node refreshes its admission cache independently of network reporting.
// Only each Owner leader reports, and a lost ACK replays the same absolute sizes.
func quotaMaintenanceWorker(runtime *node.Runtime, q *tenantquota.Service, report func(error)) func(context.Context) {
	return func(ctx context.Context) {
		refreshed := make(chan struct{})
		go func() {
			defer close(refreshed)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := q.Refresh(); err != nil && report != nil {
						report(err)
					}
				}
			}
		}()
		defer func() { <-refreshed }()
		cursor := map[string][]byte{}
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			delay := 5 * time.Second
			groups, err := runtime.Root.Groups()
			if err != nil {
				timer.Reset(delay)
				continue
			}
			for _, g := range groups {
				state, ok := runtime.Engines.Resolve(g.GroupID)
				if !ok {
					continue
				}
				status, err := runtime.Host.Status(g.GroupID)
				if err != nil || status.LeaderID != runtime.Identity.RaftID {
					delete(cursor, g.GroupID)
					continue
				}
				// Bound work by count, encoded bytes, and wall time. Ordering-index pages
				// do not rescan old keys on every maintenance tick.
				entries, next, err := state.UsagePage(cursor[g.GroupID], rootstate.MaxUsageReports)
				if err != nil {
					if report != nil {
						report(fmt.Errorf("read tenant usage: %w", err))
					}
					continue
				}
				pending := make([]rootstate.UsageReport, 0, len(entries))
				for _, r := range entries {
					t, ok, e := runtime.Root.Tenant(r.Space.TenantID)
					if e != nil {
						err = e
						break
					}
					if !ok {
						err = fmt.Errorf("tenant usage awaits local directory for %s", r.Space.TenantID)
						break
					}
					if !t.Accounting {
						continue
					}
					k, e := runtime.Root.UsageKey(r.Space, r.Key)
					if e != nil {
						err = e
						break
					}
					if k.Report == nil || !k.Report.Covers(r) {
						pending = append(pending, r)
					}
				}
				if err == nil && len(pending) > 0 {
					call, cancel := context.WithTimeout(ctx, 10*time.Second)
					err = q.Report(call, pending)
					cancel()
				}
				if err == nil {
					cursor[g.GroupID] = next
					if len(entries) == rootstate.MaxUsageReports {
						delay = 10 * time.Millisecond
					}
				} else if ctx.Err() == nil && report != nil {
					report(fmt.Errorf("report tenant usage: %w", err))
				}
			}

			timer.Reset(delay)
		}
	}
}
